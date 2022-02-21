use message_io::{
    network::{Endpoint, NetEvent, Transport},
    node::{self, NodeHandler, NodeListener},
};
use rand::Rng;
use simple_mdns::ServiceDiscovery;
use std::{
    collections::{HashMap, LinkedList},
    net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4, ToSocketAddrs},
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

use crate::{
    config::Config,
    constants::{
        CLEAR_TIMEOUT, PING_CONNECTIONS, START_CONNECTIONS_RETRIES, START_CONNECTIONS_RETRY_WAIT,
        START_NEGOTIATION_MAX, START_NEGOTIATION_MIN, VERSION,
    },
    debouncer::{debounce_action, Debouncer},
    negotiator::Negotiation,
    IronCarrierError,
};

use super::{CommandDispatcher, Commands, HandlerEvent, PeerConnection, RawMessageType};

const TRANSPORT_PROTOCOL: Transport = Transport::FramedTcp;

#[derive(Debug)]
pub enum ConnectionFlow {
    StartConnections(u8),
    CheckConnectionLiveness,
    PingConnections,
}

impl From<ConnectionFlow> for HandlerEvent {
    fn from(v: ConnectionFlow) -> Self {
        HandlerEvent::Connection(v)
    }
}

pub struct ConnectionManager {
    config: Arc<Config>,

    connections: Arc<RwLock<HashMap<String, Vec<PeerConnection>>>>,
    id_lookup: Arc<RwLock<HashMap<Endpoint, String>>>,
    waiting_identification: HashMap<Endpoint, PeerConnection>,

    event_queue: Arc<Mutex<LinkedList<Commands>>>,
    liveness_check_is_running: bool,
    can_start_negotiations: bool,

    handler: NodeHandler<HandlerEvent>,
    dispatcher: CommandDispatcher,
    listener: Option<NodeListener<HandlerEvent>>,
    service_discovery: Option<ServiceDiscovery>,
}

impl ConnectionManager {
    pub fn new(config: Arc<Config>) -> Self {
        let (handler, listener) = node::split::<HandlerEvent>();
        let service_discovery = get_service_discovery(&config);
        let connections = Arc::new(RwLock::new(HashMap::new()));
        let event_queue = Arc::new(Mutex::new(LinkedList::new()));
        let dispatcher =
            CommandDispatcher::new(handler.clone(), connections.clone(), event_queue.clone());

        Self {
            handler,
            listener: Some(listener),
            config,
            service_discovery,
            waiting_identification: HashMap::new(),
            connections,
            id_lookup: Arc::new(HashMap::new().into()),
            event_queue,
            dispatcher,
            liveness_check_is_running: false,
            can_start_negotiations: true,
        }
    }

    fn clear(&mut self) {
        self.event_queue.lock().unwrap().clear()
    }

    fn get_addresses_to_connect(&self) -> Vec<(SocketAddr, Option<String>)> {
        let mut addresses = HashMap::new();

        if let Some(service_discovery) = &self.service_discovery {
            let services: Vec<simple_mdns::InstanceInformation> = match &self.config.group {
                Some(group) => service_discovery
                    .get_known_services()
                    .into_iter()
                    .filter(|service| {
                        service
                            .attributes
                            .get("g")
                            .iter()
                            .map(|g| match g {
                                Some(g) => g == group,
                                None => false,
                            })
                            .next()
                            .unwrap_or_default()
                    })
                    .collect(),
                None => service_discovery
                    .get_known_services()
                    .into_iter()
                    .filter(|service| !service.attributes.contains_key("g"))
                    .collect(),
            };

            for instance_info in services {
                let id = &instance_info.attributes["id"];
                for addr in instance_info.get_socket_addresses() {
                    addresses.insert(addr, id.clone());
                }
            }
        }

        if let Some(peers) = &self.config.peers {
            for peer in peers {
                if let Ok(addrs) = peer.to_socket_addrs() {
                    for addr in addrs {
                        addresses.entry(addr).or_insert(None);
                    }
                }
            }
        }

        log::info!(
            "{} - {} peers are available to connect",
            self.config.node_id,
            addresses.len()
        );

        addresses.into_iter().collect()
    }

    pub fn command_dispatcher(&self) -> CommandDispatcher {
        self.dispatcher.clone()
    }

    pub fn on_command(
        mut self,
        mut command_callback: impl FnMut(Commands, Option<String>) -> crate::Result<bool>,
    ) -> crate::Result<()> {
        log::trace!("Starting on_command");

        self.send_initial_events();
        self.handler.network().listen(
            TRANSPORT_PROTOCOL,
            SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, self.config.port),
        )?;

        let dispatcher = self.command_dispatcher();
        let clear = debounce_action(Duration::from_secs(CLEAR_TIMEOUT), move || {
            dispatcher.now(Commands::Clear);
        });

        self.listener
            .take()
            .expect("on_command can only be called once")
            .for_each(|event| {
                let result = match event {
                    node::NodeEvent::Network(net_event) => self.handle_net_event(net_event),
                    node::NodeEvent::Signal(signal_event) => self.handle_signal(signal_event),
                };

                match result {
                    Ok(maybe_command) => {
                        if let Some((command, peer_id)) = maybe_command {
                            self.execute_command(command, peer_id, &mut command_callback, &clear);
                        }
                    }
                    Err(err) => {
                        log::error!("Failed to process event: {}", err);
                    }
                }
            });

        Ok(())
    }

    fn execute_command(
        &mut self,
        command: Commands,
        peer_id: Option<String>,
        command_callback: &mut impl FnMut(Commands, Option<String>) -> crate::Result<bool>,
        clear: &Debouncer,
    ) {
        match &peer_id {
            Some(peer_id) => {
                log::trace!(
                    "{} - Executing command from {}: {}",
                    self.config.node_id,
                    peer_id,
                    &command
                );
            }
            None => {
                log::trace!("{} - Executing command: {} ", self.config.node_id, &command);
            }
        }

        if !matches!(command, Commands::Clear) {
            clear.invoke();
        } else {
            self.clear();
        }

        match command_callback(command, peer_id) {
            Ok(consume_queue) => {
                if consume_queue {
                    self.handler.signals().send(HandlerEvent::ConsumeQueue);
                }
            }
            Err(err) => {
                log::error!("Failed to process command: {}", err);
            }
        }
    }

    fn handle_net_event(
        &mut self,
        net_event: NetEvent,
    ) -> crate::Result<Option<(Commands, Option<String>)>> {
        match net_event {
            NetEvent::Connected(endpoint, success) => self.handle_connected(endpoint, success),
            NetEvent::Accepted(endpoint, _) => self.handle_accepted(endpoint),
            NetEvent::Message(endpoint, data) => {
                return self.handle_network_message(endpoint, data)
            }
            NetEvent::Disconnected(endpoint) => {
                log::info!(
                    "{} - Connection lost: {}",
                    self.config.node_id,
                    endpoint.addr()
                );
                self.remove_endpoint(&endpoint);
            }
        }

        Ok(None)
    }

    fn handle_signal(
        &mut self,
        event: HandlerEvent,
    ) -> crate::Result<Option<(Commands, Option<String>)>> {
        match event {
            HandlerEvent::Command(signal_event) => Ok(Some((signal_event, None))),
            HandlerEvent::Connection(control_event) => {
                self.handle_connection_flow(control_event)?;
                Ok(None)
            }
            HandlerEvent::ConsumeQueue => Ok(self
                .event_queue
                .lock()
                .expect("Poisoned lock")
                .pop_front()
                .map(|command| (command, None))),
        }
    }

    fn send_initial_events(&self) {
        self.handler.signals().send_with_timer(
            ConnectionFlow::StartConnections(0).into(),
            Duration::from_secs(3),
        );
        self.handler.signals().send_with_timer(
            ConnectionFlow::PingConnections.into(),
            Duration::from_secs(PING_CONNECTIONS),
        );
    }

    fn start_negotiations(&mut self) {
        if !self.can_start_negotiations || !self.waiting_identification.is_empty() {
            return;
        }

        self.can_start_negotiations = false;

        let mut rng = rand::thread_rng();
        let timeout = rng.gen_range(START_NEGOTIATION_MIN..START_NEGOTIATION_MAX);

        self.handler
            .signals()
            .send_with_timer(Negotiation::Start.into(), Duration::from_secs_f64(timeout));
    }

    fn handle_connected(&mut self, endpoint: Endpoint, success: bool) {
        if !success {
            log::error!(
                "{} - Failed to connect to {}",
                self.config.node_id,
                endpoint.addr()
            );
            self.remove_endpoint(&endpoint);
        } else {
            match self.waiting_identification.get(&endpoint) {
                Some(connection) => {
                    if self.send_id(connection).is_err() {
                        log::error!("Failed to send id to peer {endpoint}");
                    }
                }
                None => {
                    log::error!(
                        "{} - Can't find connection to endpoint",
                        self.config.node_id
                    )
                }
            }
        }
    }

    fn handle_accepted(&mut self, endpoint: Endpoint) {
        // Whenever we connect to a peer or accept a new connection, we send the node_id of this peer
        log::info!(
            "{} - Accepted connection from {}",
            self.config.node_id,
            endpoint.addr()
        );
        let connection: PeerConnection = endpoint.into();
        if self.send_id(&connection).is_err() {
            log::error!("Failed to send id to peer {endpoint}");
        }
        self.start_liveness_check();
        self.waiting_identification.insert(endpoint, connection);
    }

    fn send_id(&self, connection: &PeerConnection) -> crate::Result<()> {
        let message =
            &bincode::serialize(&(self.config.node_id.to_string(), self.config.group.clone()))?;

        connection.send_raw(self.handler.network(), RawMessageType::SetId, &message[..]);

        Ok(())
    }

    fn handle_network_message(
        &mut self,
        endpoint: Endpoint,
        data: &[u8],
    ) -> crate::Result<Option<(Commands, Option<String>)>> {
        self.update_last_access(&endpoint);

        let message_type = match RawMessageType::try_from(data[0]) {
            Ok(message_type) => message_type,
            Err(err) => {
                log::error!("Received invalid message {} from {endpoint:?}", data[0]);
                return Err(err.into());
            }
        };

        match message_type {
            RawMessageType::SetId => {
                self.handle_set_id(&data[1..], endpoint)?;
            }
            RawMessageType::Command => {
                let message = bincode::deserialize(&data[1..])?;
                match self.id_lookup.read().expect("Poisoned lock").get(&endpoint) {
                    Some(peer_id) => {
                        return Ok(Some((message, Some(peer_id.clone()))));
                    }
                    None => {
                        log::error!("Received message from unknown endpoint");
                    }
                }
            }
            RawMessageType::Stream => {
                match self.id_lookup.read().expect("Poisoned lock").get(&endpoint) {
                    Some(peer_id) => {
                        return Ok(Some((
                            Commands::Stream(data[1..].to_owned()),
                            Some(peer_id.clone()),
                        )))
                    }
                    None => {
                        log::error!("Received message from unknown endpoint");
                    }
                }
            }
            RawMessageType::Ping => {
                // There is no need to do anything, the connection was already touched by this point
            }
        }

        Ok(None)
    }

    fn handle_set_id(&mut self, data: &[u8], endpoint: Endpoint) -> crate::Result<()> {
        let (node_id, group): (String, Option<String>) = bincode::deserialize(data)?;

        {
            let mut connections = self.connections.write().expect("Poisoned lock");

            match self.waiting_identification.remove(&endpoint) {
                Some(mut connection) => {
                    if group != self.config.group {
                        self.handler.network().remove(endpoint.resource_id());
                        return Ok(());
                    }

                    let mut id_lookup = self.id_lookup.write().expect("Poisoned lock");
                    id_lookup.insert(endpoint, node_id.to_string());

                    connection.touch();
                    let peer_connections = connections.entry(node_id).or_default();
                    peer_connections.push(connection);
                    peer_connections.sort();
                }
                None => {
                    log::error!(
                        "{} - No connection waiting to be identified",
                        self.config.node_id
                    );
                    self.handler.network().remove(endpoint.resource_id());
                }
            }
        }

        self.start_negotiations();
        Ok(())
    }

    fn handle_connection_flow(&mut self, event: ConnectionFlow) -> crate::Result<()> {
        match event {
            ConnectionFlow::StartConnections(attempt) => {
                self.handle_start_connections(attempt)?;
            }
            ConnectionFlow::CheckConnectionLiveness => self.handle_liveness_check(),
            ConnectionFlow::PingConnections => {
                self.handle_ping_connections();
            }
        }
        Ok(())
    }

    fn handle_start_connections(&mut self, attempt: u8) -> crate::Result<()> {
        let addresses_to_connect = self.get_addresses_to_connect();
        if addresses_to_connect.is_empty() {
            if attempt < START_CONNECTIONS_RETRIES {
                let attempt = attempt + 1;
                self.handler.signals().send_with_timer(
                    ConnectionFlow::StartConnections(attempt).into(),
                    Duration::from_secs((attempt * START_CONNECTIONS_RETRY_WAIT) as u64),
                );
            } else {
                self.can_start_negotiations = false;
                self.event_queue.lock().expect("Poisoned lock").clear();
            }
            return Ok(());
        }

        self.start_liveness_check();

        for (socket_addr, node_id) in addresses_to_connect {
            log::debug!(
                "{} - Connecting to {socket_addr} with node id {:?}",
                self.config.node_id,
                &node_id
            );
            let (endpoint, _) = self
                .handler
                .network()
                .connect(TRANSPORT_PROTOCOL, socket_addr)
                .map_err(|_| {
                    IronCarrierError::ServerStartError("Could not connect to peer".into())
                })?;

            self.waiting_identification
                .insert(endpoint, endpoint.into());
        }

        Ok(())
    }

    fn start_liveness_check(&mut self) {
        if self.liveness_check_is_running {
            return;
        }

        log::trace!("{} - Started liveness check", self.config.node_id);
        self.liveness_check_is_running = true;
        self.handler.signals().send_with_timer(
            ConnectionFlow::CheckConnectionLiveness.into(),
            Duration::from_secs(1),
        );
    }

    fn handle_liveness_check(&mut self) {
        let unidentified: Vec<Endpoint> = self
            .waiting_identification
            .values()
            .filter(|x| x.is_stale(false))
            .map(|x| x.endpoint())
            .collect();

        for connection in unidentified {
            log::debug!(
                "{} - Connection to {} is stale",
                self.config.node_id,
                connection.addr()
            );
            self.handler.network().remove(connection.resource_id());
            self.waiting_identification.remove(&connection);
        }

        let endpoints: Vec<Endpoint> = self
            .connections
            .read()
            .expect("Poisoned lock")
            .values()
            .filter_map(|connections| connections.iter().find(|c| c.is_stale(true)))
            .map(|v| v.endpoint())
            .collect();

        for endpoint in endpoints {
            log::debug!(
                "{} connection to {} is stale",
                self.config.node_id,
                endpoint.addr()
            );
            self.remove_endpoint(&endpoint)
        }

        if self.connections.read().expect("Poisoned lock").is_empty()
            && self.waiting_identification.is_empty()
        {
            log::trace!("Stoped liveness check");
            self.liveness_check_is_running = false;
        } else {
            self.handler.signals().send_with_timer(
                ConnectionFlow::CheckConnectionLiveness.into(),
                Duration::from_secs(1),
            );
            self.start_negotiations();
        }
    }

    fn handle_ping_connections(&self) {
        for connection in self
            .connections
            .read()
            .expect("Poisoned lock")
            .values()
            .filter_map(|c| c.first())
        {
            connection.send_raw(self.handler.network(), RawMessageType::Ping, &[]);
        }

        self.handler.signals().send_with_timer(
            ConnectionFlow::PingConnections.into(),
            Duration::from_secs(PING_CONNECTIONS),
        );
    }

    fn update_last_access(&mut self, endpoint: &Endpoint) {
        let id_lookup = self.id_lookup.read().expect("Poisoned Lock");
        let peer_id = match id_lookup.get(endpoint) {
            Some(peer_id) => peer_id,
            None => return,
        };

        let mut connections = self.connections.write().expect("Poisoned lock");
        let peer_connections = match connections.get_mut(peer_id) {
            Some(connections) => connections,
            None => return,
        };

        if let Some(connection) = peer_connections
            .iter_mut()
            .find(|c| c.endpoint() == *endpoint)
        {
            connection.touch();
            peer_connections.sort();
        }
    }

    fn remove_endpoint(&mut self, endpoint: &Endpoint) {
        self.handler.network().remove(endpoint.resource_id());

        let mut id_lookup = self.id_lookup.write().expect("Poisoned lock");
        let mut connections = self.connections.write().expect("Poisoned lock");

        match id_lookup.remove(endpoint) {
            Some(node_id) => match connections.entry(node_id.to_string()) {
                std::collections::hash_map::Entry::Occupied(mut e) => {
                    let peer_connections = e.get_mut();
                    peer_connections.retain(|c| c.endpoint() != *endpoint);

                    log::debug!(
                        "{} - Removed a connection to {node_id}",
                        self.config.node_id
                    );

                    if peer_connections.is_empty() {
                        e.remove_entry();
                        log::debug!(
                            "{} - Removed {node_id} from connections",
                            self.config.node_id
                        );
                    } else {
                        peer_connections.sort();
                    }
                }
                std::collections::hash_map::Entry::Vacant(_) => {}
            },
            None => {
                log::debug!(
                    "{} - Removed unidentified peer from connections, {}",
                    self.config.node_id,
                    endpoint.addr()
                );
                self.waiting_identification.remove(endpoint);
            }
        }
    }
}
fn get_service_discovery(config: &Config) -> Option<ServiceDiscovery> {
    if !config.enable_service_discovery {
        return None;
    }

    let node_id = config.node_id.clone();
    let mut sd = ServiceDiscovery::new(&node_id, "_ironcarrier._tcp.local", 600).unwrap();

    let mut service_info = simple_mdns::InstanceInformation::default();
    service_info.ports.push(config.port);
    service_info.ip_addresses = get_my_ips()?;
    service_info
        .attributes
        .insert("v".into(), Some(VERSION.into()));
    service_info.attributes.insert("id".into(), Some(node_id));

    if config.group.is_some() {
        service_info
            .attributes
            .insert("g".into(), config.group.clone());
    }

    sd.add_service_info(service_info).ok()?;

    Some(sd)
}
fn get_my_ips() -> Option<Vec<IpAddr>> {
    let addrs = if_addrs::get_if_addrs()
        .ok()?
        .iter()
        .filter_map(|iface| {
            if iface.addr.is_loopback() {
                None
            } else {
                Some(iface.addr.ip())
            }
        })
        .collect();

    Some(addrs)
}
