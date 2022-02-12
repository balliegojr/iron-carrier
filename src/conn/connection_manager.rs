use message_io::{
    network::{Endpoint, NetEvent, Transport},
    node::{self, NodeHandler, NodeListener},
};
use simple_mdns::ServiceDiscovery;
use std::{
    collections::{HashMap, LinkedList},
    net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4, ToSocketAddrs},
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

use crate::{config::Config, contants::PING_CONNECTIONS, sync::SyncEvent, IronCarrierError};

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

    connections: Arc<RwLock<HashMap<String, PeerConnection>>>,
    id_lookup: Arc<RwLock<HashMap<Endpoint, String>>>,
    waiting_identification: HashMap<Endpoint, PeerConnection>,

    event_queue: Arc<Mutex<LinkedList<Commands>>>,
    liveness_check_is_running: bool,

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
            liveness_check_is_running: false,
            dispatcher,
        }
    }
    fn get_addresses_to_connect(&self) -> Vec<(SocketAddr, Option<String>)> {
        let mut addresses = HashMap::new();

        if let Some(service_discovery) = &self.service_discovery {
            for instance_info in service_discovery.get_known_services() {
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

        log::info!("{} peers are available to connect", addresses.len());

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
                            self.execute_command(command, peer_id, &mut command_callback);
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
                log::info!("Connection lost: {}", endpoint.addr());
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
                if self.handle_connection_flow(control_event)? {
                    if self.event_queue.lock().expect("Poisoned lock").is_empty() {
                        return Ok(Some((SyncEvent::StartSync.into(), None)));
                    } else {
                        self.handler.signals().send(HandlerEvent::ConsumeQueue);
                    }
                }
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

    fn handle_connected(&mut self, endpoint: Endpoint, success: bool) {
        if !success {
            log::info!("Failed to connect to {}", endpoint.addr());
            self.remove_endpoint(&endpoint);
        } else {
            let id_lookup = self.id_lookup.read().expect("Poisoned lock");
            let mut connections = self.connections.write().expect("Poisoned lock");

            let connection = match id_lookup.get(&endpoint) {
                Some(peer_id) => {
                    log::info!("Connected to peer {} with id {}", endpoint.addr(), peer_id);
                    connections.get_mut(peer_id)
                }
                None => {
                    log::info!("Connected to peer {} without id", endpoint.addr());
                    self.waiting_identification.get_mut(&endpoint)
                }
            };

            match connection {
                Some(connection) => connection.send_raw(
                    self.handler.network(),
                    RawMessageType::SetId,
                    self.config.node_id.as_bytes(),
                ),
                None => {
                    log::error!("Can't find connection to endpoint")
                }
            }
        }
    }

    fn handle_accepted(&mut self, endpoint: Endpoint) {
        // Whenever we connect to a peer or accept a new connection, we send the node_id of this peer
        log::info!("Accepted connection from {}", endpoint.addr());
        let connection: PeerConnection = endpoint.into();
        connection.send_raw(
            self.handler.network(),
            RawMessageType::SetId,
            self.config.node_id.as_bytes(),
        );

        self.start_liveness_check();
        self.waiting_identification.insert(endpoint, connection);
    }

    fn handle_network_message(
        &mut self,
        endpoint: Endpoint,
        data: &[u8],
    ) -> crate::Result<Option<(Commands, Option<String>)>> {
        self.update_last_access(&endpoint);

        let message_type = RawMessageType::try_from(data[0])?;

        match message_type {
            RawMessageType::SetId => {
                let should_start_sync = self.handle_set_id(&data[1..], endpoint);
                if should_start_sync {
                    if self.event_queue.lock().expect("Poisoned lock").is_empty() {
                        return Ok(Some((SyncEvent::StartSync.into(), None)));
                    } else {
                        self.handler.signals().send(HandlerEvent::ConsumeQueue);
                        return Ok(None);
                    }
                }
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

    fn handle_set_id(&mut self, data: &[u8], endpoint: Endpoint) -> bool {
        let node_id = String::from_utf8_lossy(data).to_string();

        {
            let mut id_lookup = self.id_lookup.write().expect("Poisoned lock");
            let mut connections = self.connections.write().expect("Poisoned lock");

            if connections.contains_key(&node_id) && self.config.node_id.lt(&node_id) {
                log::debug!(
                    "Received duplicated node_id {}, removing endpoint {}",
                    node_id,
                    endpoint
                );
                // self.waiting_identification.remove(&endpoint);
                return false;
            } else if let std::collections::hash_map::Entry::Vacant(entry) =
                id_lookup.entry(endpoint)
            {
                entry.insert(node_id.to_string());
                match self.waiting_identification.remove(&endpoint) {
                    Some(mut connection) => {
                        connection.touch();
                        connections.insert(node_id, connection)
                    }
                    None => connections.insert(node_id, endpoint.into()),
                };
            }
        }

        self.should_start_sync()
    }

    fn should_start_sync(&self) -> bool {
        let connections = self.connections.read().expect("Poisoned lock");
        self.waiting_identification.is_empty()
            && !connections.is_empty()
            && self.config.node_id.lt(connections.keys().min().unwrap())
    }

    fn handle_connection_flow(&mut self, event: ConnectionFlow) -> crate::Result<bool> {
        match event {
            ConnectionFlow::StartConnections(attempt) => {
                self.handle_start_connections(attempt)?;
                Ok(false)
            }
            ConnectionFlow::CheckConnectionLiveness => Ok(self.handle_liveness_check()),
            ConnectionFlow::PingConnections => {
                self.handle_ping_connections();
                Ok(false)
            }
        }
    }

    fn handle_start_connections(&mut self, attempt: u8) -> crate::Result<()> {
        let addresses_to_connect = self.get_addresses_to_connect();
        if addresses_to_connect.is_empty() {
            if attempt < 3 {
                let attempt = attempt + 1;
                self.handler.signals().send_with_timer(
                    ConnectionFlow::StartConnections(attempt).into(),
                    Duration::from_secs((attempt * 10) as u64),
                );
            } else {
                self.event_queue.lock().expect("Poisoned lock").clear();
                return Ok(());
            }
        }

        self.start_liveness_check();
        for (socket_addr, node_id) in addresses_to_connect {
            log::debug!("connecting to {} with node id {:?}", socket_addr, &node_id);

            match node_id {
                Some(node_id) => {
                    let mut connections = self.connections.write().expect("Poisoned lock");
                    if connections.contains_key(&node_id) {
                        continue;
                    }

                    let (endpoint, _) = self
                        .handler
                        .network()
                        .connect(TRANSPORT_PROTOCOL, socket_addr)
                        .map_err(|_| {
                            IronCarrierError::ServerStartError("Could not connect to peer".into())
                        })?;

                    self.id_lookup
                        .write()
                        .expect("Poisoned lock")
                        .insert(endpoint, node_id.clone());

                    connections.insert(node_id, endpoint.into());
                }
                None => {
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
            }
        }

        Ok(())
    }

    fn start_liveness_check(&mut self) {
        if self.liveness_check_is_running {
            return;
        }

        log::trace!("Started liveness check");
        self.liveness_check_is_running = true;
        self.handler.signals().send_with_timer(
            ConnectionFlow::CheckConnectionLiveness.into(),
            Duration::from_secs(1),
        );
    }

    fn handle_liveness_check(&mut self) -> bool {
        let has_waiting = !self.waiting_identification.is_empty();
        let mut removed_connections = false;

        let unidentified: Vec<Endpoint> = self
            .waiting_identification
            .values()
            .filter(|x| x.is_stale(false))
            .map(|x| x.endpoint())
            .collect();

        for connection in unidentified {
            log::debug!("connection to {} is stale", connection.addr());
            self.handler.network().remove(connection.resource_id());
            self.waiting_identification.remove(&connection);
            removed_connections = true;
        }

        let endpoints: Vec<Endpoint> = self
            .connections
            .read()
            .expect("Poisoned lock")
            .values()
            .filter(|v| v.is_stale(true))
            .map(|v| v.endpoint())
            .collect();

        for endpoint in endpoints {
            log::debug!("connection to {} is stale", endpoint.addr());
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
        }

        has_waiting && removed_connections && self.should_start_sync()
    }

    fn handle_ping_connections(&self) {
        for connection in self.connections.read().expect("Poisoned lock").values() {
            connection.send_raw(self.handler.network(), RawMessageType::Ping, &[]);
        }

        self.handler.signals().send_with_timer(
            ConnectionFlow::PingConnections.into(),
            Duration::from_secs(PING_CONNECTIONS),
        );
    }

    fn update_last_access(&mut self, endpoint: &Endpoint) {
        if let Some(peer_id) = self.id_lookup.read().expect("Poisoned lock").get(endpoint) {
            if let Some(connection) = self
                .connections
                .write()
                .expect("Poisoned lock")
                .get_mut(peer_id)
            {
                connection.touch()
            }
        }
    }

    fn remove_endpoint(&mut self, endpoint: &Endpoint) {
        let mut id_lookup = self.id_lookup.write().expect("Poisoned lock");
        let mut connections = self.connections.write().expect("Poisoned lock");

        match id_lookup.remove(endpoint) {
            Some(node_id) => {
                if !id_lookup.values().any(|x| x == &node_id) {
                    log::debug!("Removed {} from connections", node_id);
                    connections.remove(&node_id);
                }
            }
            None => {
                log::debug!(
                    "Removed unidentified peer from connections, {}",
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
        .insert("v".into(), Some("0.1".into()));
    service_info.attributes.insert("id".into(), Some(node_id));

    sd.add_service_info(service_info);

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
