use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4, ToSocketAddrs},
    ops::Add,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use message_io::{
    network::{Endpoint, NetEvent, Transport},
    node::{self, NodeHandler, NodeListener},
};
use serde::{Deserialize, Serialize};
use simple_mdns::ServiceDiscovery;

use crate::{
    config::Config,
    sync::{FileHandlerEvent, SyncEvent},
    transaction_log::EventType,
    IronCarrierError,
};

use super::{peer_connection::PeerConnection, RawMessageType};

const TRANSPORT_PROTOCOL: Transport = Transport::FramedTcp;

#[derive(Debug)]
pub enum HandlerEvent {
    Command(CommandType),
    Connection(ConnectionFlow),
}

impl From<ConnectionFlow> for HandlerEvent {
    fn from(v: ConnectionFlow) -> Self {
        HandlerEvent::Connection(v)
    }
}

// impl From<CommandType> for HandlerEvent {
//     fn from(v: CommandType) -> Self {
//         HandlerEvent::Command(v)
//     }
// }

impl<T: Into<CommandType>> From<T> for HandlerEvent {
    fn from(v: T) -> Self {
        HandlerEvent::Command(v.into())
    }
}

#[derive(Debug)]
pub enum ConnectionFlow {
    StartConnections,
    CheckConnectionLiveness,
}

#[derive(Debug, Deserialize, Serialize)]
pub enum CommandType {
    Command(SyncEvent),
    FileHandler(FileHandlerEvent),
    Stream(Vec<u8>),
}

impl From<SyncEvent> for CommandType {
    fn from(v: SyncEvent) -> Self {
        CommandType::Command(v)
    }
}

impl From<FileHandlerEvent> for CommandType {
    fn from(v: FileHandlerEvent) -> Self {
        CommandType::FileHandler(v)
    }
}

pub struct ConnectionManager {
    config: Arc<Config>,

    connections: Arc<RwLock<HashMap<String, PeerConnection>>>,
    id_lookup: Arc<RwLock<HashMap<Endpoint, String>>>,
    waiting_identification: HashMap<Endpoint, PeerConnection>,

    handler: NodeHandler<HandlerEvent>,
    listener: Option<NodeListener<HandlerEvent>>,
    service_discovery: Option<ServiceDiscovery>,
}

impl ConnectionManager {
    pub fn new(config: Arc<Config>) -> Self {
        let (handler, listener) = node::split::<HandlerEvent>();
        let service_discovery = get_service_discovery(&config);

        Self {
            handler,
            listener: Some(listener),
            config,
            service_discovery,
            waiting_identification: HashMap::new(),
            connections: Arc::new(RwLock::new(HashMap::new())),
            id_lookup: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    fn get_addresses_to_connect(&self) -> Vec<(SocketAddr, Option<String>)> {
        let mut addresses = HashMap::new();

        if let Some(service_discovery) = &self.service_discovery {
            for instance_info in service_discovery.get_known_services() {
                log::debug!("{:?}", instance_info);
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

        log::debug!("{} peers are available to connect", addresses.len());

        addresses.into_iter().collect()
    }

    pub fn command_dispatcher(&self) -> CommandDispatcher {
        let handler = self.handler.clone();
        CommandDispatcher::new(handler, self.connections.clone())
    }

    pub fn on_command(
        mut self,
        mut command_callback: impl FnMut(CommandType, Option<&str>) -> crate::Result<()>,
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
                log::debug!("Received event {:?}", event);
                match event {
                    node::NodeEvent::Network(net_event) => match net_event {
                        NetEvent::Connected(endpoint, success) => {
                            self.handle_connected(endpoint, success)
                        }
                        NetEvent::Accepted(endpoint, _) => self.handle_accepted(endpoint),
                        // When we receive a network message, we first verify it's prefix
                        // This is necessary to avoid calling bincode serde for every file block,
                        // which was hurting performance really bad
                        NetEvent::Message(endpoint, data) => {
                            if let Err(err) =
                                self.handle_network_message(endpoint, data, &mut command_callback)
                            {
                                log::error!("Error processing network message: {}", err);
                            }
                        }
                        NetEvent::Disconnected(endpoint) => {
                            log::info!("Connection lost: {}", endpoint.addr());
                            self.remove_endpoint(&endpoint);
                        }
                    },
                    node::NodeEvent::Signal(signal_event) => match signal_event {
                        HandlerEvent::Command(signal_event) => {
                            if let Err(err) = command_callback(signal_event, None) {
                                log::error!("Error processing signal: {}", err);
                            }
                        }
                        HandlerEvent::Connection(control_event) => {
                            if let Err(err) = self.handle_connection_flow(control_event) {
                                log::error!("Error processing signal: {}", err);
                            }
                        }
                    },
                }
            });

        Ok(())
    }

    fn send_initial_events(&self) {
        self.handler.signals().send_with_timer(
            ConnectionFlow::StartConnections.into(),
            Duration::from_secs(3),
        );

        self.handler.signals().send_with_timer(
            ConnectionFlow::CheckConnectionLiveness.into(),
            Duration::from_secs(4),
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
        let mut connection: PeerConnection = endpoint.into();
        connection.send_raw(
            self.handler.network(),
            RawMessageType::SetId,
            self.config.node_id.as_bytes(),
        );
        self.waiting_identification.insert(endpoint, connection);
    }

    fn handle_network_message(
        &mut self,
        endpoint: Endpoint,
        data: &[u8],

        command_callback: &mut impl FnMut(CommandType, Option<&str>) -> crate::Result<()>,
    ) -> crate::Result<()> {
        self.update_last_access(&endpoint);

        let message_type = RawMessageType::try_from(data[0])?;

        match message_type {
            RawMessageType::SetId => {
                self.handle_set_id(&data[1..], endpoint);
            }
            RawMessageType::Command => {
                let message = bincode::deserialize(&data[1..])?;
                match self.id_lookup.read().expect("Poisoned lock").get(&endpoint) {
                    Some(peer_id) => {
                        command_callback(message, Some(&peer_id))?;
                    }
                    None => {
                        log::error!("Received message from unknown endpoint");
                    }
                }
            }
            RawMessageType::Stream => {
                match self.id_lookup.read().expect("Poisoned lock").get(&endpoint) {
                    Some(peer_id) => {
                        command_callback(
                            CommandType::Stream(data[1..].to_owned()),
                            Some(&peer_id),
                        )?;
                    }
                    None => {
                        log::error!("Received message from unknown endpoint");
                    }
                }
            }
        }

        Ok(())
    }

    fn handle_set_id(&mut self, data: &[u8], endpoint: Endpoint) {
        let node_id = String::from_utf8_lossy(&data[..]).to_string();

        let mut id_lookup = self.id_lookup.write().expect("Poisoned lock");
        let mut connections = self.connections.write().expect("Poisoned lock");

        if connections.contains_key(&node_id) {
            return;
        }

        if let std::collections::hash_map::Entry::Vacant(entry) = id_lookup.entry(endpoint) {
            entry.insert(node_id.to_string());
            match self.waiting_identification.remove(&endpoint) {
                Some(mut connection) => {
                    connection.touch();
                    connections.insert(node_id, connection)
                }
                None => connections.insert(node_id, endpoint.into()),
            };
        }

        self.start_sync();
    }

    fn start_sync(&self) {
        if self.waiting_identification.is_empty() {
            self.handler.signals().send(SyncEvent::StartSync(0).into());
        }
    }

    fn handle_connection_flow(&mut self, event: ConnectionFlow) -> crate::Result<()> {
        match event {
            ConnectionFlow::StartConnections => self.handle_start_connections(),
            ConnectionFlow::CheckConnectionLiveness => self.handle_liveness_check(),
        }
    }

    fn handle_start_connections(&mut self) -> crate::Result<()> {
        let addresses_to_connect = self.get_addresses_to_connect();
        if addresses_to_connect.is_empty() {
            // TODO: add exponential timeouts
            self.handler.signals().send_with_timer(
                ConnectionFlow::StartConnections.into(),
                Duration::from_secs(3),
            );
        }

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

    fn handle_liveness_check(&mut self) -> crate::Result<()> {
        let has_waiting = !self.waiting_identification.is_empty();
        let endpoints: Vec<Endpoint> = self
            .connections
            .write()
            .expect("Poisoned lock")
            .values()
            .chain(self.waiting_identification.values())
            .filter(|c| c.is_stale())
            .map(|c| c.endpoint())
            .collect();

        let is_removing = !endpoints.is_empty();
        for endpoint in endpoints {
            log::debug!("connection to {} is stale", endpoint.addr());
            self.remove_endpoint(&endpoint)
        }

        if has_waiting && is_removing {
            self.start_sync();
        }

        self.handler.signals().send_with_timer(
            ConnectionFlow::CheckConnectionLiveness.into(),
            Duration::from_secs(1),
        );

        Ok(())
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
                log::debug!("Removed {} from connections", node_id);
                connections.remove(&node_id);
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

pub struct CommandDispatcher {
    handler: NodeHandler<HandlerEvent>,
    connections: Arc<RwLock<HashMap<String, PeerConnection>>>,
}
impl CommandDispatcher {
    fn new(
        handler: NodeHandler<HandlerEvent>,
        connections: Arc<RwLock<HashMap<String, PeerConnection>>>,
    ) -> Self {
        Self {
            handler,
            connections,
        }
    }
    pub fn now<T: Into<CommandType>>(&self, event: T) {
        let event: CommandType = event.into();
        self.handler.signals().send(event.into());
    }
    pub fn after<T: Into<CommandType>>(&self, event: T, duration: Duration) {
        let event: CommandType = event.into();
        self.handler
            .signals()
            .send_with_timer(event.into(), duration)
    }
    pub fn enqueue<T: Into<CommandType>>(&self, event: T) {
        todo!()
    }

    pub fn to<T: Into<CommandType>>(&self, event: T, peer_id: &str) {
        let event: CommandType = event.into();
        let mut connections = self.connections.write().expect("Poisoned lock");
        if let Some(connection) = connections.get_mut(peer_id) {
            match event {
                CommandType::Stream(data) => {
                    connection.send_raw(self.handler.network(), RawMessageType::Stream, &data)
                }
                event => connection.send_command(self.handler.network(), &event),
            }
        }
    }
    pub fn broadcast<T: Into<CommandType>>(&self, event: T) -> usize {
        let event: CommandType = event.into();
        let mut connections = self.connections.write().expect("Poisoned lock");
        let controller = self.handler.network();

        let data = match event {
            CommandType::Stream(data) => {
                let mut stream = vec![RawMessageType::Stream as u8];
                stream.extend(data.iter());
                stream
            }
            event => {
                let mut data = vec![RawMessageType::Command as u8];
                data.extend(bincode::serialize(&event).unwrap());
                data
            }
        };

        for connection in connections.values_mut() {
            connection.send_data(controller, &data);
        }

        connections.len()
    }
    // pub fn stream_to(&self, data: &[u8], peer_id: &str) {
    //     let mut connections = self.connections.write().expect("Poisoned lock");
    //     if let Some(connection) = connections.get_mut(peer_id) {
    //         connection.send_raw(self.handler.network(), RawMessageType::Stream, data);
    //     }
    // }
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

// /// broadcast a [message](`CarrierEvent`) to given [endpoints](`message_io::network::Endpoint`) with message prefix 1
// fn broadcast_command_to<'a, T: Iterator<Item = &'a Endpoint>>(
//     controller: NetworkController,
//     message: SyncEvent,
//     endpoints: T,
// ) -> usize {
//     let mut messages_sent = 0usize;
//     let mut data = vec![RawMessageType::Command as u8];
//     data.extend(bincode::serialize(&message).unwrap());
//     for endpoint in endpoints {
//         controller.send(*endpoint, &data);
//         messages_sent += 1;
//     }

//     messages_sent
// }
