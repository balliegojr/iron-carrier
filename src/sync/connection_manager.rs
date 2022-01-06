use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use message_io::{
    network::{Endpoint, NetEvent, NetworkController, Transport},
    node::{self, NodeHandler, NodeListener},
};
use simple_mdns::ServiceDiscovery;

use crate::{config::Config, IronCarrierError};

use super::CarrierEvent;

const TRANSPORT_PROTOCOL: Transport = Transport::FramedTcp;

pub struct ConnectionManager {
    config: Arc<Config>,

    connections: Arc<RwLock<HashMap<String, PeerConnection>>>,
    id_lookup: Arc<RwLock<HashMap<Endpoint, String>>>,
    waiting_identification: HashMap<Endpoint, PeerConnection>,

    handler: NodeHandler<CarrierEvent>,
    listener: Option<NodeListener<CarrierEvent>>,
    service_discovery: Option<ServiceDiscovery>,
}

impl ConnectionManager {
    pub fn new(config: Arc<Config>) -> Self {
        let (handler, listener) = node::split::<CarrierEvent>();
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
    pub fn start(&mut self) -> crate::Result<()> {
        for (socket_addr, node_id) in self.get_addresses_to_connect() {
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

        self.handler
            .signals()
            .send_with_timer(CarrierEvent::IdentificationTimeout, Duration::from_secs(3));

        Ok(())
    }
    pub fn disconnect_all(&mut self) {
        todo!()
    }
    pub fn identification_timeout(&mut self) {
        if !self.waiting_identification.is_empty() {
            self.waiting_identification.clear();
            self.handler.signals().send(CarrierEvent::ConsumeSyncQueue);
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
            for addr in peers {
                if !addresses.contains_key(addr) {
                    addresses.insert(*addr, None);
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
        mut command_callback: impl FnMut(CommandType) -> crate::Result<()>,
    ) -> crate::Result<()> {
        self.start()?;

        self.listener
            .take()
            .expect("on_command can only be called once")
            .for_each(|event| {
                match event {
                    node::NodeEvent::Network(net_event) => match net_event {
                        NetEvent::Connected(endpoint, success) => {
                            if !success {
                                self.remove_endpoint(&endpoint);
                            } else {
                                let id_lookup = self.id_lookup.read().expect("Poisoned lock");
                                let mut connections =
                                    self.connections.write().expect("Poisoned lock");

                                let connection = match id_lookup.get(&endpoint) {
                                    Some(peer_id) => connections.get_mut(peer_id),
                                    None => self.waiting_identification.get_mut(&endpoint),
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
                        NetEvent::Accepted(endpoint, _) => {
                            // Whenever we connect to a peer or accept a new connection, we send the node_id of this peer
                            log::info!("Peer connected:  {}", endpoint);
                            let mut connection: PeerConnection = endpoint.into();
                            connection.send_raw(
                                self.handler.network(),
                                RawMessageType::SetId,
                                self.config.node_id.as_bytes(),
                            );
                            self.waiting_identification.insert(endpoint, connection);
                        }
                        // When we receive a network message, we first verify it's prefix
                        // This is necessary to avoid calling bincode serde for every file block,
                        // which was hurting performance really bad
                        NetEvent::Message(endpoint, data) => {
                            self.update_last_access(&endpoint);

                            let message_type = match RawMessageType::try_from(data[0]) {
                                Ok(message_type) => message_type,
                                Err(err) => {
                                    log::error!("{}", err);
                                    return;
                                }
                            };

                            match message_type {
                                RawMessageType::SetId => {
                                    let node_id = String::from_utf8_lossy(&data[1..]);

                                    let mut id_lookup =
                                        self.id_lookup.write().expect("Poisoned lock");
                                    let mut connections =
                                        self.connections.write().expect("Poisoned lock");

                                    if let std::collections::hash_map::Entry::Vacant(entry) =
                                        id_lookup.entry(endpoint)
                                    {
                                        entry.insert(node_id.to_string());
                                        match self.waiting_identification.remove(&endpoint) {
                                            Some(mut connection) => {
                                                connection.touch();
                                                connections.insert(node_id.to_string(), connection)
                                            }
                                            None => connections
                                                .insert(node_id.to_string(), endpoint.into()),
                                        };
                                    }
                                }
                                RawMessageType::Command => {
                                    match bincode::deserialize(&data[1..]) {
                                        Ok(message) => match self
                                            .id_lookup
                                            .read()
                                            .expect("Poisoned lock")
                                            .get(&endpoint)
                                        {
                                            Some(peer_id) => {
                                                if let Err(err) = command_callback(
                                                    CommandType::CommandFrom(message, peer_id),
                                                ) {
                                                    log::error!("Error processing event: {}", err);
                                                }
                                            }
                                            None => {
                                                log::error!(
                                                    "Received message from unknown endpoint"
                                                );
                                            }
                                        },
                                        Err(err) => {
                                            log::error!("Invalid message: {:?}", err);
                                        }
                                    };
                                }
                                RawMessageType::Stream => match self
                                    .id_lookup
                                    .read()
                                    .expect("Poisoned lock")
                                    .get(&endpoint)
                                {
                                    Some(peer_id) => {
                                        if let Err(err) = command_callback(CommandType::StreamFrom(
                                            &data[1..],
                                            peer_id,
                                        )) {
                                            log::error!("Error processing event: {}", err);
                                        }
                                    }
                                    None => {
                                        log::error!("Received message from unknown endpoint");
                                    }
                                },
                            }
                        }
                        NetEvent::Disconnected(endpoint) => {
                            self.remove_endpoint(&endpoint);
                        }
                    },
                    node::NodeEvent::Signal(signal_event) => {
                        if let Err(err) = command_callback(CommandType::Command(signal_event)) {
                            log::error!("Error processing signal: {}", err);
                        }
                    }
                }
            });

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
                connections.remove(&node_id);
            }
            None => {
                self.waiting_identification.remove(endpoint);
            }
        }
    }
}

pub enum CommandType<'a> {
    Command(CarrierEvent),
    CommandFrom(CarrierEvent, &'a str),
    StreamFrom(&'a [u8], &'a str),
}

pub struct CommandDispatcher {
    handler: NodeHandler<CarrierEvent>,
    connections: Arc<RwLock<HashMap<String, PeerConnection>>>,
}
impl CommandDispatcher {
    fn new(
        handler: NodeHandler<CarrierEvent>,
        connections: Arc<RwLock<HashMap<String, PeerConnection>>>,
    ) -> Self {
        Self {
            handler,
            connections,
        }
    }
    pub fn now(&self, event: CarrierEvent) {
        self.handler.signals().send(event);
    }
    pub fn after(&self, event: CarrierEvent, duration: Duration) {
        self.handler.signals().send_with_timer(event, duration)
    }
    pub fn to(&self, event: CarrierEvent, peer_id: &str) {
        let mut connections = self.connections.write().expect("Poisoned lock");
        if let Some(connection) = connections.get_mut(peer_id) {
            connection.send_command(self.handler.network(), &event);
        }
    }
    pub fn broadcast(&self, event: CarrierEvent) {
        let mut connections = self.connections.write().expect("Poisoned lock");
        let controller = self.handler.network();

        let mut data = vec![RawMessageType::Command as u8];
        data.extend(bincode::serialize(&event).unwrap());

        for connection in connections.values_mut() {
            connection.send_data(controller, &data);
        }
    }
    pub fn stream_to(&self, data: &[u8], peer_id: &str) {
        todo!()
    }
}

pub struct PeerConnection {
    endpoint: Endpoint,
    last_access: Instant,
}

impl PeerConnection {
    fn send_data(&mut self, controller: &NetworkController, data: &[u8]) {
        controller.send(self.endpoint, data);
        self.touch()
    }
    fn send_raw(
        &mut self,
        controller: &NetworkController,
        message_type: RawMessageType,
        data: &[u8],
    ) {
        let mut new_data = Vec::with_capacity(data.len() + 1);
        new_data.push(message_type as u8);
        new_data.extend(data);

        self.send_data(controller, &new_data);
    }
    /// send a [message](`CarrierEvent`) to [endpoint](`message_io::network::Endpoint`) with message prefix 1
    fn send_command(&mut self, controller: &NetworkController, message: &CarrierEvent) {
        let data = bincode::serialize(message).unwrap();
        self.send_raw(controller, RawMessageType::Command, &data);
    }

    fn touch(&mut self) {
        self.last_access = Instant::now();
    }
}

impl From<Endpoint> for PeerConnection {
    fn from(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            last_access: Instant::now(),
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

enum RawMessageType {
    SetId = 0,
    Command,
    Stream,
}

impl TryFrom<u8> for RawMessageType {
    type Error = crate::IronCarrierError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(RawMessageType::SetId),
            1 => Ok(RawMessageType::Command),
            2 => Ok(RawMessageType::Stream),
            _ => Err(IronCarrierError::InvalidMessage),
        }
    }
}

/// broadcast a [message](`CarrierEvent`) to given [endpoints](`message_io::network::Endpoint`) with message prefix 1
fn broadcast_command_to<'a, T: Iterator<Item = &'a Endpoint>>(
    controller: NetworkController,
    message: CarrierEvent,
    endpoints: T,
) -> usize {
    let mut messages_sent = 0usize;
    let mut data = vec![RawMessageType::Command as u8];
    data.extend(bincode::serialize(&message).unwrap());
    for endpoint in endpoints {
        controller.send(*endpoint, &data);
        messages_sent += 1;
    }

    messages_sent
}
