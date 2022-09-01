use message_io::{
    network::{Endpoint, NetEvent, Transport},
    node::NodeHandler,
};
use simple_mdns::ServiceDiscovery;
use std::{
    collections::HashMap,
    net::{SocketAddr, ToSocketAddrs},
    time::Duration,
};

use crate::{config::Config, constants::PING_CONNECTIONS, IronCarrierError};

use crate::events::{ConnectionFlow, HandlerEvent, RawMessageType};

use super::PeerConnection;

const TRANSPORT_PROTOCOL: Transport = Transport::FramedTcp;
pub struct ConnectionHandler {
    config: &'static Config,

    // connections: Arc<RwLock<HashMap<String, Vec<PeerConnection>>>>,
    connections: HashMap<String, PeerConnection>,
    id_lookup: HashMap<Endpoint, String>,
    waiting_identification: HashMap<Endpoint, PeerConnection>,

    liveness_check_is_running: bool,
    handler: NodeHandler<HandlerEvent>,
    service_discovery: Option<ServiceDiscovery>,
}

impl ConnectionHandler {
    pub fn new(config: &'static Config, handler: NodeHandler<HandlerEvent>) -> crate::Result<Self> {
        let service_discovery = super::service_discovery::get_service_discovery(config)?;
        let connections = HashMap::new();

        Ok(Self {
            handler,
            config,
            service_discovery,
            waiting_identification: HashMap::new(),
            connections,
            id_lookup: HashMap::new(),
            liveness_check_is_running: false,
        })
    }

    pub fn get_peer_id(&self, endpoint: &Endpoint) -> Option<String> {
        self.id_lookup.get(endpoint).cloned()
    }

    pub fn get_connection(&self, node_id: &str) -> Option<&PeerConnection> {
        self.connections.get(node_id)
    }

    pub fn get_connections(&self) -> impl Iterator<Item = &PeerConnection> {
        self.connections.values()
    }

    fn get_addresses_to_connect(&self) -> Vec<(SocketAddr, Option<String>)> {
        let mut addresses = self
            .service_discovery
            .as_ref()
            .map(|service_discovery| {
                get_addresses_from_service_discovery(service_discovery, &self.config.group)
            })
            .unwrap_or_default();

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

    pub fn handle_net_event(&mut self, net_event: NetEvent) {
        match net_event {
            NetEvent::Connected(endpoint, success) => self.handle_connected(endpoint, success),
            NetEvent::Accepted(endpoint, _) => self.handle_accepted(endpoint),
            NetEvent::Disconnected(endpoint) => {
                log::info!(
                    "{} - Connection lost: {}",
                    self.config.node_id,
                    endpoint.addr()
                );
                self.remove_endpoint(&endpoint);
            }
            _ => {}
        }
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

    pub fn set_node_id(&mut self, node_id: String, endpoint: Endpoint, group: Option<String>) {
        match self.waiting_identification.remove(&endpoint) {
            Some(mut connection) => {
                if group != self.config.group {
                    self.handler.network().remove(endpoint.resource_id());
                    return;
                }

                self.id_lookup.insert(endpoint, node_id.to_string());

                connection.touch();
                connection.set_is_identified(true);
                self.connections.insert(node_id, connection);
                // let peer_connections = connections.entry(node_id).or_default();
                // peer_connections.push(connection);
                // peer_connections.sort();
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

    fn send_id(&self, connection: &PeerConnection) -> crate::Result<()> {
        let message =
            &bincode::serialize(&(self.config.node_id.to_string(), self.config.group.clone()))?;

        connection.send_raw(self.handler.network(), RawMessageType::SetId, &message[..]);

        Ok(())
    }

    pub fn start_connections(&mut self) -> crate::Result<bool> {
        let addresses_to_connect = self.get_addresses_to_connect();
        if addresses_to_connect.is_empty() {
            return Ok(false);
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

        Ok(true)
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

    pub fn liveness(&mut self) {
        let to_drop = self
            .waiting_identification
            .drain_filter(|_endpoint, connection| connection.is_stale())
            .map(|(endpoint, _connection)| endpoint);

        let to_drop: Vec<_> = to_drop
            .chain(
                self.connections
                    .drain_filter(|_endpoint, connection| connection.is_stale())
                    .map(|(_endpoint, connection)| connection.endpoint()),
            )
            .collect();

        for endpoint in to_drop {
            log::debug!(
                "{} - Connection to {} is stale",
                self.config.node_id,
                endpoint.addr()
            );
            self.remove_endpoint(&endpoint)
        }

        if self.connections.is_empty() && self.waiting_identification.is_empty() {
            log::trace!("Stoped liveness check");
            self.liveness_check_is_running = false;
        } else {
            self.handler.signals().send_with_timer(
                ConnectionFlow::CheckConnectionLiveness.into(),
                Duration::from_secs(1),
            );
        }
    }

    pub fn ping_connections(&self) {
        for connection in self.connections.values() {
            connection.send_raw(self.handler.network(), RawMessageType::Ping, &[]);
        }

        self.handler.signals().send_with_timer(
            ConnectionFlow::PingConnections.into(),
            Duration::from_secs(PING_CONNECTIONS),
        );
    }

    pub fn touch_connection(&mut self, endpoint: &Endpoint) {
        let peer_id = match self.id_lookup.get(endpoint) {
            Some(peer_id) => peer_id,
            None => return,
        };

        if let Some(ref mut connection) = self.connections.get_mut(peer_id) {
            connection.touch();
        }
    }

    pub fn has_waiting_identification(&self) -> bool {
        !self.waiting_identification.is_empty()
    }
    pub fn has_connections(&self) -> bool {
        !self.connections.is_empty()
    }

    fn remove_endpoint(&mut self, endpoint: &Endpoint) {
        self.handler.network().remove(endpoint.resource_id());

        match self.id_lookup.remove(endpoint) {
            Some(node_id) => match self.connections.entry(node_id.to_string()) {
                std::collections::hash_map::Entry::Occupied(e) => {
                    e.remove_entry();

                    log::debug!(
                        "{} - Removed {node_id} from connections",
                        self.config.node_id
                    );
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

fn get_addresses_from_service_discovery(
    service_discovery: &ServiceDiscovery,
    group: &Option<String>,
) -> HashMap<SocketAddr, Option<String>> {
    let mut addresses = HashMap::new();

    let services =
        service_discovery
            .get_known_services()
            .into_iter()
            .filter(|service| match group {
                group @ Some(_) => service
                    .attributes
                    .get("g")
                    .map(|g| g.eq(group))
                    .unwrap_or_default(),
                None => !service.attributes.contains_key("g"),
            });

    for instance_info in services {
        let id = &instance_info.attributes["id"];
        for addr in instance_info.get_socket_addresses() {
            addresses.insert(addr, id.clone());
        }
    }

    addresses
}
