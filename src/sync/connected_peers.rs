use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
};

use message_io::{
    network::{Endpoint, Transport},
    node::NodeHandler,
};

use super::CarrierEvent;

pub(crate) struct ConnectedPeers {
    id_endpoint: HashMap<u64, Endpoint>,
    handler: NodeHandler<CarrierEvent>,
}

impl ConnectedPeers {
    pub fn new(handler: NodeHandler<CarrierEvent>) -> Self {
        Self {
            id_endpoint: HashMap::new(),
            handler,
        }
    }
    pub fn connect_all(&mut self, addresses: HashSet<SocketAddr>) {
        let connected_peers: HashSet<SocketAddr> = self
            .id_endpoint
            .iter()
            .map(|(_, peer)| peer.addr())
            .collect();

        for address in addresses.difference(&connected_peers) {
            log::debug!("Connecting to peer {:?}", address);
            if let Err(_) = self
                .handler
                .network()
                .connect(Transport::FramedTcp, *address)
            {
                log::error!("Error connecting to peer");
            }
        }
    }
    pub fn disconnect_all(&mut self) {
        for (_, endpoint) in self.id_endpoint.drain() {
            self.handler.network().remove(endpoint.resource_id());
        }
    }
    pub fn add_peer(&mut self, endpoint: Endpoint, peer_id: u64) {
        let old_endpoint = self.id_endpoint.insert(peer_id, endpoint);
        if let Some(endpoint) = old_endpoint {
            self.handler.network().remove(endpoint.resource_id());
        }
    }
    pub fn remove_endpoint(&mut self, endpoint: Endpoint) {
        if let Some(id) = self.get_peer_id(endpoint) {
            self.id_endpoint.remove(&id);
        }
    }
    pub fn get_peer_endpoint(&self, peer_id: u64) -> Option<Endpoint> {
        self.id_endpoint.get(&peer_id).cloned()
    }
    pub fn get_peer_id(&self, endpoint: Endpoint) -> Option<u64> {
        self.id_endpoint
            .iter()
            .find_map(|(id, e)| if *e == endpoint { Some(*id) } else { None })
    }
    pub fn get_all_peers_ids(&self) -> Vec<u64> {
        self.id_endpoint.keys().cloned().collect()
    }
    pub fn get_all_identified_endpoints(&self) -> impl Iterator<Item = &Endpoint> {
        self.id_endpoint.values().into_iter()
    }
    pub fn get_all_identified_endpoints_except(
        &self,
        peer_id: u64,
    ) -> impl Iterator<Item = &Endpoint> {
        self.id_endpoint
            .iter()
            .filter_map(move |(key, value)| if *key != peer_id { Some(value) } else { None })
    }
    pub fn has_connected_peers(&self) -> bool {
        !self.id_endpoint.is_empty()
    }
}
