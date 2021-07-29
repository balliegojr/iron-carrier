use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
};

use message_io::{network::Endpoint, node::NodeHandler};

use super::{CarrierEvent, TRANSPORT_PROTOCOL};

pub(crate) struct ConnectedPeers {
    id_endpoint: HashMap<u64, Endpoint>,
    handler: NodeHandler<CarrierEvent>,
    waiting_identification: HashSet<SocketAddr>,
}

impl ConnectedPeers {
    pub fn new(handler: NodeHandler<CarrierEvent>) -> Self {
        Self {
            id_endpoint: HashMap::new(),
            handler,
            waiting_identification: HashSet::new(),
        }
    }
    pub fn connect_all(&mut self, addresses: HashSet<SocketAddr>) -> usize {
        let connected_peers: HashSet<SocketAddr> = self
            .id_endpoint
            .iter()
            .map(|(_, peer)| peer.addr())
            .collect();

        log::info!(
            "Received request to connect to {} peers, {} are already connected",
            addresses.len(),
            connected_peers.len()
        );
        let addresses: Vec<&SocketAddr> = addresses.difference(&connected_peers).collect();
        if addresses.is_empty() {
            self.handler.signals().send(CarrierEvent::ConsumeSyncQueue);
        } else {
            for address in &addresses {
                log::debug!("Connecting to peer {:?}", address);
                match self
                    .handler
                    .network()
                    .connect(TRANSPORT_PROTOCOL, **address)
                {
                    Ok(_) => {
                        self.waiting_identification.insert(**address);
                    }
                    Err(_) => {
                        log::error!("Error connecting to peer");
                    }
                }
            }
        }

        addresses.len()
    }
    pub fn disconnect_all(&mut self) {
        for (_, endpoint) in self.id_endpoint.drain() {
            self.handler.network().remove(endpoint.resource_id());
        }
    }
    pub fn add_peer(&mut self, endpoint: Endpoint, peer_id: u64) {
        match self.id_endpoint.contains_key(&peer_id) {
            true => {
                self.handler.network().remove(endpoint.resource_id());
            }
            false => {
                self.id_endpoint.insert(peer_id, endpoint);
            }
        }

        if self.waiting_identification.remove(&endpoint.addr())
            && self.waiting_identification.is_empty()
        {
            self.handler.signals().send(CarrierEvent::ConsumeSyncQueue);
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
    pub fn get_all_identified_endpoints(&self) -> impl Iterator<Item = &Endpoint> {
        self.id_endpoint.values()
    }

    pub fn has_connected_peers(&self) -> bool {
        !self.id_endpoint.is_empty()
    }
}
