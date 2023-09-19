use std::collections::HashMap;

use crate::node_id::NodeId;

use super::connection::{Connection, ReadHalf, WriteHalf};

#[derive(Default)]
pub struct ConnectionStorage {
    connections: HashMap<NodeId, WriteHalf>,
}

impl ConnectionStorage {
    pub fn insert(&mut self, connection: Connection) -> Option<ReadHalf> {
        let (write, read) = connection.split();

        match self.connections.entry(write.node_id()) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                if entry.get().dedup_control() <= write.dedup_control() {
                    log::trace!("Already connected to {}", write.node_id());
                    return None;
                }

                entry.insert(write);
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(write);
            }
        }

        Some(read)
    }
    pub fn get_mut(&mut self, node_id: &NodeId) -> Option<&mut WriteHalf> {
        self.connections.get_mut(node_id)
    }

    pub fn remove(&mut self, node_id: &NodeId) -> Option<WriteHalf> {
        log::trace!("Removing connection {:?}", node_id);
        self.connections.remove(node_id)
    }

    pub fn connections_mut(&mut self) -> impl Iterator<Item = &mut WriteHalf> {
        self.connections.values_mut()
    }

    pub fn contains_node(&self, node_id: &NodeId) -> bool {
        self.connections.contains_key(node_id)
    }

    pub fn connected_nodes(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.connections.keys().copied()
    }

    pub fn len(&self) -> usize {
        self.connections.len()
    }

    pub fn remove_stale(&mut self) {
        self.connections
            .extract_if(|_, connection| connection.is_stale());
    }

    pub fn clear(&mut self) {
        self.connections.clear();
    }
}
