use std::{collections::HashMap, ops::DerefMut};

use crate::node_id::NodeId;

use super::connection::{ConnectionId, Identified, WriteHalf};

#[derive(Default)]
pub struct ConnectionStorage {
    connections: HashMap<ConnectionId, Identified<WriteHalf>>,
    node_to_connection: HashMap<NodeId, ConnectionId>,
}

impl ConnectionStorage {
    pub fn insert(&mut self, connection: Identified<WriteHalf>) {
        self.node_to_connection
            .insert(connection.node_id(), connection.connection_id);

        self.connections
            .insert(connection.connection_id, connection);
    }
    pub fn get_mut(&mut self, node_id: &NodeId) -> Option<&mut WriteHalf> {
        self.node_to_connection
            .get(node_id)
            .and_then(|connection_id| self.connections.get_mut(connection_id))
            .map(|connection| connection.deref_mut())
    }

    pub fn remove(&mut self, connection_id: &ConnectionId) -> Option<Identified<WriteHalf>> {
        let connection = self.connections.remove(connection_id);
        log::info!("Removing connection {:?}", connection_id);

        if let Some(c) = connection.as_ref() {
            if self
                .node_to_connection
                .get(&c.node_id())
                .map(|connection_id| *connection_id == c.connection_id)
                .unwrap_or_default()
            {
                log::info!("Removing connection to {}", c.node_id());
                self.node_to_connection.remove(&c.node_id());
            }
        }

        connection
    }

    pub fn connections_mut(&mut self) -> impl Iterator<Item = &mut WriteHalf> {
        self.connections
            .values_mut()
            .map(|connection| connection.deref_mut())
    }

    pub fn contains_peer(&self, node_id: &NodeId) -> bool {
        self.node_to_connection.contains_key(node_id)
    }

    pub fn len(&self) -> usize {
        self.node_to_connection.len()
    }

    pub fn remove_stale(&mut self) {
        self.connections
            .extract_if(|_, connection| connection.is_stale())
            .for_each(|(_, connection)| {
                self.node_to_connection.remove(&connection.node_id());
            });
    }

    pub fn clear(&mut self) {
        self.connections.clear();
        self.node_to_connection.clear();
    }
}
