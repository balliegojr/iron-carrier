use std::collections::HashMap;

use super::connection::{ConnectionId, WriteHalf};

#[derive(Default)]
pub struct ConnectionStorage {
    connections: HashMap<ConnectionId, WriteHalf>,
    peer_to_connection: HashMap<u64, ConnectionId>,
}

impl ConnectionStorage {
    pub fn insert(&mut self, connection: WriteHalf) {
        self.peer_to_connection
            .insert(connection.peer_id, connection.connection_id);

        self.connections
            .insert(connection.connection_id, connection);
    }
    pub fn get_mut(&mut self, peer_id: &u64) -> Option<&mut WriteHalf> {
        self.peer_to_connection
            .get(peer_id)
            .and_then(|connection_id| self.connections.get_mut(connection_id))
            .map(|connection| {
                connection.touch();
                connection
            })
    }

    pub fn remove(&mut self, connection_id: &ConnectionId) -> Option<WriteHalf> {
        let connection = self.connections.remove(connection_id);
        log::info!("Removing connection {:?}", connection_id);

        if let Some(c) = connection.as_ref() {
            if self
                .peer_to_connection
                .get(&c.peer_id)
                .map(|connection_id| *connection_id == c.connection_id)
                .unwrap_or_default()
            {
                log::info!("Removing connection to {}", c.peer_id);
                self.peer_to_connection.remove(&c.peer_id);
            }
        }

        connection
    }

    pub fn connections_mut(&mut self) -> impl Iterator<Item = &mut WriteHalf> {
        self.connections.values_mut().map(|connection| {
            connection.touch();
            connection
        })
    }

    pub fn contains_peer(&self, peer_id: &u64) -> bool {
        self.peer_to_connection.contains_key(peer_id)
    }

    pub fn len(&self) -> usize {
        self.peer_to_connection.len()
    }

    pub fn remove_stale(&mut self) {
        self.connections
            .drain_filter(|_, connection| connection.is_stale())
            .for_each(|(_, connection)| {
                self.peer_to_connection.remove(&connection.peer_id);
            });
    }

    pub fn clear(&mut self) {
        self.connections.clear();
        self.peer_to_connection.clear();
    }
}
