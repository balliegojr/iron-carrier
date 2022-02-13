use message_io::node::NodeHandler;
use std::{
    collections::{HashMap, LinkedList},
    sync::{Arc, Mutex, RwLock},
};

use super::{Commands, HandlerEvent, PeerConnection, RawMessageType};

#[derive(Clone)]
pub struct CommandDispatcher {
    handler: NodeHandler<HandlerEvent>,
    connections: Arc<RwLock<HashMap<String, PeerConnection>>>,
    event_queue: Arc<Mutex<LinkedList<Commands>>>,
}
impl CommandDispatcher {
    pub fn new(
        handler: NodeHandler<HandlerEvent>,
        connections: Arc<RwLock<HashMap<String, PeerConnection>>>,
        event_queue: Arc<Mutex<LinkedList<Commands>>>,
    ) -> Self {
        Self {
            handler,
            connections,
            event_queue,
        }
    }
    pub fn now<T: Into<Commands>>(&self, event: T) {
        let event: Commands = event.into();
        self.handler.signals().send(event.into());
    }

    pub fn enqueue<T: Into<Commands>>(&self, event: T) {
        let command = event.into();
        log::trace!("Enqueuing event: {:?}", &command);
        self.event_queue
            .lock()
            .expect("Poisoned lock")
            .push_back(command);
    }

    pub fn to<T: Into<Commands>>(&self, event: T, peer_id: &str) {
        let event: Commands = event.into();
        let mut connections = self.connections.write().expect("Poisoned lock");
        if let Some(connection) = connections.get_mut(peer_id) {
            match event {
                Commands::Stream(data) => {
                    connection.send_raw(self.handler.network(), RawMessageType::Stream, &data)
                }
                event => connection.send_command(self.handler.network(), &event),
            }
        }
    }
    pub fn broadcast<T: Into<Commands>>(&self, event: T) -> usize {
        let event: Commands = event.into();
        let controller = self.handler.network();
        let data = match event {
            Commands::Stream(data) => {
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

        let mut connections = self.connections.write().expect("Poisoned lock");
        for connection in connections.values_mut() {
            connection.send_data(controller, &data);
        }
        connections.len()
    }

    pub fn has_connections(&self) -> bool {
        !self.connections.read().expect("Poisoned lock").is_empty()
    }
}