use message_io::node::NodeHandler;
use std::{
    collections::LinkedList,
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::connection::ConnectionHandler;

use super::{Commands, HandlerEvent, RawMessageType};

#[derive(Clone)]
pub struct CommandDispatcher {
    handler: NodeHandler<HandlerEvent>,
    connection_handler: Arc<Mutex<ConnectionHandler>>,
    event_queue: Arc<Mutex<LinkedList<Commands>>>,
}
impl CommandDispatcher {
    pub fn new(
        handler: NodeHandler<HandlerEvent>,
        connection_handler: Arc<Mutex<ConnectionHandler>>,
        event_queue: Arc<Mutex<LinkedList<Commands>>>,
    ) -> Self {
        Self {
            handler,
            connection_handler,
            event_queue,
        }
    }
    pub fn now<T: Into<Commands>>(&self, event: T) {
        let event: Commands = event.into();
        self.handler.signals().send(event.into());
    }

    pub fn after<T: Into<Commands>>(&self, event: T, duration: Duration) {
        let event: Commands = event.into();
        self.handler
            .signals()
            .send_with_timer(event.into(), duration);
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

        match self
            .connection_handler
            .lock()
            .unwrap()
            .get_connection(peer_id)
        {
            Some(connection) => match event {
                Commands::Stream(data) => {
                    connection.send_raw(self.handler.network(), RawMessageType::Stream, &data)
                }
                event => connection.send_command(self.handler.network(), &event),
            },
            _ => {
                log::error!("Connection to {peer_id} not found");
            }
        };
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

        let mut broadcasted = 0;
        let connection_handler = self.connection_handler.lock().unwrap();
        for connection in connection_handler.get_connections() {
            connection.send_data(controller, &data);
            broadcasted += 1;
        }
        broadcasted
    }

    pub fn has_connections(&self) -> bool {
        self.connection_handler.lock().unwrap().has_connections()
    }
}
