use std::time::{Duration, Instant};

use message_io::network::{Endpoint, NetworkController};

use crate::sync::SyncEvent;

use super::{CommandType, RawMessageType};

#[derive(Debug)]
pub struct PeerConnection {
    endpoint: Endpoint,
    last_access: Instant,
}

impl PeerConnection {
    pub fn send_data(&mut self, controller: &NetworkController, data: &[u8]) {
        controller.send(self.endpoint, data);
        self.touch()
    }
    pub fn send_raw(
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
    /// send a [message](`SyncEvent`) to [endpoint](`message_io::network::Endpoint`) with message prefix 1
    pub fn send_command(&mut self, controller: &NetworkController, message: &CommandType) {
        let data = bincode::serialize(message).unwrap();
        self.send_raw(controller, RawMessageType::Command, &data);
    }

    pub fn touch(&mut self) {
        self.last_access = Instant::now();
    }

    pub fn is_stale(&self) -> bool {
        let limit = Instant::now() - Duration::from_secs(30);
        self.last_access < limit
    }

    /// Get the peer connection's endpoint.
    pub fn endpoint(&self) -> Endpoint {
        self.endpoint
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
