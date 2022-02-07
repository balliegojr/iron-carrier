use std::time::{Duration, Instant};

use message_io::network::{Endpoint, NetworkController};

use crate::contants::{PEER_IDENTIFICATION_TIMEOUT, PEER_STALE_CONNECTION};

use super::{Commands, RawMessageType};

#[derive(Debug)]
pub struct PeerConnection {
    endpoint: Endpoint,
    last_access: Instant,
}

impl PeerConnection {
    pub fn send_data(&self, controller: &NetworkController, data: &[u8]) {
        controller.send(self.endpoint, data);
    }
    pub fn send_raw(
        &self,
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
    pub fn send_command(&self, controller: &NetworkController, message: &Commands) {
        let data = bincode::serialize(message).unwrap();
        self.send_raw(controller, RawMessageType::Command, &data);
    }

    pub fn touch(&mut self) {
        self.last_access = Instant::now();
    }

    pub fn is_stale(&self, is_identified: bool) -> bool {
        let secs = if is_identified {
            PEER_STALE_CONNECTION
        } else {
            PEER_IDENTIFICATION_TIMEOUT
        };
        let limit = Instant::now() - Duration::from_secs(secs);
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
