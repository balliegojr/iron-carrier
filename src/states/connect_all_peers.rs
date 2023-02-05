use std::{collections::HashMap, fmt::Display, net::SocketAddr};

use crate::{state_machine::Step, SharedState};

#[derive(Debug)]
pub struct ConnectAllPeers {
    addresses_to_connect: HashMap<SocketAddr, Option<u64>>,
}

impl ConnectAllPeers {
    pub fn new(addresses_to_connect: HashMap<SocketAddr, Option<u64>>) -> Self {
        Self {
            addresses_to_connect,
        }
    }
}

impl Display for ConnectAllPeers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Connect to all Peers")
    }
}

impl Step for ConnectAllPeers {
    type Output = ();

    async fn execute(self, shared_state: &SharedState) -> crate::Result<Self::Output> {
        for (addr, peer_id) in self.addresses_to_connect {
            if let Err(err) = shared_state
                .connection_handler
                .connect(&addr, peer_id)
                .await
            {
                log::error!("Failed to connect to peer {addr} {err}");
            }
        }

        Ok(())
    }
}
