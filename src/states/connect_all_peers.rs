use std::{collections::HashMap, fmt::Display, net::SocketAddr};

use crate::{state_machine::StateStep, SharedState};

use super::Consensus;

#[derive(Debug)]
pub struct ConnectAllPeers {
    addresses_to_connect: HashMap<SocketAddr, Option<String>>,
}

impl ConnectAllPeers {
    pub fn new(addresses_to_connect: HashMap<SocketAddr, Option<String>>) -> Self {
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

#[async_trait::async_trait]
impl StateStep<SharedState> for ConnectAllPeers {
    async fn execute(
        self: Box<Self>,
        shared_state: &SharedState,
    ) -> crate::Result<Option<Box<dyn StateStep<SharedState>>>> {
        let mut successful = 0;
        for addr in self.addresses_to_connect.keys() {
            if let Err(err) = shared_state.connection_handler.connect(addr).await {
                log::error!("Failed to connect to peer {addr} {err}");
            } else {
                successful += 1;
            }
        }

        if successful > 0 {
            Ok(Some(Box::new(Consensus::new())))
        } else {
            Ok(shared_state.default_state())
        }
    }
}
