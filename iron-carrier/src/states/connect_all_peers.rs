use std::{collections::HashMap, fmt::Display, net::SocketAddr};

use rand::Rng;

use crate::{node_id::NodeId, state_machine::State, SharedState};

#[derive(Debug)]
pub struct ConnectAllPeers {
    addresses_to_connect: HashMap<SocketAddr, Option<NodeId>>,
}

impl ConnectAllPeers {
    pub fn new(addresses_to_connect: HashMap<SocketAddr, Option<NodeId>>) -> Self {
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

impl State for ConnectAllPeers {
    type Output = ();

    async fn execute(self, shared_state: &SharedState) -> crate::Result<Self::Output> {
        if self.addresses_to_connect.is_empty() {
            return Ok(());
        }

        // prevent colision when multiple needs start at the same time
        // this is an issue when running the tests, unlikely to be an issue for normal operation
        tokio::time::sleep(std::time::Duration::from_millis(random_wait_time())).await;

        let handles = self
            .addresses_to_connect
            .into_keys()
            .map(|addr| {
                let connection_handler = shared_state.connection_handler.clone();
                tokio::spawn(async move {
                    if let Err(err) = connection_handler.connect(addr).await {
                        log::error!("Failed to connect to {addr}: {err}");
                    }
                })
            })
            .collect::<Vec<_>>();

        for handle in handles {
            let _ = handle.await;
        }

        Ok(())
    }
}

fn random_wait_time() -> u64 {
    let mut rng = rand::thread_rng();
    rng.gen_range(150..500)
}