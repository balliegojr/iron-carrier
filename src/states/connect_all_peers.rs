use std::{collections::HashMap, fmt::Display, net::SocketAddr};

use rand::Rng;

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

    async fn execute(self, shared_state: &SharedState) -> crate::Result<Option<Self::Output>> {
        // prevent colision when multiple needs start at the same time
        // this is an issue when running the tests, unlikely to be an issue for normal operation
        tokio::time::sleep(std::time::Duration::from_millis(random_wait_time())).await;

        let connection_handler = shared_state.connection_handler;
        let handles = self
            .addresses_to_connect
            .into_iter()
            .map(|(addr, peer_id)| {
                tokio::spawn(async move {
                    if let Err(err) = connection_handler.connect(addr, peer_id).await {
                        log::error!("Failed to connect to {addr}: {err}");
                    }
                })
            })
            .collect::<Vec<_>>();

        for handle in handles {
            let _ = handle.await;
        }

        Ok(Some(()))
    }
}

fn random_wait_time() -> u64 {
    let mut rng = rand::thread_rng();
    rng.gen_range(150..500)
}
