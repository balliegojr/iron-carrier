use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    net::SocketAddr,
};

use rand::Rng;

use crate::{
    node_id::NodeId, state_machine::Result, state_machine::State, Context, StateMachineError,
};

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

    async fn execute(self, context: &Context) -> Result<Self::Output> {
        if self.addresses_to_connect.is_empty() {
            Err(StateMachineError::Abort)?;
        }

        // prevent colision when multiple nodes start at the same time
        // this is an issue when running the tests, unlikely to be an issue for normal operation
        tokio::time::sleep(std::time::Duration::from_millis(random_wait_time())).await;

        let address_by_node: HashMap<Option<NodeId>, HashSet<SocketAddr>> = self
            .addresses_to_connect
            .iter()
            .fold(HashMap::new(), |mut acc, (addr, node_id)| {
                acc.entry(*node_id).or_default().insert(*addr);
                acc
            });

        let handles = address_by_node
            .into_values()
            .map(|addresses| {
                let connection_handler = context.connection_handler.clone();
                tokio::spawn(async move {
                    for addr in addresses {
                        match connection_handler.connect(addr).await {
                            Ok(_) => break,
                            Err(err) => {
                                log::error!("Failed to connect to {addr}: {err}");
                            }
                        }
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
