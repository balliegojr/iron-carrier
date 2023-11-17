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
    type Output = HashSet<NodeId>;

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

        let mut handles = Vec::new();
        for (node_id, addresses) in address_by_node {
            match node_id {
                Some(node_id) => {
                    if context.rpc.has_connection_to(node_id).await? {
                        continue;
                    }

                    let connection_handler = context.connection_handler.clone();
                    let handle = tokio::spawn(async move {
                        for addr in addresses {
                            match connection_handler.connect(addr).await {
                                Ok(node_id) => return Ok(node_id),
                                Err(err) => {
                                    log::error!("Failed to connect to {addr}: {err}");
                                }
                            }
                        }

                        anyhow::bail!("Failed to connect to node")
                    });
                    handles.push(handle)
                }
                None => {
                    for addr in addresses {
                        let connection_handler = context.connection_handler.clone();
                        let handle = tokio::spawn(async move {
                            match connection_handler.connect(addr).await {
                                Ok(node_id) => Ok(node_id),
                                Err(err) => {
                                    anyhow::bail!("Failed to connect to node {err}")
                                }
                            }
                        });

                        handles.push(handle);
                    }
                }
            }
        }

        let mut nodes = HashSet::new();
        for handle in handles {
            if let Ok(Ok(node_id)) = handle.await {
                nodes.insert(node_id);
            }
        }

        Ok(nodes)
    }
}

fn random_wait_time() -> u64 {
    let mut rng = rand::thread_rng();
    rng.gen_range(150..500)
}
