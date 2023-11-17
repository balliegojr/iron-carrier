use std::{
    collections::HashMap,
    fmt::Display,
    net::{SocketAddr, ToSocketAddrs},
    time::Duration,
};

use crate::{
    node_id::NodeId,
    state_machine::{Result, State},
    Context,
};

#[derive(Default, Debug)]
pub struct DiscoverPeers {}

impl Display for DiscoverPeers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Discover Peers")
    }
}

impl State for DiscoverPeers {
    type Output = HashMap<SocketAddr, Option<NodeId>>;

    async fn execute(self, context: &Context) -> Result<Self::Output> {
        crate::network::service_discovery::init_service_discovery(context.config).await;
        tokio::time::sleep(Duration::from_secs(1)).await;

        let mut addresses = crate::network::service_discovery::get_nodes(context).await?;
        if let Some(peers) = &context.config.peers {
            for addrs in peers.iter().filter_map(|p| p.to_socket_addrs().ok()) {
                for addr in addrs {
                    addresses.entry(addr).or_insert(None);
                }
            }
        }

        Ok(addresses)
    }
}
