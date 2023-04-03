use std::{
    collections::HashMap,
    fmt::Display,
    net::{SocketAddr, ToSocketAddrs},
    time::Duration,
};

use crate::{state_machine::State, SharedState};

#[derive(Default, Debug)]
pub struct DiscoverPeers {}

impl Display for DiscoverPeers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Discover Peers")
    }
}

impl State for DiscoverPeers {
    type Output = HashMap<SocketAddr, Option<u64>>;

    async fn execute(self, shared_state: &SharedState) -> crate::Result<Self::Output> {
        let backoff = backoff::ExponentialBackoffBuilder::new()
            .with_max_elapsed_time(Some(Duration::from_secs(30)))
            .build();

        // this retry is here in case the network card isn't ready when initializing the daemon
        let discovery = backoff::future::retry(backoff, || async {
            crate::network::service_discovery::get_service_discovery(shared_state.config)
                .await
                .map_err(backoff::Error::from)
        })
        .await?;

        let mut addresses = match discovery {
            Some(discovery) => {
                // Wait for a while to discover peers in the network
                tokio::time::sleep(Duration::from_secs(2)).await;
                crate::network::service_discovery::get_peers(
                    discovery,
                    shared_state.config.group.as_ref(),
                )
                .await
            }
            None => Default::default(),
        };

        if let Some(peers) = &shared_state.config.peers {
            for addrs in peers.iter().filter_map(|p| p.to_socket_addrs().ok()) {
                for addr in addrs {
                    addresses.entry(addr).or_insert(None);
                }
            }
        }

        Ok(addresses)
    }
}
