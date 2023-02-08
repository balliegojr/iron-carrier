use std::{
    collections::HashMap,
    fmt::Display,
    net::{SocketAddr, ToSocketAddrs},
    time::Duration,
};

use crate::{state_machine::Step, SharedState};

#[derive(Default, Debug)]
pub struct DiscoverPeers {}

impl Display for DiscoverPeers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Discover Peers")
    }
}

impl Step for DiscoverPeers {
    type Output = HashMap<SocketAddr, Option<u64>>;

    async fn execute(self, shared_state: &SharedState) -> crate::Result<Self::Output> {
        let backoff = backoff::ExponentialBackoffBuilder::new()
            .with_max_elapsed_time(Some(Duration::from_secs(30)))
            .build();

        let discovery = backoff::future::retry(backoff, || async {
            crate::network::service_discovery::get_service_discovery(shared_state.config)
                .await
                .map_err(backoff::Error::from)
        })
        .await?;

        let mut addresses = match discovery {
            Some(discovery) => {
                tokio::time::sleep(Duration::from_secs(2)).await;
                crate::network::service_discovery::get_peers(
                    &discovery,
                    shared_state.config.group.as_ref(),
                )
                .await
            }
            None => Default::default(),
        };

        if let Some(peers) = &shared_state.config.peers {
            for peer in peers {
                if let Ok(addrs) = peer.to_socket_addrs() {
                    for addr in addrs {
                        addresses.entry(addr).or_insert(None);
                    }
                }
            }
        }

        Ok(addresses)
    }
}
