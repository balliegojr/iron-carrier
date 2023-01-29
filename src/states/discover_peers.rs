use std::{fmt::Display, net::ToSocketAddrs, time::Duration};

use crate::{state_machine::StateStep, SharedState};

use super::ConnectAllPeers;

#[derive(Default, Debug)]
pub struct DiscoverPeers {}

impl Display for DiscoverPeers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Discover Peers")
    }
}

#[async_trait::async_trait]
impl StateStep for DiscoverPeers {
    type GlobalState = SharedState;

    async fn execute(
        self: Box<Self>,
        shared_state: &SharedState,
    ) -> crate::Result<Option<Box<dyn StateStep<GlobalState = Self::GlobalState>>>> {
        let discovery = async_retry(10, || {
            crate::network::service_discovery::get_service_discovery(shared_state.config)
        })
        .await?;

        tokio::time::sleep(Duration::from_secs(2)).await;

        let mut addresses = match discovery {
            Some(discovery) => {
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

        if addresses.is_empty() {
            Ok(None)
        } else {
            Ok(Some(Box::new(ConnectAllPeers::new(addresses))))
        }
    }
}

// NOTE: move this to a utils file?
async fn async_retry<T, F, Fut>(tries: u64, retriable: F) -> crate::Result<T>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = crate::Result<T>>,
{
    let mut current_try = 0u64;
    loop {
        match (retriable)().await {
            Err(err) => {
                current_try += 1;
                if current_try == tries {
                    return Err(err)?;
                }

                log::error!("{err}");
                tokio::time::sleep(Duration::from_secs(current_try)).await;
                continue;
            }
            Ok(ok) => return Ok(ok),
        }
    }
}
