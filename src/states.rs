use std::{
    collections::HashMap,
    fmt::Display,
    net::{SocketAddr, ToSocketAddrs},
    time::Duration,
};

use simple_mdns::async_discovery::ServiceDiscovery;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::{config::Config, network, state_machine, SharedState};

#[derive(Default, Debug)]
pub struct DiscoverPeers {}

impl Display for DiscoverPeers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Discover Peers")
    }
}

#[async_trait::async_trait]
impl state_machine::StateStep<SharedState> for DiscoverPeers {
    async fn execute(
        self: Box<Self>,
        shared_state: &SharedState,
    ) -> crate::Result<state_machine::StateTransition<SharedState>> {
        let discovery =
            network::service_discovery::get_service_discovery(shared_state.config).await?;

        tokio::time::sleep(Duration::from_secs(3)).await;

        let mut addresses = match discovery {
            Some(discovery) => {
                network::service_discovery::get_peers(
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

        Ok(state_machine::StateTransition::Next(Box::new(
            ConnectAllPeers {
                addresses_to_connect: addresses,
            },
        )))
    }
}

#[derive(Debug)]
struct ConnectAllPeers {
    addresses_to_connect: HashMap<SocketAddr, Option<String>>,
}

impl Display for ConnectAllPeers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Connect to all Peers")
    }
}

#[async_trait::async_trait]
impl state_machine::StateStep<SharedState> for ConnectAllPeers {
    async fn execute(
        self: Box<Self>,
        shared_state: &SharedState,
    ) -> crate::Result<state_machine::StateTransition<SharedState>> {
        for addr in self.addresses_to_connect.keys() {
            shared_state
                .connection_handler
                .connect(addr, network::Protocol::Tcp)
                .await;
        }

        if shared_state.daemon {
            Ok(state_machine::StateTransition::Next(Box::new(
                Daemon::new(shared_state.config).await,
            )))
        } else {
            Ok(state_machine::StateTransition::Next(Box::new(Consensus {
                connected_peers: Default::default(),
            })))
        }
    }
}

#[derive(Debug)]
struct Consensus {
    connected_peers: Vec<network::ConnectionId>,
}

impl Display for Consensus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Consensus")
    }
}

#[async_trait::async_trait]
impl state_machine::StateStep<SharedState> for Consensus {
    async fn execute(
        self: Box<Self>,
        shared_state: &SharedState,
    ) -> crate::Result<state_machine::StateTransition<SharedState>> {
        Ok(state_machine::StateTransition::Done)
    }
}

struct Daemon {
    sender: Sender<()>,
    receiver: Receiver<()>,
    service_discovery: Option<ServiceDiscovery>,
}

impl Display for Daemon {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Daemon")
    }
}

impl std::fmt::Debug for Daemon {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Daemon").finish()
    }
}

impl Daemon {
    pub async fn new(config: &Config) -> Self {
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        Self {
            sender,
            receiver,
            service_discovery: network::service_discovery::get_service_discovery(config)
                .await
                .unwrap(),
        }
    }
}

#[async_trait::async_trait]
impl state_machine::StateStep<SharedState> for Daemon {
    async fn execute(
        mut self: Box<Self>,
        shared_state: &SharedState,
    ) -> crate::Result<state_machine::StateTransition<SharedState>> {
        self.receiver.recv().await;
        Ok(state_machine::StateTransition::Next(self))
    }
}
