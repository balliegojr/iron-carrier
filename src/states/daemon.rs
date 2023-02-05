use std::{collections::HashSet, fmt::Display, time::Duration};

use tokio_stream::StreamExt;

use crate::{
    network_events::{NetworkEvents, Transition},
    state_machine::{StateComposer, Step},
    states::FullSync,
    stream, IronCarrierError, SharedState,
};

use super::{ConnectAllPeers, Consensus, DiscoverPeers, FullSyncLeader};

#[derive(Default)]
pub struct Daemon {}

impl Daemon {
    pub fn new() -> Self {
        Self {}
    }
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

impl Step for Daemon {
    type Output = DaemonTask;

    async fn execute(self, shared_state: &SharedState) -> crate::Result<Self::Output> {
        let _service_discovery =
            crate::network::service_discovery::get_service_discovery(shared_state.config).await?;

        let (watcher_events_sender, watcher_events) = tokio::sync::mpsc::channel(1);
        let _watcher = crate::storage::file_watcher::get_file_watcher(
            shared_state.config,
            shared_state.transaction_log,
            watcher_events_sender,
        )?;

        let mut watcher_events = stream::fold_timeout(
            watcher_events,
            Duration::from_secs(shared_state.config.delay_watcher_events),
        );

        let mut events_stream = shared_state.connection_handler.events_stream().await;

        loop {
            tokio::select! {
                stream_event = events_stream.next() => {
                    match stream_event {
                        Some((_, NetworkEvents::ConsensusElection(_))) |
                        Some((_, NetworkEvents::RequestTransition(Transition::Consensus))) => {
                             return Ok(DaemonTask::ConsensusThenSync);
                        }
                        Some((leader_id, NetworkEvents::RequestTransition(Transition::FullSync))) => {
                            return Ok(DaemonTask::TransitionToFollower(leader_id))
                        }
                        Some(_) => {
                            log::info!("received random event");
                        }
                        // TODO: return a proper "abort execution error"
                        None => return Err(IronCarrierError::InvalidOperation.into())
                    }
                }
                Some(to_sync) = watcher_events.recv() => {
                     return Ok(DaemonTask::ConnectThenSync(to_sync));
                }
            }
        }
    }
}

#[derive(Debug)]
pub enum DaemonTask {
    ConsensusThenSync,
    ConnectThenSync(HashSet<String>),
    TransitionToFollower(u64),
}

impl Step for DaemonTask {
    type Output = ();

    async fn execute(self, shared_state: &SharedState) -> crate::Result<Self::Output> {
        match self {
            DaemonTask::ConsensusThenSync => {
                Consensus::new()
                    .and_then(FullSync::new)
                    .execute(shared_state)
                    .await
            }
            DaemonTask::ConnectThenSync(storages_to_sync) => {
                DiscoverPeers::default()
                    .and_then(ConnectAllPeers::new)
                    .and_then(move |_| FullSyncLeader::sync_just(storages_to_sync))
                    .execute(shared_state)
                    .await
            }
            DaemonTask::TransitionToFollower(leader_id) => {
                FullSync::new(leader_id).execute(shared_state).await
            }
        }
    }
}
