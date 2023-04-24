use std::{collections::HashSet, future, pin::pin, str::FromStr, time::Duration};

use crate::{
    config::Config,
    network_events::{NetworkEvents, Transition},
    state_machine::{State, StateComposer, StateMachineError},
    states::FullSync,
    stream, SharedState,
};

use super::{ConnectAllPeers, Consensus, DiscoverPeers, SyncFollower, SyncLeader};

#[derive(Default, Debug)]
pub struct Daemon {}

impl State for Daemon {
    type Output = ();

    async fn execute(self, shared_state: &SharedState) -> crate::Result<Self::Output> {
        loop {
            DaemonEventListener::default()
                .and_then(|event| event)
                .execute(shared_state)
                .await?;
        }
    }
}

#[derive(Debug, Default)]
struct DaemonEventListener;
impl State for DaemonEventListener {
    type Output = DaemonEvent;

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

        let mut full_sync_deadline = pin!(next_cron_schedule(shared_state.config));

        loop {
            tokio::select! {
                stream_event = shared_state.connection_handler.next_event() => {
                    match stream_event {
                        Some((_, NetworkEvents::ConsensusElection(_))) |
                        Some((_, NetworkEvents::RequestTransition(Transition::Consensus))) => {
                             return Ok(DaemonEvent::ConsensusRequest);
                        }
                        Some((leader_id, NetworkEvents::RequestTransition(Transition::FullSync))) => {
                            return Ok(DaemonEvent::TransitionToFolowerRequest(leader_id))
                        }
                        Some((_, NetworkEvents::Disconnected)) => {}
                        Some(ev) => {
                            log::error!("[daemon] received unexpected event {ev:?}");
                        }
                        None =>  Err(StateMachineError::Abort)?
                    }
                }
                Some(to_sync) = watcher_events.recv() => {
                     return Ok(DaemonEvent::Watcher(to_sync));
                }
                _ = &mut full_sync_deadline => {
                    return Ok(DaemonEvent::ScheduledSync);
                }
            }
        }
    }
}

#[derive(Debug)]
enum DaemonEvent {
    Watcher(HashSet<String>),
    ScheduledSync,
    ConsensusRequest,
    TransitionToFolowerRequest(u64),
}

impl State for DaemonEvent {
    type Output = ();

    async fn execute(self, shared_state: &SharedState) -> crate::Result<Self::Output> {
        match self {
            DaemonEvent::ScheduledSync => {
                DiscoverPeers::default()
                    .and_then(ConnectAllPeers::new)
                    .and_then(|_| SyncLeader::sync_everything())
                    .execute(shared_state)
                    .await
            }
            DaemonEvent::Watcher(to_sync) => {
                DiscoverPeers::default()
                    .and_then(ConnectAllPeers::new)
                    .and_then(|_| SyncLeader::sync_just(to_sync))
                    .execute(shared_state)
                    .await
            }
            DaemonEvent::ConsensusRequest => {
                Consensus::new()
                    .and_then(FullSync::new)
                    .execute(shared_state)
                    .await
            }
            DaemonEvent::TransitionToFolowerRequest(leader_id) => {
                SyncFollower::new(leader_id).execute(shared_state).await
            }
        }
    }
}

async fn next_cron_schedule(config: &Config) {
    let cron_deadline = config.schedule_sync.as_ref().and_then(|schedule_cron| {
        let schedule = cron::Schedule::from_str(schedule_cron).unwrap();

        schedule
            .upcoming(chrono::Local)
            .take(1)
            .next()
            .map(|event| {
                let deadline = std::time::Instant::now()
                    + event
                        .signed_duration_since(chrono::Local::now())
                        .to_std()
                        .unwrap();
                tokio::time::sleep_until(deadline.into())
            })
    });

    match cron_deadline {
        Some(deadline) => deadline.await,
        None => future::pending().await,
    }
}
