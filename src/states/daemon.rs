use std::{collections::HashSet, fmt::Display, future, str::FromStr, time::Duration};
use tokio::sync::mpsc::Sender;

use crate::{
    config::Config,
    network_events::{NetworkEvents, Transition},
    state_machine::{StateComposer, Step},
    states::FullSync,
    stream, SharedState,
};

use super::{ConnectAllPeers, Consensus, DiscoverPeers};

#[derive(Default)]
pub struct Daemon {
    when_full_sync: Option<Sender<()>>,
}

impl Daemon {
    pub fn new(when_full_sync: Option<Sender<()>>) -> Self {
        Self { when_full_sync }
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

    async fn execute(self, shared_state: &SharedState) -> crate::Result<Option<Self::Output>> {
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

        let full_sync_deadline = next_cron_schedule(shared_state.config);
        tokio::pin!(full_sync_deadline);

        loop {
            tokio::select! {
                stream_event = shared_state.connection_handler.next_event() => {
                    match stream_event {
                        Some((_, NetworkEvents::ConsensusElection(_))) |
                        Some((_, NetworkEvents::RequestTransition(Transition::Consensus))) => {
                             return Ok(Some(DaemonTask::ConsensusThenSync(self.when_full_sync)));
                        }
                        Some((leader_id, NetworkEvents::RequestTransition(Transition::FullSync))) => {
                            return Ok(Some(DaemonTask::TransitionToFollower(leader_id)))
                        }
                        Some(_) => {
                            log::info!("received random event");
                        }
                        None => return Ok(None)
                    }
                }
                Some(to_sync) = watcher_events.recv() => {
                     return Ok(Some(DaemonTask::ConnectThenSync(to_sync, self.when_full_sync)));
                }
                _ = &mut full_sync_deadline => {
                    return Ok(Some(DaemonTask::ConnectThenSync(Default::default(), self.when_full_sync)));
                }
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

#[derive(Debug)]
pub enum DaemonTask {
    ConsensusThenSync(Option<Sender<()>>),
    ConnectThenSync(HashSet<String>, Option<Sender<()>>),
    TransitionToFollower(u64),
}

impl Step for DaemonTask {
    type Output = ();

    async fn execute(self, shared_state: &SharedState) -> crate::Result<Option<Self::Output>> {
        match self {
            DaemonTask::ConsensusThenSync(when_sync_done) => {
                Consensus::new()
                    .and_then(|leader_id| FullSync::new(leader_id, when_sync_done))
                    .execute(shared_state)
                    .await
            }
            DaemonTask::ConnectThenSync(_storages_to_sync, when_sync_done) => {
                DiscoverPeers::default()
                    .and_then(ConnectAllPeers::new)
                    .and::<Consensus>()
                    .and_then(|leader_id| FullSync::new(leader_id, when_sync_done))
                    .execute(shared_state)
                    .await
            }
            DaemonTask::TransitionToFollower(leader_id) => {
                FullSync::new(leader_id, None).execute(shared_state).await
            }
        }
    }
}
