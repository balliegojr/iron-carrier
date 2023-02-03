use std::{fmt::Display, time::Duration};

use tokio_stream::StreamExt;

use crate::{
    network_events::{NetworkEvents, Transition},
    state_machine::StateStep,
    stream, SharedState,
};

use super::Consensus;

#[derive(Default)]
pub struct Daemon {}

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

#[async_trait::async_trait]
impl StateStep for Daemon {
    type GlobalState = SharedState;
    async fn execute(
        mut self: Box<Self>,
        shared_state: &SharedState,
    ) -> crate::Result<Option<Box<dyn StateStep<GlobalState = Self::GlobalState>>>> {
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
        // tokio::pin!(events_stream);

        loop {
            tokio::select! {
                stream_event = events_stream.next() => {
                    match stream_event {
                        Some((_, NetworkEvents::RequestTransition(Transition::Consensus))) => {
                            return Ok(Some(Box::new(Consensus::new())))
                        }
                        // In case there is an ongoing election when this daemon becomes active
                        Some((_, NetworkEvents::ConsensusElection(_))) => {
                            return Ok(Some(Box::new(Consensus::new())));
                        }
                        Some(_) => {
                            log::info!("received random event");
                        }
                        None => break Ok(None),
                    }
                }
                watcher_event = watcher_events.recv() => {
                    dbg!(watcher_event);
                }
            }
        }
    }
}
