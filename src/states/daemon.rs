use std::fmt::Display;

use tokio_stream::StreamExt;

use crate::{
    network_events::{NetworkEvents, Transition},
    state_machine::StateStep,
    SharedState,
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

        let mut events_stream = shared_state.connection_handler.events_stream().await;
        // tokio::pin!(events_stream);

        loop {
            match events_stream.next().await {
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
    }
}
