use std::fmt::Display;

use crate::{state_machine::StateStep, NetworkEvents, SharedState, Transition};

use super::{Consensus, FullSync};

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

impl Daemon {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl StateStep<SharedState> for Daemon {
    async fn execute(
        mut self: Box<Self>,
        shared_state: &SharedState,
    ) -> crate::Result<Option<Box<dyn StateStep<SharedState>>>> {
        let _service_discovery =
            crate::network::service_discovery::get_service_discovery(shared_state.config).await?;

        loop {
            match shared_state.connection_handler.next_event().await {
                Some((_, NetworkEvents::RequestTransition(transition))) => match transition {
                    Transition::Consensus => return Ok(Some(Box::new(Consensus::new()))),
                    Transition::FullSync => return Ok(Some(Box::new(FullSync::new(false)))),
                },
                Some(_) => {}
                None => break Ok(None),
            }
        }
    }
}
