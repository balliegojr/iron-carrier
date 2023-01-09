use std::{fmt::Display, time::Duration};

use crate::{state_machine::StateStep, NetworkEvents, SharedState};

#[derive(Debug)]
pub struct FullSync {
    initiator: bool,
}

impl Display for FullSync {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FullSync")
    }
}

impl FullSync {
    pub fn new(initiator: bool) -> Self {
        Self { initiator }
    }
}

#[async_trait::async_trait]
impl StateStep<SharedState> for FullSync {
    async fn execute(
        mut self: Box<Self>,
        shared_state: &SharedState,
    ) -> crate::Result<Option<Box<dyn StateStep<SharedState>>>> {
        if self.initiator {
            shared_state
                .connection_handler
                .broadcast(NetworkEvents::RequestTransition(
                    crate::Transition::FullSync,
                ))
                .await?;

            log::info!("full sync starting as initiator....");
        } else {
            log::info!("full sync starting....");
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
        log::info!("full sync end....");

        Ok(shared_state.default_state())
    }
}
