use std::fmt::Display;

pub struct StateStepper<T> {
    shared_state: T,
}

impl<T> StateStepper<T> {
    pub fn new(shared_state: T) -> Self {
        Self { shared_state }
    }

    pub async fn execute(self, initial_state: Box<dyn StateStep<T>>) {
        let mut current_state = initial_state;
        loop {
            log::debug!("Running State: {current_state}");
            log::debug!("{current_state:?}");

            match current_state.execute(&self.shared_state).await {
                Ok(Some(next_state)) => {
                    log::debug!("State Completed");
                    current_state = next_state;
                }
                Ok(None) => break,
                Err(err) => {
                    log::error!("State Failed");
                    log::error!("{err}");
                    log::debug!("State Execution Aborted");
                    break;
                }
            }
        }
    }
}

#[async_trait::async_trait]
pub trait StateStep<T>: Display + std::fmt::Debug + Sync + Send {
    async fn execute(
        self: Box<Self>,
        shared_state: &T,
    ) -> crate::Result<Option<Box<dyn StateStep<T>>>>;
}
