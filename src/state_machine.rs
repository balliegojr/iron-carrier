use std::fmt::Display;

pub struct StateStepper<T> {
    shared_state: T,
}

impl<T> StateStepper<T> {
    pub fn new(shared_state: T) -> Self {
        Self { shared_state }
    }

    pub async fn execute(self, initial_state: Box<dyn StateStep<T>>) {
        let mut current_state = StateTransition::Next(initial_state);
        loop {
            match current_state {
                StateTransition::Next(state) => {
                    log::debug!("Running State: {state}");
                    log::debug!("{state:?}");

                    match state.execute(&self.shared_state).await {
                        Ok(next_state) => {
                            log::debug!("State Completed");
                            current_state = next_state;
                        }
                        Err(err) => {
                            log::error!("State Failed");
                            log::error!("{err}");
                            log::debug!("State Execution Aborted");
                            break;
                        }
                    }
                }
                StateTransition::Done => {
                    log::debug!("Ran to completion");
                    break;
                }
            }
        }
    }
}

pub enum StateTransition<T> {
    Next(Box<dyn StateStep<T>>),
    Done,
}
#[async_trait::async_trait]
pub trait StateStep<T>: Display + std::fmt::Debug {
    async fn execute(self: Box<Self>, shared_state: &T) -> crate::Result<StateTransition<T>>;
}
