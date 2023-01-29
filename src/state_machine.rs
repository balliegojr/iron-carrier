use std::fmt::Display;

type DefaultState<T> = Option<fn() -> Box<dyn StateStep<GlobalState = T>>>;

pub struct StateStepper<T> {
    shared_state: T,
    default_state: DefaultState<T>,
}

impl<T> StateStepper<T> {
    pub fn new(shared_state: T, default_state: DefaultState<T>) -> Self {
        Self {
            shared_state,
            default_state,
        }
    }

    pub async fn execute(self, initial_state: Box<dyn StateStep<GlobalState = T>>) {
        let mut current_state = initial_state;
        loop {
            log::debug!("Running State: {current_state}");
            log::debug!("{current_state:?}");

            match current_state.execute(&self.shared_state).await {
                Ok(Some(next_state)) => {
                    log::debug!("State Completed");
                    current_state = next_state;
                }
                Ok(None) => match self.default_state {
                    Some(default_state) => current_state = (default_state)(),
                    None => break,
                },
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
pub trait StateStep: Display + std::fmt::Debug + Sync + Send {
    type GlobalState;

    async fn execute(
        self: Box<Self>,
        shared_state: &Self::GlobalState,
    ) -> crate::Result<Option<Box<dyn StateStep<GlobalState = Self::GlobalState>>>>;
}
