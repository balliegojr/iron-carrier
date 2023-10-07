use std::marker::PhantomData;

use crate::SharedState;

macro_rules! debug {
    ($arg:expr, $node_id:expr) => {
        let debug_message = format!("{:?}", $arg);
        if !debug_message.is_empty() {
            log::debug!("{} - Executing {}", $node_id, debug_message)
        }
    };
}

pub type Result<T> = std::result::Result<T, StateMachineError>;

#[derive(Debug)]
pub enum StateMachineError {
    Abort,
    Err(anyhow::Error),
}

impl std::fmt::Display for StateMachineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StateMachineError::Abort => write!(f, "Execution aborted"),
            StateMachineError::Err(err) => err.fmt(f),
        }
    }
}

impl std::error::Error for StateMachineError {}

impl From<anyhow::Error> for StateMachineError {
    fn from(value: anyhow::Error) -> Self {
        Self::Err(value)
    }
}

/// Represent a task or state to be executed
pub trait State: std::fmt::Debug {
    type Output: std::fmt::Debug;

    async fn execute(self, shared_state: &SharedState) -> Result<Self::Output>;
}

pub trait StateComposer {
    fn and<T>(self) -> And<Self, T>
    where
        Self: State + Sized,
        T: State,
    {
        And {
            previous: self,
            _next: Default::default(),
        }
    }

    fn and_then<T, F>(self, map_fn: F) -> AndThen<Self, T, F>
    where
        Self: State + Sized,
        T: State,
        F: FnOnce(Self::Output) -> T,
    {
        AndThen {
            previous: self,
            map_fn,
            _marker: Default::default(),
        }
    }

    fn then_default_to<T, F>(self, loop_fn: F) -> ThenDefault<Self, F>
    where
        Self: State + Sized,
        T: State,
        F: FnOnce() -> T,
    {
        ThenDefault {
            previous: self,
            loop_fn,
        }
    }
}

impl<T> StateComposer for T where T: State {}

pub struct AndThen<T, U, F> {
    previous: T,
    map_fn: F,
    _marker: PhantomData<U>,
}

impl<T, U, F> std::fmt::Debug for AndThen<T, U, F>
where
    T: std::fmt::Debug,
{
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl<T, U, F> State for AndThen<T, U, F>
where
    T: State,
    U: State,
    F: FnOnce(T::Output) -> U,
{
    type Output = U::Output;

    async fn execute(self, shared_state: &SharedState) -> Result<Self::Output>
    where
        Self: Sized,
    {
        debug!(self.previous, shared_state.config.node_id_hashed);
        let previous_output = self.previous.execute(shared_state).await?;
        let next_task = (self.map_fn)(previous_output);
        debug!(next_task, shared_state.config.node_id_hashed);
        next_task.execute(shared_state).await
    }
}

pub struct And<T, U> {
    previous: T,
    _next: PhantomData<U>,
}

impl<T, U> State for And<T, U>
where
    T: State,
    U: State + Default,
{
    type Output = U::Output;

    async fn execute(self, shared_state: &SharedState) -> Result<Self::Output>
    where
        Self: Sized,
    {
        debug!(self.previous, shared_state.config.node_id_hashed);
        self.previous.execute(shared_state).await?;

        let next = U::default();
        debug!(next, shared_state.config.node_id_hashed);
        next.execute(shared_state).await
    }
}

impl<T, U> std::fmt::Debug for And<T, U> {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

pub struct ThenDefault<T, F> {
    previous: T,
    loop_fn: F,
}

impl<T, F> std::fmt::Debug for ThenDefault<T, F>
where
    T: std::fmt::Debug,
{
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl<T, U, F> State for ThenDefault<T, F>
where
    T: State,
    U: State,
    F: FnOnce() -> U,
{
    type Output = U::Output;

    async fn execute(self, shared_state: &SharedState) -> Result<Self::Output> {
        debug!(self.previous, shared_state.config.node_id_hashed);
        if let Err(StateMachineError::Err(err)) = self.previous.execute(shared_state).await {
            log::error!("{err}");
        };

        let next = (self.loop_fn)();
        debug!(next, shared_state.config.node_id_hashed);
        next.execute(shared_state).await
    }
}
