use std::marker::PhantomData;

use thiserror::Error;

use crate::SharedState;

#[derive(Error, Debug)]
pub enum StateMachineError {
    #[error("Execution aborted")]
    Abort,
}

/// Represent a task or state to be executed
pub trait State: std::fmt::Debug {
    type Output: std::fmt::Debug;

    async fn execute(self, shared_state: &SharedState) -> crate::Result<Self::Output>;
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
        F: Fn() -> T,
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
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AndThen")
            .field("previous", &self.previous)
            .finish()
    }
}

impl<T, U, F> State for AndThen<T, U, F>
where
    T: State,
    U: State,
    F: FnOnce(T::Output) -> U,
{
    type Output = U::Output;

    async fn execute(self, shared_state: &SharedState) -> crate::Result<Self::Output>
    where
        Self: Sized,
    {
        log::debug!("Executing {:?}", self.previous);
        let previous_output = self.previous.execute(shared_state).await?;
        let next_task = (self.map_fn)(previous_output);
        log::debug!("Executing {next_task:?}");
        next_task.execute(shared_state).await
    }
}

#[derive(Debug)]
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

    async fn execute(self, shared_state: &SharedState) -> crate::Result<Self::Output>
    where
        Self: Sized,
    {
        log::debug!("Executing {:?}", self.previous);
        self.previous.execute(shared_state).await?;

        let next = U::default();
        log::debug!("Executing {next:?}");
        next.execute(shared_state).await
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
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ThenDefault")
            .field("previous", &self.previous)
            .finish()
    }
}

impl<T, U, F> State for ThenDefault<T, F>
where
    T: State,
    U: State,
    F: FnOnce() -> U,
{
    type Output = U::Output;

    async fn execute(self, shared_state: &SharedState) -> crate::Result<Self::Output> {
        log::debug!("Executing {:?}", self.previous);
        if let Err(err) = self.previous.execute(shared_state).await {
            if !err.is::<StateMachineError>() {
                log::error!("{err}");
            }
        };

        let next = (self.loop_fn)();
        log::debug!("Executing {next:?}");
        next.execute(shared_state).await
    }
}
