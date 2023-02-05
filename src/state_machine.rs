use std::marker::PhantomData;

use crate::SharedState;

pub trait Step: std::fmt::Debug {
    type Output: std::fmt::Debug;

    async fn execute(self, shared_state: &SharedState) -> crate::Result<Self::Output>;
}

pub trait StateComposer {
    fn and<T>(self) -> And<Self, T>
    where
        Self: Step + Sized,
        T: Step,
    {
        And {
            previous: self,
            _next: Default::default(),
        }
    }

    fn and_then<T, F>(self, map_fn: F) -> AndThen<Self, T, F>
    where
        Self: Step + Sized,
        T: Step,
        F: FnOnce(Self::Output) -> T,
    {
        AndThen {
            previous: self,
            map_fn,
        }
    }

    fn or_else<T>(self, map_fn: fn() -> T) -> OrElse<Self, T>
    where
        Self: Step + Sized,
        T: Step + Default,
    {
        OrElse {
            previous: self,
            map_fn,
        }
    }

    fn then_loop<T>(self, loop_fn: fn() -> T) -> Loop<Self, T>
    where
        Self: Step + Sized,
        T: Step,
    {
        Loop {
            previous: self,
            loop_fn,
        }
    }
}

impl<T> StateComposer for T where T: Step {}

pub struct AndThen<T, U, F>
where
    T: Step,
    U: Step,
    F: FnOnce(T::Output) -> U,
{
    previous: T,
    map_fn: F,
}

impl<T, U, F> std::fmt::Debug for AndThen<T, U, F>
where
    T: Step,
    U: Step,
    F: FnOnce(T::Output) -> U,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AndThen")
            .field("previous", &self.previous)
            .finish()
    }
}

impl<T, U, F> Step for AndThen<T, U, F>
where
    T: Step,
    U: Step,
    F: FnOnce(T::Output) -> U,
{
    type Output = U::Output;

    async fn execute(self, shared_state: &SharedState) -> crate::Result<Self::Output>
    where
        Self: Sized,
    {
        log::debug!("Executing {:?}", self.previous);
        let next: U = (self.map_fn)(self.previous.execute(shared_state).await?);
        log::debug!("Executing {next:?}");
        next.execute(shared_state).await
    }
}

#[derive(Debug)]
pub struct And<T, U>
where
    T: Step,
    U: Step,
{
    previous: T,
    _next: PhantomData<U>,
}

impl<T, U> Step for And<T, U>
where
    T: Step,
    U: Step + Default,
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

#[derive(Debug)]
pub struct OrElse<T, U>
where
    T: Step,
    U: Step,
{
    previous: T,
    map_fn: fn() -> U,
}

impl<T, U> Step for OrElse<T, U>
where
    T: Step,
    U: Step,
{
    type Output = U::Output;

    async fn execute(self, shared_state: &SharedState) -> crate::Result<Self::Output>
    where
        Self: Sized,
    {
        log::debug!("Executing {:?}", self.previous);
        if let Err(err) = self.previous.execute(shared_state).await {
            log::error!("Error executing steps {err}");
        }

        let next = (self.map_fn)();
        log::debug!("Executing {next:?}");
        next.execute(shared_state).await
    }
}

#[derive(Debug)]
pub struct Loop<T, U> {
    previous: T,
    loop_fn: fn() -> U,
}

impl<T, U> Step for Loop<T, U>
where
    T: Step,
    U: Step,
{
    type Output = T::Output;

    async fn execute(self, shared_state: &SharedState) -> crate::Result<Self::Output> {
        log::debug!("Executing {:?}", self.previous);
        if let Err(err) = self.previous.execute(shared_state).await {
            log::error!("{err}");
        };

        loop {
            let next = (self.loop_fn)();
            log::debug!("Executing {next:?}");
            if let Err(err) = next.execute(shared_state).await {
                log::error!("{err}");
            }
        }
    }
}
