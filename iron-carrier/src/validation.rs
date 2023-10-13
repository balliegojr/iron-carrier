use std::ops::Deref;

/// Verify the validity of a struct
///
/// Any struct that implement this trait can be wrapped by [Unverified<T>] and then transformed
/// into [Verified<T>] by calling [Unverified::<T>::validate]
///
/// Any [Verified<T>] is garanteed to be verified
pub trait Verifiable {
    fn is_valid(&self) -> anyhow::Result<()>;
}

pub struct Unvalidated<T> {
    inner: T,
}

impl<T> Unvalidated<T>
where
    T: Verifiable,
{
    pub fn new(inner: T) -> Self {
        Self { inner }
    }

    pub fn validate(self) -> anyhow::Result<Validated<T>> {
        self.is_valid().map(|_| Validated { inner: self.inner })
    }
}

impl<T> std::fmt::Debug for Unvalidated<T>
where
    T: Verifiable + std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Unverified")
            .field("inner", &self.inner)
            .finish()
    }
}

impl<T: Verifiable> Deref for Unvalidated<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

pub struct Validated<T> {
    inner: T,
}

#[cfg(test)]
impl<T> Validated<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T: Verifiable> Deref for Validated<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> std::fmt::Debug for Validated<T>
where
    T: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Verified")
            .field("inner", &self.inner)
            .finish()
    }
}
