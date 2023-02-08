use std::ops::Deref;

pub trait Verifiable {
    fn is_valid(&self) -> crate::Result<()>;
}

pub struct Verified<T> {
    inner: T,
}

impl<T: Verifiable> Deref for Verified<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> std::fmt::Debug for Verified<T>
where
    T: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Verified")
            .field("inner", &self.inner)
            .finish()
    }
}

pub struct Unverified<T>
where
    T: Verifiable,
{
    inner: T,
}

impl<T> Unverified<T>
where
    T: Verifiable,
{
    pub fn new(inner: T) -> Self {
        Self { inner }
    }

    pub fn validate(self) -> crate::Result<Verified<T>> {
        self.is_valid().map(|_| Verified { inner: self.inner })
    }
}

impl<T> std::fmt::Debug for Unverified<T>
where
    T: Verifiable + std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Unverified")
            .field("inner", &self.inner)
            .finish()
    }
}

impl<T: Verifiable> Deref for Unverified<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
