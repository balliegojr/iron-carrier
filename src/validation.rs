use std::ops::Deref;

pub trait Validator {}

pub trait Validate {
    fn is_valid(&self) -> crate::Result<()>;
    fn validate(self) -> crate::Result<Valid<Self>>
    where
        Self: Sized + Validate,
    {
        self.is_valid().map(|_| Valid { inner: self })
    }
}

pub struct Valid<T> {
    inner: T,
}

impl<T: Validate + Sized> Valid<T> {
    pub fn leak(self) -> &'static Self {
        Box::leak(Box::new(self))
    }
}

impl<T: Validate> Deref for Valid<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
