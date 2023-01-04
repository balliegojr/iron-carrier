/// Helper trait to implement leak() for every struct
pub trait Leak {
    /// Calls `Box::leak(Box::new(self))`
    fn leak(self) -> &'static Self
    where
        Self: Sized,
    {
        Box::leak(Box::new(self))
    }
}

impl<T> Leak for T {}
