use std::ops::Deref;

pub trait HashTypeId {
    fn id() -> TypeId;
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct TypeId(u64);

impl From<u64> for TypeId {
    fn from(value: u64) -> Self {
        TypeId(value)
    }
}

impl Deref for TypeId {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::{HashTypeId, TypeId};

    struct TypeOne;

    impl HashTypeId for TypeOne {
        fn id() -> TypeId {
            1.into()
        }
    }

    #[test]
    fn test_type_id() {
        assert_eq!(*TypeOne::id(), 1);
    }
}
