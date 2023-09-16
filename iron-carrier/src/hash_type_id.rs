use std::ops::Deref;

pub trait HashTypeId {
    const ID: TypeId;
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct TypeId(pub u64);

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
        const ID: TypeId = TypeId(1);
    }

    #[test]
    fn test_type_id() {
        assert_eq!(*TypeOne::ID, 1);
    }
}
