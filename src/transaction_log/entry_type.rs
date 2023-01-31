use std::str::FromStr;

use super::TransactionLogError;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum EntryType {
    Delete,
    Write,
    Move,
}

impl std::fmt::Display for EntryType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use EntryType::*;

        match self {
            Delete => write!(f, "delete"),
            Write => write!(f, "write"),
            Move => write!(f, "move"),
        }
    }
}

impl FromStr for EntryType {
    type Err = TransactionLogError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use EntryType::*;

        match s {
            "delete" => Ok(Delete),
            "write" => Ok(Write),
            "move" => Ok(Move),
            _ => Err(TransactionLogError::InvalidStringFormat),
        }
    }
}
