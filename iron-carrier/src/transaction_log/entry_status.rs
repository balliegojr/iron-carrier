use std::str::FromStr;

use super::TransactionLogError;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum EntryStatus {
    Pending,
    Done,
    Fail,
}

impl std::fmt::Display for EntryStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EntryStatus::Pending => write!(f, "pending"),
            EntryStatus::Done => write!(f, "done"),
            EntryStatus::Fail => write!(f, "fail"),
        }
    }
}

impl FromStr for EntryStatus {
    type Err = TransactionLogError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pending" => Ok(EntryStatus::Pending),
            "done" => Ok(EntryStatus::Done),
            "fail" => Ok(EntryStatus::Fail),
            _ => Err(TransactionLogError::InvalidStringFormat),
        }
    }
}
