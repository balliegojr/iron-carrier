use std::str::FromStr;

use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Serialize, Deserialize)]
pub enum SyncStatus {
    Started,
    Done,
    Fail,
}

impl std::fmt::Display for SyncStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SyncStatus::Started => write!(f, "started"),
            SyncStatus::Done => write!(f, "done"),
            SyncStatus::Fail => write!(f, "fail"),
        }
    }
}

impl FromStr for SyncStatus {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "started" => Ok(SyncStatus::Started),
            "done" => Ok(SyncStatus::Done),
            "fail" => Ok(SyncStatus::Fail),
            value => anyhow::bail!("Invalid SyncStatus {value}"),
        }
    }
}
