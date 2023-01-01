use std::{path::PathBuf, str::FromStr};

use serde::Deserialize;

use super::OperationMode;

#[derive(Debug, Deserialize, Default)]
pub struct PathConfig {
    pub path: PathBuf,
    pub force_sync: Option<u64>,
    pub enable_watcher: Option<bool>,
    pub mode: Option<OperationMode>,
}

impl FromStr for PathConfig {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self {
            path: PathBuf::from_str(s)?,
            ..Default::default()
        })
    }
}
