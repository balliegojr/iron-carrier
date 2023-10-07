use std::{path::PathBuf, str::FromStr};

use serde::Deserialize;

use crate::relative_path::RelativePathBuf;

/// Represents the configuration of a storage path
#[derive(Debug, Deserialize, Default)]
pub struct PathConfig {
    pub path: PathBuf,
    pub enable_watcher: Option<bool>,
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

impl PathConfig {
    pub fn get_relative_path(&self, path: PathBuf) -> anyhow::Result<RelativePathBuf> {
        RelativePathBuf::new(self, path)
    }
}
