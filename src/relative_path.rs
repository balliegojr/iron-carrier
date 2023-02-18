use std::{
    ops::Deref,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::config::PathConfig;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RelativePathBuf {
    inner: PathBuf,
}

impl RelativePathBuf {
    pub fn new(path_config: &PathConfig, path: PathBuf) -> crate::Result<Self> {
        path.strip_prefix(&path_config.path)
            .map(|inner| Self {
                inner: inner.to_path_buf(),
            })
            .map_err(Box::from)
    }

    pub fn absolute(&self, path_config: &PathConfig) -> crate::Result<PathBuf> {
        path_config
            .path
            .canonicalize()
            .map(|root_path| root_path.join(&self.inner))
            .map_err(Box::from)
    }
}

impl Deref for RelativePathBuf {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl From<&str> for RelativePathBuf {
    fn from(value: &str) -> Self {
        Self {
            inner: value.into(),
        }
    }
}
