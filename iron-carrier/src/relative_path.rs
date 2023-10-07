use std::{
    ops::Deref,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::config::PathConfig;

/// Represents a relative Path starting from the root of the storage.
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RelativePathBuf {
    inner: PathBuf,
}

impl RelativePathBuf {
    pub fn new(path_config: &PathConfig, mut path: PathBuf) -> anyhow::Result<Self> {
        if !path.has_root() {
            path = path.canonicalize()?;
        }

        path.strip_prefix(&path_config.path.canonicalize()?)
            .map(|inner| Self {
                inner: inner.to_path_buf(),
            })
            .map_err(anyhow::Error::from)
    }

    /// Returns the absolute for the given [PathConfig]
    pub fn absolute(&self, path_config: &PathConfig) -> anyhow::Result<PathBuf> {
        path_config
            .path
            .canonicalize()
            .map(|root_path| root_path.join(&self.inner))
            .map_err(anyhow::Error::from)
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

impl std::fmt::Debug for RelativePathBuf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}
