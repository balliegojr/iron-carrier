use std::collections::HashSet;

use crate::config::OperationMode;

#[derive(Debug, Default)]
pub struct SyncOptions {
    storages: HashSet<String>,
    sync_mode: OperationMode,
}

impl SyncOptions {
    pub fn auto() -> Self {
        Self {
            sync_mode: OperationMode::Auto,
            ..Default::default()
        }
    }

    pub fn manual() -> Self {
        Self {
            sync_mode: OperationMode::Manual,
            ..Default::default()
        }
    }

    pub fn with_storages(mut self, storages: HashSet<String>) -> Self {
        self.storages = storages;
        self
    }

    pub fn sync_mode(&self) -> &OperationMode {
        &self.sync_mode
    }

    pub fn storages(&self) -> &HashSet<String> {
        &self.storages
    }
}
