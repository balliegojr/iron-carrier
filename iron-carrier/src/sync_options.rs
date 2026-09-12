use std::collections::HashSet;

#[derive(Debug, Default)]
pub struct SyncOptions {
    storages: HashSet<String>,
}

impl SyncOptions {
    pub fn new(storages: HashSet<String>) -> Self {
        Self { storages }
    }

    pub fn storages(&self) -> &HashSet<String> {
        &self.storages
    }
}
