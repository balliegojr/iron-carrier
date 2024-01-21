use super::sync_status::SyncStatus;

pub struct SyncEntry {
    pub storage: String,
    pub timestamp: u64,
    pub sync_status: SyncStatus,
    pub node: String,
}

impl SyncEntry {
    pub fn new(storage: String, timestamp: u64, sync_status: SyncStatus, node: String) -> Self {
        Self {
            storage,
            timestamp,
            sync_status,
            node,
        }
    }
}
