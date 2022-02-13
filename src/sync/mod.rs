//! Handle synchronization

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

mod file_transfer_man;
pub use file_transfer_man::{FileHandlerEvent, FileTransferMan};

mod file_watcher;
pub use file_watcher::{FileWatcher, SupressionType, WatcherEvent};

mod synchronization_session;

mod synchronizer;
pub use synchronizer::Synchronizer;

use crate::fs::FileInfo;

#[derive(Debug, Serialize, Deserialize)]
pub enum SyncEvent {
    StartSync,
    EndSync,

    ExchangeStorageStates,
    QueryOutOfSyncStorages(HashMap<String, u64>),
    ReplyOutOfSyncStorages(Vec<String>),

    SyncNextStorage,

    BuildStorageIndex(String),
    SetStorageIndex(Vec<FileInfo>),
}

impl std::fmt::Display for SyncEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SyncEvent::StartSync => write!(f, "Start Sync"),
            SyncEvent::EndSync => write!(f, "End Sync"),
            SyncEvent::SyncNextStorage => write!(f, "Sync next storage"),
            SyncEvent::BuildStorageIndex(storage) => {
                write!(f, "Build Storage Index: {}", storage)
            }
            SyncEvent::SetStorageIndex(_) => write!(f, "Set Storage Index"),
            SyncEvent::ExchangeStorageStates => write!(f, "Exchange storage states"),
            SyncEvent::QueryOutOfSyncStorages(_storages) => write!(f, "Query storages"),
            SyncEvent::ReplyOutOfSyncStorages(storages) => {
                write!(f, "Storages To Sync {:?}", storages)
            }
        }
    }
}
