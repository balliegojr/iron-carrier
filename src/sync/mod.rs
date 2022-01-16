//! Handle synchronization

mod file_transfer_man;
mod file_watcher;
mod synchronization_session;
mod synchronizer;

use std::collections::{HashMap, HashSet};

pub use file_transfer_man::{FileHandlerEvent, FileSyncEvent, FileTransferMan};
pub use file_watcher::{FileWatcher, WatcherEvent};
use serde::{Deserialize, Serialize};
pub use synchronizer::Synchronizer;

use crate::fs::FileInfo;

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum QueueEventType {
    Signal,
    Peer(String),
    Broadcast,
}

impl std::fmt::Display for QueueEventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueueEventType::Signal => write!(f, "Signal"),
            QueueEventType::Peer(peer) => write!(f, "To Peer {}", peer),
            QueueEventType::Broadcast => write!(f, "Broadcast"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum SyncEvent {
    StartSync(u32),
    EndSync,
    Cleanup,

    ExchangeStorageStates,
    QueryOutOfSyncStorages(HashMap<String, u64>),
    ReplyOutOfSyncStorages(Vec<String>),

    SyncNextStorage,

    BuildStorageIndex(String),
    SetStorageIndex(HashSet<FileInfo>),
    ConsumeSyncQueue,

    DeleteFile(FileInfo),
    SendFile(FileInfo, String, bool),
    BroadcastFile(FileInfo, bool),
    RequestFile(FileInfo, bool),
    MoveFile(FileInfo, FileInfo),

    FileWatcherEvent(WatcherEvent),
    FileSyncEvent(FileSyncEvent),
}

impl std::fmt::Display for SyncEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SyncEvent::StartSync(_) => write!(f, "Start Sync"),
            SyncEvent::EndSync => write!(f, "End Sync"),
            SyncEvent::Cleanup => write!(f, "Cleanup"),
            SyncEvent::SyncNextStorage => write!(f, "Sync next storage"),
            SyncEvent::BuildStorageIndex(storage) => {
                write!(f, "Build Storage Index: {}", storage)
            }
            SyncEvent::SetStorageIndex(_) => write!(f, "Set Storage Index"),
            SyncEvent::ConsumeSyncQueue => write!(f, "Consume next event in queue"),
            SyncEvent::DeleteFile(file) => write!(f, "Delete file {:?}", file.path),
            SyncEvent::SendFile(file, peer_id, _) => {
                write!(f, "Send file {:?} to {}", file.path, peer_id)
            }
            SyncEvent::BroadcastFile(_, _) => write!(f, "Broadcast file to all peers"),
            SyncEvent::RequestFile(file, _) => write!(f, "Request File {:?}", file.path),
            SyncEvent::MoveFile(src, dest) => {
                write!(f, "Move file from {:?} to {:?}", src.path, dest.path)
            }
            SyncEvent::FileWatcherEvent(_) => write!(f, "File watcher event"),
            SyncEvent::FileSyncEvent(ev) => write!(f, "FileSync {}", ev),
            SyncEvent::ExchangeStorageStates => write!(f, "Exchange storage states"),
            SyncEvent::QueryOutOfSyncStorages(_storages) => write!(f, "Query storages"),
            SyncEvent::ReplyOutOfSyncStorages(storages) => {
                write!(f, "Storages To Sync {:?}", storages)
            }
        }
    }
}

#[derive(Debug, Hash, Eq, PartialEq)]
pub(crate) enum Origin {
    Initiator,
    Peer(String),
}
