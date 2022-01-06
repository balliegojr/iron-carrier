//! Handle synchronization

mod connection_manager;
mod file_transfer_man;
mod file_watcher;
mod synchronization_session;
mod synchronizer;

use std::collections::{HashMap, HashSet};

use crate::fs::FileInfo;
use serde::{Deserialize, Serialize};

pub use connection_manager::{CommandDispatcher, CommandType, ConnectionManager};
pub use file_transfer_man::FileTransferMan;
pub use file_watcher::FileWatcher;
pub use synchronizer::Synchronizer;

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum QueueEventType {
    Signal,
    Peer(u64),
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
pub enum CarrierEvent {
    StartSync(u32),
    EndSync,
    Cleanup,
    IdentificationTimeout,

    ExchangeStorageStates,
    QueryOutOfSyncStorages(HashMap<String, u64>),
    ReplyOutOfSyncStorages(Vec<String>),

    SyncNextStorage,

    BuildStorageIndex(String),
    SetStorageIndex(HashSet<FileInfo>),
    ConsumeSyncQueue,

    DeleteFile(FileInfo),
    SendFile(FileInfo, u64, bool),
    BroadcastFile(FileInfo, bool),
    RequestFile(FileInfo, bool),
    MoveFile(FileInfo, FileInfo),

    FileWatcherEvent(WatcherEvent),
    FileSyncEvent(FileSyncEvent),
}

impl std::fmt::Display for CarrierEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CarrierEvent::StartSync(_) => write!(f, "Start Sync"),
            CarrierEvent::EndSync => write!(f, "End Sync"),
            CarrierEvent::Cleanup => write!(f, "Cleanup"),
            CarrierEvent::SyncNextStorage => write!(f, "Sync next storage"),
            CarrierEvent::BuildStorageIndex(storage) => {
                write!(f, "Build Storage Index: {}", storage)
            }
            CarrierEvent::SetStorageIndex(_) => write!(f, "Set Storage Index"),
            CarrierEvent::ConsumeSyncQueue => write!(f, "Consume next event in queue"),
            CarrierEvent::DeleteFile(file) => write!(f, "Delete file {:?}", file.path),
            CarrierEvent::SendFile(file, peer_id, _) => {
                write!(f, "Send file {:?} to {}", file.path, peer_id)
            }
            CarrierEvent::BroadcastFile(_, _) => write!(f, "Broadcast file to all peers"),
            CarrierEvent::RequestFile(file, _) => write!(f, "Request File {:?}", file.path),
            CarrierEvent::MoveFile(src, dest) => {
                write!(f, "Move file from {:?} to {:?}", src.path, dest.path)
            }
            CarrierEvent::FileWatcherEvent(_) => write!(f, "File watcher event"),
            CarrierEvent::FileSyncEvent(ev) => write!(f, "FileSync {}", ev),
            CarrierEvent::ExchangeStorageStates => write!(f, "Exchange storage states"),
            CarrierEvent::QueryOutOfSyncStorages(_storages) => write!(f, "Query storages"),
            CarrierEvent::ReplyOutOfSyncStorages(storages) => {
                write!(f, "Storages To Sync {:?}", storages)
            }
            CarrierEvent::IdentificationTimeout => write!(f, "Peers Identification Timeout"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum WatcherEvent {
    Created(FileInfo),
    Updated(FileInfo),
    Moved(FileInfo, FileInfo),
    Deleted(FileInfo),
}

#[derive(Debug, PartialEq, Eq)]
pub enum EventSupression {
    Write,
    Delete,
    Rename,
}

#[derive(Debug, Hash, Eq, PartialEq)]
pub(crate) enum Origin {
    Initiator,
    Peer(u64),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum FileSyncEvent {
    PrepareSync(FileInfo, u64, Vec<(usize, u64)>, bool),
    SyncBlocks(u64, Vec<usize>),
    WriteChunk(u64, usize, Vec<u8>),
    EndSync(u64),
}

impl std::fmt::Display for FileSyncEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FileSyncEvent::PrepareSync(file, file_hash, _, _) => {
                write!(f, "Prepare Sync for file ({}) {:?}", file_hash, file.path)
            }
            FileSyncEvent::SyncBlocks(file_hash, blocks) => write!(
                f,
                "SyncBlocks - {} - blocks out of sync {}",
                file_hash,
                blocks.len()
            ),
            FileSyncEvent::WriteChunk(file_hash, block, _) => {
                write!(f, "WriteBlock - {} - Block Index {}", file_hash, block)
            }
            FileSyncEvent::EndSync(file_hash) => write!(f, "EndSync - {}", file_hash),
        }
    }
}
