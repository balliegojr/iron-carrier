//! Handle synchronization

mod file_watcher;
mod synchronization_session;
pub mod synchronizer;

use crate::fs::FileInfo;
use serde::{Deserialize, Serialize};

pub use synchronizer::Synchronizer;

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub(crate) enum SyncType {
    Full,
    Partial,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum QueueEventType {
    Signal,
    Peer(u64),
    Broadcast,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum CarrierEvent {
    StartSync(SyncType),
    EndSync,
    CloseConnections,
    SetPeerId(u64),
    StartSyncReply(bool),
    SyncNextStorage,

    BuildStorageIndex(String),
    SetStorageIndex(Option<Vec<FileInfo>>),
    ConsumeSyncQueue,

    DeleteFile(FileInfo),
    SendFile(FileInfo, u64),
    BroadcastFile(FileInfo),
    RequestFile(FileInfo),
    PrepareFileTransfer(FileInfo, u64),
    WriteFileChunk(u64, Vec<u8>),
    EndFileTransfer(FileInfo),
    MoveFile(FileInfo, FileInfo),

    FileWatcherEvent(WatcherEvent),
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum WatcherEvent {
    Created(FileInfo),
    Updated(FileInfo),
    Moved(FileInfo, FileInfo),
    Deleted(FileInfo),
}

#[derive(Debug, Hash, Eq, PartialEq)]
pub(crate) enum Origin {
    Initiator,
    Peer(u64),
}
