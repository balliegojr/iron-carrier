//! Handle synchronization

mod file_watcher;
pub(crate) mod file_watcher_event_blocker;
pub mod synchronizer;

use crate::fs::FileInfo;
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, path::PathBuf, sync::Arc};
use tokio::sync::Notify;

pub use synchronizer::Synchronizer;

pub type BlockingEvent = (PathBuf, SocketAddr);

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum CarrierEvent {
    StartFullSync,
    StartSync,
    SyncRequestAccepted,
    SyncRequestRejected,
    SyncNextStorage,

    BuildStorageIndex(String),
    SetStorageIndex(Vec<FileInfo>),
    ConsumeSyncQueue,

    DeleteFile(FileInfo),
    SendFile(FileInfo),
    RequestFile(FileInfo),
    PrepareFileTransfer(FileInfo, u64),
    WriteFileChunk(FileInfo, u64, Vec<u8>),
    EndFileTransfer(FileInfo),
}

/// Synchronization Event Types
#[derive(Debug)]
pub enum SyncEvent {
    /// Add peer to synchronization list
    EnqueueSyncToPeer(SocketAddr, bool),

    /// Peer signaled to start synchronization
    PeerRequestedSync(SocketAddr, Arc<Notify>, Arc<Notify>),

    /// Broadcast event to all configurated peers
    BroadcastToAllPeers(FileAction, Vec<SocketAddr>),
}

#[derive(Debug)]
pub enum FileAction {
    Create(FileInfo),
    Update(FileInfo),
    Move(FileInfo, FileInfo),
    Remove(FileInfo),
    Request(FileInfo),
}
