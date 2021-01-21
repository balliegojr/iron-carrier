//! Handle synchronization

mod file_watcher;
pub(crate) mod file_watcher_event_blocker;
pub mod synchronizer;

use crate::fs::FileInfo;
use std::{path::PathBuf, sync::Arc};
use tokio::sync::Notify;

pub use synchronizer::Synchronizer;

type PeerAddress = String;
pub type BlockingEvent = (PathBuf, String);

/// Synchronization Event Types
#[derive(Debug)]
pub enum SyncEvent {
    /// Add peer to synchronization list
    EnqueueSyncToPeer(PeerAddress, bool),

    /// Peer signaled to start synchronization
    PeerRequestedSync(PeerAddress, Arc<Notify>, Arc<Notify>),

    /// Broadcast event to all configurated peers
    BroadcastToAllPeers(FileAction, Vec<String>),
}

#[derive(Debug)]
pub enum FileAction {
    Create(FileInfo),
    Update(FileInfo),
    Move(FileInfo, FileInfo),
    Remove(FileInfo),
    Request(FileInfo),
}
