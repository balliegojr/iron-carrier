//! Handle synchronization

pub(crate) mod file_watcher_event_blocker;
mod file_watcher;
pub mod synchronizer;

use crate::fs::FileInfo;
use std::{sync::Arc, path::PathBuf};
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
