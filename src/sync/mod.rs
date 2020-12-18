//! Handle synchronization

mod file_watcher;
pub(crate) mod file_events_buffer;
pub mod synchronizer;

use std::{sync::Arc};
use tokio::sync::{ Notify }; 
use crate::fs::FileInfo;

pub use synchronizer::Synchronizer;

type PeerAddress = String;

/// Synchronization Event Types
pub(crate) enum SyncEvent {
    /// Add peer to synchronization list
    EnqueueSyncToPeer(PeerAddress, bool),

    /// Peer signaled to start synchronization
    PeerRequestedSync(PeerAddress, Arc<Notify>, Arc<Notify>),

    /// Broadcast event to all configurated peers
    BroadcastToAllPeers(FileAction, Vec<String>),
}


#[derive(Debug)]
pub(crate) enum FileAction {
    Create(FileInfo),
    Update(FileInfo),
    Move(FileInfo, FileInfo),
    Remove(FileInfo),
    Request(FileInfo)
}
