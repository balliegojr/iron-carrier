pub mod file_watcher;
pub mod synchronizer;

use std::{collections::HashMap, sync::Arc, path::PathBuf};
use tokio::sync::Notify;

use self::file_watcher::FileAction;
pub use synchronizer::Synchronizer;

type PeerAddress = String;

pub(crate) enum SyncEvent {
    EnqueueSyncToPeer(PeerAddress),

    PeerRequestedSync(PeerAddress, Arc<Notify>),
    SyncFromPeerFinished(PeerAddress, HashMap<String, u64>),

    BroadcastToAllPeers(FileAction),
    CompletedFileAction(PathBuf, String)
}


