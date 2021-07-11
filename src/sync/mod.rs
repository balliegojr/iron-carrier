//! Handle synchronization

mod connected_peers;
mod file_transfer_man;
mod file_watcher;
mod synchronization_session;
pub mod synchronizer;

use crate::fs::FileInfo;
use message_io::{
    network::{Endpoint, Transport},
    node::NodeHandler,
};
use serde::{Deserialize, Serialize};

pub use synchronizer::Synchronizer;

const TRANSPORT_PROTOCOL: Transport = Transport::FramedTcp;

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
    BroadcastAndWait,
    BroadcastExcept(u64),
}

impl std::fmt::Display for QueueEventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueueEventType::Signal => write!(f, "Signal"),
            QueueEventType::Peer(peer) => write!(f, "To Peer {}", peer),
            QueueEventType::Broadcast => write!(f, "Broadcast"),
            QueueEventType::BroadcastAndWait => write!(f, "Broadcast and wait"),
            QueueEventType::BroadcastExcept(peer_id) => {
                write!(f, "Broadcast to all peers except {}", peer_id)
            }
        }
    }
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
            CarrierEvent::StartSync(sync_type) => write!(f, "Start Sync ({:?})", sync_type),
            CarrierEvent::EndSync => write!(f, "End Sync"),
            CarrierEvent::CloseConnections => write!(f, "Close all connections"),
            CarrierEvent::SetPeerId(peer_id) => write!(f, "Set peer id to {}", peer_id),
            CarrierEvent::StartSyncReply(reply) => write!(f, "Start Sync Replied With {}", reply),
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
        }
    }
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

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum FileSyncEvent {
    PrepareSync(FileInfo, u64, Vec<(usize, u64)>),
    SyncBlocks(u64, Vec<usize>),
    WriteChunk(u64, usize, Vec<u8>),
    EndSync(u64),
}

impl std::fmt::Display for FileSyncEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FileSyncEvent::PrepareSync(file, file_hash, _) => {
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
const COMMAND_MESSAGE: u8 = 1;
const STREAM_MESSAGE: u8 = 2;

/// send a [message](`CarrierEvent`) to [endpoint](`message_io::network::Endpoint`) with message prefix 1
fn send_message(handler: &NodeHandler<CarrierEvent>, message: &CarrierEvent, endpoint: Endpoint) {
    let mut data = vec![COMMAND_MESSAGE];
    data.extend(bincode::serialize(message).unwrap());
    handler.network().send(endpoint, &data);
}

/// broadcast a [message](`CarrierEvent`) to given [endpoints](`message_io::network::Endpoint`) with message prefix 1
fn broadcast_message_to<'a, T: Iterator<Item = &'a Endpoint>>(
    handler: &NodeHandler<CarrierEvent>,
    message: CarrierEvent,
    endpoints: T,
) {
    let mut data = vec![COMMAND_MESSAGE];
    data.extend(bincode::serialize(&message).unwrap());
    for endpoint in endpoints {
        handler.network().send(*endpoint, &data);
    }
}
