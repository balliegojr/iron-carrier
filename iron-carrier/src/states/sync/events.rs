use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::{
    file_transfer::SyncFile, message_types::MessageType, node_id::NodeId,
    relative_path::RelativePathBuf, storage::Storage, transaction_log::SyncStatus,
};

#[derive(Debug, Serialize, Deserialize, Clone, MessageType)]
pub struct QueryStorageIndex {
    pub name: String,
    pub hash: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone, MessageType)]
pub struct StorageIndex {
    pub name: String,
    pub storage_index: StorageIndexStatus,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum StorageIndexStatus {
    /// Queried storage does not exist in the node, no sync will be done
    StorageMissing,
    /// Storage is in sync with the leader, further sync may be necessary
    StorageInSync,
    /// Storage is not in sync with leader, sync is necessary
    SyncNecessary(Storage),
}

#[derive(Debug, Serialize, Deserialize, Clone, MessageType)]
pub struct DeleteFile {
    pub storage: String,
    pub path: RelativePathBuf,
    pub timestamp: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone, MessageType)]
pub struct MoveFile {
    pub storage: String,
    pub src_path: RelativePathBuf,
    pub dst_path: RelativePathBuf,
    pub modified_at: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone, MessageType)]
pub struct SendFileTo {
    pub file: SyncFile,
    pub nodes: HashSet<NodeId>,
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone, MessageType)]
pub struct SyncCompleted;

#[derive(Debug, Serialize, Deserialize, Clone, MessageType)]
pub struct SaveSyncStatus<'a> {
    pub nodes: HashSet<NodeId>,
    pub storage_name: &'a str,
    pub status: SyncStatus,
}
