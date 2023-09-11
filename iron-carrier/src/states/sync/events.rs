use std::collections::HashSet;

use iron_carrier_macros::HashTypeId;
use serde::{Deserialize, Serialize};

use crate::{node_id::NodeId, storage::FileInfo};

#[derive(Debug, Serialize, Deserialize, Clone, HashTypeId)]
pub struct QueryStorageIndex {
    pub name: String,
    pub hash: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone, HashTypeId)]
pub struct StorageIndex {
    pub name: String,
    pub storage_index: StorageIndexStatus,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum StorageIndexStatus {
    /// Queried storage does not exist in the node, no sync will be done
    StorageMissing,
    /// Storage is in sync with the leader, further sync may be necessary
    StorageInSync,
    /// Storage is not in sync with leader, sync is necessary
    SyncNecessary(Vec<FileInfo>),
}

#[derive(Debug, Serialize, Deserialize, Clone, HashTypeId)]
pub struct DeleteFile {
    pub file: FileInfo,
}

#[derive(Debug, Serialize, Deserialize, Clone, HashTypeId)]
pub struct MoveFile {
    pub file: FileInfo,
}

#[derive(Debug, Serialize, Deserialize, Clone, HashTypeId)]
pub struct SendFileTo {
    pub file: FileInfo,
    pub nodes: HashSet<NodeId>,
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone, HashTypeId)]
pub struct SyncCompleted;
