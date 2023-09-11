use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::{
    file_transfer::{FileTransferEvent, TransferId},
    node_id::NodeId,
    states::consensus::ElectionEvents,
    storage,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum NetworkEvents {
    Disconnected,
    ConsensusElection(ElectionEvents),
    RequestTransition(Transition),
    Synchronization(Synchronization),
    FileTransfer(TransferId, FileTransferEvent),
}

impl From<Transition> for NetworkEvents {
    fn from(value: Transition) -> Self {
        NetworkEvents::RequestTransition(value)
    }
}

impl From<Synchronization> for NetworkEvents {
    fn from(value: Synchronization) -> Self {
        NetworkEvents::Synchronization(value)
    }
}

impl From<ElectionEvents> for NetworkEvents {
    fn from(value: ElectionEvents) -> Self {
        NetworkEvents::ConsensusElection(value)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Transition {
    Consensus,
    FullSync,
    Done,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Synchronization {
    QueryStorageIndex {
        name: String,
        hash: u64,
    },
    ReplyStorageIndex {
        name: String,
        storage_index: StorageIndexStatus,
    },
    DeleteFile {
        file: storage::FileInfo,
    },
    MoveFile {
        file: storage::FileInfo,
    },
    SendFileTo {
        file: storage::FileInfo,
        nodes: HashSet<NodeId>,
    },
    StartTransferingFiles,
    DoneTransferingFiles,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum StorageIndexStatus {
    /// Queried storage does not exist in the node, no sync will be done
    StorageMissing,
    /// Storage is in sync with the leader, further sync may be necessary
    StorageInSync,
    /// Storage is not in sync with leader, sync is necessary
    SyncNecessary(Vec<storage::FileInfo>),
}
