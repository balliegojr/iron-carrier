use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::{file_transfer::FileTransfer, states::consensus::ElectionEvents, storage};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum NetworkEvents {
    Disconnected,
    ConsensusElection(ElectionEvents),
    RequestTransition(Transition),
    Synchronization(Synchronization),
    FileTransfer(u64, FileTransfer),
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
        nodes: HashSet<u64>,
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
