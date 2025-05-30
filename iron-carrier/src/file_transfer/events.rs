use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{message_types::MessageType, storage::storage_tree::FileId};

use super::{
    SyncFile,
    block_index::{BlockIndexPosition, FullIndex},
};

#[derive(Debug, Serialize, Deserialize, Clone, MessageType)]
pub struct TransferFilesStart;

#[derive(Debug, Serialize, Deserialize, Clone, MessageType)]
pub struct TransferFilesCompleted;

#[derive(Debug, Serialize, Deserialize, MessageType)]
pub struct QueryTransferType {
    pub file: SyncFile,
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone, PartialEq, Eq, MessageType)]
pub enum TransferType {
    FullFile,
    Partial,
    NoTransfer,
}

#[derive(Debug, Serialize, Deserialize, MessageType)]
pub struct QueryRequiredBlocks {
    pub file_id: FileId,
    pub sender_block_index: FullIndex,
}

#[derive(Debug, Serialize, Deserialize, MessageType)]
pub struct RequiredBlocks {
    pub required_blocks: BTreeSet<BlockIndexPosition>,
}

#[derive(Debug, Serialize, Deserialize, MessageType)]
pub struct TransferBlock<'a> {
    pub file_id: FileId,
    pub block_index: BlockIndexPosition,
    pub block: &'a [u8],
}

#[derive(Debug, Serialize, Deserialize, MessageType)]
pub struct TransferComplete {
    pub file_id: FileId,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, MessageType)]
pub enum TransferResult {
    Success,
    Failed {
        required_blocks: BTreeSet<BlockIndexPosition>,
    },
}
