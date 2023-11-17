use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{message_types::MessageType, storage::FileInfo};

use super::{
    block_index::{BlockIndexPosition, FullIndex},
    TransferId,
};

#[derive(Debug, Serialize, Deserialize, Clone, MessageType)]
pub struct TransferFilesStart;

#[derive(Debug, Serialize, Deserialize, Clone, MessageType)]
pub struct TransferFilesCompleted;

#[derive(Debug, Serialize, Deserialize, MessageType)]
pub struct QueryTransferType {
    pub file: FileInfo,
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone, PartialEq, Eq, MessageType)]
pub enum TransferType {
    FullFile,
    Partial,
    NoTransfer,
}

#[derive(Debug, Serialize, Deserialize, MessageType)]
pub struct QueryRequiredBlocks {
    pub transfer_id: TransferId,
    pub sender_block_index: FullIndex,
}

#[derive(Debug, Serialize, Deserialize, MessageType)]
pub struct RequiredBlocks {
    pub required_blocks: BTreeSet<BlockIndexPosition>,
}

#[derive(Debug, Serialize, Deserialize, MessageType)]
pub struct TransferBlock<'a> {
    pub transfer_id: TransferId,
    pub block_index: BlockIndexPosition,
    pub block: &'a [u8],
}

#[derive(Debug, Serialize, Deserialize, MessageType)]
pub struct TransferComplete {
    pub transfer_id: TransferId,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, MessageType)]
pub enum TransferResult {
    Success,
    Failed {
        required_blocks: BTreeSet<BlockIndexPosition>,
    },
}

// #[derive(Debug, Serialize, Deserialize, Clone)]
// pub enum FileTransferEvent {
//     QueryTransferType {
//         file: FileInfo,
//     },
//     ReplyTransferType {
//         transfer_type: TransferType,
//     },
//
//     QueryRequiredBlocks {
//         sender_block_index: FullIndex,
//     },
//     ReplyRequiredBlocks {
//         required_blocks: BTreeSet<BlockIndexPosition>,
//     },
//
//     TransferBlock {
//         block_index: BlockIndexPosition,
//         block: Arc<Vec<u8>>,
//     },
//     TransferComplete,
//     TransferSucceeded,
//     TransferFailed {
//         required_blocks: BTreeSet<BlockIndexPosition>,
//     },
//     SendFileTo {
//         file: FileInfo,
//         nodes: Vec<u64>,
//     },
//
//     RemovePeer,
// }
