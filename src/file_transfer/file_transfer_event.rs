use std::{collections::BTreeSet, sync::Arc};

use serde::{Deserialize, Serialize};

use crate::storage::FileInfo;

use super::block_index::{BlockIndexPosition, FullIndex};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum FileTransferEvent {
    QueryTransferType {
        file: FileInfo,
    },
    ReplyTransferType {
        transfer_type: TransferType,
    },

    QueryRequiredBlocks {
        sender_block_index: FullIndex,
    },
    ReplyRequiredBlocks {
        required_blocks: BTreeSet<BlockIndexPosition>,
    },

    TransferBlock {
        block_index: BlockIndexPosition,
        block: Arc<Vec<u8>>,
    },
    TransferComplete,
    TransferSucceeded,
    TransferFailed {
        required_blocks: BTreeSet<BlockIndexPosition>,
    },
    SendFileTo {
        file: FileInfo,
        nodes: Vec<u64>,
    },

    RemovePeer,
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone, PartialEq, Eq)]
pub enum TransferType {
    FullFile,
    Partial,
    NoTransfer,
}
