use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::protocol::Protocol;

use super::block_index::{BlockIndexPosition, FullIndex};

#[derive(Debug, Serialize, Deserialize, Protocol)]
pub struct QueryTransferType;

#[derive(Debug, Serialize, Deserialize, Copy, Clone, PartialEq, Eq, Protocol)]
pub enum TransferType {
    FullFile,
    Partial,
    NoTransfer,
}

#[derive(Debug, Serialize, Deserialize, Protocol)]
pub struct QueryRequiredBlocks {
    pub sender_block_index: FullIndex,
}

#[derive(Debug, Serialize, Deserialize, Protocol)]
pub struct RequiredBlocks {
    pub required_blocks: BTreeSet<BlockIndexPosition>,
}

#[derive(Debug, Serialize, Deserialize, Protocol)]
pub struct TransferBlock<'a> {
    pub block_index: BlockIndexPosition,
    pub block: &'a [u8],
}

#[derive(Debug, Serialize, Deserialize, Protocol)]
pub struct TransferComplete;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Protocol)]
pub enum TransferResult {
    Success,
    Failed {
        required_blocks: BTreeSet<BlockIndexPosition>,
    },
}
