use serde::{Deserialize, Serialize};

use crate::storage::FileInfo;

/// Contains the information for the transfer between two nodes
#[derive(Debug)]
pub struct Transfer {
    pub file: FileInfo,
    pub block_size: u64,
    pub transfer_id: TransferId,
}

impl Transfer {
    pub fn new(file: FileInfo) -> crate::Result<Self> {
        let block_size = super::block_index::get_block_size(file.file_size()?);
        let transfer_id = super::hash_helper::calculate_file_hash(&file).into();

        Ok(Self {
            file,
            block_size,
            transfer_id,
        })
    }
}

#[derive(
    Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Default, Serialize, Deserialize,
)]
pub struct TransferId(u64);

impl std::fmt::Display for TransferId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl From<u64> for TransferId {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<TransferId> for u64 {
    fn from(value: TransferId) -> Self {
        value.0
    }
}
