use serde::{Deserialize, Serialize};
use tokio::sync::OwnedSemaphorePermit;

use crate::{hash_helper, storage::FileInfo};

/// Contains the information for the transfer between two nodes
#[derive(Debug)]
pub struct Transfer {
    pub file: FileInfo,
    pub block_size: u64,
    pub transfer_id: TransferId,
    _permit: OwnedSemaphorePermit,
}

impl Transfer {
    pub fn new(file: FileInfo, permit: OwnedSemaphorePermit) -> crate::Result<Self> {
        let transfer_id = Self::transfer_id(&file);
        Self::new_with_transfer_id(file, permit, transfer_id)
    }

    pub fn new_with_transfer_id(
        file: FileInfo,
        permit: OwnedSemaphorePermit,
        transfer_id: TransferId,
    ) -> crate::Result<Self> {
        let block_size = super::block_index::get_block_size(file.file_size()?);

        Ok(Self {
            file,
            block_size,
            transfer_id,
            _permit: permit,
        })
    }

    pub fn transfer_id(file: &FileInfo) -> TransferId {
        hash_helper::calculate_file_hash(file).into()
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
