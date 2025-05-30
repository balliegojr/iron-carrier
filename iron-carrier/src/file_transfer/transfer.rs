use tokio::sync::OwnedSemaphorePermit;

use super::SyncFile;

/// Contains the information for the transfer between two nodes
#[derive(Debug)]
pub struct Transfer {
    pub file: SyncFile,
    pub block_size: u64,
    _permit: OwnedSemaphorePermit,
}

impl Transfer {
    pub fn new(file: SyncFile, permit: OwnedSemaphorePermit) -> anyhow::Result<Self> {
        let block_size = super::block_index::get_block_size(file.info.size());

        Ok(Self {
            file,
            block_size,
            _permit: permit,
        })
    }
}
