//! Functions related to build the block index of a file

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::hash_helper;

const MIN_BLOCK_SIZE: u64 = 1024 * 128;
const MAX_BLOCK_SIZE: u64 = 1024 * 1024 * 16;

/// Calculates the block size for a file with given `file_size`
///
/// The minimum block size is `128 KB`
/// The maximum block size is `16 MB`
///
/// if `file_size` is smaller than then minimum block size, then the file size will be used instead
///
/// The block size is calculated on a way that there are no more than
/// **2000** blocks per file.
pub fn get_block_size(file_size: u64) -> u64 {
    let mut block_size = MIN_BLOCK_SIZE;
    while block_size < MAX_BLOCK_SIZE && file_size / block_size > 2000 {
        block_size <<= 1;
    }

    block_size
}

/// read contents of the `file` in chunks of `block_size` and calculate a hash to build the file
/// index.  
///
/// `target_file_size` is used to calculate how many blocks the index will have
/// `local_file_size` is used to read the file to the end
///
/// In case there is a file truncation, target_file_size will be bigger than local_file_size
pub async fn get_file_block_index<T: AsyncRead + Unpin>(
    file: &mut T,
    block_size: u64,
    target_file_size: u64,
    local_file_size: u64,
) -> crate::Result<FullIndex> {
    if target_file_size == 0 {
        return Ok(Default::default());
    }

    let total_blocks = (target_file_size / block_size) + 1;
    let mut block_index = Vec::with_capacity(total_blocks as usize);

    let mut buf = vec![0u8; block_size as usize];
    let mut position = 0;

    while position < local_file_size {
        let to_read = block_size.min(local_file_size - position) as usize;
        let actually_read = file.read_exact(&mut buf[..to_read]).await?;
        block_index.push(hash_helper::calculate_checksum(&buf[..actually_read]).into());
        position += actually_read as u64;
    }

    Ok(FullIndex { inner: block_index })
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct FullIndex {
    inner: Vec<BlockHash>,
}

impl FullIndex {
    pub fn to_partial(&self) -> BTreeSet<BlockIndexPosition> {
        (0..self.inner.len() as u64).map(Into::into).collect()
    }

    pub fn generate_diff(self, other: FullIndex) -> BTreeSet<BlockIndexPosition> {
        assert_eq!(
            self.inner.len(),
            other.inner.len(),
            "both full indexes must have the same length"
        );

        self.inner
            .into_iter()
            .zip(other.inner)
            .enumerate()
            .filter_map(|(i, (remote, local))| {
                if remote != local {
                    Some(BlockIndexPosition(i as u64))
                } else {
                    None
                }
            })
            .collect()
    }
}

#[derive(
    Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Default, Serialize, Deserialize,
)]
pub struct BlockIndexPosition(u64);

impl BlockIndexPosition {
    pub fn get_position(&self, block_size: u64) -> u64 {
        self.0 * block_size
    }
}

impl std::fmt::Display for BlockIndexPosition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl From<u64> for BlockIndexPosition {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<BlockIndexPosition> for u64 {
    fn from(value: BlockIndexPosition) -> Self {
        value.0
    }
}

#[derive(
    Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Default, Serialize, Deserialize,
)]
pub struct BlockHash(u64);

impl std::fmt::Display for BlockHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl From<u64> for BlockHash {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<BlockHash> for u64 {
    fn from(value: BlockHash) -> Self {
        value.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_get_block_size() {
        assert_eq!(get_block_size(10), MIN_BLOCK_SIZE);
        assert_eq!(get_block_size(MIN_BLOCK_SIZE), MIN_BLOCK_SIZE);
        assert_eq!(get_block_size(MIN_BLOCK_SIZE * 2001), MIN_BLOCK_SIZE * 2);
        assert_eq!(get_block_size(MAX_BLOCK_SIZE * 2001), MAX_BLOCK_SIZE);
    }
}
