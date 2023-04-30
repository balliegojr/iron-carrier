//! Functions related to build the block index of a file

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
) -> crate::Result<Vec<u64>> {
    if target_file_size == 0 {
        return Ok(Vec::new());
    }

    let total_blocks = (target_file_size / block_size) + 1;
    let mut block_index = Vec::with_capacity(total_blocks as usize);

    let mut buf = vec![0u8; block_size as usize];
    let mut position = 0;

    while position < local_file_size {
        let to_read = block_size.min(local_file_size - position) as usize;
        let actually_read = file.read_exact(&mut buf[..to_read]).await?;
        block_index.push(hash_helper::calculate_checksum(&buf[..actually_read]));
        position += actually_read as u64;
    }

    Ok(block_index)
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
