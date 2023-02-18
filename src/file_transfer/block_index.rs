//! Functions related to build the block index of a file

use tokio::{fs::File, io::AsyncReadExt};

use crate::hash_helper;

const MIN_BLOCK_SIZE: u64 = 1024 * 128;
const MAX_BLOCK_SIZE: u64 = 1024 * 1024 * 16;

pub fn get_block_size(file_size: u64) -> u64 {
    let mut block_size = MIN_BLOCK_SIZE;
    while block_size < MAX_BLOCK_SIZE && file_size / block_size > 2000 {
        block_size *= 2;
    }

    block_size
}

pub async fn get_file_block_index(
    file: &mut File,
    block_size: u64,
    file_size: u64,
) -> crate::Result<Vec<u64>> {
    if file_size == 0 {
        return Ok(Vec::new());
    }

    let total_blocks = (file_size / block_size) + 1;
    let mut block_index = Vec::with_capacity(total_blocks as usize);

    let mut buf = vec![0u8; block_size as usize];
    let mut position = 0u64;

    while position < file_size {
        let to_read = std::cmp::min(file_size - position, block_size) as usize;
        let actually_read = file.read_exact(&mut buf[..to_read]).await?;
        block_index.push(hash_helper::calculate_checksum(&buf[..actually_read]));
        position += actually_read as u64;
    }

    Ok(block_index)
}
