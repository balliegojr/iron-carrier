//! Hash related functions

use crc::{CRC_64_GO_ISO, Crc, Digest};
use rand::Rng;

use crate::storage::storage_tree::{DirId, FileId};

pub const HASHER: Crc<u64> = Crc::<u64>::new(&CRC_64_GO_ISO);

pub fn hashed_str<T: AsRef<str>>(value: T) -> u64 {
    calculate_checksum(value.as_ref().as_bytes())
}

/// Calculate checksum of `t`
pub fn calculate_checksum(t: &[u8]) -> u64 {
    HASHER.checksum(t)
}

/// Calculate the hash of `file` by using the file attributes only, file content is NOT considered
pub fn calculate_file_hash_digest(
    digest: &mut Digest<u64>,
    parent_id: DirId,
    file_id: FileId,
    size: u64,
    timestamp: u64,
) {
    digest.update(&parent_id.to_le_bytes());
    digest.update(&file_id.to_le_bytes());
    digest.update(&timestamp.to_le_bytes());
    digest.update(&size.to_le_bytes());
}

/// generate an id for this node.
///
/// if `machine_uid::get` is successful, a checksum of uid + port are used to generate the node_id
/// else, a random number + port is used instead
pub fn get_node_id(peer_port: u16) -> u64 {
    // The port must be used to generate the id, this way it is possible to run multiple instances in the same machine
    match machine_uid::get() {
        Ok(machine_id) => calculate_checksum(format!("{machine_id}:::{peer_port}").as_bytes()),
        Err(_) => {
            let mut rng = rand::rng();
            rng.random::<u64>() + peer_port as u64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn calc_hash() {
        assert_eq!(
            calculate_checksum("dope info".as_bytes()),
            1411611894453817004
        );
    }
}
