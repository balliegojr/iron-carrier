//! Hash related functions

use crc::{Crc, Digest, CRC_64_GO_ISO};
use rand::Rng;

use crate::storage::{FileInfo, FileInfoType};

pub const HASHER: Crc<u64> = Crc::<u64>::new(&CRC_64_GO_ISO);

pub fn hashed_str<T: AsRef<str>>(value: T) -> u64 {
    calculate_checksum(value.as_ref().as_bytes())
}

/// Calculate checksum of `t`
pub fn calculate_checksum(t: &[u8]) -> u64 {
    HASHER.checksum(t)
}

/// Calculate the hash of `file` by using the file attributes only, file content is NOT considered
pub fn calculate_file_hash(file: &FileInfo) -> u64 {
    let mut digest = HASHER.digest();
    calculate_file_hash_digest(file, &mut digest);
    digest.finalize()
}

/// Calculate the hash of `file` by using the file attributes only, file content is NOT considered
pub fn calculate_file_hash_digest(file: &FileInfo, digest: &mut Digest<u64>) {
    digest.update(file.storage.as_bytes());
    file.path.to_str().inspect(|path| {
        digest.update(path.as_bytes());
    });

    // NOTE: add a different byte depending on the match value?
    match &file.info_type {
        FileInfoType::Existent { modified_at, size } => {
            digest.update(&modified_at.to_le_bytes());
            digest.update(&size.to_le_bytes());
        }
        FileInfoType::Deleted { deleted_at } => {
            digest.update(&deleted_at.to_le_bytes());
        }
        FileInfoType::Moved { old_path, moved_at } => {
            digest.update(&moved_at.to_le_bytes());
            old_path.to_str().inspect(|path| {
                digest.update(path.as_bytes());
            });
        }
    }
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
            let mut rng = rand::thread_rng();
            rng.gen::<u64>() + peer_port as u64
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
