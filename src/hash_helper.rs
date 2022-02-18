use std::os::unix::prelude::OsStrExt;

use crc::{Crc, Digest, CRC_64_GO_ISO};
use rand::Rng;

use crate::fs::FileInfo;

pub const HASHER: Crc<u64> = Crc::<u64>::new(&CRC_64_GO_ISO);

pub fn calculate_hash(t: &[u8]) -> u64 {
    HASHER.checksum(t)
}

pub fn calculate_file_hash(file: &FileInfo) -> u64 {
    let mut digest = HASHER.digest();
    calculate_file_hash_dig(file, &mut digest);
    digest.finalize()
}
pub fn calculate_file_hash_dig(file: &FileInfo, digest: &mut Digest<u64>) {
    digest.update(file.storage.as_bytes());
    digest.update(file.path.as_os_str().as_bytes());
    if file.is_deleted() {
        digest.update(&file.deleted_at.unwrap().to_le_bytes());
    } else {
        digest.update(&file.modified_at.unwrap().to_le_bytes());
        digest.update(&file.size.unwrap().to_le_bytes());
    }
}

pub fn get_node_id(peer_port: u16) -> u64 {
    match machine_uid::get() {
        Ok(machine_id) => calculate_hash(format!("{}:::{}", machine_id, peer_port).as_bytes()),
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
        assert_eq!(calculate_hash("dope info".as_bytes()), 1411611894453817004);
    }
}
