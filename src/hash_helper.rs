use rand::Rng;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

pub fn get_node_id(peer_port: u16) -> u64 {
    match machine_uid::get() {
        Ok(machine_id) => calculate_hash(&format!("{}:::{}", machine_id, peer_port)),
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
        assert_eq!(calculate_hash(&"dope info"), 3362353728198126061);
    }
}
