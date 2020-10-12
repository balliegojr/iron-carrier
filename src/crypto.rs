use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::FileInfo;

    #[test]
    fn calc_hash() {

        let file = FileInfo { 
            path: std::path::PathBuf::from("./some/path"),
            size: 100,
            created_at: std::time::SystemTime::UNIX_EPOCH,
            modified_at: std::time::SystemTime::UNIX_EPOCH
        };

        let files = vec![file];
        assert_eq!(calculate_hash(&files), 8269395793225122601);
    }
}