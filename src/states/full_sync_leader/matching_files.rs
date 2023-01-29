use std::collections::{BTreeMap, HashMap};

use crate::storage::{FileInfo, Storage};

pub struct MatchedFilesIter {
    consolidated: BTreeMap<std::path::PathBuf, HashMap<u64, FileInfo>>,
}

impl Iterator for MatchedFilesIter {
    type Item = HashMap<u64, FileInfo>;

    fn next(&mut self) -> Option<Self::Item> {
        self.consolidated.pop_first().map(|(_, v)| v)
    }
}

pub fn match_files(
    storage: Storage,
    peer_files: HashMap<u64, Vec<FileInfo>>,
    local_node_id: u64,
) -> MatchedFilesIter {
    let mut consolidated: BTreeMap<std::path::PathBuf, HashMap<u64, FileInfo>> = Default::default();
    for file in storage.files.into_iter() {
        consolidated
            .entry(file.path.clone())
            .or_default()
            .insert(local_node_id, file);
    }

    for (peer_id, files) in peer_files.into_iter() {
        for file in files {
            consolidated
                .entry(file.path.clone())
                .or_default()
                .insert(peer_id, file);
        }
    }

    MatchedFilesIter { consolidated }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn test_matching_files() {
        fn file_with_name(path: &str) -> FileInfo {
            FileInfo {
                storage: "".to_string(),
                path: path.into(),
                size: None,
                deleted_at: None,
                modified_at: None,
                permissions: 0,
            }
        }
        let storage = Storage {
            hash: 0,
            files: vec![file_with_name("a")],
            ignored_files: crate::ignored_files::empty_ignored_files(),
        };

        let mut peer_files = HashMap::default();
        peer_files.insert(1, vec![file_with_name("a"), file_with_name("b")]);
        peer_files.insert(2, vec![file_with_name("b")]);

        let mut matched = super::match_files(storage, peer_files, 0);

        let m = matched.next().unwrap();
        assert!(m.contains_key(&0));
        assert!(m.contains_key(&1));
        assert_eq!(m[&0].path, PathBuf::from("a"));

        let m = matched.next().unwrap();
        assert!(m.contains_key(&1));
        assert!(m.contains_key(&2));
        assert_eq!(m[&1].path, PathBuf::from("b"));

        assert!(matched.next().is_none());
    }
}
