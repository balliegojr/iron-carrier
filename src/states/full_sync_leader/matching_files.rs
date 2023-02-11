use std::collections::{BTreeMap, HashMap, HashSet};

use crate::{
    config::PathConfig,
    network_events::{NetworkEvents, Synchronization},
    state_machine::Step,
    storage::{self, FileInfo, Storage},
    SharedState,
};

#[derive(Debug)]
pub struct BuildMatchingFiles {
    storage_name: &'static str,
    storage_config: &'static PathConfig,
}

impl BuildMatchingFiles {
    pub fn new(storage_name: &'static str, storage_config: &'static PathConfig) -> Self {
        Self {
            storage_name,
            storage_config,
        }
    }

    async fn wait_storage_from_peers(
        &self,
        shared_state: &SharedState,
        storage: &Storage,
    ) -> crate::Result<HashMap<u64, Vec<FileInfo>>> {
        let mut expected = shared_state
            .connection_handler
            .broadcast(
                Synchronization::QueryStorageIndex {
                    name: self.storage_name.to_string(),
                    hash: storage.hash,
                }
                .into(),
            )
            .await?;

        log::debug!("Expecting reply from {expected} nodes");

        let mut peer_storages = HashMap::default();
        while let Some((peer, event)) = shared_state.connection_handler.next_event().await {
            match event {
                NetworkEvents::Synchronization(Synchronization::ReplyStorageIndex {
                    name,
                    files,
                }) => {
                    expected -= 1;
                    if let Some(files) = files && name == self.storage_name {
                    peer_storages.insert(peer, files);
                }

                    if expected == 0 {
                        break;
                    }
                }
                e => {
                    log::error!("Received unexpected event {e:?}");
                }
            }
        }

        Ok(peer_storages)
    }
}

impl Step for BuildMatchingFiles {
    type Output = (Vec<u64>, MatchedFilesIter);

    async fn execute(self, shared_state: &SharedState) -> crate::Result<Self::Output> {
        let storage = storage::get_storage(
            self.storage_name,
            self.storage_config,
            shared_state.transaction_log,
        )
        .await?;
        let peers_storages = self.wait_storage_from_peers(shared_state, &storage).await?;

        let peers: Vec<u64> = peers_storages.keys().copied().collect();
        log::trace!(
            "Storage {0} to be synchronized with {peers:?}",
            self.storage_name
        );

        Ok((
            peers,
            match_files(storage, peers_storages, shared_state.config.node_id_hashed),
        ))
    }
}

pub struct MatchedFilesIter {
    consolidated: BTreeMap<std::path::PathBuf, HashMap<u64, FileInfo>>,
}

impl std::fmt::Debug for MatchedFilesIter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MatchedFilesIter").finish()
    }
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

    clean_moved_files_old_path(&mut consolidated);

    MatchedFilesIter { consolidated }
}

/// This function remove all entries for the 'old_path' for the moved files.  
/// We need to do this in order to prevent the file to be created back
fn clean_moved_files_old_path(
    consolidated: &mut BTreeMap<std::path::PathBuf, HashMap<u64, FileInfo>>,
) {
    let to_remove: HashSet<std::path::PathBuf> = consolidated
        .values()
        .filter_map(|files| {
            files.values().max_by(|a, b| a.date_cmp(b)).and_then(|f| {
                if let crate::storage::FileInfoType::Moved { old_path, .. } = &f.info_type {
                    Some(old_path.clone())
                } else {
                    None
                }
            })
        })
        .collect();

    consolidated.drain_filter(|k, _| to_remove.contains(k));
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
                info_type: crate::storage::FileInfoType::Existent {
                    modified_at: 0,
                    size: 0,
                },
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
