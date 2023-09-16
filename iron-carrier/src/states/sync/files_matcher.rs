use std::collections::{BTreeMap, HashMap, HashSet};

use crate::{
    config::PathConfig,
    node_id::NodeId,
    relative_path::RelativePathBuf,
    state_machine::{State, StateMachineError},
    storage::{self, FileInfo, Storage},
    SharedState,
};

use super::events::{QueryStorageIndex, StorageIndex, StorageIndexStatus};

#[derive(Debug)]
pub struct FilesMatcher {
    storage_name: &'static str,
    storage_config: &'static PathConfig,
}
impl FilesMatcher {
    pub fn new(storage_name: &'static str, storage_config: &'static PathConfig) -> Self {
        Self {
            storage_name,
            storage_config,
        }
    }

    async fn get_storage_from_nodes(
        &self,
        shared_state: &SharedState,
        storage: &Storage,
    ) -> crate::Result<HashMap<NodeId, Vec<FileInfo>>> {
        let peer_storages = shared_state
            .rpc
            .broadcast(QueryStorageIndex {
                name: self.storage_name.to_string(),
                hash: storage.hash,
            })
            .result()
            .await?;

        Ok(peer_storages
            .iter()
            .filter_map(|reply| {
                let node = reply.node_id();
                let node_storage = reply.data::<StorageIndex>().ok()?;

                if node_storage.name != self.storage_name {
                    return None;
                }

                match node_storage.storage_index {
                    StorageIndexStatus::StorageMissing => {
                        // peer doesn't have the storage...
                        None
                    }
                    StorageIndexStatus::StorageInSync => Some((node, storage.files.clone())),
                    StorageIndexStatus::SyncNecessary(files) => Some((node, files)),
                }
            })
            .collect())
    }
}

impl State for FilesMatcher {
    type Output = (Vec<NodeId>, MatchedFilesIter);

    async fn execute(self, shared_state: &SharedState) -> crate::Result<Self::Output> {
        let storage = storage::get_storage_info(
            self.storage_name,
            self.storage_config,
            &shared_state.transaction_log,
        )
        .await?;

        let peers_storages = self.get_storage_from_nodes(shared_state, &storage).await?;
        if peers_storages.is_empty() {
            log::trace!("Storage already in sync with all peers");
            Err(StateMachineError::Abort)?
        }

        let peers: Vec<NodeId> = peers_storages.keys().copied().collect();
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
    consolidated: BTreeMap<RelativePathBuf, HashMap<NodeId, FileInfo>>,
}

impl std::fmt::Debug for MatchedFilesIter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MatchedFilesIter").finish()
    }
}

impl Iterator for MatchedFilesIter {
    type Item = HashMap<NodeId, FileInfo>;

    fn next(&mut self) -> Option<Self::Item> {
        self.consolidated.pop_first().map(|(_, v)| v)
    }
}

pub fn match_files(
    storage: Storage,
    peer_files: HashMap<NodeId, Vec<FileInfo>>,
    local_node_id: NodeId,
) -> MatchedFilesIter {
    let mut consolidated: BTreeMap<RelativePathBuf, HashMap<NodeId, FileInfo>> = Default::default();
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

    clear_moved_files_old_path(&mut consolidated);

    MatchedFilesIter { consolidated }
}

/// This function remove all entries for the 'old_path' for the moved files.  
/// We need to do this in order to prevent the file to be created back
fn clear_moved_files_old_path(
    consolidated: &mut BTreeMap<RelativePathBuf, HashMap<NodeId, FileInfo>>,
) {
    let to_remove: HashSet<RelativePathBuf> = consolidated
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

    consolidated
        .extract_if(|k, _| to_remove.contains(k))
        .count();
}

#[cfg(test)]
mod tests {
    use crate::relative_path::RelativePathBuf;

    use super::*;

    #[test]
    fn test_matching_files() {
        fn file_with_name(path: &str) -> FileInfo {
            FileInfo {
                storage: "".to_string(),
                path: path.into(),
                info_type: crate::storage::FileInfoType::Existent {
                    modified_at: 0,
                    created_at: 0,
                    size: 0,
                },
                permissions: 0,
            }
        }
        let storage = Storage {
            hash: 0,
            files: vec![file_with_name("a")],
        };

        let mut peer_files = HashMap::default();
        peer_files.insert(1.into(), vec![file_with_name("a"), file_with_name("b")]);
        peer_files.insert(2.into(), vec![file_with_name("b")]);

        let mut matched = super::match_files(storage, peer_files, 0.into());

        let m = matched.next().unwrap();
        assert!(m.contains_key(&0.into()));
        assert!(m.contains_key(&1.into()));
        assert_eq!(m[&0.into()].path, RelativePathBuf::from("a"));

        let m = matched.next().unwrap();
        assert!(m.contains_key(&1.into()));
        assert!(m.contains_key(&2.into()));
        assert_eq!(m[&1.into()].path, RelativePathBuf::from("b"));

        assert!(matched.next().is_none());
    }
}