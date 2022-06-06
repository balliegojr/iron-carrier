use std::{
    collections::{hash_map::Entry, HashMap},
    path::Path,
    sync::{Arc, Mutex},
};

use globset::{Glob, GlobSet, GlobSetBuilder};

use crate::config::Config;

pub struct StorageState {
    config: Arc<Config>,
    hash_state: Mutex<HashMap<String, StorageHashState>>,
    ignore_sets: Mutex<HashMap<String, Option<GlobSet>>>,
}

impl StorageState {
    pub fn new(config: Arc<Config>) -> Self {
        Self {
            hash_state: HashMap::new().into(),
            ignore_sets: HashMap::new().into(),
            config,
        }
    }
    pub fn clear(&self) {
        self.hash_state.lock().unwrap().clear();
        self.ignore_sets.lock().unwrap().clear();
    }

    pub fn is_ignored<P: AsRef<Path>>(&self, storage: &str, path: P) -> bool {
        let mut ignore_sets = self.ignore_sets.lock().unwrap();
        let glob_set = ignore_sets
            .entry(storage.to_string())
            .or_insert_with(|| get_glob_set(&self.config.paths[storage]));

        glob_set
            .as_ref()
            .map(|set| set.is_match(path))
            .unwrap_or_default()
    }

    pub fn invalidate_state(&self, storage: &str) {
        let mut hash_state = self.hash_state.lock().unwrap();
        if let Entry::Occupied(entry) = hash_state.entry(storage.to_string()) {
            if matches!(*entry.get(), StorageHashState::CurrentHash(_)) {
                entry.remove_entry();
            }
        }
    }

    pub fn has_any_available_to_sync(&self) -> bool {
        let hash_state = self.hash_state.lock().unwrap();
        self.config.paths.keys().any(|storage| {
            hash_state
                .get(storage.as_str())
                .map(|state| matches!(state, StorageHashState::CurrentHash(_)))
                .unwrap_or(true)
        })
    }

    pub fn get_available_to_sync(&self) -> HashMap<String, u64> {
        let mut hash_state = self.hash_state.lock().unwrap();
        self.config
            .paths
            .keys()
            .filter_map(|storage| {
                let storage_state = hash_state.entry(storage.to_string()).or_insert_with(|| {
                    StorageHashState::CurrentHash(
                        get_storage_state(&self.config, storage, self).unwrap(),
                    )
                });

                match storage_state {
                    StorageHashState::CurrentHash(hash) => Some((storage.to_string(), *hash)),
                    _ => None,
                }
            })
            .collect()
    }

    pub fn release_blocked(&self) -> usize {
        let mut hash_state = self.hash_state.lock().unwrap();
        hash_state
            .drain_filter(|_storage, state| matches!(state, StorageHashState::Sync))
            .count()
    }

    pub fn release_blocked_by(&self, peer_id: &str) -> usize {
        let mut hash_state = self.hash_state.lock().unwrap();
        hash_state
            .drain_filter(
                |_storage, state| matches!(state, StorageHashState::SyncByPeer(p) if p == peer_id),
            )
            .count()
    }

    pub fn set_synching_with(
        &self,
        storages_to_sync: HashMap<String, u64>,
        peer_id: &str,
    ) -> Vec<String> {
        let available_to_sync = self.get_available_to_sync();

        let mut hash_state = self.hash_state.lock().unwrap();
        let storages_to_sync: Vec<String> = storages_to_sync
            .into_iter()
            .filter(
                |(storage, storage_hash)| match available_to_sync.get(storage.as_str()) {
                    Some(local_hash) => local_hash.ne(storage_hash),
                    None => false,
                },
            )
            .map(|(storage, _)| storage)
            .collect();

        storages_to_sync.iter().for_each(|storage| {
            hash_state
                .entry(storage.clone())
                .and_modify(|v| *v = StorageHashState::SyncByPeer(peer_id.to_string()));
        });

        storages_to_sync
    }

    pub fn block_available_for_sync(&self, to_block: Vec<String>) -> Vec<String> {
        let mut hash_state = self.hash_state.lock().unwrap();
        to_block
            .into_iter()
            .filter(|storage| {
                if let Some(state) = hash_state.get_mut(storage.as_str()) {
                    if !matches!(*state, StorageHashState::SyncByPeer(_)) {
                        *state = StorageHashState::Sync;
                        return true;
                    }
                }
                false
            })
            .collect()
    }

    pub fn is_synching_with(&self, storage: &str, peer_id: &str) -> bool {
        let hash_state = self.hash_state.lock().unwrap();
        hash_state
            .get(storage)
            .map(|state| match state {
                StorageHashState::SyncByPeer(state_peer) => state_peer.eq(peer_id),
                _ => false,
            })
            .unwrap_or_default()
    }
}

#[derive(Debug)]
pub enum StorageHashState {
    /// Represents the current hash of this storage
    CurrentHash(u64),
    /// Waiting for synchronization
    Sync,
    /// Waiting for synchronization with other peer
    SyncByPeer(String),
}

fn get_glob_set<P: AsRef<Path>>(path: P) -> Option<GlobSet> {
    let patterns = get_ignore_patterns(path.as_ref())?;

    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        let glob = match Glob::new(&pattern) {
            Ok(glob) => glob,
            Err(err) => {
                log::error!(
                    "Found invalid glob pattern {} at {:?}: {err}",
                    pattern,
                    path.as_ref()
                );
                continue;
            }
        };

        builder.add(glob);
    }

    match builder.build() {
        Ok(set) => Some(set),
        Err(err) => {
            log::error!("Failed to build glob set: {err}");
            None
        }
    }
}

fn get_ignore_patterns<P: AsRef<Path>>(path: P) -> Option<Vec<String>> {
    let ignore_file_path = path.as_ref().join(".ignore");
    if !ignore_file_path.exists() {
        return None;
    }

    let content = match std::fs::read_to_string(ignore_file_path) {
        Ok(content) => content,
        Err(_) => {
            log::error!("Failed to read ignore file at {:?}", path.as_ref());
            return None;
        }
    };
    Some(content.lines().map(|s| s.to_string()).collect())
}

fn get_storage_state(
    config: &Config,
    storage: &str,
    storage_state: &StorageState,
) -> crate::Result<u64> {
    let storage_index = crate::storage::walk_path(config, storage, storage_state)?;
    let hash = crate::storage::get_state_hash(storage_index.iter());

    log::trace!("{storage} hash is {hash}");
    Ok(hash)
}
