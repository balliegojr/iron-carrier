use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Mutex,
};

use crate::{config::Config, ignored_files::IgnoredFiles};

pub struct StorageHashCache {
    config: &'static Config,
    ignored_files: &'static IgnoredFiles,
    hash_state: Mutex<HashMap<String, StorageHashState>>,
}

impl StorageHashCache {
    pub fn new(config: &'static Config, ignored_files: &'static IgnoredFiles) -> Self {
        Self {
            hash_state: HashMap::new().into(),
            ignored_files,
            config,
        }
    }
    pub fn clear(&self) {
        self.hash_state.lock().unwrap().clear();
    }

    pub fn invalidate_cache(&self, storage: &str) {
        let mut hash_state = self.hash_state.lock().unwrap();
        if let Entry::Occupied(entry) = hash_state.entry(storage.to_string()) {
            if matches!(*entry.get(), StorageHashState::CurrentHash(_)) {
                entry.remove_entry();
            }
        }
    }

    pub fn has_any_available_to_sync(&self) -> bool {
        let hash_state = self.hash_state.lock().unwrap();
        self.config.storages.keys().any(|storage| {
            hash_state
                .get(storage.as_str())
                .map(|state| matches!(state, StorageHashState::CurrentHash(_)))
                .unwrap_or(true)
        })
    }

    pub fn get_available_to_sync(&self) -> HashMap<String, u64> {
        let mut hash_state = self.hash_state.lock().unwrap();
        self.config
            .storages
            .keys()
            .filter_map(|storage| {
                let storage_state = hash_state.entry(storage.to_string()).or_insert_with(|| {
                    StorageHashState::CurrentHash(
                        get_storage_state(self.config, storage, self.ignored_files).unwrap(),
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

fn get_storage_state(
    config: &Config,
    storage: &str,
    ignored_files: &IgnoredFiles,
) -> crate::Result<u64> {
    let storage_index = crate::storage::walk_path(config, storage, ignored_files)?;
    let hash = crate::storage::get_state_hash(storage_index.iter());

    log::trace!("{storage} hash is {hash}");
    Ok(hash)
}
