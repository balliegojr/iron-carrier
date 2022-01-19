use std::{collections::HashMap, sync::Arc, time::Duration};

use super::{synchronization_session::SynchronizationState, SyncEvent};
use crate::{config::Config, conn::CommandDispatcher, fs, sync::Origin};

#[derive(Debug)]
enum StorageState {
    CurrentHash(u64),
    Sync,
    SyncByPeer(String),
}

pub struct Synchronizer {
    config: Arc<Config>,
    session_state: SynchronizationState,
    storage_state: HashMap<String, StorageState>,
    commands: CommandDispatcher,
}

impl Synchronizer {
    pub fn new(config: Arc<Config>, commands: CommandDispatcher) -> crate::Result<Self> {
        log::debug!("Initializing synchronizer");
        let storage_state = config
            .paths
            .keys()
            .map(|storage| match get_storage_state(&config, storage) {
                Ok(state) => Ok((storage.clone(), StorageState::CurrentHash(state))),
                Err(err) => Err(err),
            })
            .collect::<crate::Result<HashMap<String, StorageState>>>()?;
        log::debug!("have state");

        let s = Synchronizer {
            config,
            session_state: SynchronizationState::new(commands.clone()),
            storage_state,
            commands,
        };

        Ok(s)
    }

    pub fn handle_signal(&mut self, signal: SyncEvent) -> crate::Result<bool> {
        match signal {
            SyncEvent::StartSync(attempt) => {
                if self.storage_state.is_empty() {
                    log::error!(
                        "There are no storages to sync, be sure your configuration file is correct"
                    );
                    return Ok(false);
                }

                if self
                    .storage_state
                    .values()
                    .all(|state| !matches!(state, &StorageState::CurrentHash(_)))
                {
                    log::info!("No storages are available for synchroniation");
                    return Ok(false);
                }

                self.commands.now(SyncEvent::ExchangeStorageStates);
            }
            SyncEvent::ExchangeStorageStates => {
                let state = self
                    .storage_state
                    .iter()
                    .filter_map(|(storage, state)| match state {
                        StorageState::CurrentHash(state) => Some((storage.clone(), *state)),
                        _ => None,
                    })
                    .collect();

                let expected_replies = self
                    .commands
                    .broadcast(SyncEvent::QueryOutOfSyncStorages(state));

                self.session_state
                    .start_sync_after_replies(expected_replies);
            }
            SyncEvent::EndSync => {
                let storages_to_reload: Vec<String> = self
                    .storage_state
                    .iter()
                    .filter(|(_, state)| matches!(**state, StorageState::Sync))
                    .map(|(storage, _)| storage.clone())
                    .collect();

                if storages_to_reload.is_empty() {
                    // self.commands
                    //     .after(SyncEvent::Cleanup, Duration::from_secs(15));
                    return Ok(false);
                }

                for storage in storages_to_reload {
                    let state =
                        StorageState::CurrentHash(get_storage_state(&self.config, &storage)?);
                    self.storage_state.entry(storage).and_modify(|v| *v = state);
                }

                self.commands.broadcast(SyncEvent::EndSync);
                self.commands.now(SyncEvent::ExchangeStorageStates);
            }
            // TODO: handle cleanup
            // SyncEvent::Cleanup => {
            //     if !self.file_transfer_man.has_pending_transfers() {
            //         transaction_log::compress_log(&self.config.log_path)?;
            //     } else {
            //         self.commands
            //             .after(SyncEvent::Cleanup, Duration::from_secs(60));
            //     }
            // }
            SyncEvent::SyncNextStorage => match self.session_state.get_next_storage() {
                Some((storage, peers)) => {
                    self.commands
                        .now(SyncEvent::BuildStorageIndex(storage.clone()));

                    self.commands
                        .broadcast(SyncEvent::BuildStorageIndex(storage));
                }
                None => {
                    self.commands.now(SyncEvent::EndSync);
                }
            },
            SyncEvent::BuildStorageIndex(storage) => {
                log::info!("Building index for {}", storage);
                let index = fs::walk_path(&self.config, &storage).unwrap();
                log::debug!("Read {} files ", index.len());

                self.commands.now(SyncEvent::SetStorageIndex(index));
            }
            SyncEvent::SetStorageIndex(index) => {
                let origin = Origin::Initiator;

                log::debug!("Setting index with origin {:?}", origin);

                let have_all_indexes = self.session_state.set_storage_index(origin, index);
                if have_all_indexes {
                    self.session_state.build_event_queue();
                    return Ok(true);
                }
            }
            _ => unreachable!(),
        }
        Ok(false)
    }

    pub fn handle_network_event(&mut self, event: SyncEvent, peer_id: &str) -> crate::Result<bool> {
        match event {
            SyncEvent::EndSync => {
                let storages: Vec<String> = self
                    .storage_state
                    .iter()
                    .filter(|(_, state)| match state {
                        StorageState::SyncByPeer(p) => p == peer_id,
                        _ => false,
                    })
                    .map(|(storage, _)| storage.clone())
                    .collect();

                for storage in storages {
                    let state =
                        StorageState::CurrentHash(get_storage_state(&self.config, &storage)?);
                    self.storage_state.entry(storage).and_modify(|v| *v = state);
                }
            }
            SyncEvent::QueryOutOfSyncStorages(mut storages) => {
                // keep only the storages that exists in this peer and have a different hash
                storages.retain(|storage, peer_storage_hash| {
                    match self.storage_state.get(storage) {
                        Some(&StorageState::CurrentHash(hash)) => hash != *peer_storage_hash,
                        _ => false,
                    }
                });

                let storages: Vec<String> = storages.into_iter().map(|(key, _)| key).collect();

                // Changes the storages state to SyncByPeer, to avoid double sync and premature connection close
                storages.iter().for_each(|storage| {
                    self.storage_state
                        .entry(storage.clone())
                        .and_modify(|v| *v = StorageState::SyncByPeer(peer_id.to_string()));
                });

                self.commands
                    .to(SyncEvent::ReplyOutOfSyncStorages(storages), peer_id);
            }
            SyncEvent::ReplyOutOfSyncStorages(mut storages) => {
                storages.retain(|storage| match self.storage_state.get(storage) {
                    Some(state) => !matches!(state, &StorageState::SyncByPeer(_)),
                    None => false,
                });

                storages.iter().for_each(|storage| {
                    let state = self.storage_state.get_mut(storage).unwrap();
                    *state = StorageState::Sync;
                });

                let have_all_replies = self.session_state.add_storages_to_sync(storages, peer_id);
                if have_all_replies {
                    self.commands.now(SyncEvent::SyncNextStorage);
                }
            }
            SyncEvent::BuildStorageIndex(storage) => {
                log::info!("Building index for {}", storage);
                if !self.config.paths.contains_key(&storage) {
                    log::error!("There is no such storage: {}", &storage);
                    // TODO: panic?
                    return Ok(false);
                }

                let index = fs::walk_path(&self.config, &storage).unwrap();
                log::debug!("Read {} files ", index.len());

                self.commands.to(SyncEvent::SetStorageIndex(index), peer_id);
            }
            SyncEvent::SetStorageIndex(index) => {
                let origin = Origin::Peer(peer_id.to_string());

                log::debug!("Setting index with origin {:?}", origin);

                let have_all_indexes = self.session_state.set_storage_index(origin, index);
                if have_all_indexes {
                    self.session_state.build_event_queue();
                    return Ok(true);
                }
            }
            _ => unreachable!(),
        }
        Ok(false)
    }
}

fn get_storage_state(config: &Config, storage: &str) -> crate::Result<u64> {
    log::trace!("get_storage_state {:?}", storage);
    let storage_index = fs::walk_path(config, storage)?;
    log::trace!("storage_index: {:?}", &storage_index);
    Ok(fs::get_state_hash(storage_index.iter()))
}
