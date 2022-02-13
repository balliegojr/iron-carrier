use std::sync::Arc;

use super::{synchronization_session::SynchronizationState, SyncEvent};
use crate::{
    config::Config, conn::CommandDispatcher, fs, storage_state::StorageState, sync::Origin,
};

pub struct Synchronizer {
    config: Arc<Config>,
    session_state: SynchronizationState,
    storage_state: Arc<StorageState>,
    commands: CommandDispatcher,
}

impl Synchronizer {
    pub fn new(
        config: Arc<Config>,
        commands: CommandDispatcher,
        storage_state: Arc<StorageState>,
    ) -> crate::Result<Self> {
        log::debug!("Initializing synchronizer");

        let s = Synchronizer {
            config,
            session_state: SynchronizationState::new(commands.clone()),
            storage_state,
            commands,
        };

        Ok(s)
    }

    pub fn clear(&mut self) {
        self.session_state = SynchronizationState::new(self.commands.clone());
    }

    pub fn handle_signal(&mut self, signal: SyncEvent) -> crate::Result<bool> {
        match signal {
            SyncEvent::StartSync => {
                if self.config.paths.is_empty() {
                    log::error!(
                        "There are no storages to sync, be sure your configuration file is correct"
                    );
                    return Ok(false);
                }

                if !self.storage_state.has_any_available_to_sync() {
                    log::info!("No storages are available for synchronization");
                    return Ok(false);
                }

                self.commands.now(SyncEvent::ExchangeStorageStates);
            }
            SyncEvent::ExchangeStorageStates => {
                let storages = self.storage_state.get_available_to_sync();
                log::trace!("Available storages to sync: {storages:?}");

                let expected_replies = self
                    .commands
                    .broadcast(SyncEvent::QueryOutOfSyncStorages(storages));

                self.session_state
                    .start_sync_after_replies(expected_replies);
            }
            SyncEvent::EndSync => {
                let released = self.storage_state.release_blocked();
                if released == 0 {
                    // self.commands
                    //     .after(SyncEvent::Cleanup, Duration::from_secs(15));
                    return Ok(false);
                }

                self.commands.broadcast(SyncEvent::EndSync);
                self.commands.now(SyncEvent::ExchangeStorageStates);
            }
            SyncEvent::SyncNextStorage => match self.session_state.get_next_storage() {
                Some(storage) => {
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
                let index = fs::walk_path(&self.config, &storage, &self.storage_state).unwrap();
                self.commands.now(SyncEvent::SetStorageIndex(index));
            }
            SyncEvent::SetStorageIndex(index) => {
                let origin = Origin::Initiator;

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
                self.storage_state.release_blocked_by(peer_id);
            }
            SyncEvent::QueryOutOfSyncStorages(storages) => {
                let storages = self.storage_state.set_synching_with(storages, peer_id);
                log::trace!("Storages available to sync with peer {peer_id} are {storages:?}");

                self.commands
                    .to(SyncEvent::ReplyOutOfSyncStorages(storages), peer_id);
            }
            SyncEvent::ReplyOutOfSyncStorages(storages) => {
                let storages = self.storage_state.block_available_for_sync(storages);
                log::trace!("storages that will be synchronized {storages:?}");

                let have_all_replies = self.session_state.add_storages_to_sync(storages, peer_id);
                if have_all_replies {
                    self.commands.now(SyncEvent::SyncNextStorage);
                }
            }
            SyncEvent::BuildStorageIndex(storage) => {
                if !self.config.paths.contains_key(&storage) {
                    log::error!("There is no such storage: {}", &storage);
                    return Ok(false);
                }

                if !self.storage_state.is_synching_with(&storage, peer_id) {
                    log::info!("storage {storage} is synchronizing with another peer");
                    return Ok(false);
                }

                let index = fs::walk_path(&self.config, &storage, &self.storage_state).unwrap();

                self.commands.to(SyncEvent::SetStorageIndex(index), peer_id);
            }
            SyncEvent::SetStorageIndex(index) => {
                let origin = Origin::Peer(peer_id.to_string());

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
