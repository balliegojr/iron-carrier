use std::{
    collections::{HashMap, LinkedList},
    fs::File,
    sync::Arc,
    time::Duration,
};

use super::{synchronization_session::SynchronizationState, QueueEventType, SyncEvent};
use crate::{
    config::Config,
    conn::CommandDispatcher,
    fs::{self, FileInfo},
    sync::Origin,
    transaction_log::{self, EventStatus, EventType, TransactionLogWriter},
};

#[derive(Debug)]
enum StorageState {
    CurrentHash(u64),
    Sync,
    SyncByPeer(String),
}

pub struct Synchronizer {
    config: Arc<Config>,
    session_state: SynchronizationState,
    events_queue: LinkedList<(SyncEvent, QueueEventType)>,
    log_writer: Option<TransactionLogWriter<File>>,
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
            session_state: SynchronizationState::new(),
            events_queue: LinkedList::new(),
            log_writer: None,
            storage_state,
            commands,
        };

        Ok(s)
    }

    pub fn handle_signal(&mut self, signal: SyncEvent) -> crate::Result<()> {
        log::debug!("{} - Received Signal {} ", self.config.node_id, signal);
        match signal {
            SyncEvent::StartSync(attempt) => {
                if self.storage_state.is_empty() {
                    log::error!(
                        "There are no storages to sync, be sure your configuration file is correct"
                    );
                    return Ok(());
                }

                if self
                    .storage_state
                    .values()
                    .all(|state| !matches!(state, &StorageState::CurrentHash(_)))
                {
                    log::info!("No storages are available for synchroniation");
                    return Ok(());
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
                    self.commands
                        .after(SyncEvent::Cleanup, Duration::from_secs(15));
                    return Ok(());
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
                    let mut event_queue = self.session_state.get_event_queue();
                    log::debug!(
                        "{} - there are {} actions to sync",
                        self.config.node_id,
                        event_queue.len()
                    );
                    log::debug!("{:?}", event_queue);
                    self.events_queue.append(&mut event_queue);
                    self.commands.now(SyncEvent::ConsumeSyncQueue);
                }
            }
            SyncEvent::ConsumeSyncQueue => {
                let action = self.events_queue.pop_front();
                log::debug!("{} - next queue action {:?}", self.config.node_id, action);
                match action {
                    Some((event, notification_type)) => match notification_type {
                        QueueEventType::Signal => {
                            self.commands.now(event);
                        }
                        QueueEventType::Peer(peer_id) => {
                            // TODO: consume queue if peer is offline
                            self.commands.to(event, &peer_id);
                        }
                        QueueEventType::Broadcast => {
                            self.commands.broadcast(event);
                            self.commands.now(SyncEvent::ConsumeSyncQueue);
                        }
                    },
                    None => {
                        // TODO: check for pending transfers
                        // if !self.file_transfer_man.has_pending_transfers() {
                        //     self.commands.now(SyncEvent::Cleanup);
                        // }
                    }
                }
            }
            SyncEvent::DeleteFile(file) => {
                self.delete_file(&file)?;
                self.commands.now(SyncEvent::ConsumeSyncQueue);
            }
            // TODO: handle file events
            // CarrierEvent::SendFile(file_info, peer_id, is_new) => {
            //     match self.connected_peers.get_peer_endpoint(peer_id) {
            //         Some(endpoint) => {
            //             self.file_transfer_man
            //                 .send_file_to_peer(file_info, endpoint, is_new, false)?;
            //         }
            //         None => {
            //             self.handler.signals().send(CarrierEvent::ConsumeSyncQueue);
            //         }
            //     }
            // }
            // CarrierEvent::BroadcastFile(file_info, is_new) => {
            //     for peer in self.connection_manager.get_all_identified_endpoints() {
            //         self.file_transfer_man.send_file_to_peer(
            //             file_info.clone(),
            //             peer,
            //             is_new,
            //             false,
            //         )?;
            //     }

            //     self.commands.now(SyncEvent::ConsumeSyncQueue);
            // }
            SyncEvent::MoveFile(src, dst) => {
                self.move_file(&src, &dst)?;
                self.commands.now(SyncEvent::ConsumeSyncQueue);
            }
            // TODO: handle file watcher events
            // CarrierEvent::FileWatcherEvent(watcher_event) => {
            //     let addresses = self.get_peer_addresses();
            //     if addresses.is_empty() {
            //         return Ok(());
            //     }

            //     let (event, event_type) = match watcher_event {
            //         super::WatcherEvent::Created(file_info) => {
            //             self.get_log_writer()?.append(
            //                 file_info.alias.clone(),
            //                 EventType::Write(file_info.path.clone()),
            //                 EventStatus::Finished,
            //             )?;

            //             (
            //                 CarrierEvent::BroadcastFile(file_info, true),
            //                 QueueEventType::Signal,
            //             )
            //         }
            //         super::WatcherEvent::Updated(file_info) => {
            //             self.get_log_writer()?.append(
            //                 file_info.alias.clone(),
            //                 EventType::Write(file_info.path.clone()),
            //                 EventStatus::Finished,
            //             )?;

            //             (
            //                 CarrierEvent::BroadcastFile(file_info, false),
            //                 QueueEventType::Signal,
            //             )
            //         }

            //         super::WatcherEvent::Moved(src, dst) => {
            //             self.get_log_writer()?.append(
            //                 src.alias.clone(),
            //                 EventType::Move(src.path.clone(), dst.path.clone()),
            //                 EventStatus::Finished,
            //             )?;

            //             (CarrierEvent::MoveFile(src, dst), QueueEventType::Broadcast)
            //         }
            //         super::WatcherEvent::Deleted(file_info) => {
            //             self.get_log_writer()?.append(
            //                 file_info.alias.clone(),
            //                 EventType::Delete(file_info.path.clone()),
            //                 EventStatus::Finished,
            //             )?;

            //             (
            //                 CarrierEvent::DeleteFile(file_info),
            //                 QueueEventType::Broadcast,
            //             )
            //         }
            //     };

            //     self.add_queue_event(event, event_type);
            //     self.connected_peers.connect_all(addresses);
            //     self.get_log_writer()?;
            // }
            _ => unreachable!(),
        }
        Ok(())
    }

    pub fn handle_network_event(&mut self, event: SyncEvent, peer_id: &str) -> crate::Result<()> {
        log::debug!("{} - Received Event {} ", self.config.node_id, event);
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
                    return Ok(());
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
                    let mut event_queue = self.session_state.get_event_queue();
                    log::debug!(
                        "{} - there are {} actions to sync",
                        self.config.node_id,
                        event_queue.len()
                    );
                    log::debug!("{:?}", event_queue);
                    self.events_queue.append(&mut event_queue);
                    self.commands.now(SyncEvent::ConsumeSyncQueue);
                }
            }
            SyncEvent::DeleteFile(file) => {
                self.delete_file(&file)?;

                // TODO: supress event
                // if let Some(ref mut file_watcher) = self.file_watcher {
                //     file_watcher.supress_next_event(file, EventSupression::Delete);
                // }
            }
            // TODO: handle file events
            // CarrierEvent::RequestFile(file_info, is_new) => {
            //     self.file_transfer_man
            //         .send_file_to_peer(file_info, peer_id, is_new, true)?;
            // }
            // CarrierEvent::MoveFile(src, dst) => {
            //     self.move_file(&src, &dst)?;
            //     if let Some(ref mut file_watcher) = self.file_watcher {
            //         file_watcher.supress_next_event(src, EventSupression::Rename);
            //     }
            // }
            // CarrierEvent::FileSyncEvent(ev) => {
            //     // TODO: fix this
            //     self.get_log_writer()?;

            //     self.file_transfer_man.file_sync_event(
            //         ev,
            //         peer_id,
            //         &mut self.file_watcher,
            //         self.log_writer.as_mut().unwrap(),
            //     )?;
            // }
            _ => unreachable!(),
        }
        Ok(())
    }

    fn delete_file(&mut self, file: &FileInfo) -> crate::Result<()> {
        let event_status = match fs::delete_file(file, &self.config) {
            Ok(_) => EventStatus::Finished,
            Err(err) => {
                log::error!("Failed to delete file {}", err);
                EventStatus::Failed
            }
        };
        self.get_log_writer()?.append(
            file.storage.to_string(),
            EventType::Delete(file.path.clone()),
            event_status,
        )?;

        Ok(())
    }

    fn move_file(&mut self, src: &FileInfo, dest: &FileInfo) -> crate::Result<()> {
        let event_status = match fs::move_file(src, dest, &self.config) {
            Err(err) => {
                log::error!("Failed to move file: {}", err);
                EventStatus::Failed
            }
            Ok(_) => EventStatus::Finished,
        };

        self.get_log_writer()?.append(
            src.storage.clone(),
            EventType::Move(src.path.clone(), dest.path.clone()),
            event_status,
        )?;

        Ok(())
    }

    fn add_queue_event(&mut self, event: SyncEvent, event_type: QueueEventType) {
        log::info!(
            "{} - adding event to queue: {} {}",
            self.config.node_id,
            event,
            event_type
        );
        self.events_queue.push_back((event, event_type));
    }
}

fn get_storage_state(config: &Config, storage: &str) -> crate::Result<u64> {
    log::trace!("get_storage_state {:?}", storage);
    let storage_index = fs::walk_path(config, storage)?;
    log::trace!("storage_index: {:?}", &storage_index);
    Ok(fs::get_state_hash(storage_index.iter()))
}
