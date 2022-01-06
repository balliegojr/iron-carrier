use std::{
    collections::{HashMap, LinkedList},
    fs::File,
    sync::Arc,
    time::Duration,
};

use super::{
    connection_manager::CommandDispatcher, synchronization_session::SynchronizationState,
    CarrierEvent, QueueEventType,
};
use crate::{
    config::Config,
    fs::{self, FileInfo},
    hash_helper,
    transaction_log::{self, EventStatus, EventType, TransactionLogWriter},
};

#[derive(Debug)]
enum StorageState {
    CurrentHash(u64),
    Sync,
    SyncByPeer(u64),
}

pub struct Synchronizer {
    node_id: u64,
    config: Arc<Config>,
    session_state: SynchronizationState,
    events_queue: LinkedList<(CarrierEvent, QueueEventType)>,
    log_writer: Option<TransactionLogWriter<File>>,
    storage_state: HashMap<String, StorageState>,
    commands: CommandDispatcher,
}

impl Synchronizer {
    pub fn new(config: Arc<Config>, commands: CommandDispatcher) -> crate::Result<Self> {
        let storage_state = config
            .paths
            .keys()
            .map(|storage| match get_storage_state(&config, storage) {
                Ok(state) => Ok((storage.clone(), StorageState::CurrentHash(state))),
                Err(err) => Err(err),
            })
            .collect::<crate::Result<HashMap<String, StorageState>>>()?;

        let s = Synchronizer {
            node_id: hash_helper::get_node_id(config.port),
            config,
            session_state: SynchronizationState::new(),
            events_queue: LinkedList::new(),
            log_writer: None,
            storage_state,
            commands,
        };

        Ok(s)
    }

    pub fn handle_signal(&mut self, signal: CarrierEvent) -> crate::Result<()> {
        // log::debug!("{} - Received Signal {} ", self.node_id, signal);
        // match signal {
        //     CarrierEvent::StartSync(attempt) => {
        //         let addresses = self.get_peer_addresses();
        //         if addresses.is_empty() {
        //             log::info!("No peers to sync");
        //             self.schedule_next_sync_attempt(attempt);
        //             return Ok(());
        //         }

        //         if self.storage_state.is_empty() {
        //             log::error!(
        //                 "There are no storages to sync, be sure your configuration file is correct"
        //             );
        //             return Ok(());
        //         }

        //         if self
        //             .storage_state
        //             .values()
        //             .all(|state| !matches!(state, &StorageState::CurrentHash(_)))
        //         {
        //             log::info!("No storages are available for synchroniation");
        //             return Ok(());
        //         }

        //         self.add_queue_event(CarrierEvent::ExchangeStorageStates, QueueEventType::Signal);
        //         self.connected_peers.connect_all(addresses);
        //     }
        //     CarrierEvent::ExchangeStorageStates => {
        //         let state = self
        //             .storage_state
        //             .iter()
        //             .filter_map(|(storage, state)| match state {
        //                 StorageState::CurrentHash(state) => Some((storage.clone(), *state)),
        //                 _ => None,
        //             })
        //             .collect();

        //         let peers = self.connection_manager.get_all_identified_endpoints();

        //         self.events
        //             .broadcast_command(CarrierEvent::QueryOutOfSyncStorages(state), &peers);

        //         self.session_state.start_sync_after_replies(peers.len());
        //     }
        //     CarrierEvent::EndSync => {
        //         let storages_to_reload: Vec<String> = self
        //             .storage_state
        //             .iter()
        //             .filter(|(_, state)| matches!(**state, StorageState::Sync))
        //             .map(|(storage, _)| storage.clone())
        //             .collect();

        //         if storages_to_reload.is_empty() {
        //             self.signals
        //                 .after_duration(CarrierEvent::Cleanup, Duration::from_secs(15));
        //             return Ok(());
        //         }

        //         for storage in storages_to_reload {
        //             let state =
        //                 StorageState::CurrentHash(get_storage_state(&self.config, &storage)?);
        //             self.storage_state.entry(storage).and_modify(|v| *v = state);
        //         }

        //         let peers = self.connection_manager.get_all_identified_endpoints();
        //         self.events.broadcast_command(CarrierEvent::EndSync, &peers);

        //         self.signals.now(CarrierEvent::ExchangeStorageStates);
        //     }
        //     CarrierEvent::Cleanup => {
        //         if !self.file_transfer_man.has_pending_transfers() {
        //             self.connection_manager.disconnect_all();
        //             transaction_log::compress_log(&self.config.log_path)?;
        //         } else {
        //             self.signals
        //                 .after_duration(CarrierEvent::Cleanup, Duration::from_secs(60));
        //         }
        //     }
        //     CarrierEvent::SyncNextStorage => match self.session_state.get_next_storage() {
        //         Some((storage, peers)) => {
        //             self.signals
        //                 .now(CarrierEvent::BuildStorageIndex(storage.clone()));

        //             self.events.broadcast_command(
        //                 CarrierEvent::BuildStorageIndex(storage),
        //                 peers.into_iter().collect().into(),
        //             );
        //         }
        //         None => {
        //             self.signals.now(CarrierEvent::EndSync);
        //         }
        //     },
        //     CarrierEvent::BuildStorageIndex(storage) => {
        //         log::info!("Building index for {}", storage);
        //         let index = fs::walk_path(&self.config, &storage).unwrap();
        //         log::debug!("Read {} files ", index.len());

        //         self.signals.now(CarrierEvent::SetStorageIndex(index));
        //     }
        //     CarrierEvent::SetStorageIndex(index) => {
        //         let origin = Origin::Initiator;

        //         log::debug!("Setting index with origin {:?}", origin);

        //         let have_all_indexes = self.session_state.set_storage_index(origin, index);
        //         if have_all_indexes {
        //             let mut event_queue = self.session_state.get_event_queue();
        //             log::debug!(
        //                 "{} - there are {} actions to sync",
        //                 self.node_id,
        //                 event_queue.len()
        //             );
        //             log::debug!("{:?}", event_queue);
        //             self.events_queue.append(&mut event_queue);
        //             self.signals.now(CarrierEvent::ConsumeSyncQueue);
        //         }
        //     }
        //     CarrierEvent::ConsumeSyncQueue => {
        //         let action = self.events_queue.pop_front();
        //         log::debug!("{} - next queue action {:?}", self.node_id, action);
        //         match action {
        //             Some((event, notification_type)) => match notification_type {
        //                 QueueEventType::Signal => {
        //                     self.signals.now(event);
        //                 }
        //                 QueueEventType::Peer(peer_id) => {
        //                     match self.connected_peers.get_peer_endpoint(peer_id) {
        //                         Some(endpoint) => send_message(&self.handler, &event, endpoint),
        //                         None => {
        //                             log::debug!("peer is offline, cant receive message");
        //                             self.signals.now(CarrierEvent::ConsumeSyncQueue);
        //                         }
        //                     }
        //                 }
        //                 QueueEventType::Broadcast => {
        //                     let peers = self.connection_manager.get_all_identified_endpoints();
        //                     self.events.broadcast_command(event, &peers);
        //                     self.signals.now(CarrierEvent::ConsumeSyncQueue);
        //                 }
        //             },
        //             None => {
        //                 if !self.file_transfer_man.has_pending_transfers() {
        //                     self.signals.now(CarrierEvent::Cleanup);
        //                 }
        //             }
        //         }
        //     }
        //     CarrierEvent::DeleteFile(file) => {
        //         self.delete_file(&file)?;
        //         self.signals.now(CarrierEvent::ConsumeSyncQueue);
        //     }
        //     CarrierEvent::SendFile(file_info, peer_id, is_new) => {
        //         match self.connected_peers.get_peer_endpoint(peer_id) {
        //             Some(endpoint) => {
        //                 self.file_transfer_man
        //                     .send_file_to_peer(file_info, endpoint, is_new, false)?;
        //             }
        //             None => {
        //                 self.handler.signals().send(CarrierEvent::ConsumeSyncQueue);
        //             }
        //         }
        //     }
        //     CarrierEvent::BroadcastFile(file_info, is_new) => {
        //         for peer in self.connection_manager.get_all_identified_endpoints() {
        //             self.file_transfer_man.send_file_to_peer(
        //                 file_info.clone(),
        //                 peer,
        //                 is_new,
        //                 false,
        //             )?;
        //         }

        //         self.signals.now(CarrierEvent::ConsumeSyncQueue);
        //     }
        //     CarrierEvent::MoveFile(src, dst) => {
        //         self.move_file(&src, &dst)?;
        //         self.signals.now(CarrierEvent::ConsumeSyncQueue);
        //     }
        //     CarrierEvent::FileWatcherEvent(watcher_event) => {
        //         let addresses = self.get_peer_addresses();
        //         if addresses.is_empty() {
        //             return Ok(());
        //         }

        //         let (event, event_type) = match watcher_event {
        //             super::WatcherEvent::Created(file_info) => {
        //                 self.get_log_writer()?.append(
        //                     file_info.alias.clone(),
        //                     EventType::Write(file_info.path.clone()),
        //                     EventStatus::Finished,
        //                 )?;

        //                 (
        //                     CarrierEvent::BroadcastFile(file_info, true),
        //                     QueueEventType::Signal,
        //                 )
        //             }
        //             super::WatcherEvent::Updated(file_info) => {
        //                 self.get_log_writer()?.append(
        //                     file_info.alias.clone(),
        //                     EventType::Write(file_info.path.clone()),
        //                     EventStatus::Finished,
        //                 )?;

        //                 (
        //                     CarrierEvent::BroadcastFile(file_info, false),
        //                     QueueEventType::Signal,
        //                 )
        //             }

        //             super::WatcherEvent::Moved(src, dst) => {
        //                 self.get_log_writer()?.append(
        //                     src.alias.clone(),
        //                     EventType::Move(src.path.clone(), dst.path.clone()),
        //                     EventStatus::Finished,
        //                 )?;

        //                 (CarrierEvent::MoveFile(src, dst), QueueEventType::Broadcast)
        //             }
        //             super::WatcherEvent::Deleted(file_info) => {
        //                 self.get_log_writer()?.append(
        //                     file_info.alias.clone(),
        //                     EventType::Delete(file_info.path.clone()),
        //                     EventStatus::Finished,
        //                 )?;

        //                 (
        //                     CarrierEvent::DeleteFile(file_info),
        //                     QueueEventType::Broadcast,
        //                 )
        //             }
        //         };

        //         self.add_queue_event(event, event_type);
        //         self.connected_peers.connect_all(addresses);
        //         self.get_log_writer()?;
        //     }
        //     CarrierEvent::IdentificationTimeout => {
        //         self.connected_peers.identification_timeout();
        //     }
        //     _ => unreachable!(),
        // }
        // Ok(())
        todo!()
    }

    pub fn handle_network_event(
        &mut self,
        event: CarrierEvent,
        peer_id: &str,
    ) -> crate::Result<()> {
        // log::debug!("{} - Received Event {} ", self.node_id, event);
        // match event {
        //     CarrierEvent::EndSync => {
        //         let storages: Vec<String> = self
        //             .storage_state
        //             .iter()
        //             .filter(|(_, state)| match state {
        //                 StorageState::SyncByPeer(p) => *p == peer_id,
        //                 _ => false,
        //             })
        //             .map(|(storage, _)| storage.clone())
        //             .collect();

        //         for storage in storages {
        //             let state =
        //                 StorageState::CurrentHash(get_storage_state(&self.config, &storage)?);
        //             self.storage_state.entry(storage).and_modify(|v| *v = state);
        //         }
        //     }
        //     CarrierEvent::SetPeerId(peer_id) => {
        //         self.connected_peers.add_peer(endpoint, peer_id);
        //     }
        //     CarrierEvent::QueryOutOfSyncStorages(mut storages) => {
        //         // keep only the storages that exists in this peer and have a different hash
        //         storages.retain(|storage, peer_storage_hash| {
        //             match self.storage_state.get(storage) {
        //                 Some(&StorageState::CurrentHash(hash)) => hash != *peer_storage_hash,
        //                 _ => false,
        //             }
        //         });

        //         let storages: Vec<String> = storages.into_iter().map(|(key, _)| key).collect();

        //         // Changes the storages state to SyncByPeer, to avoid double sync and premature connection close
        //         storages.iter().for_each(|storage| {
        //             self.storage_state
        //                 .entry(storage.clone())
        //                 .and_modify(|v| *v = StorageState::SyncByPeer(peer_id));
        //         });

        //         self.events
        //             .command_to(CarrierEvent::ReplyOutOfSyncStorages(storages), peer_id);
        //     }
        //     CarrierEvent::ReplyOutOfSyncStorages(mut storages) => {
        //         storages.retain(|storage| match self.storage_state.get(storage) {
        //             Some(state) => !matches!(state, &StorageState::SyncByPeer(_)),
        //             None => false,
        //         });

        //         storages.iter().for_each(|storage| {
        //             let state = self.storage_state.get_mut(storage).unwrap();
        //             *state = StorageState::Sync;
        //         });

        //         let have_all_replies = self.session_state.add_storages_to_sync(storages, peer_id);

        //         if have_all_replies {
        //             self.signals.now(CarrierEvent::SyncNextStorage);
        //         }
        //     }
        //     CarrierEvent::BuildStorageIndex(storage) => {
        //         log::info!("Building index for {}", storage);
        //         if !self.config.paths.contains_key(&storage) {
        //             log::error!("There is no such storage: {}", &storage);
        //             // TODO: panic?
        //             return Ok(());
        //         }

        //         let index = fs::walk_path(&self.config, &storage).unwrap();
        //         log::debug!("Read {} files ", index.len());

        //         self.events
        //             .command_to(CarrierEvent::SetStorageIndex(index), peer_id);
        //     }
        //     CarrierEvent::SetStorageIndex(index) => {
        //         let origin = Origin::Peer(peer_id);

        //         log::debug!("Setting index with origin {:?}", origin);

        //         let have_all_indexes = self.session_state.set_storage_index(origin, index);
        //         if have_all_indexes {
        //             let mut event_queue = self.session_state.get_event_queue();
        //             log::debug!(
        //                 "{} - there are {} actions to sync",
        //                 self.node_id,
        //                 event_queue.len()
        //             );
        //             log::debug!("{:?}", event_queue);
        //             self.events_queue.append(&mut event_queue);
        //             self.signals.now(CarrierEvent::ConsumeSyncQueue);
        //         }
        //     }
        //     CarrierEvent::DeleteFile(file) => {
        //         self.delete_file(&file)?;

        //         if let Some(ref mut file_watcher) = self.file_watcher {
        //             file_watcher.supress_next_event(file, EventSupression::Delete);
        //         }
        //     }
        //     CarrierEvent::RequestFile(file_info, is_new) => {
        //         self.file_transfer_man
        //             .send_file_to_peer(file_info, peer_id, is_new, true)?;
        //     }
        //     CarrierEvent::MoveFile(src, dst) => {
        //         self.move_file(&src, &dst)?;
        //         if let Some(ref mut file_watcher) = self.file_watcher {
        //             file_watcher.supress_next_event(src, EventSupression::Rename);
        //         }
        //     }
        //     CarrierEvent::FileSyncEvent(ev) => {
        //         // TODO: fix this
        //         self.get_log_writer()?;

        //         self.file_transfer_man.file_sync_event(
        //             ev,
        //             peer_id,
        //             &mut self.file_watcher,
        //             self.log_writer.as_mut().unwrap(),
        //         )?;
        //     }
        //     _ => unreachable!(),
        // }
        // Ok(())
        todo!()
    }

    fn get_log_writer(&mut self) -> crate::Result<&mut TransactionLogWriter<File>> {
        match self.log_writer {
            Some(ref mut log_writer) => Ok(log_writer),
            None => {
                let log_writer = transaction_log::get_log_writer(&self.config.log_path)?;
                self.log_writer = Some(log_writer);
                Ok(self.log_writer.as_mut().unwrap())
            }
        }
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

    fn add_queue_event(&mut self, event: CarrierEvent, event_type: QueueEventType) {
        log::info!(
            "{} - adding event to queue: {} {}",
            self.node_id,
            event,
            event_type
        );
        self.events_queue.push_back((event, event_type));
    }

    fn schedule_next_sync_attempt(&self, attempt: u32) {
        let seconds = if attempt < 12 {
            2u64.pow(attempt)
        } else {
            3600
        };

        self.commands.after(
            CarrierEvent::StartSync(attempt + 1),
            Duration::from_secs(seconds),
        );
    }
}

fn get_storage_state(config: &Config, storage: &str) -> crate::Result<u64> {
    let storage_index = fs::walk_path(config, storage)?;
    Ok(fs::get_state_hash(storage_index.iter()))
}
