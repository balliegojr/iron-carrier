use simple_mdns::ServiceDiscovery;
use std::{
    collections::{HashMap, LinkedList},
    fs::File,
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use super::{
    connected_peers::ConnectedPeers, file_transfer_man::FileTransferMan, file_watcher::FileWatcher,
    send_message, synchronization_session::SynchronizationState, CarrierEvent, Origin,
    QueueEventType, COMMAND_MESSAGE, STREAM_MESSAGE,
};
use crate::{
    config::Config,
    fs::{self, FileInfo},
    hash_helper,
    sync::{broadcast_message_to, EventSupression, TRANSPORT_PROTOCOL},
    transaction_log::{self, EventStatus, EventType, TransactionLogWriter},
};

use message_io::{
    network::Endpoint,
    node::{self, NodeHandler},
};
use message_io::{network::NetEvent, node::NodeListener};

use rand::Rng;

enum StorageState {
    CurrentHash(u64),
    Sync,
    SyncByPeer(u64),
}

pub struct Synchronizer {
    node_id: u64,
    config: Arc<Config>,
    file_watcher: Option<FileWatcher>,
    service_discovery: Option<ServiceDiscovery>,
    handler: NodeHandler<CarrierEvent>,
    session_state: SynchronizationState,
    connected_peers: ConnectedPeers,
    file_transfer_man: FileTransferMan,
    events_queue: LinkedList<(CarrierEvent, QueueEventType)>,
    log_writer: Option<TransactionLogWriter<File>>,
    storage_state: HashMap<String, StorageState>,
}

impl Synchronizer {
    pub fn new(config: Config) -> crate::Result<Self> {
        let (handler, listener) = node::split::<CarrierEvent>();

        let config = Arc::new(config);
        let service_discovery = get_service_discovery(&config);
        let file_watcher = get_file_watcher(config.clone(), handler.clone());
        let connected_peers = ConnectedPeers::new(handler.clone());
        let file_transfer_man = FileTransferMan::new(handler.clone(), config.clone());

        let mut s = Synchronizer {
            node_id: hash_helper::get_node_id(config.port),
            config,
            file_watcher,
            service_discovery,
            handler,
            connected_peers,
            session_state: SynchronizationState::new(),
            file_transfer_man,
            events_queue: LinkedList::new(),
            log_writer: None,
            storage_state: HashMap::new(),
        };

        s.start(listener)?;
        Ok(s)
    }

    fn start(&mut self, listener: NodeListener<CarrierEvent>) -> crate::Result<()> {
        log::debug!("starting syncronizer");
        let mut rng = rand::thread_rng();
        let wait = rng.gen_range(3.0..5.0);
        self.handler
            .signals()
            .send_with_timer(CarrierEvent::StartSync, Duration::from_secs_f64(wait));

        for storage in self.config.paths.keys() {
            self.storage_state.insert(
                storage.clone(),
                StorageState::CurrentHash(self.get_storage_state(&storage)?),
            );
        }

        self.listen_for_events(listener)
    }
    fn listen_for_events(&mut self, listener: NodeListener<CarrierEvent>) -> crate::Result<()> {
        self.handler
            .network()
            .listen(TRANSPORT_PROTOCOL, format!("0.0.0.0:{}", self.config.port))
            .unwrap();

        listener.for_each(move |event| match event {
            node::NodeEvent::Network(net_event) => match net_event {
                NetEvent::Connected(endpoint, _) | NetEvent::Accepted(endpoint, _) => {
                    // Whenever we connect to a peer or accept a new connection, we send the node_id of this peer
                    log::info!("Peer connected:  {}", endpoint);
                    send_message(
                        &self.handler,
                        &CarrierEvent::SetPeerId(self.node_id),
                        endpoint,
                    );
                }
                // When we receive a network message, we first verify it's prefix
                // This is necessary to avoid calling bincode serde for every file block,
                // which was hurting performance really bad
                NetEvent::Message(endpoint, data) => match data[0] {
                    COMMAND_MESSAGE => {
                        match bincode::deserialize(&data[1..]) {
                            Ok(message) => {
                                if let Err(err) = self.handle_network_event(message, endpoint) {
                                    log::error!("Error processing event: {}", err);
                                }
                            }
                            Err(err) => {
                                log::error!("Invalid message: {:?}", err);
                            }
                        };
                    }
                    STREAM_MESSAGE => {
                        let usize_size = std::mem::size_of::<usize>();
                        let file_hash = bincode::deserialize(&data[1..9]).unwrap();
                        let block_index = bincode::deserialize(&data[9..9 + usize_size]).unwrap();
                        let buf = &data[17..];
                        if let Err(err) =
                            self.file_transfer_man
                                .handle_write_block(file_hash, block_index, buf)
                        {
                            log::error!("{}", err);
                        }
                    }
                    message_type => {
                        log::error!("Invalid message prefix {:?}", message_type);
                    }
                },
                NetEvent::Disconnected(endpoint) => {
                    // Whenever a peer disconnects, we remove it's id from the connected peers and verify if we should abort current sync
                    log::info!("Peer disconnected:  {}", endpoint);
                    self.connected_peers.remove_endpoint(endpoint);
                    self.abort_sync_if_no_more_peers();
                }
            },
            node::NodeEvent::Signal(signal_event) => {
                if let Err(err) = self.handle_signal(signal_event) {
                    log::error!("Error processing signal: {}", err);
                }
            }
        });

        Ok(())
    }

    fn handle_signal(&mut self, signal: CarrierEvent) -> crate::Result<()> {
        log::debug!("{} - Received Signal {} ", self.node_id, signal);
        match signal {
            CarrierEvent::StartSync => {
                let addresses = self.get_peer_addresses();
                if addresses.is_empty() {
                    return Ok(());
                }

                if self
                    .storage_state
                    .values()
                    .all(|state| !matches!(state, &StorageState::CurrentHash(_)))
                {
                    return Ok(());
                }

                self.add_queue_event(CarrierEvent::ExchangeStorageStates, QueueEventType::Signal);
                self.connected_peers.connect_all(addresses);
            }
            CarrierEvent::ExchangeStorageStates => {
                let state = self
                    .storage_state
                    .iter()
                    .filter_map(|(storage, state)| match state {
                        StorageState::CurrentHash(state) => Some((storage.clone(), *state)),
                        _ => None,
                    })
                    .collect();

                let expected_replies = broadcast_message_to(
                    &self.handler,
                    CarrierEvent::QueryOutOfSyncStorages(state),
                    self.connected_peers.get_all_identified_endpoints(),
                );

                self.session_state
                    .start_sync_after_replies(expected_replies);
            }
            CarrierEvent::EndSync => {
                let storages: Vec<String> = self
                    .storage_state
                    .iter()
                    .filter(|(_, state)| matches!(**state, StorageState::Sync))
                    .map(|(storage, _)| storage.clone())
                    .collect();

                if storages.is_empty() {
                    self.handler
                        .signals()
                        .send_with_timer(CarrierEvent::CloseConnections, Duration::from_secs(15));
                    return Ok(());
                }

                for storage in storages {
                    let state = StorageState::CurrentHash(self.get_storage_state(&storage)?);
                    self.storage_state.entry(storage).and_modify(|v| *v = state);
                }

                broadcast_message_to(
                    &self.handler,
                    CarrierEvent::EndSync,
                    self.connected_peers.get_all_identified_endpoints(),
                );

                self.handler
                    .signals()
                    .send(CarrierEvent::ExchangeStorageStates);
            }
            CarrierEvent::CloseConnections => {
                if !self.file_transfer_man.has_pending_transfers() {
                    self.connected_peers.disconnect_all();
                } else {
                    self.handler
                        .signals()
                        .send_with_timer(CarrierEvent::CloseConnections, Duration::from_secs(60));
                }
            }
            CarrierEvent::SyncNextStorage => match self.session_state.get_next_storage() {
                Some((storage, peers)) => {
                    self.handler
                        .signals()
                        .send(CarrierEvent::BuildStorageIndex(storage.clone()));

                    let peers: Vec<Endpoint> = peers
                        .into_iter()
                        .filter_map(|peer_id| self.connected_peers.get_peer_endpoint(peer_id))
                        .collect();

                    broadcast_message_to(
                        &self.handler,
                        CarrierEvent::BuildStorageIndex(storage),
                        peers.iter(),
                    );
                }
                None => {
                    self.handler.signals().send(CarrierEvent::EndSync);
                }
            },
            CarrierEvent::BuildStorageIndex(storage) => {
                log::info!("Building index for {}", storage);
                let index = fs::walk_path(&self.config, &storage).unwrap();
                log::debug!("Read {} files ", index.len());

                self.handler
                    .signals()
                    .send(CarrierEvent::SetStorageIndex(index));
            }
            CarrierEvent::SetStorageIndex(index) => {
                let origin = Origin::Initiator;

                log::debug!("Setting index with origin {:?}", origin);

                let have_all_indexes = self.session_state.set_storage_index(origin, index);
                if have_all_indexes {
                    let mut event_queue = self.session_state.get_event_queue();
                    log::debug!(
                        "{} - there are {} actions to sync",
                        self.node_id,
                        event_queue.len()
                    );
                    log::debug!("{:?}", event_queue);
                    self.events_queue.append(&mut event_queue);
                    self.handler.signals().send(CarrierEvent::ConsumeSyncQueue);
                }
            }
            CarrierEvent::ConsumeSyncQueue => {
                let action = self.events_queue.pop_front();
                log::debug!("{} - next queue action {:?}", self.node_id, action);
                match action {
                    Some((event, notification_type)) => match notification_type {
                        QueueEventType::Signal => {
                            self.handler.signals().send(event);
                        }
                        QueueEventType::Peer(peer_id) => {
                            match self.connected_peers.get_peer_endpoint(peer_id) {
                                Some(endpoint) => send_message(&self.handler, &event, endpoint),
                                None => {
                                    log::debug!("peer is offline, cant receive message");
                                    self.handler.signals().send(CarrierEvent::ConsumeSyncQueue);
                                }
                            }
                        }
                        QueueEventType::Broadcast => {
                            broadcast_message_to(
                                &self.handler,
                                event,
                                self.connected_peers.get_all_identified_endpoints(),
                            );
                            self.handler.signals().send(CarrierEvent::ConsumeSyncQueue);
                        }
                    },
                    None => {
                        if !self.file_transfer_man.has_pending_transfers() {
                            self.handler.signals().send(CarrierEvent::CloseConnections);
                        }
                    }
                }
            }
            CarrierEvent::DeleteFile(file) => {
                self.delete_file(&file)?;
                self.handler.signals().send(CarrierEvent::ConsumeSyncQueue);
            }
            CarrierEvent::SendFile(file_info, peer_id, is_new) => {
                match self.connected_peers.get_peer_endpoint(peer_id) {
                    Some(endpoint) => {
                        self.file_transfer_man
                            .send_file_to_peer(file_info, endpoint, is_new, false)?;
                    }
                    None => {
                        self.handler.signals().send(CarrierEvent::ConsumeSyncQueue);
                    }
                }
            }
            CarrierEvent::BroadcastFile(file_info, is_new) => {
                for peer in self.connected_peers.get_all_identified_endpoints() {
                    self.file_transfer_man.send_file_to_peer(
                        file_info.clone(),
                        *peer,
                        is_new,
                        false,
                    )?;
                }

                self.handler.signals().send(CarrierEvent::ConsumeSyncQueue);
            }
            CarrierEvent::MoveFile(src, dst) => {
                self.move_file(&src, &dst)?;
                self.handler.signals().send(CarrierEvent::ConsumeSyncQueue);
            }
            CarrierEvent::FileWatcherEvent(watcher_event) => {
                let addresses = self.get_peer_addresses();
                if addresses.is_empty() {
                    return Ok(());
                }

                let (event, event_type) = match watcher_event {
                    super::WatcherEvent::Created(file_info) => {
                        self.get_log_writer()?.append(
                            file_info.alias.clone(),
                            EventType::Write(file_info.path.clone()),
                            EventStatus::Finished,
                        )?;

                        (
                            CarrierEvent::BroadcastFile(file_info, true),
                            QueueEventType::Signal,
                        )
                    }
                    super::WatcherEvent::Updated(file_info) => {
                        self.get_log_writer()?.append(
                            file_info.alias.clone(),
                            EventType::Write(file_info.path.clone()),
                            EventStatus::Finished,
                        )?;

                        (
                            CarrierEvent::BroadcastFile(file_info, false),
                            QueueEventType::Signal,
                        )
                    }

                    super::WatcherEvent::Moved(src, dst) => {
                        self.get_log_writer()?.append(
                            src.alias.clone(),
                            EventType::Move(src.path.clone(), dst.path.clone()),
                            EventStatus::Finished,
                        )?;

                        (CarrierEvent::MoveFile(src, dst), QueueEventType::Broadcast)
                    }
                    super::WatcherEvent::Deleted(file_info) => {
                        self.get_log_writer()?.append(
                            file_info.alias.clone(),
                            EventType::Delete(file_info.path.clone()),
                            EventStatus::Finished,
                        )?;

                        (
                            CarrierEvent::DeleteFile(file_info),
                            QueueEventType::Broadcast,
                        )
                    }
                };

                self.add_queue_event(event, event_type);
                self.connected_peers.connect_all(addresses);
                self.get_log_writer()?;
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    fn handle_network_event(
        &mut self,
        event: CarrierEvent,
        endpoint: Endpoint,
    ) -> crate::Result<()> {
        log::debug!("{} - Received Event {} ", self.node_id, event);
        match event {
            CarrierEvent::EndSync => {
                let peer_id = self.connected_peers.get_peer_id(endpoint).unwrap();
                let storages: Vec<String> = self
                    .storage_state
                    .iter()
                    .filter(|(_, state)| match state {
                        StorageState::SyncByPeer(p) => *p == peer_id,
                        _ => false,
                    })
                    .map(|(storage, _)| storage.clone())
                    .collect();

                for storage in storages {
                    let state = StorageState::CurrentHash(self.get_storage_state(&storage)?);
                    self.storage_state.entry(storage).and_modify(|v| *v = state);
                }
            }
            CarrierEvent::SetPeerId(peer_id) => {
                self.connected_peers.add_peer(endpoint, peer_id);
            }
            CarrierEvent::QueryOutOfSyncStorages(mut storages) => {
                // keep only the storages that exists in this peer and have a different hash
                storages.retain(|storage, peer_storage_hash| {
                    match self.storage_state.get(storage) {
                        Some(&StorageState::CurrentHash(hash)) => hash != *peer_storage_hash,
                        _ => false,
                    }
                });

                let storages: Vec<String> = storages.into_iter().map(|(key, _)| key).collect();

                let peer_id = self.connected_peers.get_peer_id(endpoint).unwrap();
                // Changes the storages state to SyncByPeer, to avoid double sync and premature connection close
                storages.iter().for_each(|storage| {
                    self.storage_state
                        .entry(storage.clone())
                        .and_modify(|v| *v = StorageState::SyncByPeer(peer_id));
                });

                send_message(
                    &self.handler,
                    &CarrierEvent::ReplyOutOfSyncStorages(storages),
                    endpoint,
                );
            }
            CarrierEvent::ReplyOutOfSyncStorages(mut storages) => {
                storages.retain(|storage| match self.storage_state.get(storage) {
                    Some(state) => !matches!(state, &StorageState::SyncByPeer(_)),
                    None => false,
                });

                storages.iter().for_each(|storage| {
                    let state = self.storage_state.get_mut(storage).unwrap();
                    *state = StorageState::Sync;
                });

                let have_all_replies = self.session_state.add_storages_to_sync(
                    storages,
                    self.connected_peers.get_peer_id(endpoint).unwrap(),
                );

                if have_all_replies {
                    self.handler.signals().send(CarrierEvent::SyncNextStorage);
                }
            }
            CarrierEvent::BuildStorageIndex(storage) => {
                log::info!("Building index for {}", storage);
                if !self.config.paths.contains_key(&storage) {
                    log::error!("There is no such storage: {}", &storage);
                    // TODO: panic?
                    return Ok(());
                }

                let index = fs::walk_path(&self.config, &storage).unwrap();
                log::debug!("Read {} files ", index.len());

                send_message(
                    &self.handler,
                    &CarrierEvent::SetStorageIndex(index),
                    endpoint,
                );
            }
            CarrierEvent::SetStorageIndex(index) => {
                let origin = Origin::Peer(self.connected_peers.get_peer_id(endpoint).unwrap());

                log::debug!("Setting index with origin {:?}", origin);

                let have_all_indexes = self.session_state.set_storage_index(origin, index);
                if have_all_indexes {
                    let mut event_queue = self.session_state.get_event_queue();
                    log::debug!(
                        "{} - there are {} actions to sync",
                        self.node_id,
                        event_queue.len()
                    );
                    log::debug!("{:?}", event_queue);
                    self.events_queue.append(&mut event_queue);
                    self.handler.signals().send(CarrierEvent::ConsumeSyncQueue);
                }
            }
            CarrierEvent::DeleteFile(file) => {
                self.delete_file(&file)?;

                if let Some(ref mut file_watcher) = self.file_watcher {
                    file_watcher.supress_next_event(file, EventSupression::Delete);
                }
            }
            CarrierEvent::RequestFile(file_info, is_new) => {
                self.file_transfer_man
                    .send_file_to_peer(file_info, endpoint, is_new, true)?;
            }
            CarrierEvent::MoveFile(src, dst) => {
                self.move_file(&src, &dst)?;
                if let Some(ref mut file_watcher) = self.file_watcher {
                    file_watcher.supress_next_event(src, EventSupression::Rename);
                }
            }
            CarrierEvent::FileSyncEvent(ev) => {
                // TODO: fix this
                self.get_log_writer()?;

                self.file_transfer_man.file_sync_event(
                    ev,
                    endpoint,
                    &mut self.file_watcher,
                    self.log_writer.as_mut().unwrap(),
                )?;
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    fn get_storage_state(&self, storage: &str) -> crate::Result<u64> {
        let storage_index = fs::walk_path(&self.config, storage)?;
        Ok(fs::get_state_hash(storage_index.iter()))
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
        let event_status = match fs::delete_file(&file, &self.config) {
            Ok(_) => EventStatus::Finished,
            Err(err) => {
                log::error!("Failed to delete file {}", err);
                EventStatus::Failed
            }
        };
        self.get_log_writer()?.append(
            file.alias.to_string(),
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
            src.alias.clone(),
            EventType::Move(src.path.clone(), dest.path.clone()),
            event_status,
        )?;

        Ok(())
    }

    fn abort_sync_if_no_more_peers(&mut self) {
        if !self.connected_peers.has_connected_peers() {
            self.events_queue.clear();
            self.handler.signals().send(CarrierEvent::EndSync);
        }
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

    fn get_peer_addresses(&self) -> std::collections::HashSet<SocketAddr> {
        let from_sd = self
            .service_discovery
            .iter()
            .flat_map(|sd| sd.get_known_services());
        let from_config = self
            .config
            .peers
            .iter()
            .flat_map(|peers| peers.iter().copied());

        from_sd.chain(from_config).collect()
    }
}

fn get_file_watcher(
    config: Arc<Config>,
    handler: NodeHandler<CarrierEvent>,
) -> Option<FileWatcher> {
    if config.enable_file_watcher {
        FileWatcher::new(handler, config).ok()
    } else {
        None
    }
}

fn get_service_discovery(config: &Config) -> Option<ServiceDiscovery> {
    if !config.enable_service_discovery {
        return None;
    }

    let mut sd = ServiceDiscovery::new("_ironcarrier._tcp.local", 600).unwrap();
    sd.add_port(config.port);
    log::debug!("Registered port: {}", config.port);
    for addr in get_my_ips()? {
        sd.add_ip_address(addr);
        log::debug!("Registered address: {:?}", addr);
    }

    Some(sd)
}
fn get_my_ips() -> Option<Vec<IpAddr>> {
    let addrs = if_addrs::get_if_addrs()
        .ok()?
        .iter()
        .filter_map(|iface| {
            if iface.addr.is_loopback() {
                None
            } else {
                Some(iface.addr.ip())
            }
        })
        .collect();

    Some(addrs)
}
