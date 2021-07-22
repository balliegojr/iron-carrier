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
    send_message, synchronization_session::SynchronizationSession, CarrierEvent, Origin,
    QueueEventType, COMMAND_MESSAGE, STREAM_MESSAGE,
};
use crate::{
    config::Config,
    fs::{self, FileInfo},
    hash_helper,
    sync::{broadcast_message_to, SyncType, TRANSPORT_PROTOCOL},
    transaction_log::{self, EventStatus, EventType, TransactionLogWriter},
};

use message_io::{
    network::Endpoint,
    node::{self, NodeHandler},
};
use message_io::{network::NetEvent, node::NodeListener};

use rand::Rng;

#[derive(Debug, Eq, PartialEq)]
enum NodeStatus {
    Idle,
    BusyFullSync(bool),
    BusyPartialSync(bool),
}

#[derive(Debug, Eq, PartialEq, Hash)]
enum SyncStep {
    PeerIdentification,
    PeerSyncReply,
}

pub struct Synchronizer {
    node_id: u64,
    config: Arc<Config>,
    node_status: NodeStatus,
    sync_steps: HashMap<SyncStep, usize>,
    file_watcher: Option<FileWatcher>,
    service_discovery: Option<ServiceDiscovery>,
    handler: NodeHandler<CarrierEvent>,
    session_state: Option<SynchronizationSession>,
    connected_peers: ConnectedPeers,
    file_transfer_man: FileTransferMan,
    events_queue: LinkedList<(CarrierEvent, QueueEventType)>,
    log_writer: Option<TransactionLogWriter<File>>,
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
            node_status: NodeStatus::Idle,
            sync_steps: HashMap::new(),
            handler,
            connected_peers,
            session_state: None,
            file_transfer_man,
            events_queue: LinkedList::new(),
            log_writer: None,
        };

        s.start(listener)?;
        Ok(s)
    }

    fn start(&mut self, listener: NodeListener<CarrierEvent>) -> crate::Result<()> {
        log::debug!("starting syncronizer");
        let mut rng = rand::thread_rng();
        let wait = rng.gen_range(3.0..5.0);

        self.handler.signals().send_with_timer(
            CarrierEvent::StartSync(SyncType::Full),
            Duration::from_secs_f64(wait),
        );

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
            CarrierEvent::StartSync(sync_type) => {
                if self.node_status != NodeStatus::Idle {
                    return Ok(());
                }

                let addresses = self.get_peer_addresses();
                if addresses.is_empty() {
                    return Ok(());
                }

                match sync_type {
                    SyncType::Full => {
                        if !self.set_node_status(NodeStatus::BusyFullSync(true)) {
                            return Ok(());
                        }

                        let storages = self.config.paths.keys().cloned().collect();
                        self.session_state = Some(SynchronizationSession::new(storages));
                        self.file_watcher = None;

                        self.add_queue_event(
                            CarrierEvent::StartSync(SyncType::Full),
                            QueueEventType::BroadcastAndWait,
                        );

                        self.add_queue_event(CarrierEvent::SyncNextStorage, QueueEventType::Signal);
                    }
                    SyncType::Partial => {
                        if !self.set_node_status(NodeStatus::BusyPartialSync(true)) {
                            return Ok(());
                        }

                        self.events_queue.push_front((
                            CarrierEvent::StartSync(SyncType::Partial),
                            QueueEventType::BroadcastAndWait,
                        ));
                    }
                }

                self.sync_steps = HashMap::new();
                self.sync_steps
                    .insert(SyncStep::PeerIdentification, addresses.len());
                self.sync_steps
                    .insert(SyncStep::PeerSyncReply, addresses.len());
                self.connected_peers.connect_all(addresses);
            }
            CarrierEvent::EndSync => {
                let sync_type = match self.node_status {
                    NodeStatus::Idle => {
                        return Ok(());
                    }
                    NodeStatus::BusyFullSync(_) => {
                        self.session_state = None;
                        self.file_watcher =
                            get_file_watcher(self.config.clone(), self.handler.clone());
                        SyncType::Full
                    }
                    NodeStatus::BusyPartialSync(is_initiator) => {
                        if is_initiator && !self.events_queue.is_empty() {
                            self.handler.signals().send(CarrierEvent::ConsumeSyncQueue);
                            return Ok(());
                        }
                        SyncType::Partial
                    }
                };

                let mut log_writer = self.log_writer.take().unwrap();
                self.connected_peers
                    .get_all_identifications()
                    .try_for_each(|id| {
                        log_writer.append(EventType::Sync(sync_type, *id), EventStatus::Finished)
                    })?;

                broadcast_message_to(
                    &self.handler,
                    CarrierEvent::EndSync,
                    self.connected_peers.get_all_identified_endpoints(),
                );
                self.set_node_status(NodeStatus::Idle);
                self.handler
                    .signals()
                    .send_with_timer(CarrierEvent::CloseConnections, Duration::from_secs(15));
            }
            CarrierEvent::CloseConnections => {
                if self.node_status == NodeStatus::Idle {
                    self.connected_peers.disconnect_all();
                } else {
                    self.handler
                        .signals()
                        .send_with_timer(CarrierEvent::CloseConnections, Duration::from_secs(60));
                }
            }
            CarrierEvent::SyncNextStorage => {
                match self.session_state.as_mut().unwrap().get_next_storage() {
                    Some(storage) => {
                        self.handler
                            .signals()
                            .send(CarrierEvent::BuildStorageIndex(storage.clone()));
                        broadcast_message_to(
                            &self.handler,
                            CarrierEvent::BuildStorageIndex(storage),
                            self.connected_peers.get_all_identified_endpoints(),
                        );
                    }
                    None => {
                        self.handler.signals().send(CarrierEvent::EndSync);
                    }
                }
            }
            CarrierEvent::BuildStorageIndex(storage) => {
                log::info!("Building index for {}", storage);
                let index = fs::walk_path(&self.config, &storage).unwrap();
                log::debug!("Read {} files ", index.len());

                self.handler
                    .signals()
                    .send(CarrierEvent::SetStorageIndex(Some(index)));
            }
            CarrierEvent::SetStorageIndex(index) => {
                let origin = Origin::Initiator;

                log::debug!("Setting index with origin {:?}", origin);

                let session = self.session_state.as_mut().unwrap();
                session.set_storage_index(origin, index);

                let peers: Vec<u64> = self
                    .connected_peers
                    .get_all_identifications()
                    .copied()
                    .collect();

                if session.have_index_for_peers(&peers[..]) {
                    let mut event_queue = session.get_event_queue(&peers[..]);
                    self.events_queue.append(&mut event_queue);
                    self.handler.signals().send(CarrierEvent::ConsumeSyncQueue);
                }
            }
            CarrierEvent::ConsumeSyncQueue => {
                let action = self.events_queue.pop_front();
                log::debug!("next queue action {:?}", action);
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
                        QueueEventType::BroadcastAndWait => broadcast_message_to(
                            &self.handler,
                            event,
                            self.connected_peers.get_all_identified_endpoints(),
                        ),
                    },
                    None => {
                        let is_initiator = match self.node_status {
                            NodeStatus::Idle => false,
                            NodeStatus::BusyFullSync(is_initiator)
                            | NodeStatus::BusyPartialSync(is_initiator) => is_initiator,
                        };

                        if is_initiator && !self.file_transfer_man.has_pending_transfers() {
                            self.handler.signals().send(CarrierEvent::EndSync);
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
                            .send_file_to_peer(file_info, endpoint, is_new)?;
                    }
                    None => {
                        self.handler.signals().send(CarrierEvent::ConsumeSyncQueue);
                    }
                }
            }
            CarrierEvent::BroadcastFile(file_info, is_new) => {
                for peer in self.connected_peers.get_all_identified_endpoints() {
                    self.file_transfer_man
                        .send_file_to_peer(file_info.clone(), *peer, is_new)?;
                }

                self.handler.signals().send(CarrierEvent::ConsumeSyncQueue);
            }
            CarrierEvent::MoveFile(src, dst) => {
                self.move_file(&src, &dst)?;
                self.handler.signals().send(CarrierEvent::ConsumeSyncQueue);
            }
            CarrierEvent::FileWatcherEvent(watcher_event) => {
                let (event, event_type) = match watcher_event {
                    super::WatcherEvent::Created(file_info) => {
                        self.get_log_writer()?.append(
                            EventType::FileWrite(file_info.alias.clone(), file_info.path.clone()),
                            EventStatus::Finished,
                        )?;

                        (
                            CarrierEvent::BroadcastFile(file_info, true),
                            QueueEventType::Signal,
                        )
                    }
                    super::WatcherEvent::Updated(file_info) => {
                        self.get_log_writer()?.append(
                            EventType::FileWrite(file_info.alias.clone(), file_info.path.clone()),
                            EventStatus::Finished,
                        )?;

                        (
                            CarrierEvent::BroadcastFile(file_info, false),
                            QueueEventType::Signal,
                        )
                    }

                    super::WatcherEvent::Moved(src, dst) => {
                        self.get_log_writer()?.append(
                            EventType::FileMove(
                                src.alias.clone(),
                                src.path.clone(),
                                dst.path.clone(),
                            ),
                            EventStatus::Finished,
                        )?;

                        (CarrierEvent::MoveFile(src, dst), QueueEventType::Broadcast)
                    }
                    super::WatcherEvent::Deleted(file_info) => {
                        self.get_log_writer()?.append(
                            EventType::FileDelete(file_info.alias.clone(), file_info.path.clone()),
                            EventStatus::Finished,
                        )?;

                        (
                            CarrierEvent::DeleteFile(file_info),
                            QueueEventType::Broadcast,
                        )
                    }
                };

                self.add_queue_event(event, event_type);
                if self.node_status == NodeStatus::Idle {
                    self.handler
                        .signals()
                        .send(CarrierEvent::StartSync(SyncType::Partial));
                }
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
            CarrierEvent::StartSync(sync_type) => {
                if self.node_status != NodeStatus::Idle {
                    send_message(
                        &self.handler,
                        &CarrierEvent::StartSyncReply(false),
                        endpoint,
                    );
                    return Ok(());
                }

                match sync_type {
                    SyncType::Full => {
                        self.set_node_status(NodeStatus::BusyFullSync(false));
                        self.file_watcher = None;
                    }
                    SyncType::Partial => {
                        self.set_node_status(NodeStatus::BusyPartialSync(false));
                    }
                }
                let peer_id = self.connected_peers.get_peer_id(endpoint).unwrap();
                self.get_log_writer()?
                    .append(EventType::Sync(sync_type, peer_id), EventStatus::Started)?;

                send_message(&self.handler, &CarrierEvent::StartSyncReply(true), endpoint);
            }
            CarrierEvent::EndSync => {
                let sync_type = match self.node_status {
                    NodeStatus::Idle => {
                        return Ok(());
                    }
                    NodeStatus::BusyFullSync(_) => {
                        self.session_state = None;
                        self.file_watcher =
                            get_file_watcher(self.config.clone(), self.handler.clone());
                        SyncType::Full
                    }
                    NodeStatus::BusyPartialSync(_) => SyncType::Partial,
                };

                let peer_id = self.connected_peers.get_peer_id(endpoint).unwrap();
                self.get_log_writer()?
                    .append(EventType::Sync(sync_type, peer_id), EventStatus::Finished)?;

                self.set_node_status(NodeStatus::Idle);
                self.handler
                    .signals()
                    .send_with_timer(CarrierEvent::CloseConnections, Duration::from_secs(15));
            }
            CarrierEvent::SetPeerId(peer_id) => {
                self.connected_peers.add_peer(endpoint, peer_id);
                if self.node_status != NodeStatus::Idle {
                    self.increment_step(SyncStep::PeerIdentification);
                }
            }
            CarrierEvent::StartSyncReply(accepted) => {
                if !accepted {
                    self.connected_peers.remove_endpoint(endpoint);
                }

                let sync_type = match self.node_status {
                    NodeStatus::Idle => unreachable!(),
                    NodeStatus::BusyFullSync(_) => SyncType::Full,
                    NodeStatus::BusyPartialSync(_) => SyncType::Partial,
                };

                let peer_id = self.connected_peers.get_peer_id(endpoint).unwrap();
                self.get_log_writer()?
                    .append(EventType::Sync(sync_type, peer_id), EventStatus::Started)?;

                self.increment_step(SyncStep::PeerSyncReply);
            }
            CarrierEvent::BuildStorageIndex(storage) => {
                log::info!("Building index for {}", storage);
                let index = match self.config.paths.contains_key(&storage) {
                    true => {
                        let index = fs::walk_path(&self.config, &storage).unwrap();
                        log::debug!("Read {} files ", index.len());

                        Some(index)
                    }
                    false => {
                        log::debug!("There is no such storage: {}", &storage);
                        None
                    }
                };

                send_message(
                    &self.handler,
                    &CarrierEvent::SetStorageIndex(index),
                    endpoint,
                );
            }
            CarrierEvent::SetStorageIndex(index) => {
                let origin = Origin::Peer(self.connected_peers.get_peer_id(endpoint).unwrap());

                log::debug!("Setting index with origin {:?}", origin);

                let session = self.session_state.as_mut().unwrap();
                session.set_storage_index(origin, index);

                let peers: Vec<u64> = self
                    .connected_peers
                    .get_all_identifications()
                    .copied()
                    .collect();
                if session.have_index_for_peers(&peers[..]) {
                    let mut event_queue = session.get_event_queue(&peers[..]);
                    self.events_queue.append(&mut event_queue);
                    self.handler.signals().send(CarrierEvent::ConsumeSyncQueue);
                }
            }
            CarrierEvent::DeleteFile(file) => {
                self.delete_file(&file)?;

                if let Some(ref mut file_watcher) = self.file_watcher {
                    file_watcher.supress_next_event(file);
                }
            }
            CarrierEvent::RequestFile(file_info, is_new) => {
                self.file_transfer_man
                    .send_file_to_peer(file_info, endpoint, is_new)?;
            }
            CarrierEvent::MoveFile(src, dst) => {
                self.move_file(&src, &dst)?;
                if let Some(ref mut file_watcher) = self.file_watcher {
                    file_watcher.supress_next_event(src);
                }
            }
            CarrierEvent::FileSyncEvent(ev) => {
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
            EventType::FileDelete(file.alias.to_string(), file.path.clone()),
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
            EventType::FileMove(src.alias.clone(), src.path.clone(), dest.path.clone()),
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

    fn increment_step(&mut self, step: SyncStep) {
        let c = self
            .sync_steps
            .get_mut(&step)
            .expect("There should be a increment step at this point");
        *c -= 1;

        if *c == 0 {
            self.handler.signals().send(CarrierEvent::ConsumeSyncQueue);
        }
    }

    fn set_node_status(&mut self, new_status: NodeStatus) -> bool {
        let is_valid_state_change = match &self.node_status {
            NodeStatus::Idle => match new_status {
                NodeStatus::Idle => false,
                NodeStatus::BusyFullSync(_) | NodeStatus::BusyPartialSync(_) => true,
            },
            NodeStatus::BusyFullSync(_) | NodeStatus::BusyPartialSync(_) => {
                matches!(new_status, NodeStatus::Idle)
            }
        };

        if is_valid_state_change {
            log::info!(
                "{} - Node Status: {:?} -> {:?}",
                self.node_id,
                self.node_status,
                new_status
            );
            self.node_status = new_status;
        } else {
            log::info!(
                "{} - Node Status: Invalid transition, node is already {:?}",
                self.node_id,
                self.node_status
            );
        }

        is_valid_state_change
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
