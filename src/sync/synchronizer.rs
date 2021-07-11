use simple_mdns::ServiceDiscovery;
use std::{
    collections::{HashMap, LinkedList},
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use super::{
    connected_peers::ConnectedPeers, file_transfer_man::FileTransferMan, file_watcher::FileWatcher,
    send_message, synchronization_session::SynchronizationSession, CarrierEvent, Origin,
    QueueEventType,
};
use crate::{
    config::Config,
    fs::{self, FileInfo},
    hash_helper,
    sync::SyncType,
};

use message_io::{
    network::Endpoint,
    node::{self, NodeHandler},
};
use message_io::{
    network::{NetEvent, Transport},
    node::NodeListener,
};

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
    received_file_events: HashMap<FileInfo, u64>,
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
            received_file_events: HashMap::new(),
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
            .listen(
                Transport::FramedTcp,
                format!("0.0.0.0:{}", self.config.port),
            )
            .unwrap();

        // Read incoming network events.
        listener.for_each(move |event| match event {
            node::NodeEvent::Network(net_event) => match net_event {
                NetEvent::Connected(endpoint, _) | NetEvent::Accepted(endpoint, _) => {
                    log::debug!("Peer connected:  {}", endpoint);
                    send_message(
                        &self.handler,
                        &CarrierEvent::SetPeerId(self.node_id),
                        endpoint,
                    );
                }
                NetEvent::Message(endpoint, data) => {
                    match bincode::deserialize(data) {
                        Ok(message) => {
                            if let Err(err) = self.process_event(message, Some(endpoint)) {
                                log::error!("Error processing event: {}", err);
                            }
                        }
                        Err(err) => {
                            log::error!("Invalid message: {:?}", err);
                        }
                    };
                }
                NetEvent::Disconnected(endpoint) => {
                    log::debug!("Peer disconnected:  {}", endpoint);
                    if let Some(session) = self.session_state.as_mut() {
                        if let Some(id) = self.connected_peers.get_peer_id(endpoint) {
                            session.remove_peer_from_session(id)
                        }
                    }
                    self.connected_peers.remove_endpoint(endpoint);
                    self.abort_sync_if_no_more_peers();
                }
            },
            node::NodeEvent::Signal(signal_event) => {
                if let Err(err) = self.process_event(signal_event, None) {
                    log::error!("Error processing event: {}", err);
                }
            }
        });

        Ok(())
    }

    fn process_event(
        &mut self,
        event: CarrierEvent,
        endpoint: Option<Endpoint>,
    ) -> crate::Result<()> {
        log::debug!(
            "{} - Received {} {} ",
            self.node_id,
            if endpoint.is_none() {
                "Signal"
            } else {
                "Event"
            },
            event
        );
        match event {
            CarrierEvent::SetPeerId(peer_id) => {
                self.connected_peers.add_peer(endpoint.unwrap(), peer_id);
                if self.node_status != NodeStatus::Idle {
                    self.increment_step(SyncStep::PeerIdentification);
                }
            }
            CarrierEvent::StartSync(sync_type) => match (sync_type, endpoint) {
                (super::SyncType::Full, None) => {
                    if self.set_node_status(NodeStatus::BusyFullSync(true)) {
                        let addresses = self.get_peer_addresses();

                        if addresses.is_empty() {
                            self.set_node_status(NodeStatus::Idle);
                        } else {
                            let storages = self.config.paths.keys().cloned().collect();
                            self.session_state = Some(SynchronizationSession::new(storages));
                            self.file_watcher = None;
                            self.sync_steps = HashMap::new();
                            self.sync_steps
                                .insert(SyncStep::PeerIdentification, addresses.len());
                            self.sync_steps
                                .insert(SyncStep::PeerSyncReply, addresses.len());
                            self.connected_peers.connect_all(addresses);

                            self.add_queue_event(
                                CarrierEvent::StartSync(SyncType::Full),
                                QueueEventType::BroadcastAndWait,
                            );

                            self.add_queue_event(
                                CarrierEvent::SyncNextStorage,
                                QueueEventType::Signal,
                            );
                        }
                    }
                }
                (super::SyncType::Full, Some(endpoint)) => {
                    if self.set_node_status(NodeStatus::BusyFullSync(false)) {
                        send_message(&self.handler, &CarrierEvent::StartSyncReply(true), endpoint);
                        self.file_watcher = None;
                    } else {
                        send_message(
                            &self.handler,
                            &CarrierEvent::StartSyncReply(false),
                            endpoint,
                        );
                    }
                }
                (super::SyncType::Partial, None) => {
                    if self.set_node_status(NodeStatus::BusyPartialSync(true)) {
                        let addresses = self.get_peer_addresses();
                        self.sync_steps = HashMap::new();
                        self.sync_steps
                            .insert(SyncStep::PeerIdentification, addresses.len());
                        self.sync_steps
                            .insert(SyncStep::PeerSyncReply, addresses.len());
                        self.connected_peers.connect_all(addresses);

                        self.events_queue.push_front((
                            CarrierEvent::StartSync(SyncType::Partial),
                            QueueEventType::BroadcastAndWait,
                        ));
                    }
                }
                (super::SyncType::Partial, Some(endpoint)) => {
                    if self.set_node_status(NodeStatus::BusyPartialSync(false)) {
                        send_message(&self.handler, &CarrierEvent::StartSyncReply(true), endpoint);
                    } else {
                        send_message(
                            &self.handler,
                            &CarrierEvent::StartSyncReply(false),
                            endpoint,
                        );
                    }
                }
            },
            CarrierEvent::EndSync => {
                match self.node_status {
                    NodeStatus::Idle => {
                        return Ok(());
                    }
                    NodeStatus::BusyFullSync(_) => {
                        self.session_state = None;
                        self.file_watcher =
                            get_file_watcher(self.config.clone(), self.handler.clone());
                    }
                    NodeStatus::BusyPartialSync(initiator) => {
                        if initiator && !self.events_queue.is_empty() {
                            self.handler.signals().send(CarrierEvent::ConsumeSyncQueue);
                            return Ok(());
                        }
                    }
                }

                if endpoint.is_none() {
                    self.broadcast_message_to(
                        CarrierEvent::EndSync,
                        self.connected_peers.get_all_identified_endpoints(),
                    );
                }
                self.set_node_status(NodeStatus::Idle);
                self.handler
                    .signals()
                    .send_with_timer(CarrierEvent::CloseConnections, Duration::from_secs(15));
            }

            CarrierEvent::CloseConnections => {
                if self.node_status == NodeStatus::Idle {
                    self.connected_peers.disconnect_all();
                } else {
                    self.handler.signals().send_with_timer(
                        CarrierEvent::CloseConnections,
                        Duration::from_secs(1 * 60),
                    );
                }
            }

            CarrierEvent::BuildStorageIndex(storage) => {
                log::info!("Building index for {}", storage);
                match self.config.paths.get(&storage) {
                    Some(storage_path) => {
                        let index = fs::walk_path(&storage_path, &storage).unwrap();

                        log::debug!("Read {} files ", index.len());

                        match endpoint {
                            Some(endpoint) => {
                                send_message(
                                    &self.handler,
                                    &CarrierEvent::SetStorageIndex(Some(index)),
                                    endpoint,
                                );
                            }
                            None => self
                                .handler
                                .signals()
                                .send(CarrierEvent::SetStorageIndex(Some(index))),
                        };
                    }
                    None => {
                        log::debug!("There is no such storage: {}", &storage);
                        send_message(
                            &self.handler,
                            &CarrierEvent::SetStorageIndex(None),
                            endpoint.unwrap(),
                        );
                    }
                }
            }
            CarrierEvent::StartSyncReply(accepted) => {
                if accepted {
                    if let NodeStatus::BusyFullSync(_) = self.node_status {
                        let session = self.session_state.as_mut().unwrap();
                        if let Some(peer_id) = self.connected_peers.get_peer_id(endpoint.unwrap()) {
                            session.add_peer_to_session(peer_id);
                        }
                    }
                } else {
                    self.connected_peers.remove_endpoint(endpoint.unwrap());
                }

                self.increment_step(SyncStep::PeerSyncReply);
            }
            CarrierEvent::SyncNextStorage => {
                match self.session_state.as_mut().unwrap().get_next_storage() {
                    Some(storage) => {
                        self.handler
                            .signals()
                            .send(CarrierEvent::BuildStorageIndex(storage.clone()));
                        self.broadcast_message_to(
                            CarrierEvent::BuildStorageIndex(storage),
                            self.connected_peers.get_all_identified_endpoints(),
                        );
                    }
                    None => {
                        self.handler.signals().send(CarrierEvent::EndSync);
                    }
                }
            }
            CarrierEvent::SetStorageIndex(index) => {
                let origin = match endpoint {
                    Some(endpoint) => {
                        Origin::Peer(self.connected_peers.get_peer_id(endpoint).unwrap())
                    }
                    None => Origin::Initiator,
                };

                log::debug!("Setting index with origin {:?}", origin);

                let session = self.session_state.as_mut().unwrap();
                session.set_storage_index(origin, index);

                if session.have_all_indexes_loaded() {
                    let mut event_queue = session.get_event_queue();
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
                            self.broadcast_message_to(
                                event,
                                self.connected_peers.get_all_identified_endpoints(),
                            );
                            self.handler.signals().send(CarrierEvent::ConsumeSyncQueue);
                        }
                        QueueEventType::BroadcastAndWait => self.broadcast_message_to(
                            event,
                            self.connected_peers.get_all_identified_endpoints(),
                        ),
                        QueueEventType::BroadcastExcept(to_be_ignored) => self
                            .broadcast_message_to(
                                event,
                                self.connected_peers
                                    .get_all_identified_endpoints_except(to_be_ignored),
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
                if let Err(err) = fs::delete_file(&file, &self.config) {
                    log::error!("Failed to delete file {}", err);
                }

                if endpoint.is_none() {
                    self.handler.signals().send(CarrierEvent::ConsumeSyncQueue);
                } else if self.file_watcher.is_some() {
                    self.received_file_events.insert(
                        file,
                        self.connected_peers.get_peer_id(endpoint.unwrap()).unwrap(),
                    );
                }
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
                match self.received_file_events.remove(&file_info) {
                    Some(peer_id) => {
                        for peer in self
                            .connected_peers
                            .get_all_identified_endpoints_except(peer_id)
                        {
                            self.file_transfer_man.send_file_to_peer(
                                file_info.clone(),
                                *peer,
                                is_new,
                            )?;
                        }
                    }
                    None => {
                        for peer in self.connected_peers.get_all_identified_endpoints() {
                            self.file_transfer_man.send_file_to_peer(
                                file_info.clone(),
                                *peer,
                                is_new,
                            )?;
                        }
                    }
                };

                self.handler.signals().send(CarrierEvent::ConsumeSyncQueue);
            }
            CarrierEvent::RequestFile(file_info, is_new) => {
                let endpoint = endpoint.unwrap();
                self.file_transfer_man
                    .send_file_to_peer(file_info, endpoint, is_new)?;
            }

            CarrierEvent::MoveFile(src, dst) => {
                if let Err(err) = fs::move_file(&src, &dst, &self.config) {
                    log::error!("Failed to move file: {}", err)
                }
                if endpoint.is_none() {
                    self.handler.signals().send(CarrierEvent::ConsumeSyncQueue);
                } else if self.file_watcher.is_none() {
                    self.received_file_events.insert(
                        src,
                        self.connected_peers.get_peer_id(endpoint.unwrap()).unwrap(),
                    );
                }
            }
            CarrierEvent::FileWatcherEvent(watcher_event) => {
                let (event, event_type) = match watcher_event {
                    super::WatcherEvent::Created(file_info) => (
                        CarrierEvent::BroadcastFile(file_info, true),
                        QueueEventType::Signal,
                    ),
                    super::WatcherEvent::Updated(file_info) => (
                        CarrierEvent::BroadcastFile(file_info, false),
                        QueueEventType::Signal,
                    ),
                    super::WatcherEvent::Moved(src, dst) => {
                        let broadcast_type = match self.received_file_events.remove(&src) {
                            Some(peer_id) => QueueEventType::BroadcastExcept(peer_id),
                            None => QueueEventType::Broadcast,
                        };
                        (CarrierEvent::MoveFile(src, dst), broadcast_type)
                    }
                    super::WatcherEvent::Deleted(file_info) => {
                        let broadcast_type = match self.received_file_events.remove(&file_info) {
                            Some(peer_id) => QueueEventType::BroadcastExcept(peer_id),
                            None => QueueEventType::Broadcast,
                        };

                        (CarrierEvent::DeleteFile(file_info), broadcast_type)
                    }
                };

                self.add_queue_event(event, event_type);
                if self.node_status == NodeStatus::Idle {
                    self.handler
                        .signals()
                        .send(CarrierEvent::StartSync(SyncType::Partial));
                }
            }
            CarrierEvent::FileSyncEvent(ev) => {
                let endpoint = endpoint.unwrap();
                self.file_transfer_man.file_sync_event(
                    ev,
                    endpoint,
                    self.connected_peers.get_peer_id(endpoint).unwrap(),
                    &mut self.received_file_events,
                )?;
            }
        };

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

    fn broadcast_message_to<'a, T: Iterator<Item = &'a Endpoint>>(
        &'a self,
        message: CarrierEvent,
        endpoints: T,
    ) {
        let data = bincode::serialize(&message).unwrap();
        for endpoint in endpoints {
            self.handler.network().send(*endpoint, &data);
        }
    }

    fn set_node_status(&mut self, new_status: NodeStatus) -> bool {
        let is_valid_state_change = match &self.node_status {
            NodeStatus::Idle => match new_status {
                NodeStatus::Idle => false,
                NodeStatus::BusyFullSync(_) | NodeStatus::BusyPartialSync(_) => true,
            },
            NodeStatus::BusyFullSync(_) | NodeStatus::BusyPartialSync(_) => match new_status {
                NodeStatus::Idle => true,
                _ => false,
            },
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
            .flat_map(|peers| peers.iter().cloned());

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
