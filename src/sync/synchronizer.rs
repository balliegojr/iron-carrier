use simple_mdns::ServiceDiscovery;
use std::{
    collections::{HashMap, HashSet, LinkedList},
    fs::File,
    io::{Read, Write},
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use super::{
    file_watcher::FileWatcher, synchronization_session::SynchronizationSession, CarrierEvent,
    Origin, QueueEventType,
};
use crate::{config::Config, fs, fs::FileInfo, hash_helper, sync::SyncType};

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
    BusyFullSync,
    BusyPartialSync,
}

#[derive(Debug, Eq, PartialEq, Hash)]
enum SyncStep {
    PeerIdentification,
    PeerSyncReply,
}

struct SessionPeers {
    expected: usize,
    id_endpoint: HashMap<u64, Endpoint>,
    handler: NodeHandler<CarrierEvent>,
}

impl SessionPeers {
    pub fn new(handler: NodeHandler<CarrierEvent>) -> Self {
        Self {
            expected: 0,
            id_endpoint: HashMap::new(),
            handler,
        }
    }
    pub fn connect_all(&mut self, addresses: HashSet<SocketAddr>) {
        self.expected = addresses.len();
        let connected_peers: HashSet<SocketAddr> = self
            .id_endpoint
            .iter()
            .map(|(_, peer)| peer.addr())
            .collect();
        for address in addresses.difference(&connected_peers) {
            log::debug!("Connecting to peer {:?}", address);
            if let Err(_) = self
                .handler
                .network()
                .connect(Transport::FramedTcp, *address)
            {
                log::error!("Error connecting to peer");
                self.expected -= 1;
            }
        }
    }
    pub fn disconnect_all(&mut self) {
        for (_, endpoint) in self.id_endpoint.drain() {
            self.handler.network().remove(endpoint.resource_id());
        }
        self.expected = 0;
    }
    pub fn add_peer(&mut self, endpoint: Endpoint, peer_id: u64) {
        self.id_endpoint.insert(peer_id, endpoint);
    }
    pub fn remove_endpoint(&mut self, endpoint: Endpoint) {
        if let Some(id) = self.get_peer_id(endpoint) {
            self.id_endpoint.remove(&id);
            self.expected -= 1;
        }
    }
    pub fn peers_in_this_sync(&self) -> usize {
        self.expected
    }
    pub fn get_peer_endpoint(&self, peer_id: u64) -> Option<Endpoint> {
        self.id_endpoint.get(&peer_id).cloned()
    }
    pub fn get_peer_id(&self, endpoint: Endpoint) -> Option<u64> {
        self.id_endpoint
            .iter()
            .find_map(|(id, e)| if *e == endpoint { Some(*id) } else { None })
    }
    pub fn get_all_peers_ids(&self) -> Vec<u64> {
        self.id_endpoint.keys().cloned().collect()
    }
    pub fn get_all_identified_endpoints(&self) -> Vec<Endpoint> {
        self.id_endpoint.values().cloned().collect()
    }
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
    session_peers: SessionPeers,
    current_transfering_file: Option<File>,
    events_queue: LinkedList<(CarrierEvent, QueueEventType)>,
}

impl Synchronizer {
    pub fn new(config: Config) -> crate::Result<Self> {
        let (handler, listener) = node::split::<CarrierEvent>();

        let config = Arc::new(config);
        let service_discovery = get_service_discovery(&config);
        let file_watcher = get_file_watcher(config.clone(), handler.clone());
        let session_peers = SessionPeers::new(handler.clone());

        let mut s = Synchronizer {
            node_id: hash_helper::get_node_id(config.port),
            config,
            file_watcher,
            service_discovery,
            node_status: NodeStatus::Idle,
            sync_steps: HashMap::new(),
            handler,
            session_peers,
            session_state: None,
            current_transfering_file: None,
            events_queue: LinkedList::new(),
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

        // Listen for TCP, UDP and WebSocket messages at the same time.
        self.handler
            .network()
            .listen(
                Transport::FramedTcp,
                format!("0.0.0.0:{}", self.config.port),
            )
            .unwrap();

        // Read incoming network events.
        listener.for_each(move |event| match event {
            node::NodeEvent::Network(net_event) => {
                match net_event {
                    NetEvent::Connected(endpoint, _) => {
                        log::debug!("Peer connected:  {}", endpoint);
                    } // Used for explicit connections.
                    NetEvent::Accepted(endpoint, _listener) => {
                        self.send_message(&CarrierEvent::SetPeerId(self.node_id), endpoint);
                    }
                    NetEvent::Message(endpoint, data) => {
                        match bincode::deserialize(data) {
                            Ok(message) => self.process_event(message, Some(endpoint)),
                            Err(err) => {
                                log::error!("Invalid message: {:?}", err);
                            }
                        };
                    }
                    NetEvent::Disconnected(endpoint) => {
                        log::debug!("Peer disconnected:  {}", endpoint);
                        if let Some(session) = self.session_state.as_mut() {
                            if let Some(id) = self.session_peers.get_peer_id(endpoint) {
                                session.remove_peer_from_session(id)
                            }
                        }
                        self.session_peers.remove_endpoint(endpoint);
                        // TODO: pause sync when there are no more connected peers
                    }
                }
            }
            node::NodeEvent::Signal(signal_event) => {
                self.process_event(signal_event, None);
            }
        });

        Ok(())
    }

    fn process_event(&mut self, event: CarrierEvent, endpoint: Option<Endpoint>) {
        log::debug!("received event {:?}", event);
        match event {
            CarrierEvent::SetPeerId(peer_id) => {
                self.session_peers.add_peer(endpoint.unwrap(), peer_id);
                self.increment_step(SyncStep::PeerIdentification);
            }
            CarrierEvent::StartSync(sync_type) => match (sync_type, endpoint) {
                (super::SyncType::Full, None) => {
                    if self.set_node_status(NodeStatus::BusyFullSync) {
                        let addresses = self.get_peer_addresses();

                        if addresses.is_empty() {
                            self.set_node_status(NodeStatus::Idle);
                        } else {
                            let storages = self.config.paths.keys().cloned().collect();
                            self.session_state = Some(SynchronizationSession::new(storages));
                            self.file_watcher = None;
                            self.sync_steps = HashMap::new();
                            self.session_peers.connect_all(addresses);

                            self.events_queue.push_back((
                                CarrierEvent::StartSync(SyncType::Full),
                                QueueEventType::Broadcast,
                            ));

                            self.events_queue
                                .push_back((CarrierEvent::SyncNextStorage, QueueEventType::Signal));
                        }
                    }
                }
                (super::SyncType::Full, Some(endpoint)) => {
                    if self.set_node_status(NodeStatus::BusyFullSync) {
                        self.send_message(&CarrierEvent::StartSyncReply(true), endpoint);
                        self.file_watcher = None;
                    } else {
                        self.send_message(&CarrierEvent::StartSyncReply(false), endpoint);
                    }
                }
                (super::SyncType::Partial, None) => {
                    if self.set_node_status(NodeStatus::BusyPartialSync) {
                        let addresses = self.get_peer_addresses();
                        self.sync_steps = HashMap::new();
                        self.session_peers.connect_all(addresses);

                        self.events_queue.push_back((
                            CarrierEvent::StartSync(SyncType::Partial),
                            QueueEventType::Broadcast,
                        ));
                    }
                }
                (super::SyncType::Partial, Some(endpoint)) => {
                    if self.set_node_status(NodeStatus::BusyPartialSync) {
                        self.send_message(&CarrierEvent::StartSyncReply(true), endpoint);
                    } else {
                        self.send_message(&CarrierEvent::StartSyncReply(false), endpoint);
                    }
                }
            },
            CarrierEvent::EndSync => {
                match self.node_status {
                    NodeStatus::Idle => unreachable!(),
                    NodeStatus::BusyFullSync => {
                        self.session_state = None;
                        self.file_watcher =
                            get_file_watcher(self.config.clone(), self.handler.clone());
                    }
                    NodeStatus::BusyPartialSync => {
                        if !self.events_queue.is_empty() {
                            self.events_queue
                                .push_back((CarrierEvent::EndSync, QueueEventType::Signal));
                            self.handler.signals().send(CarrierEvent::ConsumeSyncQueue);
                            return;
                        }
                    }
                }

                if endpoint.is_none() {
                    self.broadcast_message(CarrierEvent::EndSync);
                }
                self.set_node_status(NodeStatus::Idle);
                self.handler.signals().send(CarrierEvent::CloseConnections);
            }

            CarrierEvent::CloseConnections => {
                if self.node_status == NodeStatus::Idle {
                    self.session_peers.disconnect_all();
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
                                self.send_message(
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
                        self.send_message(&CarrierEvent::SetStorageIndex(None), endpoint.unwrap());
                    }
                }
            }
            CarrierEvent::StartSyncReply(accepted) => {
                if accepted {
                    if self.node_status == NodeStatus::BusyFullSync {
                        let session = self.session_state.as_mut().unwrap();
                        if let Some(peer_id) = self.session_peers.get_peer_id(endpoint.unwrap()) {
                            session.add_peer_to_session(peer_id);
                        }
                    }
                } else {
                    self.session_peers.remove_endpoint(endpoint.unwrap());
                }

                self.increment_step(SyncStep::PeerSyncReply);
            }
            CarrierEvent::SyncNextStorage => {
                match self.session_state.as_mut().unwrap().get_next_storage() {
                    Some(storage) => {
                        self.handler
                            .signals()
                            .send(CarrierEvent::BuildStorageIndex(storage.clone()));
                        self.broadcast_message(CarrierEvent::BuildStorageIndex(storage));
                    }
                    None => {
                        self.handler.signals().send(CarrierEvent::EndSync);
                    }
                }
            }
            CarrierEvent::SetStorageIndex(index) => {
                let origin = match endpoint {
                    Some(endpoint) => {
                        Origin::Peer(self.session_peers.get_peer_id(endpoint).unwrap())
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
                            match self.session_peers.get_peer_endpoint(peer_id) {
                                Some(endpoint) => self.send_message(&event, endpoint),
                                None => {
                                    log::debug!("peer is offline, cant receive message");
                                    // TODO: add retry logic
                                }
                            }
                        }
                        QueueEventType::Broadcast => self.broadcast_message(event),
                    },
                    None => {
                        self.handler.signals().send(CarrierEvent::EndSync);
                    }
                }
            }
            CarrierEvent::DeleteFile(file) => {
                if let Err(err) = fs::delete_file(&file, &self.config) {
                    log::error!("Failed to delete file {}", err);
                }

                if endpoint.is_none() {
                    self.handler.signals().send(CarrierEvent::ConsumeSyncQueue);
                }
            }
            CarrierEvent::SendFile(file_info, peer_id) => {
                // TODO: improve file transfer
                match self.session_peers.get_peer_endpoint(peer_id) {
                    Some(endpoint) => {
                        self.send_file(file_info, endpoint);
                    }
                    None => {
                        todo!()
                    }
                }
                self.handler.signals().send(CarrierEvent::ConsumeSyncQueue);
            }
            CarrierEvent::BroadcastFile(file_info) => {
                for peer in self.session_peers.get_all_peers_ids() {
                    self.events_queue.push_front((
                        CarrierEvent::SendFile(file_info.clone(), peer),
                        QueueEventType::Signal,
                    ));
                }
                self.handler.signals().send(CarrierEvent::ConsumeSyncQueue);
            }
            CarrierEvent::RequestFile(file_info) => {
                self.send_file(file_info, endpoint.unwrap());
                self.send_message(&CarrierEvent::ConsumeSyncQueue, endpoint.unwrap());
            }
            CarrierEvent::PrepareFileTransfer(file, chunks) => {
                self.current_transfering_file =
                    Some(fs::get_temp_file(&file, &self.config).unwrap());
            }
            CarrierEvent::WriteFileChunk(chunk, bytes) => {
                self.current_transfering_file
                    .as_mut()
                    .unwrap()
                    .write_all(&bytes);
            }
            CarrierEvent::EndFileTransfer(file) => {
                self.current_transfering_file = None;
                fs::flush_temp_file(&file, &self.config).unwrap();
            }
            CarrierEvent::MoveFile(src, dst) => {
                if let Err(err) = fs::move_file(&src, &dst, &self.config) {
                    log::error!("Failed to move file: {}", err)
                }
            }
            CarrierEvent::FileWatcherEvent(watcher_event) => {
                let event = match watcher_event {
                    super::WatcherEvent::Created(file_info)
                    | super::WatcherEvent::Updated(file_info) => (
                        CarrierEvent::BroadcastFile(file_info),
                        QueueEventType::Signal,
                    ),
                    super::WatcherEvent::Moved(src, dst) => {
                        (CarrierEvent::MoveFile(src, dst), QueueEventType::Broadcast)
                    }
                    super::WatcherEvent::Deleted(file_info) => (
                        CarrierEvent::DeleteFile(file_info),
                        QueueEventType::Broadcast,
                    ),
                };

                self.events_queue.push_back(event);
                if self.node_status == NodeStatus::Idle {
                    self.handler
                        .signals()
                        .send(CarrierEvent::StartSync(SyncType::Partial));
                }
            }
        }
    }

    fn increment_step(&mut self, step: SyncStep) {
        let c = self.sync_steps.entry(step).or_insert(0);
        *c += 1;

        if *c >= self.session_peers.peers_in_this_sync() {
            self.handler.signals().send(CarrierEvent::ConsumeSyncQueue);
        }
    }

    fn send_message(&self, message: &CarrierEvent, endpoint: Endpoint) {
        let data = bincode::serialize(message).unwrap();
        self.handler.network().send(endpoint, &data);
        // log::debug!("sent message {:?} to {:?}", message, endpoint);
    }

    fn broadcast_message(&self, message: CarrierEvent) {
        let data = bincode::serialize(&message).unwrap();
        for endpoint in self.session_peers.get_all_identified_endpoints() {
            self.handler.network().send(endpoint, &data);
        }
    }

    fn set_node_status(&mut self, new_status: NodeStatus) -> bool {
        let is_valid_state_change = match &self.node_status {
            NodeStatus::Idle => match new_status {
                NodeStatus::Idle => false,
                NodeStatus::BusyFullSync | NodeStatus::BusyPartialSync => true,
            },
            NodeStatus::BusyFullSync | NodeStatus::BusyPartialSync => match new_status {
                NodeStatus::Idle => true,
                _ => false,
            },
        };

        if is_valid_state_change {
            log::info!("Node Status: {:?} -> {:?}", self.node_status, new_status);
            self.node_status = new_status;
        } else {
            log::info!(
                "Node Status: Invalid transition, node is already {:?}",
                self.node_status
            );
        }

        is_valid_state_change
    }

    fn send_file(&self, file_info: FileInfo, endpoint: Endpoint) -> crate::Result<()> {
        let chunk_size = get_chunk_size(file_info.size.unwrap());
        let chunks = (file_info.size.unwrap() / 65000) + 1;
        let file_path = file_info.get_absolute_path(&self.config)?;
        self.send_message(
            &CarrierEvent::PrepareFileTransfer(file_info.clone(), chunks),
            endpoint,
        );

        let mut file = std::fs::File::open(file_path)?;

        let mut buf = [0u8; 65000];
        let mut chunk = 0;
        loop {
            match file.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    chunk += 1;
                    self.send_message(
                        &CarrierEvent::WriteFileChunk(chunk, Vec::from(&buf[..n])),
                        endpoint,
                    );
                }
                Err(_) => break,
            }
        }

        self.send_message(&CarrierEvent::EndFileTransfer(file_info), endpoint);
        Ok(())
    }

    fn broadcast_file(&self, file_info: FileInfo, endpoints: Vec<Endpoint>) -> crate::Result<()> {
        let chunks = (file_info.size.unwrap() / 65000) + 1;
        let file_path = file_info.get_absolute_path(&self.config)?;
        let prepare_message = CarrierEvent::PrepareFileTransfer(file_info.clone(), chunks);
        let end_message = CarrierEvent::EndFileTransfer(file_info);

        endpoints
            .iter()
            .for_each(|e| self.send_message(&prepare_message, *e));

        let mut file = std::fs::File::open(file_path)?;

        let mut buf = [0u8; 65000];
        let mut chunk = 0;
        loop {
            match file.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    chunk += 1;
                    let chunk_message = &CarrierEvent::WriteFileChunk(chunk, Vec::from(&buf[..n]));
                    endpoints
                        .iter()
                        .for_each(|e| self.send_message(&chunk_message, *e));
                }
                Err(_) => break,
            }
        }

        endpoints
            .iter()
            .for_each(|e| self.send_message(&end_message, *e));

        Ok(())
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

    let mut sd = ServiceDiscovery::new("_ironcarrier._tcp.local", 600, true).unwrap();
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
fn get_chunk_size(_file_size: u64) -> u64 {
    65000
}
