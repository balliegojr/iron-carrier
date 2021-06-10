use simple_mdns::ServiceDiscovery;
use std::{
    collections::{HashMap, HashSet, LinkedList},
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::sync::{mpsc, mpsc::Receiver, mpsc::Sender};

use super::{file_watcher::FileWatcher, BlockingEvent, CarrierEvent, FileAction, SyncEvent};
use crate::{config::Config, fs, fs::FileInfo, /*network::peer::Peer, network::server::Server*/};

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
enum SynchronizerState {
    Idle,
    Initiator,
    Synchronizing,
}

#[derive(Debug, Hash, Eq, PartialEq)]
enum Origin {
    Initiator,
    Peer(Endpoint),
}

#[derive(Debug)]
struct SynchronizationSession {
    peers: Vec<Endpoint>,
    storages: Vec<String>,
    // current_storage: String,
    current_storage_index: HashMap<Origin, Vec<FileInfo>>,
    sync_queue: LinkedList<(CarrierEvent, Option<Endpoint>)>,
}

impl SynchronizationSession {
    pub fn new(storages: Vec<String>) -> Self {
        Self {
            peers: Vec::new(),
            storages,
            // current_storage: (),
            current_storage_index: HashMap::new(),
            sync_queue: LinkedList::new(),
        }
    }

    pub fn get_next_storage(&mut self) -> Option<String> {
        self.storages.pop()
    }

    pub fn set_storage_index(&mut self, origin: Origin, index: Vec<FileInfo>) {
        self.current_storage_index.insert(origin, index);
    }

    pub fn received_all_indexes(&self) -> bool {
        self.current_storage_index.len() == self.peers.len() + 1
    }

    pub fn build_sync_queue(&mut self) {
        let consolidated_index = self.get_consolidated_index();
        let actions = consolidated_index
            .iter()
            .flat_map(|(_, v)| self.convert_to_actions(v))
            .collect();

        self.sync_queue = actions;
        dbg!(&self.sync_queue);
    }

    pub fn get_next_action(&mut self) -> Option<(CarrierEvent, Option<Endpoint>)> {
        self.sync_queue.pop_front()
    }

    fn get_consolidated_index(&self) -> HashMap<&FileInfo, HashMap<&Origin, &FileInfo>> {
        let mut consolidated_index: HashMap<&FileInfo, HashMap<&Origin, &FileInfo>> =
            HashMap::new();
        for (origin, index) in &self.current_storage_index {
            for file in index {
                consolidated_index
                    .entry(&file)
                    .or_default()
                    .insert(origin, file);
            }
        }
        let mut all_origins: HashSet<Origin> =
            self.peers.iter().map(|peer| Origin::Peer(*peer)).collect();
        all_origins.insert(Origin::Initiator);

        consolidated_index.retain(|file, files| {
            files.len() <= self.current_storage_index.len()
                || files.values().any(|w| file.is_out_of_sync(w))
        });

        consolidated_index
    }
    fn convert_to_actions(
        &self,
        files: &HashMap<&Origin, &FileInfo>,
    ) -> Vec<(CarrierEvent, Option<Endpoint>)> {
        let mut actions = Vec::new();
        let (source, source_file) = files
            .iter()
            .max_by(|(_, a), (_, b)| {
                a.deleted_at
                    .unwrap_or(a.modified_at.unwrap())
                    .cmp(&b.deleted_at.unwrap_or(b.modified_at.unwrap()))
            })
            .unwrap();

        match *source {
            Origin::Initiator => {
                actions.extend(
                    self.peers
                        .iter()
                        .filter(|peer| match files.get(&Origin::Peer(**peer)) {
                            Some(f) => f.is_out_of_sync(source_file),
                            None => !source_file.is_deleted(),
                        })
                        .map(|peer| {
                            if source_file.is_deleted() {
                                (
                                    CarrierEvent::DeleteFile((*source_file).clone()),
                                    Some(*peer),
                                )
                            } else {
                                (CarrierEvent::SendFile((*source_file).clone()), Some(*peer))
                            }
                        }),
                );
            }
            Origin::Peer(endpoint) => {
                actions.push(if source_file.is_deleted() {
                    (CarrierEvent::DeleteFile((*source_file).clone()), None)
                } else {
                    (
                        CarrierEvent::RequestFile((*source_file).clone()),
                        Some(*endpoint),
                    )
                });
                actions.extend(
                    self.peers
                        .iter()
                        .filter(|peer| {
                            **peer != *endpoint
                                && match files.get(&Origin::Peer(**peer)) {
                                    Some(f) => f.is_out_of_sync(source_file),
                                    None => !source_file.is_deleted(),
                                }
                        })
                        .map(|peer| {
                            if source_file.is_deleted() {
                                (
                                    CarrierEvent::DeleteFile((*source_file).clone()),
                                    Some(*peer),
                                )
                            } else {
                                (CarrierEvent::SendFile((*source_file).clone()), Some(*peer))
                            }
                        }),
                );
            }
        }

        return actions;
    }
}

pub struct Synchronizer {
    config: Arc<Config>,
    state: SynchronizerState,
    connected_peers: Vec<Endpoint>,
    // events_channel_receiver: Receiver<SyncEvent>,
    // file_watcher: FileWatcher,
    service_discovery: Option<ServiceDiscovery>,
    handler: NodeHandler<CarrierEvent>,
    session: Option<SynchronizationSession>,
}

impl Synchronizer {
    pub fn new(config: Config) -> crate::Result<Self> {
        let config = Arc::new(config);

        // let (events_channel_sender, events_channel_receiver) = mpsc::channel(50);
        // let file_watcher = FileWatcher::new(events_channel_sender.clone(), config.clone());

        // let server = Server::new(config.clone(), file_watcher.get_events_blocker());
        let service_discovery = get_service_discovery(&config);
        let (handler, listener) = node::split::<CarrierEvent>();

        let mut s = Synchronizer {
            config,
            // server,
            // events_channel_sender,
            // events_channel_receiver,
            // file_watcher,
            service_discovery,
            state: SynchronizerState::Idle,
            handler,
            connected_peers: Vec::new(),
            session: None,
        };

        s.start(listener)?;
        Ok(s)
    }

    fn start(&mut self, listener: NodeListener<CarrierEvent>) -> crate::Result<()> {
        log::debug!("starting syncronizer");
        // self.server
        //     .start(self.events_channel_sender.clone())
        //     .await?;
        // // self.file_watcher.start();
        // self.start_peers_full_sync().await?;
        // self.sync_events().await;
        let mut rng = rand::thread_rng();
        let wait = rng.gen_range(3.0..5.0);
        self.handler
            .signals()
            .send_with_timer(CarrierEvent::StartFullSync, Duration::from_secs_f64(wait));

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
                        self.connected_peers.push(endpoint);
                    } // Used for explicit connections.
                    NetEvent::Accepted(_endpoint, _listener) => {}
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
                        self.connected_peers.retain(|p| *p != endpoint);
                        if self.connected_peers.is_empty()
                            && self.state == SynchronizerState::Synchronizing
                        {
                            self.set_state(SynchronizerState::Idle);
                            self.session = None;
                        }
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
            CarrierEvent::StartFullSync => {
                log::info!("Received StartAsync event");
                if self.set_state(SynchronizerState::Initiator) {
                    let peers = self.get_peers();
                    if peers.is_empty() {
                        self.set_state(SynchronizerState::Idle);
                    } else {
                        let storages = self.config.paths.keys().cloned().collect();
                        self.session = Some(SynchronizationSession::new(storages));
                        for peer in peers {
                            log::debug!("Connecting to peer {:?}", peer);
                            match self
                                .handler
                                .network()
                                .connect_sync(Transport::FramedTcp, peer)
                            {
                                Ok((endpoint, _)) => {
                                    self.send_message(CarrierEvent::StartSync, endpoint);
                                }
                                Err(_) => log::error!("Error connecting to peer"),
                            }
                        }
                    }
                }
            }
            CarrierEvent::BuildStorageIndex(storage) => {
                log::info!("Building index for {}", storage);
                let storage_path = self.config.paths.get(&storage).unwrap().clone();
                let index = fs::walk_path(&storage_path, &storage).unwrap();

                log::debug!("Read {} files ", index.len());

                match endpoint {
                    Some(endpoint) => {
                        let data =
                            bincode::serialize(&CarrierEvent::SetStorageIndex(index)).unwrap();
                        self.handler.network().send(endpoint, &data);
                    }
                    None => self
                        .handler
                        .signals()
                        .send(CarrierEvent::SetStorageIndex(index)),
                };
            }
            CarrierEvent::StartSync => {
                if self.set_state(SynchronizerState::Synchronizing) {
                    self.send_message(CarrierEvent::SyncRequestAccepted, endpoint.unwrap());
                } else {
                    self.send_message(CarrierEvent::SyncRequestRejected, endpoint.unwrap());
                }
            }
            CarrierEvent::SyncRequestAccepted => {
                let session = self.session.as_mut().unwrap();
                session.peers.push(endpoint.unwrap());

                if session.peers.len() == self.connected_peers.len() {
                    self.handler.signals().send(CarrierEvent::SyncNextStorage);
                }
            }
            CarrierEvent::SyncRequestRejected => {
                log::debug!("Peer reject sync request");
                self.handler
                    .network()
                    .remove(endpoint.unwrap().resource_id());
            }
            CarrierEvent::SyncNextStorage => {
                match self.session.as_mut().unwrap().get_next_storage() {
                    Some(storage) => {
                        self.handler
                            .signals()
                            .send(CarrierEvent::BuildStorageIndex(storage.clone()));
                        self.broadcast_message(CarrierEvent::BuildStorageIndex(storage));
                    }
                    None => {
                        // TODO: finish sync
                        todo!()
                    }
                }
            }
            CarrierEvent::SetStorageIndex(index) => {
                let origin = match endpoint {
                    Some(endpoint) => Origin::Peer(endpoint),
                    None => Origin::Initiator,
                };

                log::debug!("Setting index with origin {:?}", origin);

                let session = self.session.as_mut().unwrap();
                session.set_storage_index(origin, index);

                if session.received_all_indexes() {
                    // self.handler.signals().send(CarrierEvent::BuildSyncQueue);
                    session.build_sync_queue();
                }
            }
            CarrierEvent::ConsumeSyncQueue => {
                let session = self.session.as_mut().unwrap();
                let action = session.get_next_action();
                match action {
                    Some((event, endpoint)) => {}
                    None => {
                        self.handler.signals().send(CarrierEvent::SyncNextStorage);
                    }
                }
            }
            CarrierEvent::DeleteFile(file) => {
                fs::delete_file(&file, &self.config);
            }
            CarrierEvent::SendFile(_) => todo!(),
            CarrierEvent::RequestFile(_) => todo!(),
            CarrierEvent::PrepareFileTransfer(_, _) => todo!(),
            CarrierEvent::WriteFileChunk(_, _, _) => todo!(),
            CarrierEvent::EndFileTransfer(_) => todo!(),
        }
    }

    fn convert_to_actions(
        &self,
        files: &HashMap<&Origin, &FileInfo>,
    ) -> Vec<(CarrierEvent, Option<Endpoint>)> {
        let mut actions = Vec::new();
        let (source, source_file) = files
            .iter()
            .max_by(|(_, a), (_, b)| {
                a.deleted_at
                    .unwrap_or(a.modified_at.unwrap())
                    .cmp(&b.deleted_at.unwrap_or(b.modified_at.unwrap()))
            })
            .unwrap();

        match *source {
            Origin::Initiator => {
                actions.extend(
                    self.connected_peers
                        .iter()
                        .filter(|peer| match files.get(&Origin::Peer(**peer)) {
                            Some(f) => f.is_out_of_sync(source_file),
                            None => !source_file.is_deleted(),
                        })
                        .map(|peer| {
                            if source_file.is_deleted() {
                                (
                                    CarrierEvent::DeleteFile((*source_file).clone()),
                                    Some(*peer),
                                )
                            } else {
                                (CarrierEvent::SendFile((*source_file).clone()), Some(*peer))
                            }
                        }),
                );
            }
            Origin::Peer(endpoint) => {
                actions.push(if source_file.is_deleted() {
                    (CarrierEvent::DeleteFile((*source_file).clone()), None)
                } else {
                    (
                        CarrierEvent::RequestFile((*source_file).clone()),
                        Some(*endpoint),
                    )
                });
                actions.extend(
                    self.connected_peers
                        .iter()
                        .filter(|peer| {
                            **peer != *endpoint
                                && match files.get(&Origin::Peer(**peer)) {
                                    Some(f) => f.is_out_of_sync(source_file),
                                    None => !source_file.is_deleted(),
                                }
                        })
                        .map(|peer| {
                            if source_file.is_deleted() {
                                (
                                    CarrierEvent::DeleteFile((*source_file).clone()),
                                    Some(*peer),
                                )
                            } else {
                                (CarrierEvent::SendFile((*source_file).clone()), Some(*peer))
                            }
                        }),
                );
            }
        }

        return actions;
    }

    fn send_message(&self, message: CarrierEvent, endpoint: Endpoint) {
        let data = bincode::serialize(&message).unwrap();
        self.handler.network().send(endpoint, &data);
        log::debug!("sent message {:?} to {:?}", message, endpoint);
    }

    fn broadcast_message(&self, message: CarrierEvent) {
        let data = bincode::serialize(&message).unwrap();
        for endpoint in &self.connected_peers {
            self.handler.network().send(*endpoint, &data);
        }
    }

    fn set_state(&mut self, state: SynchronizerState) -> bool {
        match (&self.state, &state) {
            (SynchronizerState::Initiator, SynchronizerState::Synchronizing)
            | (SynchronizerState::Synchronizing, SynchronizerState::Initiator) => {
                log::info!("Invalid state change {:?} -> {:?}", self.state, state);
                false
            }
            _ => {
                log::info!("State change {:?} -> {:?}", self.state, state);
                self.state = state;
                true
            }
        }
    }

    fn get_peers(&self) -> std::collections::HashSet<SocketAddr> {
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

    // async fn start_peers_full_sync(&self) -> crate::Result<()> {
    //     let peers = loop {
    //         let p = self.get_peers();
    //         if p.len() == 0 {
    //             log::debug!("No peers found, waiting 2 seconds");
    //             tokio::time::sleep(Duration::from_secs(2)).await;
    //             continue;
    //         }

    //         break p;
    //     };

    //     for peer_address in peers {
    //         log::info!("schedulling peer {} for synchonization", peer_address);
    //         self.events_channel_sender
    //             .send(SyncEvent::EnqueueSyncToPeer(peer_address, false))
    //             .await?;
    //     }
    //     Ok(())
    // }

    // async fn sync_events(&mut self) {
    //     loop {
    //         match self.events_channel_receiver.recv().await {
    //             Some(event) => match event {
    //                 SyncEvent::EnqueueSyncToPeer(peer_address, two_way_sync) => {
    //                     let config = self.config.clone();
    //                     let events_blocker = self.file_watcher.get_events_blocker();

    //                     tokio::spawn(async move {
    //                         match Synchronizer::sync_peer(
    //                             peer_address,
    //                             two_way_sync,
    //                             &config,
    //                             events_blocker,
    //                         )
    //                         .await
    //                         {
    //                             Ok(_) => {
    //                                 log::info!("Peer synchronization successful")
    //                             }
    //                             Err(e) => {
    //                                 log::error!("Peer synchronization failed: {}", e);
    //                             }
    //                         }
    //                     });
    //                 }
    //                 SyncEvent::PeerRequestedSync(peer_address, sync_starter, sync_ended) => {
    //                     log::info!("Peer requested synchronization: {}", peer_address);
    //                     sync_starter.notify_one();
    //                     sync_ended.notified().await;

    //                     log::info!("Peer synchronization ended");
    //                 }
    //                 SyncEvent::BroadcastToAllPeers(action, peers) => {
    //                     log::debug!("file changed on disk: {:?}", action);

    //                     for peer in peers {
    //                         if let Err(e) = self.sync_peer_single_action(peer, &action).await {
    //                             log::error!("Peer Synchronization failed: {}", e);
    //                         };
    //                     }
    //                 }
    //             },
    //             None => {
    //                 break;
    //             }
    //         }
    //     }
    // }

    // async fn sync_peer_single_action<'b>(
    //     &self,
    //     peer_address: SocketAddr,
    //     action: &'b FileAction,
    // ) -> crate::Result<()> {
    //     let mut peer = Peer::new(
    //         peer_address,
    //         &self.config,
    //         self.file_watcher.get_events_blocker(),
    //     )
    //     .await?;
    //     peer.sync_action(action).await
    // }

    // async fn sync_peer<'b>(
    //     peer_address: SocketAddr,
    //     two_way_sync: bool,
    //     config: &Config,
    //     events_blocker: Sender<BlockingEvent>,
    // ) -> crate::Result<()> {
    //     let mut peer = Peer::new(peer_address, config, events_blocker.clone()).await?;
    //     log::info!("Peer full synchronization started: {}", peer.get_address());

    //     peer.start_sync().await?;

    //     for (alias, path) in &config.paths {
    //         let (hash, mut local_files) = fs::get_files_with_hash(path, alias).await?;
    //         if !peer.need_to_sync(&alias, hash) {
    //             continue;
    //         }

    //         let mut peer_files = peer.fetch_files_for_alias(alias).await?;

    //         for file in find_deletable_local_files(&mut peer_files, &mut local_files) {
    //             let _ = events_blocker
    //                 .send((file.get_absolute_path(&config)?, peer_address))
    //                 .await;
    //             fs::delete_file(&file, &config).await?;
    //         }

    //         while let Some(local_file) = local_files.pop() {
    //             let peer_file = lookup_file(&local_file, &mut peer_files);
    //             let peer_action = get_file_action(local_file, peer_file);
    //             if let Some(peer_action) = peer_action {
    //                 peer.sync_action(&peer_action).await?
    //             }
    //         }

    //         while let Some(peer_file) = peer_files.pop() {
    //             peer.sync_action(&FileAction::Request(peer_file)).await?;
    //         }
    //     }

    //     peer.finish_sync(two_way_sync).await
    // }
}

fn find_deletable_local_files(
    peer_files: &mut Vec<FileInfo>,
    local_files: &mut Vec<FileInfo>,
) -> Vec<FileInfo> {
    let mut deletable_files = Vec::new();

    let mut i = 0;
    while i != peer_files.len() {
        if peer_files[i].is_deleted() {
            let peer_file = peer_files.remove(i);
            match lookup_file(&peer_file, local_files) {
                Some(local_file) if !local_file.is_deleted() => deletable_files.push(local_file),
                _ => {}
            }
        } else {
            i += 1;
        }
    }

    deletable_files
}

fn get_file_action(local_file: FileInfo, peer_file: Option<FileInfo>) -> Option<FileAction> {
    if local_file.is_deleted() {
        return if peer_file.is_none() {
            None
        } else {
            Some(FileAction::Remove(local_file))
        };
    }

    if peer_file.is_none() {
        return Some(FileAction::Update(local_file));
    }

    let peer_file = peer_file.unwrap();
    match local_file
        .modified_at
        .unwrap()
        .cmp(&peer_file.modified_at.unwrap())
    {
        std::cmp::Ordering::Less => Some(FileAction::Request(local_file)),
        std::cmp::Ordering::Greater => Some(FileAction::Update(local_file)),
        _ => None,
    }
}

/// lookup for the file in collection
/// if the file is found, return the file and remove it from collection
fn lookup_file(file: &FileInfo, file_collection: &mut Vec<FileInfo>) -> Option<FileInfo> {
    // let file = RemoteFile::from(file);
    match file_collection.binary_search_by(|f| f.cmp(&file)) {
        Ok(index) => Some(file_collection.remove(index)),
        Err(_) => None,
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
