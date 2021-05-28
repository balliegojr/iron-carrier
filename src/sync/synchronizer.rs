use simple_mdns::ServiceDiscovery;
use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Duration,
};
use tokio::sync::{mpsc, mpsc::Receiver, mpsc::Sender};

use super::{file_watcher::FileWatcher, BlockingEvent, FileAction, SyncEvent};
use crate::{config::Config, fs, fs::FileInfo, network::peer::Peer, network::server::Server};

pub struct Synchronizer {
    config: Arc<Config>,
    server: Server,
    events_channel_sender: Sender<SyncEvent>,
    events_channel_receiver: Receiver<SyncEvent>,
    file_watcher: FileWatcher,
    service_discovery: Option<ServiceDiscovery>,
}

impl Synchronizer {
    pub fn new(config: Config) -> Self {
        let config = Arc::new(config);

        let (events_channel_sender, events_channel_receiver) = mpsc::channel(50);
        let file_watcher = FileWatcher::new(events_channel_sender.clone(), config.clone());

        let server = Server::new(config.clone(), file_watcher.get_events_blocker());
        let service_discovery = get_service_discovery(&config);

        Synchronizer {
            config,
            server,
            events_channel_sender,
            events_channel_receiver,
            file_watcher,
            service_discovery,
        }
    }

    pub async fn start(&mut self) -> crate::Result<()> {
        log::debug!("starting syncronizer");
        self.server
            .start(self.events_channel_sender.clone())
            .await?;
        self.file_watcher.start();
        self.start_peers_full_sync().await?;
        self.sync_events().await;

        Ok(())
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

    async fn start_peers_full_sync(&self) -> crate::Result<()> {
        let peers = loop {
            let p = self.get_peers();
            if p.len() == 0 {
                log::debug!("No peers found, waiting 2 seconds");
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }

            break p;
        };

        for peer_address in peers {
            log::info!("schedulling peer {} for synchonization", peer_address);
            self.events_channel_sender
                .send(SyncEvent::EnqueueSyncToPeer(peer_address, false))
                .await?;
        }
        Ok(())
    }

    async fn sync_events(&mut self) {
        loop {
            match self.events_channel_receiver.recv().await {
                Some(event) => match event {
                    SyncEvent::EnqueueSyncToPeer(peer_address, two_way_sync) => {
                        let config = self.config.clone();
                        let events_blocker = self.file_watcher.get_events_blocker();

                        tokio::spawn(async move {
                            match Synchronizer::sync_peer(
                                peer_address,
                                two_way_sync,
                                &config,
                                events_blocker,
                            )
                            .await
                            {
                                Ok(_) => {
                                    log::info!("Peer synchronization successful")
                                }
                                Err(e) => {
                                    log::error!("Peer synchronization failed: {}", e);
                                }
                            }
                        });
                    }
                    SyncEvent::PeerRequestedSync(peer_address, sync_starter, sync_ended) => {
                        log::info!("Peer requested synchronization: {}", peer_address);
                        sync_starter.notify_one();
                        sync_ended.notified().await;

                        log::info!("Peer synchronization ended");
                    }
                    SyncEvent::BroadcastToAllPeers(action, peers) => {
                        log::debug!("file changed on disk: {:?}", action);

                        for peer in peers {
                            if let Err(e) = self.sync_peer_single_action(peer, &action).await {
                                log::error!("Peer Synchronization failed: {}", e);
                            };
                        }
                    }
                },
                None => {
                    break;
                }
            }
        }
    }

    async fn sync_peer_single_action<'b>(
        &self,
        peer_address: SocketAddr,
        action: &'b FileAction,
    ) -> crate::Result<()> {
        let mut peer = Peer::new(
            peer_address,
            &self.config,
            self.file_watcher.get_events_blocker(),
        )
        .await?;
        peer.sync_action(action).await
    }

    async fn sync_peer<'b>(
        peer_address: SocketAddr,
        two_way_sync: bool,
        config: &Config,
        events_blocker: Sender<BlockingEvent>,
    ) -> crate::Result<()> {
        let mut peer = Peer::new(peer_address, config, events_blocker.clone()).await?;
        log::info!("Peer full synchronization started: {}", peer.get_address());

        peer.start_sync().await?;

        for (alias, path) in &config.paths {
            let (hash, mut local_files) = fs::get_files_with_hash(path, alias).await?;
            if !peer.need_to_sync(&alias, hash) {
                continue;
            }

            let mut peer_files = peer.fetch_files_for_alias(alias).await?;

            for file in find_deletable_local_files(&mut peer_files, &mut local_files) {
                let _ = events_blocker
                    .send((file.get_absolute_path(&config)?, peer_address))
                    .await;
                fs::delete_file(&file, &config).await?;
            }

            while let Some(local_file) = local_files.pop() {
                let peer_file = lookup_file(&local_file, &mut peer_files);
                let peer_action = get_file_action(local_file, peer_file);
                if let Some(peer_action) = peer_action {
                    peer.sync_action(&peer_action).await?
                }
            }

            while let Some(peer_file) = peer_files.pop() {
                peer.sync_action(&FileAction::Request(peer_file)).await?;
            }
        }

        peer.finish_sync(two_way_sync).await
    }
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
