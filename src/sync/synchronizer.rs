use std::sync::Arc;
use tokio::sync::{mpsc, mpsc::Receiver, mpsc::Sender};

use super::{BlockingEvent, FileAction, SyncEvent, file_watcher::FileWatcher, file_watcher_event_blocker::FileWatcherEventBlocker};
use crate::{config::Config, fs, fs::FileInfo, network::peer::Peer, network::server::Server};

pub struct Synchronizer {
    config: Arc<Config>,
    server: Server,
    events_channel_sender: Sender<SyncEvent>,
    events_channel_receiver: Receiver<SyncEvent>,
    file_watcher: FileWatcher
}

/// lookup for the peer file  
/// if the file is found, return the file and remove it from peer_files
fn get_peer_file(file: &FileInfo, peer_files: &mut Vec<FileInfo>) -> Option<FileInfo> {
    // let file = RemoteFile::from(file);
    match peer_files.binary_search_by(|f| f.cmp(&file)) {
        Ok(index) => Some(peer_files.remove(index)),
        Err(_) => None,
    }
}

impl Synchronizer {
    pub fn new(config: Config) -> Self {
        let config = Arc::new(config);

        let (events_channel_sender, events_channel_receiver) = mpsc::channel(50);
        let file_watcher = FileWatcher::new(
            events_channel_sender.clone(),
            config.clone(),
        );

        let server = Server::new(config.clone(), file_watcher.get_events_blocker());


        Synchronizer {
            config: config,
            server,
            events_channel_sender,
            events_channel_receiver,
            file_watcher
        }
    }

    pub async fn start(&mut self) -> crate::Result<()> {
        log::debug!("starting syncronizer");
        self.server.start(self.events_channel_sender.clone()).await?;
        self.file_watcher.start();
        self.start_peers_full_sync().await?;
        self.sync_events().await;

        Ok(())
    }

    async fn start_peers_full_sync(&self) -> crate::Result<()> {
        if let Some(peers) = &self.config.peers {
            for peer_address in peers.iter() {
                log::info!("schedulling peer {} for synchonization", peer_address);
                self.events_channel_sender
                    .send(SyncEvent::EnqueueSyncToPeer(peer_address.to_owned(), false))
                    .await?;
            }
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

                        for peer in peers.iter() {
                            self.sync_peer_single_action(peer, &action).await;
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
        peer_address: &str,
        action: &'b FileAction,
    ) -> crate::Result<()> {
        let mut peer = Peer::new(peer_address, &self.config).await?;
        peer.sync_action(action).await
    }

    async fn sync_peer<'b>(
        peer_address: String,
        two_way_sync: bool,
        config: &Config,
        events_blocker: Sender<BlockingEvent>,
    ) -> crate::Result<()> {
        let mut peer = Peer::new(&peer_address, config).await?;
        log::info!("Peer full synchronization started: {}", peer.get_address());

        peer.start_sync().await?;

        for (alias, path) in &config.paths {
            let (hash, mut local_files) = fs::get_files_with_hash(path, alias).await?;
            if !peer.need_to_sync(&alias, hash) {
                continue;
            }

            let mut peer_files = peer.fetch_files_for_alias(alias).await?;
            while let Some(local_file) = local_files.pop() {
                let peer_action = match get_peer_file(&local_file, &mut peer_files) {
                    Some(peer_file) => {
                        if local_file.deleted_at.is_some() && peer_file.deleted_at.is_some() {
                            //both files deleted, ignore
                            continue;
                        } else if local_file.deleted_at.is_some() && peer_file.deleted_at.is_none()
                        {
                            //remove remote file
                            FileAction::Remove(local_file)
                        } else if local_file.deleted_at.is_none() && peer_file.deleted_at.is_some()
                        {
                            events_blocker.send((local_file.get_absolute_path(&config)?, peer_address.clone())).await;
                            fs::delete_file(&local_file, &config).await?;
                            continue;
                        } else {
                            match local_file
                                .modified_at
                                .unwrap()
                                .cmp(&peer_file.modified_at.unwrap())
                            {
                                std::cmp::Ordering::Less => {
                                    events_blocker.send((local_file.get_absolute_path(&config)?, peer_address.clone())).await;
                                    FileAction::Request(local_file);
                                    continue;
                                }
                                std::cmp::Ordering::Equal => {
                                    continue;
                                }
                                std::cmp::Ordering::Greater => FileAction::Update(local_file),
                            }
                        }
                    }
                    None => {
                        if local_file.deleted_at.is_some() {
                            // deleted local file doesn't exist on remote
                            continue;
                        } else {
                            // local file, doesn't exist on remote
                            // create file
                            FileAction::Create(local_file)
                        }
                    }
                };

                peer.sync_action(&peer_action).await?
            }

            while let Some(peer_file) = peer_files.pop() {
                if peer_file.deleted_at.is_some() {
                    events_blocker.send((peer_file.get_absolute_path(&config)?, peer_address.clone())).await;
                    fs::delete_file(&peer_file, &config).await?;
                } else {
                    events_blocker.send((peer_file.get_absolute_path(&config)?, peer_address.clone())).await;
                    peer.sync_action(&FileAction::Request(peer_file)).await?;
                }
            }
        }

        peer.finish_sync(two_way_sync).await
    }
}
