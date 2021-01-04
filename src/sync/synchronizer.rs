use std::sync::Arc;
use tokio::sync::{mpsc, mpsc::Receiver, mpsc::Sender};

use super::{
    file_events_buffer::FileEventsBuffer, file_watcher::FileWatcher, FileAction, SyncEvent,
};
use crate::{config::Config, fs, fs::FileInfo, network::peer::Peer, network::server::Server};

pub struct Synchronizer {
    config: Arc<Config>,
    server: Server,
    file_watcher: Option<FileWatcher>,
    events_buffer: Arc<FileEventsBuffer>,
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
        let events_buffer = Arc::new(FileEventsBuffer::new(config.clone()));
        let server = Server::new(config.clone(), events_buffer.clone());

        Synchronizer {
            config: config,
            events_buffer,
            server,
            file_watcher: None,
        }
    }

    pub async fn start(&mut self, _auto_exit: bool) -> crate::Result<()> {
        let (sync_events_sender, sync_events_receiver) = mpsc::channel(50);

        log::debug!("starting syncronizer");
        self.server.start(sync_events_sender.clone()).await?;

        if self.config.enable_file_watcher {
            match FileWatcher::new(
                sync_events_sender.clone(),
                self.config.clone(),
                self.events_buffer.clone(),
            ) {
                Ok(file_watcher) => {
                    self.file_watcher = Some(file_watcher);
                }
                Err(err) => {
                    log::error!("some error ocurred with the file watcher");
                    log::error!("{}", err)
                }
            }
        }

        self.schedule_peers(sync_events_sender).await?;
        self.sync_events(sync_events_receiver).await;

        Ok(())
    }

    async fn schedule_peers(&self, sync_events: Sender<SyncEvent>) -> crate::Result<()> {
        if let Some(peers) = &self.config.peers {
            for peer_address in peers.iter() {
                log::info!("schedulling peer {} for synchonization", peer_address);
                sync_events
                    .send(SyncEvent::EnqueueSyncToPeer(peer_address.to_owned(), false))
                    .await?;
            }
        }

        Ok(())
    }

    async fn sync_events(&self, mut events_receiver: Receiver<SyncEvent>) {
        loop {
            match events_receiver.recv().await {
                Some(event) => match event {
                    SyncEvent::EnqueueSyncToPeer(peer_address, two_way_sync) => {
                        let config = self.config.clone();
                        let events_buffer = self.events_buffer.clone();

                        tokio::spawn(async move {
                            match Synchronizer::sync_peer(
                                peer_address,
                                two_way_sync,
                                &config,
                                &events_buffer,
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
        let mut peer = Peer::new(peer_address, &self.config, &self.events_buffer).await?;
        peer.sync_action(action).await
    }

    async fn sync_peer<'b>(
        peer_address: String,
        two_way_sync: bool,
        config: &Config,
        events_buffer: &FileEventsBuffer,
    ) -> crate::Result<()> {
        let mut peer = Peer::new(&peer_address, config, events_buffer).await?;
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
                            events_buffer.add_event(&local_file, &peer_address);
                            fs::delete_file(&local_file, &config).await?;
                            continue;
                        } else {
                            match local_file
                                .modified_at
                                .unwrap()
                                .cmp(&peer_file.modified_at.unwrap())
                            {
                                std::cmp::Ordering::Less => {
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
                    events_buffer.add_event(&peer_file, &peer_address);
                    fs::delete_file(&peer_file, &config).await?;
                } else {
                    peer.sync_action(&FileAction::Request(peer_file)).await?;
                }
            }
        }

        peer.finish_sync(two_way_sync).await
    }
}
