
use std::{
    sync::{
        Arc
    }
};
use tokio::sync::{
    mpsc, 
    mpsc::Receiver, 
    mpsc::Sender
};

use crate::{config::Config, fs, fs::FileInfo, network::peer::Peer, network::server::Server};
use super::{
    FileAction, 
    file_events_buffer::FileEventsBuffer, 
    SyncEvent, 
    file_watcher::FileWatcher
};

pub struct Synchronizer {
    config: Arc<Config>,
    server: Server,
    file_watcher: Option<FileWatcher>,
    events_buffer: Arc<FileEventsBuffer>
}

/// lookup for the peer file  
/// if the file is found, return the file and remove it from peer_files
fn get_peer_file(file: &FileInfo, peer_files: &mut Vec<FileInfo>) -> Option<FileInfo> {
    // let file = RemoteFile::from(file);
    match peer_files.binary_search_by(|f| f.cmp(&file)) {
        Ok(index) => Some(peer_files.remove(index)),
        Err(_) => None
    }
}

impl Synchronizer {
    pub fn new(config: Config) -> Self {
        let config = Arc::new(config);
        
        let events_buffer = Arc::new(FileEventsBuffer::new(config.clone()));
        Synchronizer {
            config: config.clone(),
            events_buffer: events_buffer.clone(),
            server: Server::new(config, events_buffer),
            file_watcher: None,
        }
    }
    
    pub async fn start(&mut self) -> crate::Result<()> {
        let (sync_events_sender, sync_events_receiver) = mpsc::channel(50);

        self.server.start(sync_events_sender.clone()).await?;
        
        if self.config.enable_file_watcher {
            match FileWatcher::new(sync_events_sender.clone(), self.config.clone(), self.events_buffer.clone()) {
                Ok(file_watcher) => { self.file_watcher = Some(file_watcher); }
                Err(err) => { eprintln!("{}", err) }
            }
        }

        self.sync_peers(sync_events_sender).await;
        self.sync_events(sync_events_receiver).await;

        Ok(())
    }

    async fn sync_peers(&self, sync_events: Sender<SyncEvent>) {
        if let Some(peers) = &self.config.peers {
            for peer_address in peers.iter() {
                sync_events.send(SyncEvent::EnqueueSyncToPeer(peer_address.to_owned(), false)).await;
            }
        }
    }

    
    
    async fn sync_events(&mut self, mut events_receiver: Receiver<SyncEvent>) {
        loop { 
            match events_receiver.recv().await {
                Some(event) => {
                    match event {
                        SyncEvent::EnqueueSyncToPeer(peer_address, two_way_sync) => {
                            match self.sync_peer(peer_address, two_way_sync).await {
                                Ok(_) => { println!("Peer synchronization successful") }
                                Err(e) => {
                                    println!("Peer synchronization failed: {}", e);
                                }
                            }
                        }
                        SyncEvent::PeerRequestedSync(peer_address, sync_starter, sync_ended) => {
                            println!("Peer requested synchronization: {}", peer_address);
                            sync_starter.notify_one();
                            sync_ended.notified().await;

                            println!("Peer synchronization ended");
                        }
                        SyncEvent::BroadcastToAllPeers(action, peers) => {
                            println!("file changed on disk: {:?}", action);

                            for peer in peers.iter()
                                .map(|p| Peer::new(p, &self.config)) {
                                    println!("sending file to {}", peer.get_address());
                                    self.sync_peer_single_action(peer, &action).await;
                            }
                        }
                    }
                }
                None => { break; }
            }
        }
    }

    async fn sync_peer_single_action<'b>(&self, mut peer: Peer<'b>, action: &'b FileAction) -> crate::Result<()> {
        peer.connect().await?;
        peer.start_sync().await?;
        peer.sync_action(action).await
    }

    async fn sync_peer<'b>(&'b mut self, peer_address: String, two_way_sync: bool) -> crate::Result<()> {
        let mut peer = Peer::new(&peer_address, &self.config);
        println!("Peer full synchronization started: {}", peer.get_address());
        
        peer.connect().await?;
        peer.start_sync().await?;

        for (alias, path) in &self.config.paths {
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
                        } else if local_file.deleted_at.is_some() && peer_file.deleted_at.is_none() {
                            //remove remote file
                            FileAction::Remove(local_file)
                        } else if local_file.deleted_at.is_none() && peer_file.deleted_at.is_some() {
                            self.events_buffer.add_event(local_file.get_absolute_path(&self.config)?, &peer_address);
                            fs::delete_file(&local_file, &self.config).await?;
                            continue;
                        } else  {
                            match local_file.modified_at.unwrap().cmp(&peer_file.modified_at.unwrap()) {
                                std::cmp::Ordering::Less => {
                                    self.events_buffer.add_event(peer_file.get_absolute_path(&self.config)?, &peer_address);
                                    FileAction::Request(local_file);
                                    continue;
                                }
                                std::cmp::Ordering::Equal => {
                                    continue;
                                }
                                std::cmp::Ordering::Greater => {
                                    FileAction::Update(local_file)
                                }
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
                    self.events_buffer.add_event(peer_file.get_absolute_path(&self.config)?, &peer_address);
                    fs::delete_file(&peer_file, &self.config).await?;
                } else {
                    self.events_buffer.add_event(peer_file.get_absolute_path(&self.config)?, &peer_address);
                    peer.sync_action(&FileAction::Request(peer_file)).await?;
                }

            }

        }

        peer.finish_sync(two_way_sync).await
    }
}