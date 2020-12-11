
use std::{
    collections::HashMap, 
    path::PathBuf, 
    sync::{
        RwLock,
        Arc
    }
};
use tokio::sync::{
    mpsc, 
    mpsc::Receiver, 
    mpsc::Sender
};

use crate::{config::Config, deletion_tracker::DeletionTracker, fs, fs::FileInfo, network::peer::Peer, network::server::Server};
use super::{file_watcher::FileWatcher, SyncEvent, FileAction};

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


pub(crate) struct FileEventsBuffer {
    config: Arc<Config>,
    events: Arc<RwLock<HashMap<PathBuf, (String, std::time::Instant)>>>,
}

impl FileEventsBuffer {
    pub fn new(config: Arc<Config>) -> Self {
        FileEventsBuffer {
            events: Arc::new(RwLock::new(HashMap::new())),
            config
        }
    }

    pub fn ignore_event(&self, file_action: &FileAction, peer_address: &str) -> bool {
        let absolute_path = match file_action {
            FileAction::Create(file) 
            | FileAction::Update(file)
            | FileAction::Remove(file) 
            | FileAction::Request(file) => { file.get_absolute_path(&self.config) }
            FileAction::Move(file, _) => { file.get_absolute_path(&self.config) }
        };

        let absolute_path = match absolute_path {
            Ok(path) => { path }
            Err(_) => { return false }
        };

        let limit = std::time::Instant::now() - std::time::Duration::from_secs(self.config.debounce_events_seconds * 2);

        let received_file_events = self.events.read().unwrap();
        match received_file_events.get(&absolute_path) {
            Some((event_peer_address, event_time)) => {
                *event_peer_address == peer_address && event_time > &limit
            }
            None => { false }
        }
    }

    pub fn add_event(&self, file_path: PathBuf, peer_address: &str) {
        let mut received_events_guard = self.events.write().unwrap();
        received_events_guard.insert(file_path.clone(), (peer_address.to_owned(), std::time::Instant::now()));

        let received_events = self.events.clone();
        let debounce_time = self.config.debounce_events_seconds + 1;
        tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_secs(debounce_time)).await;

            let mut received_events_guard = received_events.write().unwrap();
            received_events_guard.remove(&file_path);

        });
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
            match FileWatcher::new(sync_events_sender.clone(), self.config.clone()) {
                Ok(file_watcher) => { self.file_watcher = Some(file_watcher); }
                Err(err) => { eprintln!("{}", err) }
            }
        }

        self.sync_peers(sync_events_sender).await;
        self.sync_events(sync_events_receiver).await;

        Ok(())
    }

    async fn sync_peers(&self, sync_events: Sender<SyncEvent>) {
        for peer_address in self.config.peers.iter() {
            sync_events.send(SyncEvent::EnqueueSyncToPeer(peer_address.to_owned(), false)).await;
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
                        SyncEvent::BroadcastToAllPeers(action) => {
                            println!("file changed on disk: {:?}", action);

                            match &action {
                                FileAction::Create(file) => {
                                    let root_path = self.config.paths.get(&file.alias).unwrap();
                                    DeletionTracker::new(root_path).remove_entry(&file.path).await;
                                }
                                FileAction::Move(file, _) | FileAction::Remove(file) => { 
                                    let root_path = self.config.paths.get(&file.alias).unwrap();
                                    DeletionTracker::new(root_path).add_entry(&file.path).await;
                                }
                                _ => {}
                            }

                            for peer in  self.config.peers.iter()
                                .filter(|p|!self.events_buffer.ignore_event(&action, p))
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
                    // local file, doesn't exist on remote
                    // create file
                    self.events_buffer.add_event(peer_file.get_absolute_path(&self.config)?, &peer_address);
                    peer.sync_action(&FileAction::Request(peer_file)).await?;
                }

            }

        }

        peer.finish_sync(two_way_sync).await
    }
}