use std::{collections::{HashMap}, path::Path, path::PathBuf, time::Duration};
use std::sync::Arc;
use tokio::sync::{Notify, mpsc, mpsc::Receiver, mpsc::Sender};
use notify::{DebouncedEvent, RecursiveMode, Watcher, watcher};

use crate::{RSyncError, config::Config, fs, fs::{LocalFile, RemoteFile}, network::peer::{Peer, SyncAction}, network::server::{Server, ServerStatus}};

const EVENT_DEBOUNCE_SECS: u64 = 30;
type PeerAddress = String;

#[derive(Debug)]
pub(crate) enum FileAction {
    Create(LocalFile),
    Update(LocalFile),
    Move(LocalFile, LocalFile),
    Remove(LocalFile)
}
pub(crate) enum SyncEvent {
    EnqueueSyncToPeer(PeerAddress),

    PeerRequestedSync(PeerAddress, Arc<Notify>),
    SyncFromPeerFinished(PeerAddress, HashMap<String, u64>),

    BroadcastToAllPeers(String, FileAction),
}

pub struct Synchronizer {
    config: Arc<Config>,
    server: Server,
    sync_hash: HashMap<String, u64>,
}

fn need_to_sync_file(file: &LocalFile, peer_files: &mut Vec<RemoteFile>) -> bool {
    let file = RemoteFile::from(file);
    match peer_files.binary_search_by(|f| f.cmp(&file)) {
        Ok(index) => { 
            let peer_file = peer_files.remove(index);
            file.modified_at > peer_file.modified_at || file.size != peer_file.size
        }
        Err(_) => { true }
    }
}

impl <'a> Synchronizer {
    pub fn new(config: Config) -> Self {
        let config = Arc::new(config);
        
        Synchronizer {
            config: config.clone(),
            server: Server::new(config),
            sync_hash: HashMap::new(),
        }
    }
    
    pub async fn start(&'a mut self) -> Result<(), RSyncError> {
        self.load_sync_hash().await?;
        
        let (tx, rx) = mpsc::channel(50);
        self.server.start(tx.clone()).await?;
        
        if self.config.enable_file_watcher {
            // self.start_watcher(tx.clone());
        }
        self.sync_peers(tx.clone()).await;
        self.sync_events(rx, tx).await;

        Ok(())
    }

    async fn load_sync_hash(&mut self) -> Result<(), RSyncError> {
        self.sync_hash = fs::get_hash_for_alias(&self.config.paths).await?;
        Ok(())
    }

    async fn sync_peers(&'a mut self, sync_events: Sender<SyncEvent>) {
        for peer_address in self.config.peers.iter() {
            sync_events.send(SyncEvent::EnqueueSyncToPeer(peer_address.to_owned())).await;
        }
    }
    
    async fn sync_events(&'a mut self, mut events_receiver: Receiver<SyncEvent>, events_sender: Sender<SyncEvent>) {
        loop { 
            match events_receiver.recv().await {
                Some(event) => {
                    match event {
                        SyncEvent::EnqueueSyncToPeer(peer_address) => {
                            match self.sync_peer(peer_address).await {
                                Ok(_) => { println!("Peer synchronization successful") }
                                Err(e) => {
                                    self.server.set_status(ServerStatus::Idle);
                                    println!("Peer synchronization failed: {}", e);
                                }
                            }
                        }
                        SyncEvent::PeerRequestedSync(peer_address, notify) => {
                            println!("Peer requested synchronization: {}", peer_address);
                            self.server.set_status(ServerStatus::ReceivingFiles(peer_address));
                            notify.notify_one();
                        }
                        SyncEvent::SyncFromPeerFinished(peer_address, peer_sync_hash) => {
                            println!("Peer synchronization ended");
                            self.load_sync_hash().await;
                            for (alias, hash) in self.sync_hash.iter() {
                                if let Some(peer_hash) = peer_sync_hash.get(alias) {
                                    if *peer_hash != *hash {
                                        events_sender.send(SyncEvent::EnqueueSyncToPeer(peer_address.clone())).await;
                                        break;
                                    }
                                }
                            }

                            self.server.set_status(ServerStatus::Idle);
                        }
                        SyncEvent::BroadcastToAllPeers(alias, action) => {
                            println!("file changed on disk: {:?}", action);

                            let sync_action = match &action {
                                FileAction::Create(file) | FileAction::Update(file) => SyncAction::Send(&alias, file),
                                FileAction::Move(src, dest) => SyncAction::Move(&alias, src, dest),
                                FileAction::Remove(file) => SyncAction::Delete(&alias, file)
                            };

                            for peer in  self.config.peers.iter().map(|p| Peer::new(p)) {
                                self.sync_peer_single_action(peer, &sync_action).await;
                            }

                        }
                    }
                }
                None => { break; }
            }
        }
    }

    async fn sync_peer_single_action<'b>(&self, mut peer: Peer<'b>, action: &'b SyncAction<'b>) -> Result<(), RSyncError> {
        peer.connect().await?;
        peer.start_sync().await?;
        peer.sync_action(action).await
    }

    async fn sync_peer<'b>(&'b mut self, peer_address: String) -> Result<(), RSyncError> {
        let mut peer = Peer::new(&peer_address);
        println!("Peer full synchronization started: {}", peer.get_address());
        
        self.server.set_status(ServerStatus::SendingFiles(peer.get_address().to_string()));

        peer.connect().await?;
        peer.start_sync().await?;

        for (alias, hash) in &self.sync_hash {
            if !peer.need_to_sync(&alias, *hash) {
                continue;
            }

            let local_files = fs::walk_path(self.config.paths.get(alias).unwrap())
                .await
                .map_err(|_| RSyncError::ErrorReadingLocalFiles)?;

            let mut peer_files = peer.fetch_files_for_alias(alias).await?;

            for file_info in local_files.iter() {
                if need_to_sync_file(file_info, &mut peer_files){
                    peer.sync_action(&SyncAction::Send(alias, file_info)).await?
                }
            }
        }

        peer.finish_sync(self.sync_hash.clone()).await
    }

    

    fn start_watcher(&self, sync_event_sender: Sender<SyncEvent>) {
        let config = self.config.clone();

        tokio::task::spawn_blocking(move || {
            let (tx, rx) = std::sync::mpsc::channel();
    
            let mut watcher = match watcher(tx, Duration::from_secs(EVENT_DEBOUNCE_SECS)) {
                Ok(watcher) => { watcher }
                Err(_) => {
                    eprintln!("Cannot start watcher");
                    return;
                }
            };

            for (_, path) in config.paths.iter() {
                if let Err(_) = watcher.watch(path, RecursiveMode::Recursive) {
                    eprintln!("Cannot watch path");
                }
            }

            loop {
                match rx.recv() {
                   Ok(event) => {
                        let event= map_to_sync_event(event, &config.paths);
                        event.and_then(|event| { sync_event_sender.blocking_send(event).ok() });
                   },
                   Err(e) => println!("watch error: {:?}", e),
                }
            }
        });
    }
}


fn get_alias_for_path(file_path: &Path, paths: &HashMap<String, PathBuf>) -> Option<(String, PathBuf)>{
    let file_path = file_path.canonicalize().ok()?;

    for (alias, config_path) in paths.iter() {
         let config_path = match config_path.canonicalize() {
             Ok(config_path) => { config_path }
             Err(_) => { return None }
         };

         if file_path.starts_with(&config_path) {
            return Some((alias.clone(), config_path.clone()));
         }
    }

    return None;
}

/// Map a [DebouncedEvent] to a [SyncEvent]`(` alias, file_path)
///
/// Returns [Some]`(`[SyncEvent]`)` if success  
/// Returns [None] for ignored events
fn map_to_sync_event(event: DebouncedEvent, paths: &HashMap<String, PathBuf>) -> Option<SyncEvent> {
    match event {
        notify::DebouncedEvent::Create(file_path) => {
            let (alias, root) = get_alias_for_path(&file_path, paths)?;
            let absolute_path = file_path.canonicalize().ok()?;
            let relative_path = file_path.strip_prefix(&root).ok()?;

            Some(SyncEvent::BroadcastToAllPeers(alias, FileAction::Create(LocalFile::new(absolute_path, relative_path.to_owned()).ok()?)))
        }

        notify::DebouncedEvent::Write(file_path) => {
            let (alias, root) = get_alias_for_path(&file_path, paths)?;
            let absolute_path = file_path.canonicalize().ok()?;
            let relative_path = file_path.strip_prefix(&root).ok()?;

            Some(SyncEvent::BroadcastToAllPeers(alias, FileAction::Update(LocalFile::new(absolute_path, relative_path.to_owned()).ok()?)))
        }
        notify::DebouncedEvent::Remove(file_path) => {
            let (alias, root) = get_alias_for_path(&file_path, paths)?;
            let absolute_path = file_path.canonicalize().ok()?;
            let relative_path = file_path.strip_prefix(&root).ok()?;

            Some(SyncEvent::BroadcastToAllPeers(alias, FileAction::Remove(LocalFile::new(absolute_path, relative_path.to_owned()).ok()?)))
        }
        notify::DebouncedEvent::Rename(src_path, dest_path) => {
            let (alias, root) = get_alias_for_path(&src_path, paths)?;
            let src_absolute = src_path.canonicalize().ok()?;
            let src_relative = src_path.strip_prefix(&root).ok()?;
            let src_file = LocalFile::new(src_absolute, src_relative.to_owned()).ok()?;

            let dest_absolute = dest_path.canonicalize().ok()?;
            let dest_relative = dest_path.strip_prefix(&root).ok()?;
            let dest_file = LocalFile::new(dest_absolute, dest_relative.to_owned()).ok()?;

            Some(SyncEvent::BroadcastToAllPeers(alias, FileAction::Move(src_file, dest_file)))
        }
        _ => { None }
    }

}