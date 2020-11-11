use std::{collections::{HashMap, VecDeque}, path::Path, path::PathBuf, time::Duration};
use std::sync::Arc;
use tokio::sync::{Notify, RwLock, mpsc, mpsc::Receiver, mpsc::Sender};
use notify::{DebouncedEvent, RecursiveMode, Watcher, watcher};

use crate::{
    RSyncError, 
    config::Config, 
    fs, 
    fs::FileInfo,
    network::peer::{Peer, SyncAction}, 
    network::server::{Server, ServerStatus}, 
};

const EVENT_DEBOUNCE_SECS: u64 = 30;
type PeerAddress = String;

#[derive(Debug)]
pub(crate) enum FileAction {
    Create(PathBuf),
    Update(PathBuf),
    Move(PathBuf, PathBuf),
    Remove(PathBuf)
}
pub(crate) enum SyncEvent {
    EnqueueSyncToPeer(PeerAddress),
    SyncToPeerStarted(PeerAddress),
    SyncToPeerFailed(PeerAddress),
    SyncToPeerSuccess(PeerAddress),

    PeerRequestedSync(PeerAddress, Arc<Notify>),
    SyncFromPeerStarted(PeerAddress),
    SyncFromPeerFinished(PeerAddress, HashMap<String, u64>),

    BroadcastToAllPeers(String, FileAction),
}

enum SyncQueue {
    SyncToPeer(PeerAddress),
    SyncFromPeer(PeerAddress, Arc<Notify>),
    BroadcastToAllPeers(String, FileAction)
}
pub struct Synchronizer {
    config: Arc<Config>,
    peers: HashMap<String, RwLock<Peer>>,
    server: Server,
    sync_hash: HashMap<String, u64>,
    sync_queue: VecDeque<SyncQueue>
}

fn need_to_sync_file<'a>(file: &FileInfo, peer_files: &mut Vec<FileInfo>) -> bool {
    match peer_files.binary_search_by(|f| f.cmp(file)) {
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
            peers: HashMap::new(),
            server: Server::new(config),
            sync_hash: HashMap::new(),
            sync_queue: VecDeque::new()
        }
    }
    
    pub async fn start(&'a mut self) -> Result<(), RSyncError> {
        self.load_sync_hash().await?;
        
        let (tx, rx) = mpsc::channel(10);
        self.server.start(tx.clone()).await?;
        
        self.start_watcher(tx.clone());
        self.load_peers(tx).await;
        self.sync_events(rx).await;

        Ok(())
    }

    async fn load_sync_hash(&mut self) -> Result<(), RSyncError> {
        self.sync_hash = fs::get_hash_for_alias(&self.config.paths).await?;
        Ok(())
    }

    async fn load_peers(&'a mut self, sync_events: Sender<SyncEvent>) {
        for peer_address in self.config.peers.iter() {
            let peer = Peer::new(peer_address.clone(), sync_events.clone());
            self.peers.insert(peer_address.clone(), RwLock::new(peer));
            
            sync_events.send(SyncEvent::EnqueueSyncToPeer(peer_address.clone())).await;
        }
    }

    async fn work_on_queue(&mut self) {
        println!("Server Status: {:?}", self.server.get_status());
        if self.server.get_status() != ServerStatus::Idle {
            println!("Server is not idle");
            return;
        }

        println!("Work Queue: {}", self.sync_queue.len());
        if let Some(work_item) = self.sync_queue.pop_front() {
            match work_item {
                SyncQueue::SyncToPeer(peer_address) => {
                    if let Err(_) = self.sync_peer(&peer_address).await {
                        eprintln!("Sync to peer {} failed", &peer_address);
                        let peer = self.peers.get(&peer_address).unwrap();
                        let mut peer = peer.write().await;
                        peer.disconnect().await;
                        self.server.set_status(ServerStatus::Idle);
                    }
                }
                SyncQueue::SyncFromPeer(peer_address, notify) => {
                    self.server.set_status(ServerStatus::SyncInProgress(peer_address));
                    notify.notify_one();
                }
                SyncQueue::BroadcastToAllPeers(alias, file_action) => {
                    println!("broadcast: {} {:?}", alias, file_action);
                    todo!()
                }
            }
        }
    }

    async fn sync_events(&'a mut self, mut rx: Receiver<SyncEvent>) {
        loop { 
            match rx.recv().await {
                Some(event) => {
                    match event {
                        SyncEvent::EnqueueSyncToPeer(peer_address) => {
                            self.sync_queue.push_back(SyncQueue::SyncToPeer(peer_address));
                        }
                        SyncEvent::SyncToPeerStarted(_) => {}
                        SyncEvent::SyncToPeerFailed(_) | SyncEvent::SyncToPeerSuccess(_) => {
                            self.server.set_status(ServerStatus::Idle);
                        }
                        SyncEvent::PeerRequestedSync(peer_address, notify) => {
                            self.sync_queue.push_back(SyncQueue::SyncFromPeer(peer_address, notify));
                        }
                        SyncEvent::SyncFromPeerStarted(_) => {}
                        SyncEvent::SyncFromPeerFinished(peer_address, peer_sync_hash) => {
                            self.load_sync_hash().await;
                            for (alias, hash) in self.sync_hash.iter() {
                                if let Some(peer_hash) = peer_sync_hash.get(alias) {
                                    if *peer_hash != *hash {
                                        self.sync_queue.push_back(SyncQueue::SyncToPeer(peer_address));
                                        break;
                                    }
                                }
                            }

                            self.server.set_status(ServerStatus::Idle);
                        }
                        SyncEvent::BroadcastToAllPeers(alias, action) => {
                            self.sync_queue.push_back(SyncQueue::BroadcastToAllPeers(alias, action));
                        }
                    }
                }
                None => { break; }
            }

            self.work_on_queue().await;
        }
    }

    async fn sync_peer(&'a mut self, peer_address: &str) -> Result<(), RSyncError> {
        println!("Syncing to peer: {}", peer_address);
        
        let peer = match self.peers.get(peer_address) {
            Some(peer) => { peer }
            None => { return Err(RSyncError::InvalidPeerAddress)}
        };
        
        let mut peer = peer.write().await;

        self.server.set_status(ServerStatus::SyncInProgress(peer_address.to_string()));

        peer.connect().await;
        peer.start_sync().await?;

        for (alias, hash) in &self.sync_hash {
            if !peer.need_to_sync(&alias, *hash) {
                continue;
            }

            let local_files = fs::walk_path(self.config.paths.get(alias).unwrap().clone())
                .await
                .map_err(|_| RSyncError::ErrorReadingLocalFiles)?;

            let mut peer_files = peer.fetch_files_for_alias(alias).await?;

            for file_info in local_files.iter() {
                if need_to_sync_file(file_info, &mut peer_files){
                    peer.sync_action(SyncAction::Send(alias, self.config.paths.get(alias).unwrap().clone(), file_info.clone())).await?
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
                        let event= get_sync_event(event, &config.paths);
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

fn get_sync_event(event: DebouncedEvent, paths: &HashMap<String, PathBuf>) -> Option<SyncEvent> {
    match event {
        notify::DebouncedEvent::Create(file_path) => {
            let (alias, root) = get_alias_for_path(&file_path, paths)?;
            let file_path = file_path.canonicalize().ok()?;
            let file_path = file_path.strip_prefix(&root).ok()?;

            Some(SyncEvent::BroadcastToAllPeers(alias, FileAction::Create(file_path.to_owned())))
        }

        notify::DebouncedEvent::Write(file_path) => {
            let (alias, root) = get_alias_for_path(&file_path, paths)?;
            let file_path = file_path.canonicalize().ok()?;
            let file_path = file_path.strip_prefix(&root).ok()?;

            Some(SyncEvent::BroadcastToAllPeers(alias, FileAction::Update(file_path.to_owned())))
        }
        notify::DebouncedEvent::Remove(file_path) => {
            let (alias, root) = get_alias_for_path(&file_path, paths)?;
            let file_path = file_path.canonicalize().ok()?;
            let file_path = file_path.strip_prefix(&root).ok()?;

            Some(SyncEvent::BroadcastToAllPeers(alias, FileAction::Remove(file_path.to_owned())))
        }
        notify::DebouncedEvent::Rename(src_path, dest_path) => {
            let (alias, root) = get_alias_for_path(&src_path, paths)?;
            let src_path = src_path.canonicalize().ok()?;
            let src_path = src_path.strip_prefix(&root).ok()?;

            let dest_path = dest_path.canonicalize().ok()?;
            let dest_path = dest_path.strip_prefix(&root).ok()?;

            Some(SyncEvent::BroadcastToAllPeers(alias, FileAction::Move(src_path.to_owned(), dest_path.to_owned())))
        }
        _ => { None }
    }

}