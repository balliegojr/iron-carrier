use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use fs::FileInfo;
use tokio::sync::{Notify, RwLock, mpsc, mpsc::Receiver, mpsc::Sender};

use crate::{RSyncError, config::Config, fs, network::peer::Peer, network::peer::SyncAction, network::server::Server, network::server::ServerStatus};

type PeerAddress = String;
pub enum SyncEvent {
    EnqueueSyncToPeer(PeerAddress),
    SyncToPeerStarted(PeerAddress),
    SyncToPeerFailed(PeerAddress),
    SyncToPeerSuccess(PeerAddress),

    PeerRequestedSync(PeerAddress, Arc<Notify>),
    SyncFromPeerStarted(PeerAddress),
    SyncFromPeerFinished(PeerAddress, HashMap<String, u64>),

}

enum SyncQueue {
    SyncToPeer(PeerAddress),
    SyncFromPeer(PeerAddress, Arc<Notify>)
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
                        SyncEvent::SyncToPeerFailed(_) => {
                            self.server.set_status(ServerStatus::Idle);
                        }
                        SyncEvent::SyncToPeerSuccess(_) => { 
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

            let local_files = fs::walk_path(&self.config.paths.get(alias).unwrap())
                .await
                .map_err(|_| RSyncError::ErrorReadingLocalFiles)?;

            let mut peer_files = peer.fetch_files_for_alias(alias).await?;

            for file_info in local_files.iter() {
                if need_to_sync_file(file_info, &mut peer_files){
                    peer.sync_action(SyncAction::Send(alias, &self.config.paths.get(alias).unwrap(), file_info.clone())).await?
                }
            }
        }

        peer.finish_sync(self.sync_hash.clone()).await
    }

    pub fn start_watcher(&self) {
        todo!()
    }
}