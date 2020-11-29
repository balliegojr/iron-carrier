
use std::{collections::{HashMap}, path::PathBuf};
use std::sync::Arc;
use tokio::sync::{mpsc, mpsc::Receiver, mpsc::Sender};

use crate::{config::Config, fs, fs::FileInfo, network::peer::Peer, network::server::{Server, ServerStatus}};
use super::{file_watcher::FileWatcher, SyncEvent, FileAction};

pub struct Synchronizer {
    config: Arc<Config>,
    server: Server,
    sync_hash: HashMap<String, u64>,
    file_watcher: Option<FileWatcher>,
    received_file_events: HashMap<PathBuf, (String, std::time::Instant)>
}

fn need_to_sync_file(file: &FileInfo, peer_files: &mut Vec<FileInfo>) -> bool {
    // let file = RemoteFile::from(file);
    match peer_files.binary_search_by(|f| f.cmp(&file)) {
        Ok(index) => { 
            let peer_file = peer_files.remove(index);
            fs::is_local_file_newer_than_remote(file, &peer_file)
        }
        Err(_) => { true }
    }
}

impl Synchronizer {
    pub fn new(config: Config) -> Self {
        let config = Arc::new(config);
        
        Synchronizer {
            config: config.clone(),
            server: Server::new(config),
            sync_hash: HashMap::new(),
            file_watcher: None,
            received_file_events: HashMap::new()
        }
    }
    
    pub async fn start(&mut self) -> crate::Result<()> {
        self.load_sync_hash().await?;
        
        let (tx, rx) = mpsc::channel(50);
        self.server.start(tx.clone()).await?;
        
        if self.config.enable_file_watcher {
            match FileWatcher::new(tx.clone(), self.config.clone()) {
                Ok(file_watcher) => { self.file_watcher = Some(file_watcher); }
                Err(err) => { eprintln!("{}", err) }
            }
        }
        self.sync_peers(tx.clone()).await;
        self.sync_events(rx, tx).await;

        Ok(())
    }

    async fn load_sync_hash(&mut self) -> crate::Result<()> {
        self.sync_hash = fs::get_hash_for_alias(&self.config.paths).await?;
        Ok(())
    }

    async fn sync_peers(&self, sync_events: Sender<SyncEvent>) {
        for peer_address in self.config.peers.iter() {
            sync_events.send(SyncEvent::EnqueueSyncToPeer(peer_address.to_owned())).await;
        }
    }

    fn send_event_for_this_peer(&self, file_action: &FileAction, peer_address: &str) -> bool {
        let absolute_path = match file_action {
            FileAction::Create(file) | FileAction::Update(file)=> { file.get_absolute_path(&self.config) }
            FileAction::Move(file, _) => { file.get_absolute_path(&self.config) }
            FileAction::Remove(file) => { file.get_absolute_path(&self.config) }
        };

        let absolute_path = match absolute_path {
            Ok(path) => { path }
            Err(_) => { return false }
        };

        let limit = std::time::Instant::now() - std::time::Duration::from_secs(self.config.debounce_watcher_events * 2);

        match self.received_file_events.get(&absolute_path) {
            Some((event_peer_address, event_time)) => { 
                !(event_peer_address == peer_address && event_time > &limit)
            }
            None => { true }
        }
    }
    
    async fn sync_events(&mut self, mut events_receiver: Receiver<SyncEvent>, events_sender: Sender<SyncEvent>) {
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
                        SyncEvent::BroadcastToAllPeers(action) => {
                            println!("file changed on disk: {:?}", action);

                            for peer in  self.config.peers.iter().filter(|p|self.send_event_for_this_peer(&action, p)).map(|p| Peer::new(p, &self.config)) {
                                println!("sending file to {}", peer.get_address());
                                self.sync_peer_single_action(peer, &action).await;
                            }

                        }
                        SyncEvent::CompletedFileAction(path, peer_address) => {
                            let limit = std::time::Instant::now() - std::time::Duration::from_secs(&self.config.debounce_watcher_events * 2);

                            self.received_file_events.retain(|_, (_, instant)| { *instant > limit });
                            self.received_file_events.insert(path, (peer_address, std::time::Instant::now()));
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

    async fn sync_peer<'b>(&'b mut self, peer_address: String) -> crate::Result<()> {
        let mut peer = Peer::new(&peer_address, &self.config);
        println!("Peer full synchronization started: {}", peer.get_address());
        
        self.server.set_status(ServerStatus::SendingFiles(peer.get_address().to_string()));

        peer.connect().await?;
        peer.start_sync().await?;

        for (alias, hash) in &self.sync_hash {
            if !peer.need_to_sync(&alias, *hash) {
                continue;
            }

            let mut local_files = fs::walk_path(self.config.paths.get(alias).unwrap(), alias).await?;
            let mut peer_files = peer.fetch_files_for_alias(alias).await?;

            while let Some(file_info) = local_files.pop() {
                if need_to_sync_file(&file_info, &mut peer_files){
                    peer.sync_action(&FileAction::Update(file_info)).await?
                }
            }
        }

        peer.finish_sync(self.sync_hash.clone()).await
    }
}