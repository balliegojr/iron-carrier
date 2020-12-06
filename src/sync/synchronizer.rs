
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

use crate::{
    config::Config, 
    fs, 
    fs::FileInfo, 
    network::peer::Peer, 
    network::server::Server
};
use super::{file_watcher::FileWatcher, SyncEvent, FileAction};

pub struct Synchronizer {
    config: Arc<Config>,
    server: Server,
    file_watcher: Option<FileWatcher>,
    received_file_events: Arc<RwLock<HashMap<PathBuf, (String, std::time::Instant)>>>
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
            file_watcher: None,
            received_file_events: Arc::new(RwLock::new(HashMap::new()))
        }
    }
    
    pub async fn start(&mut self) -> crate::Result<()> {
        let (sync_events_sender, sync_events_receiver) = mpsc::channel(50);
        let (file_events_sender, file_events_receiver) = mpsc::channel(1);

        self.server.start(sync_events_sender.clone(), file_events_sender).await?;
        
        if self.config.enable_file_watcher {
            match FileWatcher::new(sync_events_sender.clone(), self.config.clone()) {
                Ok(file_watcher) => { self.file_watcher = Some(file_watcher); }
                Err(err) => { eprintln!("{}", err) }
            }
        }

        self.handle_file_events(file_events_receiver);
        self.sync_peers(sync_events_sender).await;
        self.sync_events(sync_events_receiver).await;

        Ok(())
    }

    async fn sync_peers(&self, sync_events: Sender<SyncEvent>) {
        for peer_address in self.config.peers.iter() {
            sync_events.send(SyncEvent::EnqueueSyncToPeer(peer_address.to_owned(), true)).await;
        }
    }

    fn send_event_to_peer(&self, file_action: &FileAction, peer_address: &str) -> bool {
        let absolute_path = match file_action {
            FileAction::Create(file) | FileAction::Update(file)=> { file.get_absolute_path(&self.config) }
            FileAction::Move(file, _) => { file.get_absolute_path(&self.config) }
            FileAction::Remove(file) => { file.get_absolute_path(&self.config) }
        };

        let absolute_path = match absolute_path {
            Ok(path) => { path }
            Err(_) => { return false }
        };

        let limit = std::time::Instant::now() - std::time::Duration::from_secs(self.config.debounce_events_seconds * 2);

        let received_file_events = self.received_file_events.read().unwrap();
        match received_file_events.get(&absolute_path) {
            Some((event_peer_address, event_time)) => { 
                !(event_peer_address == peer_address && event_time > &limit)
            }
            None => { true }
        }
    }

    fn handle_file_events(&self, mut file_events_receiver: Receiver<(PathBuf, String)>) {
        let received_events = self.received_file_events.clone();
        let debounce_time = self.config.debounce_events_seconds * 2;

        tokio::spawn(async move {
            loop { 
                match file_events_receiver.recv().await {
                    Some((path, peer_address)) => {

                        let limit = std::time::Instant::now() - std::time::Duration::from_secs(debounce_time);

                        let mut received_events = received_events.write().unwrap();

                        received_events.retain(|_, (_, instant)| { *instant > limit });
                        received_events.insert(path, (peer_address, std::time::Instant::now()));

                    }
                    None => { break; }
                }

            }
        });
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

                            for peer in  self.config.peers.iter().filter(|p|self.send_event_to_peer(&action, p)).map(|p| Peer::new(p, &self.config)) {
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
            while let Some(file_info) = local_files.pop() {
                if need_to_sync_file(&file_info, &mut peer_files){
                    peer.sync_action(&FileAction::Update(file_info)).await?
                }
            }
        }

        peer.finish_sync(two_way_sync).await
    }
}