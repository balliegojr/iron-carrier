use std::{
    collections::HashMap, 
    path::PathBuf, 
    sync::{
        RwLock,
        Arc
    }
};

use crate::{config::Config, fs::FileInfo};

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

    pub fn allowed_peers_for_event(&self, file: &FileInfo) -> Option<Vec<String>> {
        let mut peers = self.config.peers.clone()?;

        let absolute_path = file.get_absolute_path(&self.config);
        let absolute_path = match absolute_path {
            Ok(path) => { path }
            Err(_) => { return Some(peers) }
        };

        let limit = std::time::Instant::now() - std::time::Duration::from_secs(self.config.delay_watcher_events * 2);

        let received_file_events = self.events.read().unwrap();
        
        match received_file_events.get(&absolute_path) {
            Some((event_peer_address, event_time)) => {
                if event_time < &limit {
                    None
                } else {
                    peers.retain(|p| *p != *event_peer_address);
                    Some(peers)
                }
            }
            None => Some(peers)
        }
     }

    pub fn add_event(&self, file_path: PathBuf, peer_address: &str) {
        let mut received_events_guard = self.events.write().unwrap();
        received_events_guard.insert(file_path.clone(), (peer_address.to_owned(), std::time::Instant::now()));

        let received_events = self.events.clone();
        let debounce_time = self.config.delay_watcher_events + 1;
        tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_secs(debounce_time)).await;

            let mut received_events_guard = received_events.write().unwrap();
            received_events_guard.remove(&file_path);

        });
    }
}
