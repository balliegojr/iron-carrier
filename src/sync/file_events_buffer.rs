use std::{
    collections::HashMap, 
    path::PathBuf, 
    sync::{
        RwLock,
        Arc
    }
};

use crate::{config::Config, fs::FileInfo};

/// Keeps track of received events to avoid sending the same events back
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

    /// Returns a [Vec]<`[String]`> containing peer address that can receive events for this [FileInfo]
    pub fn allowed_peers_for_event(&self, file: &FileInfo) -> Option<Vec<String>> {
        let mut peers = self.config.peers.clone()?;

        let absolute_path = file.get_absolute_path(&self.config);
        let absolute_path = match absolute_path {
            Ok(path) => { path }
            Err(_) => { return Some(peers) }
        };

        let limit = std::time::Instant::now() - std::time::Duration::from_secs(self.config.delay_watcher_events * 2);
        let received_file_events = self.events.read().unwrap();

        if let Some((event_peer_address, event_time)) = received_file_events.get(&absolute_path) {
            if event_time > &limit {
                peers.retain(|p| *p != *event_peer_address);
            }
        }
        Some(peers)
     }

    pub fn add_event(&self, file_info: &FileInfo, peer_address: &str) {
        let absolute_path = file_info.get_absolute_path(&self.config).unwrap();
        
        let mut received_events_guard = self.events.write().unwrap();
        received_events_guard.insert(absolute_path.clone(), (peer_address.to_owned(), std::time::Instant::now()));

        let received_events = self.events.clone();
        let debounce_time = self.config.delay_watcher_events + 1;
        tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_secs(debounce_time)).await;

            let mut received_events_guard = received_events.write().unwrap();
            received_events_guard.remove(&absolute_path);

        });
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    pub async fn filter_events() {
        let config = Config::parse_content("peers = [\"a\", \"b\"]
        delay_watcher_events=1
        [paths]
        a = \"./tmp\"
        ".into()).unwrap();

        let buffer = FileEventsBuffer::new(Arc::new(config));
        let file_info = FileInfo::new_deleted("a".into(), "file".into(), None);

        assert_eq!(2, buffer.allowed_peers_for_event(&file_info).unwrap().len());

        buffer.add_event(&file_info, "a");
        assert_eq!(1, buffer.allowed_peers_for_event(&file_info).unwrap().len());

        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(2, buffer.allowed_peers_for_event(&file_info).unwrap().len());

        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(2, buffer.allowed_peers_for_event(&file_info).unwrap().len());

    }
}