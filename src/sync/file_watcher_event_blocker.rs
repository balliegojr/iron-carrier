use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, RwLock},
};

use crate::{config::Config, fs::FileInfo};

/// Keeps track of received events to avoid sending the same events back
pub(crate) struct FileWatcherEventBlocker {
    config: Arc<Config>,
    events: RwLock<HashMap<PathBuf, (String, std::time::Instant)>>,
}

impl FileWatcherEventBlocker {
    pub fn new(config: Arc<Config>) -> Self {
        Self {
            events: RwLock::new(HashMap::new()),
            config,
        }
    }

    /// Returns a [Vec]<`[String]`> containing peer address that can receive events for this [FileInfo]
    pub fn allowed_peers_for_event(&self, file: &FileInfo) -> Option<Vec<String>> {
        let mut peers = self.config.peers.clone()?;

        let absolute_path = file.get_absolute_path(&self.config);
        let absolute_path = match absolute_path {
            Ok(path) => path,
            Err(_) => return Some(peers),
        };

        let limit = std::time::Instant::now()
            - std::time::Duration::from_secs(self.config.delay_watcher_events * 2);
        let mut received_file_events = self.events.write().unwrap();

        if let Some((event_peer_address, event_time)) = received_file_events.remove(&absolute_path) {
            if event_time > limit {
                peers.retain(|p| !p.starts_with(&event_peer_address));
            }
        }
        Some(peers)
    }
    
    pub fn block_next_event(&self, absolute_path: PathBuf, peer_address: String) {
        let mut received_events_guard = self.events.write().unwrap();
        received_events_guard.insert(
            absolute_path.clone(),
            (peer_address, std::time::Instant::now()),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    pub async fn filter_events() {
        let config = Arc::new(Config::parse_content(
            "peers = [\"a\", \"b\"]
        delay_watcher_events=1
        [paths]
        a = \"./tmp\"
        "
            .into(),
        )
        .unwrap());


        let buffer = FileWatcherEventBlocker::new(config.clone());
        let file_info = FileInfo::new_deleted("a".into(), "file".into(), None);

        assert_eq!(2, buffer.allowed_peers_for_event(&file_info).unwrap().len());

        buffer.block_next_event(file_info.get_absolute_path(&config).unwrap(), "a".into());
        assert_eq!(1, buffer.allowed_peers_for_event(&file_info).unwrap().len());
        assert_eq!(2, buffer.allowed_peers_for_event(&file_info).unwrap().len());
    }
}
