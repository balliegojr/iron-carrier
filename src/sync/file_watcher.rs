use std::{collections::HashMap, path::Path, path::PathBuf, sync::Arc, time::Duration};

use notify::{watcher, DebouncedEvent, RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::mpsc::{Receiver, Sender};

use super::{BlockingEvent, FileAction, SyncEvent, file_watcher_event_blocker::FileWatcherEventBlocker};
use crate::{config::Config, deletion_tracker::DeletionTracker, fs::FileInfo};


pub(crate) struct FileWatcher {
    event_sender: Sender<SyncEvent>,
    config: Arc<Config>,
    _notify_watcher: Option<RecommendedWatcher>,
    event_blocker_sender: Sender<BlockingEvent>,
    event_blocker_receiver: Option<Receiver<BlockingEvent>>
    // event_blocker: FileWatcherEventBlocker<'a>,
}

impl FileWatcher {
    pub fn new(
        event_sender: Sender<SyncEvent>,
        config: Arc<Config>,
    ) -> Self {
        let (event_blocker_sender, event_blocker_receiver) = tokio::sync::mpsc::channel(1);
        
        FileWatcher {
            event_sender,
            config,
            _notify_watcher: None,
            event_blocker_sender,
            event_blocker_receiver: Some(event_blocker_receiver),
        }
    }

    pub fn get_events_blocker(&self) -> Sender<BlockingEvent> {
        self.event_blocker_sender.clone()
    }

    pub fn start(&mut self) -> crate::Result<()> {
        if self.config.enable_file_watcher {
            let (tx, rx) = std::sync::mpsc::channel();
    
            let mut notify_watcher = watcher(tx, Duration::from_secs(self.config.delay_watcher_events))?;
            for (_, path) in self.config.paths.iter() {
                let path = path.canonicalize().unwrap();
                if let Err(_) = notify_watcher.watch(path, RecursiveMode::Recursive) {
                    eprintln!("Cannot watch path");
                }
            }
    
            self._notify_watcher = Some(notify_watcher);
            self.start_event_processing(rx);
        } else {
            self.ignore_events();
        }

        Ok(())
    }

    fn start_event_processing(&mut self, notify_events_receiver: std::sync::mpsc::Receiver<DebouncedEvent>) {
        let config = self.config.clone();
        
        let event_blocker = Arc::new(FileWatcherEventBlocker::new(config.clone()));
        self.event_blocker_event_processing(event_blocker.clone());
        self.watcher_event_processing(notify_events_receiver, event_blocker);
        
    }
    
    fn event_blocker_event_processing(&mut self, event_blocker: Arc<FileWatcherEventBlocker>) {
        let mut receiver = self.event_blocker_receiver.take().unwrap();
        tokio::spawn(async move {
            while let Some((path, peer_address)) = receiver.recv().await {
                event_blocker.block_next_event(path, peer_address);
            }
        });
    }
    fn watcher_event_processing(&self, 
        notify_events_receiver: std::sync::mpsc::Receiver<DebouncedEvent>,
        event_blocker: Arc<FileWatcherEventBlocker>

    ) {
        let sync_event_sender = self.event_sender.clone();
        let config = self.config.clone();


        tokio::task::spawn_blocking(move || loop {
           
            match notify_events_receiver.recv() {
                Ok(event) => {
                    let config = config.clone();
                    let sync_event_sender = sync_event_sender.clone();
                    let event_blocker = event_blocker.clone();

                    tokio::spawn(async move {
                        if let Some(event) =
                            map_to_sync_event(event, &config.paths, &event_blocker).await
                        {
                            sync_event_sender.send(event).await.ok();
                        }
                    });
                }
                Err(_) => break,
            }
        });
    }

    fn ignore_events(&mut self) {
        let mut receiver = self.event_blocker_receiver.take().unwrap();
        tokio::spawn(async move {
            while let Some(_ev) = receiver.recv().await {
                break;
            }
        });
    }
}

fn get_alias_for_path(
    file_path: &Path,
    paths: &HashMap<String, PathBuf>,
) -> Option<(String, PathBuf)> {
    let file_path = if file_path.is_relative() {
        file_path.canonicalize().ok()?
    } else {
        file_path.to_owned()
    };

    for (alias, config_path) in paths.iter() {
        let config_path = match config_path.canonicalize() {
            Ok(config_path) => config_path,
            Err(_) => return None,
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
async fn map_to_sync_event<'b>(
    event: DebouncedEvent,
    paths: &HashMap<String, PathBuf>,
    events_buffer: &'b FileWatcherEventBlocker,
) -> Option<SyncEvent> {
    match event {
        notify::DebouncedEvent::Create(file_path) => {
            if crate::fs::is_special_file(&file_path) || file_path.is_dir() {
                return None;
            }

            let (alias, root) = get_alias_for_path(&file_path, paths)?;
            let metadata = file_path.metadata().ok()?;
            let relative_path = file_path.strip_prefix(&root).ok()?;

            let file = FileInfo::new(alias, relative_path.to_owned(), metadata);
            events_buffer
                .allowed_peers_for_event(&file)
                .and_then(|peers| {
                    Some(SyncEvent::BroadcastToAllPeers(
                        FileAction::Create(file),
                        peers.to_vec(),
                    ))
                })
        }

        notify::DebouncedEvent::Write(file_path) => {
            if crate::fs::is_special_file(&file_path) || file_path.is_dir() {
                return None;
            }

            let (alias, root) = get_alias_for_path(&file_path, paths)?;
            let metadata = file_path.metadata().ok()?;
            let relative_path = file_path.strip_prefix(&root).ok()?;

            let file = FileInfo::new(alias, relative_path.to_owned(), metadata);
            DeletionTracker::new(&root).remove_entry(&file.path).await;

            events_buffer
                .allowed_peers_for_event(&file)
                .and_then(|peers| {
                    Some(SyncEvent::BroadcastToAllPeers(
                        FileAction::Update(file),
                        peers.to_vec(),
                    ))
                })
        }
        notify::DebouncedEvent::Remove(file_path) => {
            if crate::fs::is_special_file(&file_path) {
                return None;
            }

            let (alias, root) = get_alias_for_path(&file_path, paths)?;
            let relative_path = file_path.strip_prefix(&root).ok()?;

            let file = FileInfo::new_deleted(alias, relative_path.to_owned(), None);
            DeletionTracker::new(&root).add_entry(&file.path).await;

            events_buffer
                .allowed_peers_for_event(&file)
                .and_then(|peers| {
                    Some(SyncEvent::BroadcastToAllPeers(
                        FileAction::Remove(file),
                        peers.to_vec(),
                    ))
                })
        }
        notify::DebouncedEvent::Rename(src_path, dest_path) => {
            if crate::fs::is_special_file(&src_path) || crate::fs::is_special_file(&dest_path) {
                return None;
            }

            let (alias, root) = get_alias_for_path(&src_path, paths)?;
            let relative_path = src_path.strip_prefix(&root).ok()?;
            let src_file = FileInfo::new_deleted(alias.clone(), relative_path.to_owned(), None);
            DeletionTracker::new(&root).add_entry(&src_file.path).await;

            let metadata = dest_path.metadata().ok();
            let relative_path = dest_path.strip_prefix(&root).ok()?;
            let dest_file = FileInfo::new(alias, relative_path.to_owned(), metadata?);

            events_buffer
                .allowed_peers_for_event(&src_file)
                .and_then(|peers| {
                    Some(SyncEvent::BroadcastToAllPeers(
                        FileAction::Move(src_file, dest_file),
                        peers.to_vec(),
                    ))
                })
        }
        _ => None,
    }
}
