use std::{collections::HashMap, path::Path, path::PathBuf, sync::Arc, time::Duration};

use notify::{watcher, DebouncedEvent, Error, RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::mpsc::Sender;

use super::{file_events_buffer::FileEventsBuffer, FileAction, SyncEvent};
use crate::{config::Config, deletion_tracker::DeletionTracker, fs::FileInfo};

pub(crate) struct FileWatcher {
    event_sender: Sender<SyncEvent>,
    config: Arc<Config>,
    notify_watcher: RecommendedWatcher,
    events_buffer: Arc<FileEventsBuffer>,
}

impl FileWatcher {
    pub fn new(
        event_sender: Sender<SyncEvent>,
        config: Arc<Config>,
        events_buffer: Arc<FileEventsBuffer>,
    ) -> Result<Self, Box<Error>> {
        let (tx, rx) = std::sync::mpsc::channel();

        let mut notify_watcher = watcher(tx, Duration::from_secs(config.delay_watcher_events))?;
        for (_, path) in config.paths.iter() {
            let path = path.canonicalize().unwrap();
            if let Err(_) = notify_watcher.watch(path, RecursiveMode::Recursive) {
                eprintln!("Cannot watch path");
            }
        }

        let file_watcher = FileWatcher {
            event_sender,
            config,
            notify_watcher,
            events_buffer,
        };

        file_watcher.process_events(rx);

        Ok(file_watcher)
    }

    fn process_events(&self, notify_events_receiver: std::sync::mpsc::Receiver<DebouncedEvent>) {
        let config = self.config.clone();
        let events_buffer = self.events_buffer.clone();
        let sync_event_sender = self.event_sender.clone();

        tokio::task::spawn_blocking(move || loop {
            match notify_events_receiver.recv() {
                Ok(event) => {
                    let config = config.clone();
                    let sync_event_sender = sync_event_sender.clone();
                    let events_buffer = events_buffer.clone();

                    tokio::spawn(async move {
                        if let Some(event) =
                            map_to_sync_event(event, &config.paths, &events_buffer).await
                        {
                            sync_event_sender.send(event).await.ok();
                        }
                    });
                }
                Err(_) => break,
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
    events_buffer: &'b FileEventsBuffer,
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
