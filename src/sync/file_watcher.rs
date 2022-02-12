use std::{
    collections::HashMap,
    fmt::Display,
    fs::File,
    path::Path,
    path::PathBuf,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use notify::{watcher, DebouncedEvent, RecommendedWatcher, RecursiveMode, Watcher};
use serde::{Deserialize, Serialize};

use super::FileHandlerEvent;
use crate::{
    config::Config,
    conn::CommandDispatcher,
    fs::FileInfo,
    storage_state::StorageState,
    transaction_log::{EventStatus, EventType, TransactionLogWriter},
};

#[derive(Debug, Deserialize, Serialize)]
pub enum WatcherEvent {
    Supress(FileInfo, SupressionType),
}

impl Display for WatcherEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WatcherEvent::Supress(file_info, supress) => {
                write!(f, "supress {:?} for {:?}", supress, &file_info.path)
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SupressionType {
    Write,
    Delete,
    Rename,
}

pub struct FileWatcher {
    _notify_watcher: RecommendedWatcher,
    event_supression: Arc<Mutex<HashMap<FileInfo, SupressionType>>>,
}

impl FileWatcher {
    pub fn new(
        dispatcher: CommandDispatcher,
        config: Arc<Config>,
        log_writer: TransactionLogWriter<File>,
        storage_state: Arc<StorageState>,
    ) -> crate::Result<Self> {
        let (tx, rx) = std::sync::mpsc::channel();
        let mut _notify_watcher = watcher(tx, Duration::from_secs(config.delay_watcher_events))?;
        for (_, path) in config.paths.iter() {
            let path = path.canonicalize().unwrap();
            if _notify_watcher
                .watch(path, RecursiveMode::Recursive)
                .is_err()
            {
                eprintln!("Cannot watch path");
            }
        }

        let file_watcher = FileWatcher {
            _notify_watcher,
            event_supression: Arc::new(Mutex::new(HashMap::new())),
        };
        file_watcher.start_event_processing(rx, dispatcher, config, log_writer, storage_state);

        Ok(file_watcher)
    }

    pub fn handle_event(&self, event: WatcherEvent) -> crate::Result<bool> {
        match event {
            WatcherEvent::Supress(file_info, event_type) => {
                self.supress_next_event(file_info, event_type)
            }
        }

        Ok(false)
    }

    fn supress_next_event(&self, file_info: FileInfo, event_type: SupressionType) {
        log::trace!(
            "Adding supression for {:?} {:?}",
            file_info.path,
            event_type
        );
        self.event_supression
            .lock()
            .unwrap()
            .insert(file_info, event_type);
    }

    fn start_event_processing(
        &self,
        notify_events_receiver: std::sync::mpsc::Receiver<DebouncedEvent>,
        dispatcher: CommandDispatcher,
        config: Arc<Config>,
        mut log_writer: TransactionLogWriter<File>,
        storage_state: Arc<StorageState>,
    ) {
        let event_supression = self.event_supression.clone();
        let debounce_delay = match config.delay_watcher_events {
            0 => Duration::from_secs(10),
            n => Duration::from_secs(n),
        };

        let debouncer = {
            let dispatcher = dispatcher.clone();
            crate::debouncer::debounce_action(debounce_delay, move || {
                dispatcher.now(crate::sync::SyncEvent::StartSync);
            })
        };

        thread::spawn(move || {
            while let Ok(event) = notify_events_receiver.recv() {
                if !dispatcher.has_connections() {
                    continue;
                }

                let mut supression_guard = event_supression.lock().unwrap();
                if let Some((storage, event)) = map_to_sync_event(
                    event,
                    &config.paths,
                    &mut supression_guard,
                    &mut log_writer,
                    &storage_state,
                ) {
                    debouncer.invoke();

                    match event {
                        event @ FileHandlerEvent::BroadcastFile(_, _) => {
                            dispatcher.now(event);
                        }
                        event => {
                            dispatcher.broadcast(event);
                        }
                    }
                }
            }
        });
    }
}

fn get_storage_for_path(
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
            return Some((alias.clone(), config_path));
        }
    }

    None
}

/// Map a [DebouncedEvent] to a [SyncEvent]`(` alias, file_path)
///
/// Returns [Some]`(`[CarrierEvent]`)` if success  
/// Returns [None] for ignored events
fn map_to_sync_event(
    event: DebouncedEvent,
    paths: &HashMap<String, PathBuf>,
    event_supression: &mut HashMap<FileInfo, SupressionType>,
    log_writer: &mut TransactionLogWriter<File>,
    storage_state: &StorageState,
) -> Option<(String, FileHandlerEvent)> {
    match event {
        notify::DebouncedEvent::Create(file_path) => {
            match get_file_info(
                paths,
                event_supression,
                file_path,
                SupressionType::Write,
                storage_state,
            ) {
                Some(file) => {
                    log_writer.append(
                        file.storage.clone(),
                        EventType::Write(file.path.clone()),
                        EventStatus::Finished,
                    );

                    Some((
                        file.storage.clone(),
                        FileHandlerEvent::BroadcastFile(file, true),
                    ))
                }
                None => None,
            }
        }

        notify::DebouncedEvent::Write(file_path) => {
            match get_file_info(
                paths,
                event_supression,
                file_path,
                SupressionType::Write,
                storage_state,
            ) {
                Some(file) => {
                    log_writer.append(
                        file.storage.clone(),
                        EventType::Write(file.path.clone()),
                        EventStatus::Finished,
                    );

                    Some((
                        file.storage.clone(),
                        FileHandlerEvent::BroadcastFile(file, false),
                    ))
                }
                None => None,
            }
        }
        notify::DebouncedEvent::Remove(file_path) => {
            log::trace!("Received remove event for {:?}", file_path);
            match get_file_info(
                paths,
                event_supression,
                file_path,
                SupressionType::Delete,
                storage_state,
            ) {
                Some(file) => {
                    log_writer.append(
                        file.storage.clone(),
                        EventType::Delete(file.path.clone()),
                        EventStatus::Finished,
                    );

                    Some((file.storage.clone(), FileHandlerEvent::DeleteFile(file)))
                }
                None => None,
            }
        }
        notify::DebouncedEvent::Rename(src_path, dest_path) => {
            let src_file = get_file_info(
                paths,
                event_supression,
                src_path,
                SupressionType::Delete,
                storage_state,
            );
            let dest_file = get_file_info(
                paths,
                event_supression,
                dest_path,
                SupressionType::Rename,
                storage_state,
            );

            match (src_file, dest_file) {
                (Some(src_file), Some(dest_file)) => {
                    log_writer.append(
                        src_file.storage.clone(),
                        EventType::Move(src_file.path.clone(), dest_file.path.clone()),
                        EventStatus::Finished,
                    );
                    Some((
                        src_file.storage.clone(),
                        FileHandlerEvent::MoveFile(src_file, dest_file),
                    ))
                }
                _ => None,
            }
        }
        _ => None,
    }
}

fn get_file_info(
    paths: &HashMap<String, PathBuf>,
    event_supression: &mut HashMap<FileInfo, SupressionType>,
    file_path: PathBuf,
    supression_type: SupressionType,
    storage_state: &StorageState,
) -> Option<FileInfo> {
    if crate::fs::is_special_file(&file_path) || file_path.is_dir() {
        log::trace!("Event for {:?} ignored", file_path);
        return None;
    }

    let (storage, root) = get_storage_for_path(&file_path, paths)?;
    let relative_path = file_path.strip_prefix(&root).ok()?;

    if storage_state.is_ignored(&storage, &relative_path) {
        return None;
    }

    let file = match supression_type {
        SupressionType::Delete => FileInfo::new_deleted(storage, relative_path.to_owned(), None),
        _ => {
            let metadata = file_path.metadata().ok()?;
            FileInfo::new(storage, relative_path.to_owned(), metadata)
        }
    };

    match event_supression.entry(file.clone()) {
        std::collections::hash_map::Entry::Occupied(entry) => {
            let k = entry.key();
            if k.modified_at == file.modified_at
                && k.size == file.size
                && k.deleted_at.is_some() == file.deleted_at.is_some()
            {
                log::trace!("supressed {:?} event for {:?}", supression_type, file_path);
                entry.remove_entry();
                None
            } else {
                Some(file)
            }
        }
        std::collections::hash_map::Entry::Vacant(_) => Some(file),
    }
}
