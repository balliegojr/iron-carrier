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
        file_watcher.start_event_processing(rx, dispatcher, config, log_writer);

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
    ) {
        let event_supression = self.event_supression.clone();

        thread::spawn(move || loop {
            match notify_events_receiver.recv() {
                Ok(event) => {
                    let mut supression_guard = event_supression.lock().unwrap();
                    if let Some(event) = map_to_sync_event(
                        event,
                        &config.paths,
                        &mut supression_guard,
                        &mut log_writer,
                    ) {
                        dispatcher.broadcast(event);
                    }
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
) -> Option<FileHandlerEvent> {
    match event {
        notify::DebouncedEvent::Create(file_path) => {
            match get_file_info(paths, event_supression, file_path, SupressionType::Write) {
                Some(file) => {
                    log_writer.append(
                        file.storage.clone(),
                        EventType::Write(file.path.clone()),
                        EventStatus::Finished,
                    );

                    Some(FileHandlerEvent::SendFile(file, String::new(), true))
                }
                None => None,
            }
        }

        notify::DebouncedEvent::Write(file_path) => {
            match get_file_info(paths, event_supression, file_path, SupressionType::Write) {
                Some(file) => {
                    log_writer.append(
                        file.storage.clone(),
                        EventType::Write(file.path.clone()),
                        EventStatus::Finished,
                    );

                    Some(FileHandlerEvent::SendFile(file, String::new(), false))
                }
                None => None,
            }
        }
        notify::DebouncedEvent::Remove(file_path) => {
            log::trace!("Received remove event for {:?}", file_path);
            match get_file_info(paths, event_supression, file_path, SupressionType::Delete) {
                Some(file) => {
                    log_writer.append(
                        file.storage.clone(),
                        EventType::Delete(file.path.clone()),
                        EventStatus::Finished,
                    );

                    Some(FileHandlerEvent::DeleteFile(file))
                }
                None => None,
            }
        }
        notify::DebouncedEvent::Rename(src_path, dest_path) => {
            let src_file = get_file_info(paths, event_supression, src_path, SupressionType::Delete);
            let dest_file =
                get_file_info(paths, event_supression, dest_path, SupressionType::Rename);

            match (src_file, dest_file) {
                (Some(src_file), Some(dest_file)) => {
                    log_writer.append(
                        src_file.storage.clone(),
                        EventType::Move(src_file.path.clone(), dest_file.path.clone()),
                        EventStatus::Finished,
                    );
                    Some(FileHandlerEvent::MoveFile(src_file, dest_file))
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
) -> Option<FileInfo> {
    if crate::fs::is_special_file(&file_path) || file_path.is_dir() {
        log::trace!("Event for {:?} ignored", file_path);
        return None;
    }

    let (alias, root) = get_alias_for_path(&file_path, paths)?;
    let relative_path = file_path.strip_prefix(&root).ok()?;

    let file = match supression_type {
        SupressionType::Delete => FileInfo::new_deleted(alias, relative_path.to_owned(), None),
        _ => {
            let metadata = file_path.metadata().ok()?;
            FileInfo::new(alias, relative_path.to_owned(), metadata)
        }
    };

    match event_supression.get(&file) {
        Some(supression) if *supression == supression_type => {
            log::trace!("supressed {:?} event for {:?}", supression_type, file_path);
            event_supression.remove(&file);
            None
        }
        _ => Some(file),
    }
}
