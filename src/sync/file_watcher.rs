use std::{
    collections::{HashMap, HashSet},
    path::Path,
    path::PathBuf,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use message_io::node::NodeHandler;
use notify::{watcher, DebouncedEvent, RecommendedWatcher, RecursiveMode, Watcher};

use super::{CarrierEvent, WatcherEvent};
use crate::{config::Config, fs::FileInfo};

pub(crate) struct FileWatcher {
    _notify_watcher: RecommendedWatcher,
    event_supression: Arc<Mutex<HashSet<FileInfo>>>,
}

impl FileWatcher {
    pub fn new(handler: NodeHandler<CarrierEvent>, config: Arc<Config>) -> crate::Result<Self> {
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
            event_supression: Arc::new(Mutex::new(HashSet::new())),
        };
        file_watcher.start_event_processing(rx, handler, config);

        Ok(file_watcher)
    }

    pub fn supress_next_event(&self, file_info: FileInfo) {
        self.event_supression.lock().unwrap().insert(file_info);
    }

    fn start_event_processing(
        &self,
        notify_events_receiver: std::sync::mpsc::Receiver<DebouncedEvent>,
        handler: NodeHandler<CarrierEvent>,
        config: Arc<Config>,
    ) {
        let event_supression = self.event_supression.clone();

        thread::spawn(move || loop {
            match notify_events_receiver.recv() {
                Ok(event) => {
                    let mut supression_guard = event_supression.lock().unwrap();
                    if let Some(event) =
                        map_to_carrier_event(event, &config.paths, &mut supression_guard)
                    {
                        handler.signals().send(event)
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

/// Map a [DebouncedEvent] to a [CarrierEvent]`(` alias, file_path)
///
/// Returns [Some]`(`[CarrierEvent]`)` if success  
/// Returns [None] for ignored events
fn map_to_carrier_event(
    event: DebouncedEvent,
    paths: &HashMap<String, PathBuf>,
    event_supression: &mut HashSet<FileInfo>,
) -> Option<CarrierEvent> {
    match event {
        notify::DebouncedEvent::Create(file_path) => {
            if crate::fs::is_special_file(&file_path) || file_path.is_dir() {
                return None;
            }

            let (alias, root) = get_alias_for_path(&file_path, paths)?;
            let metadata = file_path.metadata().ok()?;
            let relative_path = file_path.strip_prefix(&root).ok()?;

            let file = FileInfo::new(alias, relative_path.to_owned(), metadata);
            if event_supression.remove(&file) {
                None
            } else {
                Some(CarrierEvent::FileWatcherEvent(WatcherEvent::Created(file)))
            }
        }

        notify::DebouncedEvent::Write(file_path) => {
            if crate::fs::is_special_file(&file_path) || file_path.is_dir() {
                return None;
            }

            let (alias, root) = get_alias_for_path(&file_path, paths)?;
            let metadata = file_path.metadata().ok()?;
            let relative_path = file_path.strip_prefix(&root).ok()?;

            let file = FileInfo::new(alias, relative_path.to_owned(), metadata);
            if event_supression.remove(&file) {
                None
            } else {
                Some(CarrierEvent::FileWatcherEvent(WatcherEvent::Updated(file)))
            }
        }
        notify::DebouncedEvent::Remove(file_path) => {
            if crate::fs::is_special_file(&file_path) {
                return None;
            }

            let (alias, root) = get_alias_for_path(&file_path, paths)?;
            let relative_path = file_path.strip_prefix(&root).ok()?;

            let file = FileInfo::new_deleted(alias, relative_path.to_owned(), None);
            if event_supression.remove(&file) {
                None
            } else {
                Some(CarrierEvent::FileWatcherEvent(WatcherEvent::Deleted(file)))
            }
        }
        notify::DebouncedEvent::Rename(src_path, dest_path) => {
            if crate::fs::is_special_file(&src_path) || crate::fs::is_special_file(&dest_path) {
                return None;
            }

            let (alias, root) = get_alias_for_path(&src_path, paths)?;
            let relative_path = src_path.strip_prefix(&root).ok()?;
            let src_file = FileInfo::new_deleted(alias.clone(), relative_path.to_owned(), None);

            let metadata = dest_path.metadata().ok();
            let relative_path = dest_path.strip_prefix(&root).ok()?;
            let dest_file = FileInfo::new(alias, relative_path.to_owned(), metadata?);

            if event_supression.remove(&src_file) {
                None
            } else {
                Some(CarrierEvent::FileWatcherEvent(WatcherEvent::Moved(
                    src_file, dest_file,
                )))
            }
        }
        _ => None,
    }
}
