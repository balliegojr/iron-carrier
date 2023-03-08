//! Expectations about file watcher
//!
//! Two big issues plagued the previous version of iron-carrier.
//! 1. File changes were frequently missed by the notify watcher, this meant missing
//!    synchronization, since the events where "pushed" to other online nodes
//! 2. Receiving a change from other node meant a file change event, that would be pushed to other
//!    nodes, this would lead to an endless loop of changes propagated to online peers. To prevent
//!    this, it was necessary to have a complex mechanism to avoid generating a change event when
//!    the node just received the event from other node. The whole thing was needles complex and
//!    very error prone.
//!
//! This new implementation have different expectations
//! 1. There is no partial synchronization/pushing of events. File change events will generate a
//!    full synchronization for the storage that had the event. The only "partial" synchronization
//!    now is actually sync just the storage that is needed.
//! 2. The watcher does not run when a synchronization is happening
//!
//! With the two expectations above in place, the final implementation is a simpler. When a change
//! is detected, just write to the transaction log and start a full synchronization process

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use notify::{Event, RecommendedWatcher, RecursiveMode, Watcher};

use crate::{
    config::{Config, PathConfig},
    ignored_files::{IgnoredFiles, IgnoredFilesCache},
    relative_path::RelativePathBuf,
    transaction_log::{EntryStatus, EntryType, LogEntry, TransactionLog},
    IronCarrierError,
};

/// Creates and return a file watcher that watch for file changes in all the storages that have the
/// watcher enabled.
pub fn get_file_watcher(
    config: &'static Config,
    transaction_log: &'static TransactionLog,
    output: tokio::sync::mpsc::Sender<String>,
) -> crate::Result<Option<RecommendedWatcher>> {
    let mut storages: HashMap<PathBuf, String> = Default::default();
    for (storage, storage_config) in config
        .storages
        .iter()
        .filter(|(_, p)| p.enable_watcher.unwrap_or(config.enable_file_watcher))
    {
        storages.insert(storage_config.path.canonicalize()?, storage.clone());
    }

    if storages.is_empty() {
        return Ok(None);
    }

    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let mut watcher = notify::recommended_watcher(move |res| match res {
        Ok(event) => {
            let _ = tx.blocking_send(event);
        }
        Err(e) => {
            log::error!("Watcher error {e}");
        }
    })?;

    for path in storages.keys() {
        watcher.watch(path, RecursiveMode::Recursive)?;
    }

    tokio::task::spawn(async move {
        let mut ignored_files_cache = IgnoredFilesCache::default();

        while let Some(event) = rx.recv().await {
            log::debug!("File Watcher event: {event:?}");
            match register_event(
                &storages,
                config,
                transaction_log,
                event,
                &mut ignored_files_cache,
            )
            .await
            {
                Ok(Some(storage)) => {
                    if output.send(storage).await.is_err() {
                        break;
                    }
                }
                Ok(_) => {
                    log::error!("No storage found for the event");
                }
                Err(err) => {
                    log::error!("There was an error registering the event {err}");
                }
            }
        }
    });

    Ok(Some(watcher))
}

async fn register_event(
    storages: &HashMap<PathBuf, String>,
    config: &Config,
    transaction_log: &TransactionLog,
    event: Event,
    ignored_files_cache: &mut IgnoredFilesCache,
) -> crate::Result<Option<String>> {
    let storage = event
        .paths
        .get(0)
        .and_then(|p| get_storage_for_path(storages, p))
        .ok_or(IronCarrierError::InvalidOperation)?;

    let storage_config = config.storages.get(&storage).unwrap();
    let ignored_files = ignored_files_cache.get(storage_config).await;

    let src = event.paths.get(0);
    let dst = event.paths.get(1);

    // We don't need to write every kind of event to the transaction log, just the ones that are
    // related to delete or move a file. These are important to keep track so we don't recreate the
    // file when trying to sync with other nodes

    match event.kind {
        // This event represents a move where the origin and destination are both inside the same
        // watche directory.
        // Since this event don't make a distinction between a file or directory, it is necessary
        // to handle both scenarios
        notify::EventKind::Modify(notify::event::ModifyKind::Name(
            notify::event::RenameMode::Both,
        )) => {
            if let (Some(src_path), Some(dst_path)) = (&src, &dst) {
                let timestamp = dst_path
                    .metadata()
                    .and_then(|m| m.modified())
                    .map(super::system_time_to_secs)
                    .ok();

                for file_moved in list_files_moved_pair(
                    storage_config,
                    dst_path.to_path_buf(),
                    src_path.to_path_buf(),
                )? {
                    write_moved_event(
                        transaction_log,
                        ignored_files,
                        &storage,
                        file_moved,
                        timestamp,
                    )
                    .await
                }
            }
        }
        notify::EventKind::Modify(notify::event::ModifyKind::Name(
            notify::event::RenameMode::From,
        ))
        | notify::EventKind::Remove(notify::event::RemoveKind::File) => {
            if let Some(path) = &src {
                let relative_path = RelativePathBuf::new(storage_config, path.to_path_buf())?;
                if !ignored_files.is_ignored(&relative_path) {
                    write_deleted_event(transaction_log, &storage, &relative_path).await;
                }
            }
        }
        _ => {}
    }

    Ok(Some(storage))
}

/// Write the delete event in the transaction log
async fn write_deleted_event(
    transaction_log: &TransactionLog,
    storage: &str,
    path: &RelativePathBuf,
) {
    if let Err(err) = transaction_log
        .append_entry(
            storage,
            path,
            None,
            LogEntry::new(
                EntryType::Delete,
                EntryStatus::Done,
                std::time::UNIX_EPOCH.elapsed().unwrap().as_secs(),
            ),
        )
        .await
    {
        log::error!("Error writing event {err}");
    }
}

/// Write the moved and deleted events to the transaction log.
///
/// Move events have a few different scenarios
/// 1. File moved to inside the watched directory don't need any log, they are considered a
///    "created" file change
/// 2. File moved from the watched directory to outside is considered a delete event and handled by
///    the delete flow
/// 3. File moved inside the watched directory (handled by this function).  
///    If the origin is in the ignore pattern, nothing is done, it is considered a "created" event
///    If the destination is in the ignore pattern, it is considered a delete and just the
///    "deleted" log entry is created
///    If no paths are ignored, a delete event is written to the origin and a moved event is
///    written for the destination
async fn write_moved_event(
    transaction_log: &TransactionLog,
    ignored_files: &IgnoredFiles,
    storage: &str,
    file_moved: MovedFilePair,
    timestamp: Option<u64>,
) {
    if ignored_files.is_ignored(&file_moved.from) {
        return;
    }

    write_deleted_event(transaction_log, storage, &file_moved.from).await;

    if ignored_files.is_ignored(&file_moved.to) {
        return;
    }

    if let Err(err) = transaction_log
        .append_entry(
            storage,
            &file_moved.to,
            Some(&file_moved.from),
            LogEntry::new(
                EntryType::Move,
                EntryStatus::Done,
                timestamp.unwrap_or_else(|| std::time::UNIX_EPOCH.elapsed().unwrap().as_secs()),
            ),
        )
        .await
    {
        log::error!("Error writing event {err}");
    }
}

/// If `dst_file` is a path to file, return a vec with the pair (dst_path and src_path)
/// If `dst_file` is a path to a directory, traverse the directory and return a vec with all the
/// pairs inside the directory.
fn list_files_moved_pair(
    storage: &PathConfig,
    dst_path: PathBuf,
    src_path: PathBuf,
) -> crate::Result<Vec<MovedFilePair>> {
    if dst_path.is_file() {
        return Ok(vec![MovedFilePair {
            from: RelativePathBuf::new(storage, src_path)?,
            to: RelativePathBuf::new(storage, dst_path)?,
        }]);
    }

    let mut paths = vec![dst_path.clone()];
    let mut files = vec![];

    while let Some(path) = paths.pop() {
        for entry in std::fs::read_dir(path)? {
            let path = entry?.path();

            if path.is_dir() {
                paths.push(path);
                continue;
            }

            let from = RelativePathBuf::new(
                storage,
                src_path.join(path.strip_prefix(dst_path.as_path())?),
            )?;
            let to = RelativePathBuf::new(storage, path)?;
            files.push(MovedFilePair { from, to });
        }
    }

    Ok(files)
}

struct MovedFilePair {
    from: RelativePathBuf,
    to: RelativePathBuf,
}

/// Get the name of the storage that `file_path` belongs to, In theory this operation is
/// infallible, but an Option is used, just for safety
fn get_storage_for_path(storages: &HashMap<PathBuf, String>, file_path: &Path) -> Option<String> {
    for (storage_path, storage) in storages.iter() {
        if file_path.starts_with(storage_path) {
            return Some(storage.clone());
        }
    }

    None
}

// Watcher events reference

// creation
// event: Event { kind: Create(File), paths: ["/home/junior/sources/iron-carrier/tmp/a/test"], attr:tracker: None, attr:flag: None, attr:info: None, attr:source: None }
// event: Event { kind: Modify(Metadata(Any)), paths: ["/home/junior/sources/iron-carrier/tmp/a/test"], attr:tracker: None, attr:flag: None, attr:info: None, attr:source: None }
// event: Event { kind: Access(Close(Write)), paths: ["/home/junior/sources/iron-carrier/tmp/a/test"], attr:tracker: None, attr:flag: None, attr:info: None, attr:source: None }
//
// Write
// event: Event { kind: Modify(Data(Any)), paths: ["/home/junior/sources/iron-carrier/tmp/a/test"], attr:tracker: None, attr:flag: None, attr:info: None, attr:source: None }
// event: Event { kind: Access(Close(Write)), paths: ["/home/junior/sources/iron-carrier/tmp/a/test"], attr:tracker: None, attr:flag: None, attr:info: None, attr:source: None }
//
// move
// event: Event { kind: Modify(Name(From)), paths: ["/home/junior/sources/iron-carrier/tmp/a/test"], attr:tracker: Some(4170), attr:flag: None, attr:info: None, attr:source: None }
// event: Event { kind: Modify(Name(To)), paths: ["/home/junior/sources/iron-carrier/tmp/a/test2"], attr:tracker: Some(4170), attr:flag: None, attr:info: None, attr:source: None }
// event: Event { kind: Modify(Name(Both)), paths: ["/home/junior/sources/iron-carrier/tmp/a/test", "/home/junior/sources/iron-carrier/tmp/a/test2"], attr:tracker: Some(4170), attr:flag: None, attr:info: None, attr:source: None }
//
// delete
// event: Event { kind: Remove(File), paths: ["/home/junior/sources/iron-carrier/tmp/a/test2"], attr:tracker: None, attr:flag: None, attr:info: None, attr:source: None }
