use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use notify::{Event, RecommendedWatcher, RecursiveMode, Watcher};

use crate::{
    config::Config,
    transaction_log::{EntryStatus, EntryType, LogEntry, TransactionLog},
};

pub fn get_file_watcher(
    config: &Config,
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
        while let Some(event) = rx.recv().await {
            match register_event(&storages, transaction_log, event).await {
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
    transaction_log: &TransactionLog,
    event: Event,
) -> crate::Result<Option<String>> {
    let src = event
        .paths
        .get(0)
        .and_then(|p| get_storage_and_relative_path(storages, p));

    let dst = event
        .paths
        .get(0)
        .and_then(|p| get_storage_and_relative_path(storages, p));

    match event.kind {
        notify::EventKind::Modify(notify::event::ModifyKind::Name(
            notify::event::RenameMode::Both,
        )) => {
            // TODO: handle dirs
            if let Some((storage, src_path)) = &src {
                if let Some((_, dst_path)) = &dst {
                    transaction_log
                        .append_entry(
                            storage,
                            dst_path,
                            Some(src_path),
                            LogEntry::new(EntryType::Move, EntryStatus::Done),
                        )
                        .await?;
                }
            }
        }
        notify::EventKind::Modify(notify::event::ModifyKind::Name(
            notify::event::RenameMode::From,
        ))
        | notify::EventKind::Remove(notify::event::RemoveKind::File) => {
            // TODO: handle dirs
            if let Some((storage, path)) = &src {
                transaction_log
                    .append_entry(
                        storage,
                        path,
                        None,
                        LogEntry::new(EntryType::Delete, EntryStatus::Done),
                    )
                    .await?;
            }
        }
        _ => {}
    }

    Ok(src.map(|(storage, _)| storage))
}

fn get_dir_files(storage_path: &Path, path: &Path) -> Vec<PathBuf> {
    todo!()
}

fn get_storage_and_relative_path(
    storages: &HashMap<PathBuf, String>,
    file_path: &Path,
) -> Option<(String, PathBuf)> {
    for (storage_path, storage) in storages.iter() {
        if file_path.starts_with(storage_path) {
            dbg!(&storage_path, storage);
            return file_path
                .strip_prefix(storage_path)
                .map(|relative_path| (storage.clone(), relative_path.to_path_buf()))
                .ok();
        }
    }

    None
}

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
