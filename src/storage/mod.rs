//! This module is responsible for handling file system operations

mod file_info;
pub use file_info::{FileInfo, FileInfoType};

use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use crate::{
    config::{Config, PathConfig},
    constants::IGNORE_FILE_NAME,
    hash_helper::{self, HASHER},
    ignored_files::IgnoredFiles,
    transaction_log::{LogEntry, TransactionLog},
    IronCarrierError,
};

#[derive(Debug)]
pub struct Storage {
    pub hash: u64,
    pub files: Vec<FileInfo>,
    pub ignored_files: IgnoredFiles,
}

pub async fn get_storage(
    name: &str,
    storage_path_config: &PathConfig,
    transaction_log: &TransactionLog,
) -> crate::Result<Storage> {
    let ignored_files =
        crate::ignored_files::load_ignored_file_pattern(&storage_path_config.path).await;
    let files = walk_path(
        transaction_log,
        name,
        &storage_path_config.path,
        &ignored_files,
    )
    .await?;
    let hash = get_state_hash(files.iter());

    Ok(Storage {
        // name,
        ignored_files,
        files,
        hash,
    })
}

fn system_time_to_secs(time: SystemTime) -> u64 {
    time.duration_since(SystemTime::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .expect("failed to get wall time")
}

/// Returns a sorted vector with the entire folder structure for the given path
///
/// This function will look for deletes files in the [DeletionTracker] log and append all entries to the return list  
/// files with name or extension `.ironcarrier` will be ignored
pub async fn walk_path(
    transaction_log: &TransactionLog,
    storage_name: &str,
    root_path: &Path,
    ignored_files: &IgnoredFiles,
) -> crate::Result<Vec<FileInfo>> {
    // TODO: change to async functions?
    let mut paths = vec![root_path.to_owned()];
    let mut files = get_deleted_files(transaction_log, storage_name).await?;
    let mut moved_files = get_moved_files(transaction_log, storage_name).await?;

    let failed_writes = transaction_log.get_failed_writes(storage_name).await?;

    while let Some(path) = paths.pop() {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();

            if is_special_file(&path) {
                continue;
            }

            if path.is_dir() {
                paths.push(path);
                continue;
            }

            let relative_path = path.strip_prefix(root_path)?.to_owned();
            if ignored_files.is_ignored(&relative_path) {
                continue;
            }

            if failed_writes.contains(&relative_path) {
                continue;
            }

            let metadata = path.metadata()?;
            let permissions = get_permissions(&metadata);
            let modified_at = metadata.modified().map(system_time_to_secs)?;
            let size = metadata.len();

            let file_info = FileInfo::existent(
                storage_name.to_owned(),
                relative_path,
                modified_at,
                size,
                permissions,
            );

            // When a file is moved, two entries are added to the log.
            // A deleted entry for the old file path
            // A moved entry for the new file path
            //
            // Because of this, we need to remove the old file entry from the list, otherwise the
            // file can be deleted before being moved
            match moved_files.remove(&file_info.path) {
                Some(moved_file) => {
                    let old_path = match &moved_file.info_type {
                        FileInfoType::Moved { old_path, .. } => old_path,
                        _ => unreachable!(),
                    };

                    files.drain_filter(|f| f.path.eq(old_path.as_path()));
                    files.replace(moved_file);
                }
                _ => {
                    files.replace(file_info);
                }
            }
        }
    }

    let mut files: Vec<FileInfo> = files.into_iter().collect();
    files.sort();

    Ok(files)
}

pub fn get_state_hash<'a, T: Iterator<Item = &'a FileInfo>>(files: T) -> u64 {
    let mut digest = HASHER.digest();

    for file in files.filter(|f| f.is_existent()) {
        hash_helper::calculate_file_hash_digest(file, &mut digest);
    }

    digest.finalize()
}

async fn get_deleted_files(
    transaction_log: &TransactionLog,
    storage: &str,
) -> crate::Result<HashSet<FileInfo>> {
    transaction_log.get_deleted(storage).await.map(|files| {
        files
            .into_iter()
            .map(|(path, timestamp)| FileInfo::deleted(storage.to_string(), path, timestamp))
            .collect()
    })
}

async fn get_moved_files(
    transaction_log: &TransactionLog,
    storage: &str,
) -> crate::Result<HashMap<PathBuf, FileInfo>> {
    transaction_log.get_moved(storage).await.map(|files| {
        files
            .into_iter()
            .map(|(path, old_path, timestamp)| {
                (
                    path.clone(),
                    FileInfo::moved(storage.to_string(), path, old_path, timestamp),
                )
            })
            .collect()
    })
}
pub async fn delete_file(
    config: &Config,
    transaction_log: &TransactionLog,
    file_info: &FileInfo,
) -> crate::Result<()> {
    let path = file_info.get_absolute_path(config)?;
    if !path.exists() {
        log::debug!("delete_file: given path doesn't exist ({:?})", path);
        return Err(Box::new(std::io::Error::from(std::io::ErrorKind::NotFound)));
    } else if path.is_dir() {
        log::debug!("delete_file: {:?} is dir, removing whole dir", path);
        tokio::fs::remove_dir_all(&path).await?;
    } else {
        log::debug!("delete_file: removing file {:?}", path);
        tokio::fs::remove_file(&path).await?;
    }

    log::debug!("{:?} removed", path);
    transaction_log
        .append_entry(
            &file_info.storage,
            &file_info.path,
            None,
            LogEntry::new(
                crate::transaction_log::EntryType::Delete,
                crate::transaction_log::EntryStatus::Done,
            ),
        )
        .await
}

pub async fn move_file<'b>(
    config: &Config,
    transaction_log: &TransactionLog,
    file: &'b FileInfo,
) -> crate::Result<()> {
    let dest_path = file.get_absolute_path(config)?;
    let src_path = if let FileInfoType::Moved { old_path, .. } = &file.info_type {
        config
            .storages
            .get(&file.storage)
            .map(|p| p.path.canonicalize())
            .ok_or_else(|| IronCarrierError::StorageNotAvailable(file.storage.clone()))??
            .join(old_path)
    } else {
        return Err(Box::new(IronCarrierError::InvalidOperation));
    };

    if dest_path.exists() || !src_path.exists() {
        log::error!("can't move {src_path:?} to {dest_path:?}");
        return Err(Box::new(IronCarrierError::InvalidOperation));
    }

    log::debug!("moving file {src_path:?} to {dest_path:?}");
    tokio::fs::rename(&src_path, &dest_path).await?;

    // It is necessary to add two entries in the log, one for the new path as moved, one for the
    // old path as deleted
    //
    // If there is a future change to the file, before any synchronization, the new entry will
    // become a write one, hence the old file will need to be deleted. It is possible to improve
    // this flow by chaining a move with a send action
    transaction_log
        .append_entry(
            &file.storage,
            &src_path,
            None,
            LogEntry::new(
                crate::transaction_log::EntryType::Delete,
                crate::transaction_log::EntryStatus::Done,
            ),
        )
        .await?;

    transaction_log
        .append_entry(
            &file.storage,
            &dest_path,
            Some(&src_path),
            LogEntry::new(
                crate::transaction_log::EntryType::Move,
                crate::transaction_log::EntryStatus::Done,
            ),
        )
        .await
}

pub fn fix_times_and_permissions(file_info: &FileInfo, config: &Config) -> crate::Result<()> {
    let file_path = file_info.get_absolute_path(config)?;

    if let FileInfoType::Existent { modified_at, .. } = file_info.info_type {
        let mod_time = SystemTime::UNIX_EPOCH + Duration::from_secs(modified_at);
        log::trace!("setting {file_path:?} modification time to {mod_time:?}");
        filetime::set_file_mtime(&file_path, filetime::FileTime::from_system_time(mod_time))?;
    }

    if file_info.permissions > 0 {
        set_file_permissions(&file_path, file_info.permissions)?;
    }

    Ok(())
}

#[cfg(unix)]
fn get_permissions(metadata: &std::fs::Metadata) -> u32 {
    metadata.permissions().mode()
}

#[cfg(not(unix))]
fn get_permissions(metadata: &std::fs::Metadata) -> u32 {
    //TODO: figure out how to handle windows permissions
    0
}

#[cfg(unix)]
fn set_file_permissions(path: &Path, perm: u32) -> std::io::Result<()> {
    let perm = std::fs::Permissions::from_mode(perm);
    std::fs::set_permissions(path, perm)
}

#[cfg(not(unix))]
fn set_file_permissions(path: &Path, perm: u32) -> std::io::Result<()> {
    //TODO: figure out how to handle windows permissions
    Ok(())
}

/// Returns true if `path` name or extension are .ironcarrier
pub fn is_special_file(path: &Path) -> bool {
    path.file_name()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.ends_with("ironcarrier") || ext.ends_with(IGNORE_FILE_NAME))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use crate::{leak::Leak, validation::Unverified};

    use super::*;

    #[tokio::test]
    async fn can_read_local_files() -> crate::Result<()> {
        let config = r#"
[storages]
a = "./src/"
"#
        .parse::<Unverified<Config>>()?
        .leak();

        let storage = config.storages.get("a").unwrap();

        let files = walk_path(
            &TransactionLog::memory().await?,
            "a",
            &storage.path.canonicalize().unwrap(),
            &crate::ignored_files::empty_ignored_files(),
        )
        .await?;

        assert!(!files.is_empty());
        assert!(files.is_sorted());

        Ok(())
    }

    #[test]
    fn test_is_special_file() {
        assert!(!is_special_file(Path::new("some_file.txt")));
        assert!(is_special_file(Path::new("some_file.ironcarrier")));
        assert!(is_special_file(Path::new(".ironcarrier")));
    }
}
