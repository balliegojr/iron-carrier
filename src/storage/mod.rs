//! This module is responsible for handling file system operations

mod file_info;
pub mod file_operations;
pub mod file_watcher;
pub use file_info::{FileInfo, FileInfoType};

use std::{
    collections::{HashMap, HashSet},
    fs,
    path::Path,
    time::{Duration, SystemTime},
};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use crate::{
    config::{Config, PathConfig},
    hash_helper::{self, HASHER},
    ignored_files::IgnoredFiles,
    relative_path::RelativePathBuf,
    transaction_log::TransactionLog,
};

#[derive(Debug)]
pub struct Storage {
    pub hash: u64,
    pub files: Vec<FileInfo>,
}

/// Gets the file list and hash for the given storage
pub async fn get_storage_info(
    name: &str,
    storage_path_config: &PathConfig,
    transaction_log: &TransactionLog,
) -> crate::Result<Storage> {
    let ignored_files = crate::ignored_files::IgnoredFiles::new(storage_path_config).await;
    let files = walk_path(transaction_log, name, storage_path_config, &ignored_files).await?;
    let hash = calculate_storage_hash(files.iter());

    Ok(Storage {
        // name,
        files,
        hash,
    })
}

fn system_time_to_secs(time: SystemTime) -> u64 {
    time.duration_since(SystemTime::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .expect("failed to get wall time")
}

/// Returns a sorted vector with the entire folder structure (recursive read) for the given path.  
///
/// The list will contain deleted and moved files, by reading the transaction log.  
pub async fn walk_path(
    transaction_log: &TransactionLog,
    storage_name: &str,
    storage_config: &PathConfig,
    ignored_files: &IgnoredFiles,
) -> crate::Result<Vec<FileInfo>> {
    let mut paths = vec![storage_config.path.to_owned()];
    let mut files = get_deleted_files(transaction_log, storage_name).await?;
    let mut moved_files = get_moved_files(transaction_log, storage_name).await?;

    let failed_writes = transaction_log.get_failed_writes(storage_name).await?;

    while let Some(path) = paths.pop() {
        for entry in fs::read_dir(path)? {
            let path = entry?.path();

            if is_special_file(&path) {
                continue;
            }

            if path.is_dir() {
                paths.push(path);
                continue;
            }

            let metadata = path.metadata()?;
            let relative_path = storage_config.get_relative_path(path)?;
            if ignored_files.is_ignored(&relative_path) {
                continue;
            }

            if failed_writes.contains(&relative_path) {
                continue;
            }

            let permissions = get_permissions(&metadata);
            let created_at = metadata.created().map(system_time_to_secs)?;
            let modified_at = metadata.modified().map(system_time_to_secs)?;
            let size = metadata.len();

            let file_info = FileInfo::existent(
                storage_name.to_owned(),
                relative_path,
                modified_at,
                created_at,
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

                    files.drain_filter(|f| f.path.eq(old_path));
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

/// Calculate a "shallow" hash for the files by hashing the attributes, it doesn't open the file to
/// read the contents
pub fn calculate_storage_hash<'a, T: Iterator<Item = &'a FileInfo>>(files: T) -> u64 {
    let mut digest = HASHER.digest();

    for file in files.filter(|f| f.is_existent()) {
        hash_helper::calculate_file_hash_digest(file, &mut digest);
    }

    digest.finalize()
}

/// Fetch deleted file events from the transaction log
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

/// Fetch moved file events from the transaction log
async fn get_moved_files(
    transaction_log: &TransactionLog,
    storage: &str,
) -> crate::Result<HashMap<RelativePathBuf, FileInfo>> {
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

pub fn fix_times_and_permissions(file_info: &FileInfo, config: &Config) -> crate::Result<()> {
    let file_path = file_info.get_absolute_path(config)?;

    let mod_time = SystemTime::UNIX_EPOCH + Duration::from_secs(file_info.get_date());
    log::trace!("setting {file_path:?} modification time to {mod_time:?}");
    filetime::set_file_mtime(&file_path, filetime::FileTime::from_system_time(mod_time))?;

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
        .map(|ext| ext.ends_with("ironcarrier"))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use crate::{leak::Leak, validation::Unvalidated};

    use super::*;

    #[tokio::test]
    async fn can_read_local_files() -> crate::Result<()> {
        let config = r#"
[storages]
a = "./src/"
"#
        .parse::<Unvalidated<Config>>()?
        .leak();

        let storage = config.storages.get("a").unwrap();

        let files = walk_path(
            &TransactionLog::memory().await?,
            "a",
            storage,
            &crate::ignored_files::IgnoredFiles::empty(),
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
