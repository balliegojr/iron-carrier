//! This module is responsible for handling file system operations

mod file_info;
pub use file_info::FileInfo;

use std::{
    collections::HashSet,
    fs,
    path::Path,
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

fn system_time_to_secs(time: SystemTime) -> Option<u64> {
    time.duration_since(SystemTime::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .ok()
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

            let metadata = path.metadata()?;
            let path = path.strip_prefix(root_path)?.to_owned();
            if ignored_files.is_ignored(&path) {
                continue;
            }

            if failed_writes.contains(&path) {
                continue;
            }

            files.replace(FileInfo::new(storage_name.to_owned(), path, metadata));
        }
    }

    let mut files: Vec<FileInfo> = files.into_iter().collect();
    files.sort();

    Ok(files)
}

pub fn get_state_hash<'a, T: Iterator<Item = &'a FileInfo>>(files: T) -> u64 {
    let mut digest = HASHER.digest();

    for file in files.filter(|f| !f.is_deleted()) {
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
            .map(|(path, timestamp)| {
                FileInfo::new_deleted(storage.to_string(), path, Some(timestamp))
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
    src_file: &'b FileInfo,
    dest_file: &'b FileInfo,
    // storage_state: &StorageHashCache,
) -> crate::Result<()> {
    let src_path = src_file.get_absolute_path(config)?;
    let dest_path = dest_file.get_absolute_path(config)?;

    log::debug!("moving file {:?} to {:?}", src_path, dest_path);

    tokio::fs::rename(src_path, dest_path).await?;
    transaction_log
        .append_entry(
            &src_file.storage,
            &src_file.path,
            Some(&dest_file.path),
            LogEntry::new(
                crate::transaction_log::EntryType::Move,
                crate::transaction_log::EntryStatus::Done,
            ),
        )
        .await
}

pub fn fix_times_and_permissions(file_info: &FileInfo, config: &Config) -> crate::Result<()> {
    let file_path = file_info.get_absolute_path(config)?;

    let mod_time = SystemTime::UNIX_EPOCH + Duration::from_secs(file_info.modified_at.unwrap());
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
        .map(|ext| ext.ends_with("ironcarrier") || ext.ends_with(IGNORE_FILE_NAME))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use crate::{leak::Leak, validation::Unverified};

    use super::*;

    #[tokio::test]
    async fn can_read_local_files() -> crate::Result<()> {
        fs::create_dir_all("./tmp/fs/read_local_files")?;
        File::create("./tmp/fs/read_local_files/file_1")?;
        File::create("./tmp/fs/read_local_files/file_2")?;

        let config = r#"
log_path = "./tmp/fs/logfile.log"
[storages]
a = "./tmp/fs/read_local_files"
"#
        .parse::<Unverified<Config>>()?
        .leak();

        let storage = config.storages.get("a").unwrap();

        let mut files: Vec<FileInfo> = walk_path(
            &TransactionLog::memory().await?,
            "a",
            &storage.path,
            &crate::ignored_files::empty_ignored_files(),
        )
        .await?
        .into_iter()
        .collect();
        files.sort();

        assert_eq!(files[0].path.to_str(), Some("file_1"));
        assert_eq!(files[1].path.to_str(), Some("file_2"));

        fs::remove_dir_all("./tmp/fs/read_local_files")?;

        Ok(())
    }

    #[test]
    fn test_is_special_file() {
        assert!(!is_special_file(Path::new("some_file.txt")));
        assert!(is_special_file(Path::new("some_file.ironcarrier")));
        assert!(is_special_file(Path::new(".ironcarrier")));
    }
}
