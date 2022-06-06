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
    config::Config,
    hash_helper::{self, HASHER},
    storage_state::StorageState,
    transaction_log::{get_log_reader, EventType},
};

fn system_time_to_secs(time: SystemTime) -> Option<u64> {
    time.duration_since(SystemTime::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .ok()
}

/// Returns a sorted vector with the entire folder structure for the given path
///
/// This function will look for deletes files in the [DeletionTracker] log and append all entries to the return list  
/// files with name or extension `.ironcarrier` will be ignored
pub fn walk_path(
    config: &Config,
    storage: &str,
    storage_state: &StorageState,
) -> crate::Result<Vec<FileInfo>> {
    let root_path = config.paths.get(storage).expect("Unexpected storage");
    let mut paths = vec![root_path.to_owned()];

    let mut files = get_deleted_files(config, storage)?;

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
            if storage_state.is_ignored(storage, &path) {
                continue;
            }

            files.insert(FileInfo::new(storage.to_owned(), path, metadata));
        }
    }

    let failed_writes = get_failed_writes(config, storage)?;
    files.retain(|file| !failed_writes.contains(file));
    let mut files: Vec<FileInfo> = files.into_iter().collect();
    files.sort();

    Ok(files)
}

pub fn get_state_hash<'a, T: Iterator<Item = &'a FileInfo>>(files: T) -> u64 {
    let mut digest = HASHER.digest();

    for file in files.filter(|f| !f.is_deleted()) {
        hash_helper::calculate_file_hash_dig(file, &mut digest);
    }

    digest.finalize()
}

fn get_deleted_files(config: &Config, storage: &str) -> crate::Result<HashSet<FileInfo>> {
    match get_log_reader(&config.log_path) {
        Ok(log_reader) => {
            let files = log_reader
                .get_log_events()
                .filter_map(|event| match event {
                    Ok(event) if event.storage == storage => match event.event_type {
                        EventType::Delete(file_path) => Some(FileInfo::new_deleted(
                            event.storage,
                            file_path,
                            Some(event.timestamp),
                        )),
                        _ => None,
                    },
                    _ => None,
                })
                .collect();
            Ok(files)
        }
        Err(_) => Ok(HashSet::new()),
    }
}

fn get_failed_writes(config: &Config, storage: &str) -> crate::Result<HashSet<FileInfo>> {
    match get_log_reader(&config.log_path) {
        Ok(log_reader) => {
            let files = log_reader
                .get_failed_events(storage.to_string())
                .map(|path| FileInfo {
                    storage: storage.to_string(),
                    path,
                    modified_at: None,
                    // created_at: None,
                    deleted_at: None,
                    size: None,
                    permissions: 0,
                })
                .collect();
            Ok(files)
        }
        Err(_) => Ok(HashSet::new()),
    }
}

pub fn delete_file(
    file_info: &FileInfo,
    config: &Config,
    storage_state: &StorageState,
) -> crate::Result<()> {
    let path = file_info.get_absolute_path(config)?;
    if !path.exists() {
        log::debug!("delete_file: given path doesn't exist ({:?})", path);
        return Err(Box::new(std::io::Error::from(std::io::ErrorKind::NotFound)));
    } else if path.is_dir() {
        log::debug!("delete_file: {:?} is dir, removing whole dir", path);
        std::fs::remove_dir_all(&path)?;
    } else {
        log::debug!("delete_file: removing file {:?}", path);
        std::fs::remove_file(&path)?;
    }

    log::debug!("{:?} removed", path);

    storage_state.invalidate_state(&file_info.storage);

    Ok(())
}

pub fn move_file<'b>(
    src_file: &'b FileInfo,
    dest_file: &'b FileInfo,
    config: &Config,
    storage_state: &StorageState,
) -> crate::Result<()> {
    let src_path = src_file.get_absolute_path(config)?;
    let dest_path = dest_file.get_absolute_path(config)?;

    log::debug!("moving file {:?} to {:?}", src_path, dest_path);

    std::fs::rename(src_path, dest_path)?;
    storage_state.invalidate_state(&src_file.storage);

    Ok(())
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
        .map(|ext| ext.ends_with("ironcarrier") || ext.ends_with(".ignore"))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::{fs::File, sync::Arc};

    use super::*;

    #[test]
    fn can_read_local_files() -> Result<(), Box<dyn std::error::Error>> {
        fs::create_dir_all("./tmp/fs/read_local_files")?;
        File::create("./tmp/fs/read_local_files/file_1")?;
        File::create("./tmp/fs/read_local_files/file_2")?;

        let config = Arc::new(
            Config::new_from_str(
                r#"
log_path = "./tmp/fs/logfile.log"
[paths]
a = "./tmp/fs/read_local_files"
"#
                .to_string(),
            )
            .expect("Failed to parse config"),
        );

        let mut files: Vec<FileInfo> = walk_path(&config, "a", &StorageState::new(config.clone()))
            .unwrap()
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
