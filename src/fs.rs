//! This module is responsible for handling file system operations

use serde::{Deserialize, Serialize};
use std::{
    cmp::Ord,
    collections::HashMap,
    fs::{self, File},
    hash::Hash,
    path::{Path, PathBuf},
    time::Duration,
    time::SystemTime,
};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use crate::{config::Config, deletion_tracker::DeletionTracker, IronCarrierError};

/// Holds the information for a file inside a mapped folder  
///
/// If the file exists, `modified_at`, `created_at` and `size` will be [Some]  
/// Otherwise, only `deleted_at` will be [Some]
///
/// The `path` will always be relative to the alias root folder
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FileInfo {
    pub alias: String,
    /// File path, it is always relative to the alias root  
    /// The relative path will always be the same, no matter the machine
    pub path: PathBuf,

    pub modified_at: Option<u64>,
    pub created_at: Option<u64>,
    pub deleted_at: Option<u64>,
    pub size: Option<u64>,
    pub permissions: u32,
}

impl FileInfo {
    pub fn new(alias: String, relative_path: PathBuf, metadata: std::fs::Metadata) -> Self {
        FileInfo {
            alias,
            path: relative_path,
            created_at: metadata.created().ok().and_then(system_time_to_secs),
            modified_at: metadata.modified().ok().and_then(system_time_to_secs),
            size: Some(metadata.len()),
            deleted_at: None,
            permissions: get_permissions(&metadata),
        }
    }

    pub fn is_deleted(&self) -> bool {
        self.deleted_at.is_some()
    }

    pub fn new_deleted(
        alias: String,
        relative_path: PathBuf,
        deleted_at: Option<SystemTime>,
    ) -> Self {
        FileInfo {
            alias,
            path: relative_path,
            created_at: None,
            modified_at: None,
            size: None,
            deleted_at: deleted_at
                .or_else(|| Some(SystemTime::now()))
                .and_then(system_time_to_secs),
            permissions: 0,
        }
    }

    pub fn is_out_of_sync(&self, other: &FileInfo) -> bool {
        if self.deleted_at.is_some() && other.deleted_at.is_some() {
            return false;
        }

        self.modified_at != other.modified_at
            || self.deleted_at != other.deleted_at
            || self.size != other.size
    }

    pub fn is_local_file_newer(&self, config: &Config) -> bool {
        if self.deleted_at.is_some() {
            true
        } else {
            self.get_absolute_path(config)
                .ok()
                .and_then(|path| path.metadata().ok())
                .and_then(|metadata| metadata.modified().ok())
                .and_then(system_time_to_secs)
                .and_then(|created_at| Some(created_at > self.modified_at.unwrap()))
                .unwrap_or(false)
        }
    }

    /// Returns the absolute path of the file for this file system  
    /// Using the provided root path for the alias in [Config]
    pub fn get_absolute_path(&self, config: &Config) -> crate::Result<PathBuf> {
        match config.paths.get(&self.alias) {
            Some(path) => match path.canonicalize() {
                Ok(mut root_path) => {
                    root_path.extend(self.path.components());
                    Ok(root_path)
                }
                Err(_) => {
                    log::error!(
                        "cannot get absolute path for alias {}, check if the path is valid",
                        self.alias
                    );
                    Err(IronCarrierError::AliasNotAvailable(self.alias.to_owned()).into())
                }
            },
            None => {
                log::error!("provided alias does not exist in this node: {}", self.alias);
                Err(IronCarrierError::AliasNotAvailable(self.alias.to_owned()).into())
            }
        }
    }
}

impl Hash for FileInfo {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.alias.hash(state);
        self.path.hash(state);
    }
}

impl Eq for FileInfo {}

impl PartialEq for FileInfo {
    fn eq(&self, other: &Self) -> bool {
        self.alias.eq(&other.alias) && self.path.eq(&other.path)
    }
}

impl PartialOrd for FileInfo {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.path.partial_cmp(&other.path)
    }
}

impl Ord for FileInfo {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.path.cmp(&other.path)
    }
}

fn system_time_to_secs(time: SystemTime) -> Option<u64> {
    time.duration_since(SystemTime::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .ok()
}

/// This function returns the result of [walk_path] along with the hash for the file list
// pub fn  get_files_with_hash<'a>(path: &Path, alias: &'a str) -> crate::Result<(u64, Vec<FileInfo>)> {
//     let files = walk_path(path, alias)?;
//     let hash = crate::crypto::calculate_hash(&files);

//     log::debug!(
//         "found {} files for alias {} with hash {}",
//         files.len(),
//         alias,
//         hash
//     );

//     return Ok((hash, files));
// }

/// This function will return a [HashMap] containing the alias as key and the hash as value
// pub fn get_hash_for_alias(
//     alias_path: &HashMap<String, PathBuf>,
// ) -> crate::Result<HashMap<String, u64>> {
//     let mut result = HashMap::new();

//     for (alias, path) in alias_path {
//         let (hash, _) = get_files_with_hash(path.as_path(), alias)?;
//         result.insert(alias.to_string(), hash);
//     }

//     Ok(result)
// }

/// Returns a sorted vector with the entire folder structure for the given path
///
/// This function will look for deletes files in the [DeletionTracker] log and append all entries to the return list  
/// files with name or extension `.ironcarrier` will be ignored
pub fn walk_path<'a>(root_path: &Path, alias: &'a str) -> crate::Result<Vec<FileInfo>> {
    let mut paths = vec![root_path.to_owned()];

    let deletion_tracker = DeletionTracker::new(root_path);
    let mut files: Vec<FileInfo> = deletion_tracker
        .get_files()?
        .into_iter()
        .map(|(k, v)| FileInfo::new_deleted(alias.to_owned(), k, Some(v)))
        .collect();

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
            files.push(FileInfo::new(
                alias.to_owned(),
                path.strip_prefix(root_path)?.to_owned(),
                metadata,
            ));
        }
    }

    files.sort();

    return Ok(files);
}

pub fn delete_file(file_info: &FileInfo, config: &Config) -> crate::Result<()> {
    let path = file_info.get_absolute_path(config)?;
    if !path.exists() {
        log::debug!("delete_file: given path doesn't exist ({:?})", path);
        return Ok(());
    } else if path.is_dir() {
        log::debug!("delete_file: {:?} is dir, removing whole dir", path);
        std::fs::remove_dir_all(&path)?;
    } else {
        log::debug!("delete_file: removing file {:?}", path);
        std::fs::remove_file(&path)?;
    }

    log::debug!("{:?} removed", path);

    Ok(())
}

pub async fn move_file<'b>(
    src_file: &'b FileInfo,
    dest_file: &'b FileInfo,
    config: &Config,
) -> crate::Result<()> {
    let src_path = src_file.get_absolute_path(config)?;
    let dest_path = dest_file.get_absolute_path(config)?;

    log::debug!("moving file {:?} to {:?}", src_path, dest_path);

    tokio::fs::rename(src_path, dest_path).await?;

    Ok(())
}

pub fn get_temp_file(file_info: &FileInfo, config: &Config) -> crate::Result<File> {
    let mut temp_path = file_info.get_absolute_path(config)?;
    temp_path.set_extension("ironcarrier");

    if let Some(parent) = temp_path.parent() {
        if !parent.exists() {
            log::debug!("creating folders {:?}", parent);
            std::fs::create_dir_all(parent)?;
        }
    }

    log::debug!("creating temp file {:?}", temp_path);
    Ok(File::create(&temp_path)?)
}

pub async fn flush_temp_file(file_info: &FileInfo, config: &Config) -> crate::Result<()> {
    let final_path = file_info.get_absolute_path(config)?;
    let mut temp_path = final_path.clone();

    temp_path.set_extension("ironcarrier");

    log::debug!("moving temp file to {:?}", final_path);
    tokio::fs::rename(&temp_path, &final_path).await?;

    log::debug!("setting file modification time");
    let mod_time = SystemTime::UNIX_EPOCH + Duration::from_secs(file_info.modified_at.unwrap());
    filetime::set_file_mtime(&final_path, filetime::FileTime::from_system_time(mod_time))?;

    if file_info.permissions > 0 {
        set_file_permissions(&final_path, file_info.permissions).await?;
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
async fn set_file_permissions(path: &Path, perm: u32) -> tokio::io::Result<()> {
    let perm = std::fs::Permissions::from_mode(perm);
    tokio::fs::set_permissions(path, perm).await
}

#[cfg(not(unix))]
async fn set_file_permissions(path: &Path, perm: u32) -> tokio::io::Result<()> {
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
    use super::*;
    use crate::crypto::calculate_hash;

    #[test]
    fn can_read_local_files() -> Result<(), Box<dyn std::error::Error>> {
        fs::create_dir_all("./tmp/fs/read_local_files")?;
        File::create("./tmp/fs/read_local_files/file_1")?;
        File::create("./tmp/fs/read_local_files/file_2")?;

        let files = walk_path(&PathBuf::from("./tmp/fs/read_local_files"), "a").unwrap();

        assert_eq!(files[0].path.to_str(), Some("file_1"));
        assert_eq!(files[1].path.to_str(), Some("file_2"));

        fs::remove_dir_all("./tmp/fs/read_local_files")?;

        Ok(())
    }

    #[test]
    fn calc_hash() {
        let file = FileInfo {
            alias: "a".to_owned(),
            created_at: Some(0),
            modified_at: Some(0),
            path: Path::new("./some_file_path").to_owned(),
            size: Some(100),
            deleted_at: None,
            permissions: 0,
        };

        let files = vec![file];
        assert_eq!(calculate_hash(&files), 1762848629165523426);
    }

    #[test]
    fn test_is_special_file() {
        assert!(!is_special_file(Path::new("some_file.txt")));
        assert!(is_special_file(Path::new("some_file.ironcarrier")));
        assert!(is_special_file(Path::new(".ironcarrier")));
    }

    #[test]
    fn is_local_file_newer() {
        std::fs::create_dir_all("./tmp/fs").unwrap();

        let path = PathBuf::from("./tmp/fs/mtime");
        std::fs::File::create(&path).unwrap();
        filetime::set_file_mtime(
            &path,
            filetime::FileTime::from_system_time(SystemTime::UNIX_EPOCH),
        )
        .unwrap();

        let file = FileInfo {
            alias: "a".to_string(),
            modified_at: system_time_to_secs(SystemTime::now()),
            created_at: None,
            deleted_at: None,
            path: PathBuf::from("mtime"),
            size: None,
            permissions: 0,
        };

        let config = Config::new_from_str(
            "
        [paths]
        a = \"./tmp/fs\""
                .to_string(),
        )
        .unwrap();

        assert!(!file.is_local_file_newer(&config));
    }
}
