use serde::{Deserialize, Serialize};

use std::{cmp::Ord, hash::Hash, path::PathBuf, time::SystemTime};

use crate::{config::Config, IronCarrierError};

use super::{get_permissions, system_time_to_secs};

/// Holds the information for a file inside a mapped folder  
///
/// If the file exists, `modified_at`, `created_at` and `size` will be [Some]  
/// Otherwise, only `deleted_at` will be [Some]
///
/// The `path` will always be relative to the alias root folder
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FileInfo {
    pub storage: String,
    /// File path, it is always relative to the alias root  
    /// The relative path will always be the same, no matter the machine
    pub path: PathBuf,

    pub modified_at: Option<u64>,
    // pub created_at: Option<u64>,
    pub deleted_at: Option<u64>,
    pub size: Option<u64>,
    pub permissions: u32,
}

impl FileInfo {
    pub fn new(storage: String, relative_path: PathBuf, metadata: std::fs::Metadata) -> Self {
        FileInfo {
            storage,
            path: relative_path,
            // created_at: metadata.created().ok().and_then(system_time_to_secs),
            modified_at: metadata.modified().ok().and_then(system_time_to_secs),
            size: Some(metadata.len()),
            deleted_at: None,
            permissions: get_permissions(&metadata),
        }
    }

    pub fn is_deleted(&self) -> bool {
        self.deleted_at.is_some()
    }

    pub fn new_deleted(alias: String, relative_path: PathBuf, deleted_at: Option<u64>) -> Self {
        FileInfo {
            storage: alias,
            path: relative_path,
            // created_at: None,
            modified_at: None,
            size: None,
            deleted_at: deleted_at
                .or_else(|| Some(SystemTime::UNIX_EPOCH.elapsed().unwrap().as_secs())),
            permissions: 0,
        }
    }

    pub fn is_out_of_sync(&self, other: &FileInfo) -> bool {
        if self.deleted_at.is_some() && other.deleted_at.is_some() {
            return false;
        }

        self.modified_at != other.modified_at
            || self.deleted_at.is_some() != other.deleted_at.is_some()
            || self.size != other.size
    }

    /// Returns the absolute path of the file for this file system  
    /// Using the provided root path for the alias in [Config]
    pub fn get_absolute_path(&self, config: &Config) -> crate::Result<PathBuf> {
        match config.storages.get(&self.storage) {
            Some(path) => match path.path.canonicalize() {
                Ok(mut root_path) => {
                    root_path.extend(self.path.components());
                    Ok(root_path)
                }
                Err(_) => {
                    log::error!(
                        "cannot get absolute path for storage {}, check if the path is valid",
                        self.storage
                    );
                    Err(IronCarrierError::StorageNotAvailable(self.storage.to_owned()).into())
                }
            },
            None => {
                log::error!(
                    "provided storage does not exist in this node: {}",
                    self.storage
                );
                Err(IronCarrierError::StorageNotAvailable(self.storage.to_owned()).into())
            }
        }
    }

    pub fn date_cmp(&self, other: &Self) -> std::cmp::Ordering {
        let self_date = self.deleted_at.unwrap_or_else(|| self.modified_at.unwrap());
        let other_date = other
            .deleted_at
            .unwrap_or_else(|| other.modified_at.unwrap());

        self_date.cmp(&other_date)
    }

    pub fn get_local_file_info(&self, config: &Config) -> crate::Result<Self> {
        self.get_absolute_path(config)?
            .metadata()
            .map(|metadata| Self::new(self.storage.clone(), self.path.clone(), metadata))
            .map_err(Box::from)
    }
}

impl Hash for FileInfo {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.storage.hash(state);
        self.path.hash(state);
    }
}

impl Eq for FileInfo {}

impl PartialEq for FileInfo {
    fn eq(&self, other: &Self) -> bool {
        self.storage.eq(&other.storage) && self.path.eq(&other.path)
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

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::hash_helper;

    use super::*;

    #[test]
    fn calc_hash() {
        let mut file = FileInfo {
            storage: "a".to_owned(),
            path: Path::new("./some_file_path").to_owned(),
            // created_at: None,
            modified_at: Some(0),
            size: Some(0),
            deleted_at: None,
            permissions: 0,
        };

        assert_eq!(hash_helper::calculate_file_hash(&file), 4552872816654674580);

        file.path = Path::new("./some_other_file").to_owned();
        assert_ne!(hash_helper::calculate_file_hash(&file), 4552872816654674580);
    }

    #[test]
    fn test_is_out_of_sync() {
        let mut file_info = FileInfo {
            storage: "a".to_owned(),
            // created_at: Some(0),
            modified_at: Some(0),
            path: Path::new("./some_file_path").to_owned(),
            size: Some(100),
            deleted_at: None,
            permissions: 0,
        };

        let mut other_file = file_info.clone();
        assert!(!file_info.is_out_of_sync(&other_file));

        other_file.modified_at = Some(1);
        assert!(file_info.is_out_of_sync(&other_file));

        let mut other_file = file_info.clone();
        other_file.size = Some(101);

        assert!(file_info.is_out_of_sync(&other_file));

        let mut other_file = file_info.clone();
        other_file.deleted_at = Some(1);
        assert!(file_info.is_out_of_sync(&other_file));

        let mut other_file = file_info.clone();
        other_file.deleted_at = Some(10);
        file_info.deleted_at = Some(1);

        assert!(!file_info.is_out_of_sync(&other_file));
    }
}
