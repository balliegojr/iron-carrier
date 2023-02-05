use serde::{Deserialize, Serialize};

use std::{cmp::Ord, hash::Hash, path::PathBuf};

use crate::{config::Config, IronCarrierError};

use super::{get_permissions, system_time_to_secs};

/// Holds the information for a file inside a mapped folder  
///
/// The `path` will always be relative to the alias root folder
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FileInfo {
    pub storage: String,
    /// File path, it is always relative to the alias root  
    /// The relative path will always be the same, no matter the machine
    pub path: PathBuf,

    pub info_type: FileInfoType,
    pub permissions: u32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum FileInfoType {
    Existent { modified_at: u64, size: u64 },
    Deleted { deleted_at: u64 },
    Moved { old_path: PathBuf, moved_at: u64 },
}

impl FileInfo {
    pub fn existent(
        storage: String,
        path: PathBuf,
        modified_at: u64,
        size: u64,
        permissions: u32,
    ) -> Self {
        FileInfo {
            storage,
            path,
            info_type: FileInfoType::Existent { modified_at, size },
            permissions,
        }
    }
    pub fn deleted(storage: String, path: PathBuf, deleted_at: u64) -> Self {
        FileInfo {
            storage,
            path,
            info_type: FileInfoType::Deleted { deleted_at },
            permissions: 0,
        }
    }

    pub fn moved(storage: String, path: PathBuf, old_path: PathBuf, moved_at: u64) -> Self {
        Self {
            storage,
            path,
            info_type: FileInfoType::Moved { old_path, moved_at },
            permissions: 0,
        }
    }

    /// matches existent and moved files
    pub fn is_existent(&self) -> bool {
        !matches!(self.info_type, FileInfoType::Deleted { .. })
    }

    pub fn is_out_of_sync(&self, other: &FileInfo) -> bool {
        match (&self.info_type, &other.info_type) {
            (
                FileInfoType::Existent { modified_at, size },
                FileInfoType::Existent {
                    modified_at: other_modified_at,
                    size: other_size,
                },
            ) => modified_at != other_modified_at || size != other_size,
            (FileInfoType::Deleted { .. }, FileInfoType::Deleted { .. }) => false,
            (
                FileInfoType::Moved { old_path, .. },
                FileInfoType::Moved {
                    old_path: other_old_path,
                    ..
                },
            ) => old_path != other_old_path,
            _ => true,
        }
    }

    pub fn file_size(&self) -> crate::Result<u64> {
        if let FileInfoType::Existent { size, .. } = self.info_type {
            Ok(size)
        } else {
            Err(Box::new(IronCarrierError::InvalidOperation))
        }
    }

    /// Returns the absolute path of the file for this file system  
    /// Using the provided root path for the alias in [Config]
    pub fn get_absolute_path(&self, config: &Config) -> crate::Result<PathBuf> {
        match config.storages.get(&self.storage) {
            Some(path) => path
                .path
                .canonicalize()
                .map(|root_path| root_path.join(&self.path))
                .map_err(Box::from),
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
        let self_date = match self.info_type {
            FileInfoType::Existent { modified_at, .. } => modified_at,
            FileInfoType::Deleted { deleted_at } => deleted_at,
            FileInfoType::Moved { moved_at, .. } => moved_at,
        };

        let other_date = match other.info_type {
            FileInfoType::Existent { modified_at, .. } => modified_at,
            FileInfoType::Deleted { deleted_at } => deleted_at,
            FileInfoType::Moved { moved_at, .. } => moved_at,
        };

        self_date.cmp(&other_date)
    }

    pub fn get_local_file_info(&self, config: &Config) -> crate::Result<Self> {
        let metadata = self.get_absolute_path(config)?.metadata()?;

        let modified_at = metadata.modified().map(system_time_to_secs)?;
        let size = metadata.len();

        Ok(Self::existent(
            self.storage.clone(),
            self.path.clone(),
            modified_at,
            size,
            get_permissions(&metadata),
        ))
    }

    pub fn get_date(&self) -> u64 {
        match self.info_type {
            FileInfoType::Existent { modified_at, .. } => modified_at,
            FileInfoType::Deleted { deleted_at } => deleted_at,
            FileInfoType::Moved { moved_at, .. } => moved_at,
        }
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
            info_type: FileInfoType::Existent {
                modified_at: 0,
                size: 0,
            },
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
            path: Path::new("./some_file_path").to_owned(),
            permissions: 0,
            info_type: FileInfoType::Existent {
                modified_at: 0,
                size: 100,
            },
        };

        let mut other_file = file_info.clone();
        assert!(!file_info.is_out_of_sync(&other_file));

        other_file.info_type = FileInfoType::Existent {
            modified_at: 1,
            size: 100,
        };
        assert!(file_info.is_out_of_sync(&other_file));

        other_file.info_type = FileInfoType::Existent {
            modified_at: 0,
            size: 101,
        };
        assert!(file_info.is_out_of_sync(&other_file));

        other_file.info_type = FileInfoType::Deleted { deleted_at: 1 };
        assert!(file_info.is_out_of_sync(&other_file));

        file_info.info_type = FileInfoType::Deleted { deleted_at: 1 };
        assert!(!file_info.is_out_of_sync(&other_file));
    }
}
