use serde::{Deserialize, Serialize};

use std::{cmp::Ord, hash::Hash, path::PathBuf};

use crate::{config::Config, relative_path::RelativePathBuf};

use super::{get_permissions, system_time_to_secs};

/// Represents a file in the storage.  
///
/// The state of the file is represented by the [FileInfoType] enum, where a file can be existent,
/// deleted or moved
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FileInfo {
    pub storage: String,
    pub path: RelativePathBuf,

    pub info_type: FileInfoType,
    pub permissions: u32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum FileInfoType {
    /// Represent an existing file in the storage
    Existent {
        modified_at: u64,
        created_at: u64,
        size: u64,
    },

    /// Represent a deleted file, this information comes from the transaction log
    Deleted { deleted_at: u64 },

    /// Represent a moved file, this information comes from the transaction log
    Moved {
        old_path: RelativePathBuf,
        moved_at: u64,
    },
}

impl FileInfo {
    pub fn existent(
        storage: String,
        path: RelativePathBuf,
        modified_at: u64,
        created_at: u64,
        size: u64,
        permissions: u32,
    ) -> Self {
        FileInfo {
            storage,
            path,
            info_type: FileInfoType::Existent {
                modified_at,
                created_at,
                size,
            },
            permissions,
        }
    }
    pub fn deleted(storage: String, path: RelativePathBuf, deleted_at: u64) -> Self {
        FileInfo {
            storage,
            path,
            info_type: FileInfoType::Deleted { deleted_at },
            permissions: 0,
        }
    }

    pub fn moved(
        storage: String,
        path: RelativePathBuf,
        old_path: RelativePathBuf,
        moved_at: u64,
    ) -> Self {
        Self {
            storage,
            path,
            info_type: FileInfoType::Moved { old_path, moved_at },
            permissions: 0,
        }
    }

    /// Returns true if modification date, size or entry type are different
    pub fn is_out_of_sync(&self, other: &FileInfo) -> bool {
        match (&self.info_type, &other.info_type) {
            (
                FileInfoType::Existent {
                    modified_at, size, ..
                },
                FileInfoType::Existent {
                    modified_at: other_modified_at,
                    size: other_size,
                    ..
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

    /// Return the file size if the file type is existent
    pub fn file_size(&self) -> anyhow::Result<u64> {
        if let FileInfoType::Existent { size, .. } = self.info_type {
            Ok(size)
        } else {
            anyhow::bail!("Invalid operation: File size is only available for existing files");
        }
    }

    /// Returns the absolute path of the file for this file system  
    /// Using the provided root path for the alias in [Config]
    pub fn get_absolute_path(&self, config: &Config) -> anyhow::Result<PathBuf> {
        match config.storages.get(&self.storage) {
            Some(path) => self.path.absolute(path),
            None => {
                anyhow::bail!(
                    "provided storage does not exist in this node: {}",
                    self.storage
                );
            }
        }
    }

    /// Compare the dates for two files, regardless of entry type
    pub fn date_cmp(&self, other: &Self) -> std::cmp::Ordering {
        let self_date = match self.info_type {
            FileInfoType::Existent {
                modified_at,
                created_at,
                ..
            } => modified_at.max(created_at),
            FileInfoType::Deleted { deleted_at } => deleted_at,
            FileInfoType::Moved { moved_at, .. } => moved_at,
        };

        let other_date = match other.info_type {
            FileInfoType::Existent {
                modified_at,
                created_at,
                ..
            } => modified_at.max(created_at),
            FileInfoType::Deleted { deleted_at } => deleted_at,
            FileInfoType::Moved { moved_at, .. } => moved_at,
        };

        self_date.cmp(&other_date)
    }

    pub fn get_local_file_info(&self, config: &Config) -> anyhow::Result<Self> {
        let metadata = self.get_absolute_path(config)?.metadata()?;

        let modified_at = metadata.modified().map(system_time_to_secs)?;
        let created_at = metadata.created().map(system_time_to_secs)?;
        let size = metadata.len();

        Ok(Self::existent(
            self.storage.clone(),
            self.path.clone(),
            modified_at,
            created_at,
            size,
            get_permissions(&metadata),
        ))
    }

    /// Get the date of the file.  
    ///
    /// For existent files, this is the modified date
    /// For deleted files, this is the deleted date
    /// For moved files, this is the moved date
    pub fn get_date(&self) -> u64 {
        match self.info_type {
            FileInfoType::Existent { modified_at, .. } => modified_at,
            FileInfoType::Deleted { deleted_at } => deleted_at,
            FileInfoType::Moved { moved_at, .. } => moved_at,
        }
    }

    /// Get a new [`FileInfo`] using the old path of the file if `info_type` is [`FileInfoType::Moved`]
    pub fn as_deleted_file(&self) -> Option<FileInfo> {
        match &self.info_type {
            FileInfoType::Moved { old_path, moved_at } => Some(FileInfo::deleted(
                self.storage.clone(),
                old_path.clone(),
                *moved_at,
            )),
            _ => None,
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
        Some(self.cmp(other))
    }
}

impl Ord for FileInfo {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.path.cmp(&other.path)
    }
}

#[cfg(test)]
mod tests {
    use crate::hash_helper;

    use super::*;

    #[test]
    fn calc_hash() {
        let mut file = FileInfo {
            storage: "a".to_owned(),
            path: "./some_file_path".into(),
            info_type: FileInfoType::Existent {
                modified_at: 0,
                created_at: 0,
                size: 0,
            },
            permissions: 0,
        };

        assert_eq!(hash_helper::calculate_file_hash(&file), 4552872816654674580);

        file.path = "./some_other_file".into();
        assert_ne!(hash_helper::calculate_file_hash(&file), 4552872816654674580);
    }

    #[test]
    fn test_is_out_of_sync() {
        let mut file_info = FileInfo {
            storage: "a".to_owned(),
            path: "./some_file_path".into(),
            permissions: 0,
            info_type: FileInfoType::Existent {
                modified_at: 0,
                created_at: 0,
                size: 100,
            },
        };

        let mut other_file = file_info.clone();
        assert!(!file_info.is_out_of_sync(&other_file));

        other_file.info_type = FileInfoType::Existent {
            modified_at: 1,
            created_at: 1,
            size: 100,
        };
        assert!(file_info.is_out_of_sync(&other_file));

        other_file.info_type = FileInfoType::Existent {
            modified_at: 0,
            created_at: 1,
            size: 101,
        };
        assert!(file_info.is_out_of_sync(&other_file));

        other_file.info_type = FileInfoType::Deleted { deleted_at: 1 };
        assert!(file_info.is_out_of_sync(&other_file));

        file_info.info_type = FileInfoType::Deleted { deleted_at: 1 };
        assert!(!file_info.is_out_of_sync(&other_file));
    }
}
