//! This module is responsible for handling file system operations

mod file_info;
pub mod file_operations;
pub mod file_watcher;
pub use file_info::{FileInfo, FileInfoType};
use serde::{Deserialize, Serialize};

use std::{collections::HashSet, path::Path};

use crate::{
    config::PathConfig,
    context::Context,
    hash_helper::{self, HASHER},
    ignored_files::IgnoredFiles,
    relative_path::RelativePathBuf,
    transaction_log::TransactionLog,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct Storage {
    /// Hash is calculated with only existing files
    pub hash: u64,
    pub files: HashSet<FileInfo>,
    pub moved: HashSet<FileInfo>,
    pub deleted: HashSet<FileInfo>,
}

/// Gets the file list and hash for the given storage
pub async fn get_storage_info(
    context: &Context,
    name: &str,
    storage_path_config: &'static PathConfig,
) -> anyhow::Result<Storage> {
    let ignored_files = crate::ignored_files::IgnoredFiles::new(context, storage_path_config).await;
    let files = walk_path(context, name, &ignored_files).await?;
    let hash = calculate_storage_hash(&files);

    let files: HashSet<FileInfo> = files.into_iter().collect();
    let mut deleted = get_deleted_files(&context.transaction_log, name).await?;
    let mut moved = get_moved_files(&context.transaction_log, name).await?;

    deleted.retain(|f| !files.contains(f));
    moved.retain(|f| files.contains(f));

    Ok(Storage {
        files,
        deleted,
        moved,
        hash,
    })
}

/// Returns a sorted vector with the entire directory structure (recursive read) for the given path.  
///
/// The list will contain deleted and moved files, by reading the transaction log.  
pub async fn walk_path(
    context: &Context,
    storage_name: &str,
    ignored_files: &IgnoredFiles,
) -> anyhow::Result<Vec<FileInfo>> {
    let mut paths = vec![RelativePathBuf::root()];
    let mut files = Vec::new();

    let failed_writes = context
        .transaction_log
        .get_failed_writes(storage_name)
        .await?;

    let storage_config = context
        .config
        .storages
        .get(storage_name)
        .ok_or_else(|| anyhow::anyhow!("Storage {storage_name} not found"))?;

    while let Some(dir) = paths.pop() {
        let entries = context.fs.read_dir(storage_config, &dir).await?;
        for entry in entries {
            let entry = entry?;

            if is_special_file(entry.path().as_path()) {
                continue;
            }

            let metadata = entry.metadata();
            if metadata.is_dir() {
                paths.push(entry.into_path());
                continue;
            }

            if ignored_files.is_ignored(entry.path()) {
                continue;
            }

            if failed_writes.contains(entry.path()) {
                continue;
            }

            let permissions = metadata.permissions();
            let created_at = metadata.created_as_secs();
            let modified_at = metadata.modified_as_secs();
            let size = metadata.len();

            let file_info = FileInfo::existent(
                storage_name.to_owned(),
                entry.into_path(),
                modified_at,
                created_at,
                size,
                permissions,
            );

            files.push(file_info);
        }
    }

    files.sort();

    Ok(files)
}

/// Calculate a "shallow" hash for the files by hashing the attributes, it doesn't open the file to
/// read the contents
pub fn calculate_storage_hash(files: &[FileInfo]) -> u64 {
    let mut digest = HASHER.digest();

    for file in files {
        hash_helper::calculate_file_hash_digest(file, &mut digest);
    }

    digest.finalize()
}

/// Fetch deleted file events from the transaction log
async fn get_deleted_files(
    transaction_log: &TransactionLog,
    storage: &str,
) -> anyhow::Result<HashSet<FileInfo>> {
    transaction_log
        .get_deleted_files(storage)
        .await
        .map(|files| {
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
) -> anyhow::Result<HashSet<FileInfo>> {
    transaction_log.get_moved_files(storage).await.map(|files| {
        files
            .into_iter()
            .map(|(path, old_path, timestamp)| {
                FileInfo::moved(storage.to_string(), path, old_path, timestamp)
            })
            .collect()
    })
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
    use std::str::FromStr;

    use crate::{
        config::Config, context::test_context, fs::MetadataB, leak::Leak, validation::Validated,
    };

    use super::*;

    #[tokio::test]
    async fn walk_can_read_local_files() -> anyhow::Result<()> {
        let config = crate::validation::Validated::new(Config {
            storages: [("a".to_string(), PathConfig::from_str("./src/").unwrap())].into(),
            ..Default::default()
        })
        .leak();

        let context = test_context(config, crate::fs::TokioFS.leak()).leak();
        let files = walk_path(context, "a", &crate::ignored_files::IgnoredFiles::empty()).await?;

        assert!(!files.is_empty());
        assert!(files.is_sorted());

        Ok(())
    }

    #[tokio::test]
    async fn walk_exclude_ignored_files() -> anyhow::Result<()> {
        let config = get_config();
        let context = test_context(config, mem_fs()).leak();

        let ignored = crate::ignored_files::IgnoredFiles::new(
            context,
            context.config.storages.get("a").unwrap(),
        )
        .await;

        let files = walk_path(context, "a", &ignored).await?;
        assert_eq!(3, files.len());

        files.iter().for_each(|file| {
            assert!(
                file.path
                    .as_path()
                    .file_name()
                    .is_none_or(|ext| ext != "b.ig")
            );
        });

        Ok(())
    }

    #[tokio::test]
    async fn walk_exclude_failed_writes() -> anyhow::Result<()> {
        let config = get_config();
        let context = test_context(config, mem_fs()).leak();

        append_failed_write(context, "/a").await;
        append_failed_write(context, "/dir/a").await;

        let ignored = crate::ignored_files::IgnoredFiles::new(
            context,
            context.config.storages.get("a").unwrap(),
        )
        .await;

        let files = walk_path(context, "a", &ignored).await?;
        assert_eq!(1, files.len());

        Ok(())
    }

    #[tokio::test]
    async fn get_storage_include_deleted_files() -> anyhow::Result<()> {
        let config = get_config();
        let context = test_context(config, mem_fs()).leak();

        append_deleted(context, "/deleted").await;

        let info = get_storage_info(context, "a", config.storages.get("a").unwrap())
            .await
            .expect("Failed to get storage info");

        assert_eq!(1, info.deleted.len());
        assert!(
            info.deleted
                .iter()
                .any(|file| file.path == RelativePathBuf::from("deleted"))
        );

        Ok(())
    }

    #[test]
    fn test_is_special_file() {
        assert!(!is_special_file(Path::new("some_file.txt")));
        assert!(is_special_file(Path::new("some_file.ironcarrier")));
        assert!(is_special_file(Path::new(".ironcarrier")));
    }

    fn get_config() -> &'static Validated<Config> {
        Validated::new(Config {
            storages: [("a".to_string(), PathConfig::from_str("/").unwrap())].into(),
            ..Default::default()
        })
        .leak()
    }

    fn mem_fs() -> &'static crate::fs::MemFS {
        let ignore = r#"**.ig
        "#;

        let files = [
            (
                "/.ignore",
                ignore.bytes().collect(),
                MetadataB::new().build(),
            ),
            ("/dir/", Vec::new(), MetadataB::dir().build()),
            ("/dir/a", Vec::new(), MetadataB::new().build()),
            ("/dir/b.ig", Vec::new(), MetadataB::new().build()),
            ("/a", Vec::new(), MetadataB::new().build()),
            ("/b.ig", Vec::new(), MetadataB::new().build()),
        ]
        .into_iter();
        crate::fs::MemFS::new(files).leak()
    }

    async fn append_failed_write(context: &Context, path: &str) {
        context
            .transaction_log
            .append_log_entry(
                "a",
                &context
                    .config
                    .storages
                    .get("a")
                    .unwrap()
                    .get_relative_path(Path::new(path).to_owned())
                    .unwrap(),
                None,
                crate::transaction_log::LogEntry {
                    timestamp: 0,
                    event_type: crate::transaction_log::EntryType::Write,
                    event_status: crate::transaction_log::EntryStatus::Pending,
                },
            )
            .await
            .expect("Failed to append log entry");
    }

    async fn append_deleted(context: &Context, path: &str) {
        context
            .transaction_log
            .append_log_entry(
                "a",
                &context
                    .config
                    .storages
                    .get("a")
                    .unwrap()
                    .get_relative_path(Path::new(path).to_owned())
                    .unwrap(),
                None,
                crate::transaction_log::LogEntry {
                    timestamp: 0,
                    event_type: crate::transaction_log::EntryType::Delete,
                    event_status: crate::transaction_log::EntryStatus::Done,
                },
            )
            .await
            .expect("Failed to append log entry");
    }
}
