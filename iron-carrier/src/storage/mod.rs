//! This module is responsible for handling file system operations

use serde::{Deserialize, Serialize};

use crate::{
    context::Context,
    hash_helper::{self, HASHER},
    ignored_files::IgnoredFiles,
    relative_path::RelativePathBuf,
    transaction_log::TransactionLog,
};

pub mod file_operations;
pub mod file_watcher;
use storage_tree::{DeletedFileInfo, ExistingFileInfo, MovedFileInfo, StorageFile, StorageTree};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Storage {
    /// A hash of the current files
    pub hash: u64,
    pub name: String,

    pub current: StorageTree<ExistingFileInfo>,
    pub moved: StorageTree<MovedFileInfo>,
    pub deleted: StorageTree<DeletedFileInfo>,
}

pub mod storage_tree;

/// Gets the file list and hash for the given storage
pub async fn build(context: &Context, name: &str) -> anyhow::Result<Storage> {
    let storage_config = context.config.path(name)?;

    let ignored_files = crate::ignored_files::IgnoredFiles::new(context, storage_config).await;
    let current = walk_path(context, name, &ignored_files).await?;

    let hash = calculate_storage_hash(&current);

    let deleted = get_deleted_files(&context.transaction_log, name, &current).await?;
    let moved = get_moved_files(&context.transaction_log, name, &current).await?;

    Ok(Storage {
        hash,
        name: name.to_string(),
        current,
        moved,
        deleted,
    })
}

/// Returns a sorted vector with the entire directory structure (recursive read) for the given path.  
pub async fn walk_path(
    context: &Context,
    storage_name: &str,
    ignored_files: &IgnoredFiles,
) -> anyhow::Result<StorageTree<ExistingFileInfo>> {
    let mut paths = vec![RelativePathBuf::root()];
    let mut tree = StorageTree::<ExistingFileInfo>::new();

    let failed_writes = context
        .transaction_log
        .get_failed_writes(storage_name)
        .await?;

    let storage_config = context.config.path(storage_name)?;
    while let Some(dir) = paths.pop() {
        let entries = context
            .fs
            .read_dir(storage_config, &dir, ignored_files)
            .await?;

        for entry in entries {
            let entry = entry?;
            if is_special_file(entry.path()) || failed_writes.contains(entry.path()) {
                continue;
            }

            let metadata = entry.metadata();
            if metadata.is_dir() {
                paths.push(entry.into_path());
                continue;
            }

            let file = ExistingFileInfo::new(entry.path().as_path(), metadata);
            tree.insert(file, entry.path().parent());
        }
    }

    Ok(tree)
}

/// Calculate a "shallow" hash for the files by hashing the attributes, it doesn't open the file to
/// read the contents
pub fn calculate_storage_hash(files: &StorageTree<ExistingFileInfo>) -> u64 {
    let mut digest = HASHER.digest();

    for file in files.files() {
        hash_helper::calculate_file_hash_digest(
            &mut digest,
            file.parent(),
            file.id(),
            file.size(),
            file.date(),
        );
    }

    digest.finalize()
}

/// Fetch deleted file events from the transaction log
async fn get_deleted_files(
    transaction_log: &TransactionLog,
    storage: &str,
    current: &StorageTree<ExistingFileInfo>,
) -> anyhow::Result<StorageTree<DeletedFileInfo>> {
    let files = transaction_log.get_deleted_files(storage).await?;
    let mut tree = StorageTree::new();

    for (path, deleted_at) in files {
        let file = DeletedFileInfo::new(path.as_path(), deleted_at);
        if current.contains_id(file.id()) {
            continue;
        }

        tree.insert(file, path.parent());
    }

    Ok(tree)
}

/// Fetch moved file events from the transaction log
async fn get_moved_files(
    transaction_log: &TransactionLog,
    storage: &str,
    current: &StorageTree<ExistingFileInfo>,
) -> anyhow::Result<StorageTree<MovedFileInfo>> {
    let files = transaction_log.get_moved_files(storage).await?;
    let mut tree = StorageTree::new();

    for (path, old_path, moved_at) in files {
        let file = MovedFileInfo::new(path.as_path(), old_path, moved_at);
        if !current.contains_id(file.id()) {
            continue;
        }

        tree.insert(file, path.parent());
    }

    Ok(tree)
}

/// Returns true if `path` name or extension are .ironcarrier
pub fn is_special_file(path: &RelativePathBuf) -> bool {
    path.build_path()
        .file_name()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.ends_with("ironcarrier"))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, str::FromStr};

    use crate::{
        config::{Config, PathConfig},
        context::test_context,
        fs::MetadataB,
        leak::Leak,
        transaction_log::{append_deleted, append_failed_write},
        validation::Validated,
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
        let tree = walk_path(context, "a", &crate::ignored_files::IgnoredFiles::empty()).await?;

        assert!(!tree.is_empty());

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

        files.files().for_each(|file| {
            assert!(file.name() != "b.ig");
        });

        Ok(())
    }

    #[tokio::test]
    async fn walk_exclude_failed_writes() -> anyhow::Result<()> {
        let config = get_config();
        let context = test_context(config, mem_fs()).leak();

        append_failed_write(context, "a", 0).await;
        append_failed_write(context, "dir/a", 0).await;

        let ignored = crate::ignored_files::IgnoredFiles::new(
            context,
            context.config.storages.get("a").unwrap(),
        )
        .await;

        let files = walk_path(context, "a", &ignored).await?;
        let paths: HashSet<_> = files
            .files()
            .filter_map(|f| files.get_path(f.id()))
            .collect();

        assert!(!paths.contains(&RelativePathBuf::from("a")));
        assert!(!paths.contains(&RelativePathBuf::from("dir/a")));

        Ok(())
    }

    #[tokio::test]
    async fn build_include_deleted_files() -> anyhow::Result<()> {
        let config = get_config();
        let context = test_context(config, mem_fs()).leak();

        append_deleted(context, "deleted", 0).await;

        let info = build(context, "a")
            .await
            .expect("Failed to get storage info");

        assert_eq!(1, info.deleted.len());
        assert!(info.deleted.files().any(|file| file.name() == "deleted"));

        Ok(())
    }

    #[test]
    fn test_is_special_file() {
        assert!(!is_special_file(&"some_file.txt".into()));
        assert!(is_special_file(&"some_file.ironcarrier".into()));
        assert!(is_special_file(&".ironcarrier".into()));
    }

    fn get_config() -> &'static Validated<Config> {
        Validated::new(Config {
            storages: [("a".to_string(), PathConfig::from_str("").unwrap())].into(),
            ..Default::default()
        })
        .leak()
    }

    fn mem_fs() -> &'static crate::fs::MemFS {
        let ignore = r#"
*.ig
**.ig
"#;

        let files = [
            (
                ".ignore",
                ignore.bytes().collect(),
                MetadataB::new().build(),
            ),
            ("dir/", Vec::new(), MetadataB::dir().build()),
            ("dir/a", Vec::new(), MetadataB::new().build()),
            ("dir/b.ig", Vec::new(), MetadataB::new().build()),
            ("a", Vec::new(), MetadataB::new().build()),
            ("b.ig", Vec::new(), MetadataB::new().build()),
        ]
        .into_iter();
        crate::fs::MemFS::new(files).leak()
    }
}
