use tokio::fs::OpenOptions;

use crate::{
    config::Config,
    ignored_files::IgnoredFilesCache,
    transaction_log::{LogEntry, TransactionLog},
};

use super::{fix_times_and_permissions, FileInfo, FileInfoType};

/// Move the file in the storage, this  operation files if FileInfoType is not Moved
pub async fn move_file<'b>(
    config: &Config,
    transaction_log: &TransactionLog,
    file: &'b FileInfo,
    ignored_files_cache: &mut IgnoredFilesCache,
) -> anyhow::Result<()> {
    let path_config = config
        .storages
        .get(&file.storage)
        .ok_or_else(|| anyhow::anyhow!("Storage {} not available", file.storage))?;

    let ignored_files = ignored_files_cache.get(path_config).await;
    if ignored_files.is_ignored(&file.path) {
        return Ok(());
    }

    let dest_path_abs = file.path.absolute(path_config)?;
    let src_path = if let FileInfoType::Moved { old_path, .. } = &file.info_type {
        old_path
    } else {
        anyhow::bail!(
            "Invalid Operation: called move for file that was not moved ({:?})",
            file.path
        );
    };

    if ignored_files.is_ignored(src_path) {
        return Ok(());
    }

    let src_path_abs = src_path.absolute(path_config)?;
    if !src_path_abs.exists() {
        anyhow::bail!(
            "Invalid Operation: source does not exist, cannot move {:?} to {:?}",
            src_path_abs,
            dest_path_abs
        );
    }

    if let Some(parent) = dest_path_abs.parent() {
        if !parent.exists() {
            log::debug!("creating folders {:?}", parent);
            std::fs::create_dir_all(parent)?;
        }
    }

    tokio::fs::rename(&src_path_abs, &dest_path_abs).await?;
    fix_times_and_permissions(file, config)?;

    log::info!("{src_path_abs:?} moved to {dest_path_abs:?}");

    // It is necessary to add two entries in the log, one for the new path as moved, one for the
    // old path as deleted
    //
    // If there is a future change to the file, before any synchronization, the new entry will
    // become a write one, hence the old file will need to be deleted. It is possible to improve
    // this flow by chaining a move with a send action
    transaction_log
        .append_log_entry(
            &file.storage,
            src_path,
            None,
            LogEntry::new(
                crate::transaction_log::EntryType::Delete,
                crate::transaction_log::EntryStatus::Done,
                file.get_date(),
            ),
        )
        .await?;

    transaction_log
        .append_log_entry(
            &file.storage,
            &file.path,
            Some(src_path),
            LogEntry::new(
                crate::transaction_log::EntryType::Move,
                crate::transaction_log::EntryStatus::Done,
                file.get_date(),
            ),
        )
        .await
}

/// Delete the file in the storage
pub async fn delete_file(
    config: &Config,
    transaction_log: &TransactionLog,
    file_info: &FileInfo,
    ignored_files_cache: &mut IgnoredFilesCache,
) -> anyhow::Result<()> {
    if let Some(storage_config) = config.storages.get(&file_info.storage) {
        let ignored_files = ignored_files_cache.get(storage_config).await;
        if ignored_files.is_ignored(&file_info.path) {
            return Ok(());
        }
    }

    let path = file_info.get_absolute_path(config)?;
    if !path.exists() {
        log::warn!("{:?} path does not exist", path);
        anyhow::bail!("File not found");
    } else if path.is_dir() {
        tokio::fs::remove_dir_all(&path).await?;
    } else {
        tokio::fs::remove_file(&path).await?;
    }

    log::info!("{:?} deleted", path);
    transaction_log
        .append_log_entry(
            &file_info.storage,
            &file_info.path,
            None,
            LogEntry::new(
                crate::transaction_log::EntryType::Delete,
                crate::transaction_log::EntryStatus::Done,
                file_info.get_date(),
            ),
        )
        .await
}

pub async fn open_file_for_reading(
    config: &Config,
    file_info: &FileInfo,
) -> anyhow::Result<tokio::fs::File> {
    let file_path = file_info.get_absolute_path(config)?;
    tokio::fs::File::open(file_path)
        .await
        .map_err(anyhow::Error::from)
}

pub async fn open_file_for_writing(
    config: &Config,
    file_info: &FileInfo,
) -> anyhow::Result<tokio::fs::File> {
    let file_path = file_info.get_absolute_path(config)?;

    if let Some(parent) = file_path.parent() {
        if !parent.exists() {
            log::debug!("creating folders {:?}", parent);
            std::fs::create_dir_all(parent)?;
        }
    }

    OpenOptions::new()
        .write(true)
        .create(true)
        .read(true)
        .open(file_path)
        .await
        .map_err(anyhow::Error::from)
}
