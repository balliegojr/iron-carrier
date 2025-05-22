use crate::{context::Context, ignored_files::IgnoredFilesCache, transaction_log::LogEntry};

use super::{FileInfo, FileInfoType};

/// Move the file in the storage, this  operation fails if FileInfoType is not Moved
pub async fn move_file(
    context: &Context,
    file: &FileInfo,
    ignored_files_cache: &mut IgnoredFilesCache,
) -> anyhow::Result<()> {
    let path_config = context
        .config
        .storages
        .get(&file.storage)
        .ok_or_else(|| anyhow::anyhow!("Storage {} not available", file.storage))?;

    let ignored_files = ignored_files_cache.get(context, path_config).await;
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

    context.fs.rename(&src_path_abs, &dest_path_abs).await?;

    let permissions = file.get_permissions();
    let modified = file.get_date();
    context
        .fs
        .set_metadata(&dest_path_abs, permissions, modified)
        .await?;

    log::info!("{src_path_abs:?} moved to {dest_path_abs:?}");

    // It is necessary to add two entries in the log, one for the new path as moved, one for the
    // old path as deleted
    //
    // If there is a future change to the file, before any synchronization, the new entry will
    // become a write one, hence the old file will need to be deleted. It is possible to improve
    // this flow by chaining a move with a send action
    context
        .transaction_log
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

    context
        .transaction_log
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
    context: &crate::Context,
    file_info: &FileInfo,
    ignored_files_cache: &mut IgnoredFilesCache,
) -> anyhow::Result<()> {
    if let Some(storage_config) = context.config.storages.get(&file_info.storage) {
        let ignored_files = ignored_files_cache.get(context, storage_config).await;
        if ignored_files.is_ignored(&file_info.path) {
            return Ok(());
        }
    }

    let path = file_info.get_absolute_path(context.config)?;
    if !context.fs.exists(&path).await {
        log::warn!("{:?} path does not exist", path);
        anyhow::bail!("File not found");
    } else {
        context.fs.remove(&path).await?;
    }

    log::info!("{:?} deleted", path);
    context
        .transaction_log
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
