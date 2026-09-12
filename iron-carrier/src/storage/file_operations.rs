use crate::{
    context::Context, ignored_files::IgnoredFilesCache, relative_path::RelativePathBuf,
    transaction_log::LogEntry,
};

/// Move the file in the storage, this  operation fails if FileInfoType is not Moved
pub async fn move_file(
    context: &Context,
    ignored_files_cache: &mut IgnoredFilesCache,
    storage: &str,
    src_path: &RelativePathBuf,
    dst_path: &RelativePathBuf,
    modified_at: u64,
) -> anyhow::Result<()> {
    let path_config = context.config.path(storage)?;

    let ignored_files = ignored_files_cache.get(context, path_config).await;
    let dst = dst_path.build_path();
    if ignored_files.is_ignored(&dst) {
        return Ok(());
    }
    let dest_path_abs = dst_path.absolute(path_config)?;

    let src = src_path.build_path();
    if ignored_files.is_ignored(&src) {
        return Ok(());
    }
    let src_path_abs = src_path.absolute(path_config)?;

    context.fs.rename(&src_path_abs, &dest_path_abs).await?;

    context
        .fs
        .set_metadata(&dest_path_abs, 0, modified_at)
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
            storage,
            &src,
            None,
            LogEntry::new(
                crate::transaction_log::EntryType::Delete,
                crate::transaction_log::EntryStatus::Done,
                modified_at,
            ),
        )
        .await?;

    context
        .transaction_log
        .append_log_entry(
            storage,
            &dst,
            Some(&src),
            LogEntry::new(
                crate::transaction_log::EntryType::Move,
                crate::transaction_log::EntryStatus::Done,
                modified_at,
            ),
        )
        .await
}

/// Delete the file in the storage
pub async fn delete_file(
    context: &crate::Context,
    ignored_files_cache: &mut IgnoredFilesCache,
    storage: &str,
    path: &RelativePathBuf,
    timestamp: u64,
) -> anyhow::Result<()> {
    let path_config = context.config.path(storage)?;

    let p = path.build_path();
    let ignored_files = ignored_files_cache.get(context, path_config).await;
    if ignored_files.is_ignored(&path.build_path()) {
        return Ok(());
    }

    let abs_p = path.absolute(path_config)?;
    if !context.fs.exists(&abs_p).await {
        log::warn!("{:?} path does not exist", path);
        anyhow::bail!("File not found");
    } else {
        context.fs.remove(&abs_p).await?;
    }

    log::info!("{:?} deleted", path);
    context
        .transaction_log
        .append_log_entry(
            storage,
            &p,
            None,
            LogEntry::new(
                crate::transaction_log::EntryType::Delete,
                crate::transaction_log::EntryStatus::Done,
                timestamp,
            ),
        )
        .await
}

// TODO: write tests
