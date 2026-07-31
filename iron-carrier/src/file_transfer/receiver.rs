use std::{collections::BTreeSet, io::SeekFrom, pin::Pin, sync::Arc};

use tokio::{
    io::{AsyncSeekExt, AsyncWriteExt},
    sync::{Mutex, Semaphore},
};
use tokio_stream::StreamExt;

use crate::{
    Context,
    file_transfer::SyncFile,
    fs::FileW,
    ignored_files::IgnoredFilesCache,
    network::{Subscription, rpc::RPCMessage},
    protocol::MessageTypes,
    states::sync::events::ReceiveFile,
    storage::storage_tree::{FileId, StorageFile},
};

use super::{
    BlockIndexPosition,
    block_index::{self},
    events::{QueryRequiredBlocks, RequiredBlocks, TransferBlock, TransferResult, TransferType},
};

type FileHandle = Pin<Box<dyn FileW>>;

pub async fn receive_file(
    context: Context,
    ignored_files_cache: Arc<Mutex<IgnoredFilesCache>>,
    request: RPCMessage,
    transfers_semaphore: Arc<Semaphore>,
) -> anyhow::Result<()> {
    let _permit = super::acquire_permit(transfers_semaphore, &request).await;
    let ReceiveFile { file } = request.data()?;

    let context = context.subprocess(FileId::new(&file.path.as_path()).into());
    let events = context
        .rpc
        .subscribe([
            MessageTypes::QueryTransferType,
            MessageTypes::QueryRequiredBlocks,
            MessageTypes::TransferBlock,
            MessageTypes::TransferComplete,
            // MessageTypes::TransferFilesCompleted,
        ])
        .await?;

    let transfer_type = get_transfer_type(&context, ignored_files_cache, &file).await?;

    if matches!(transfer_type, TransferType::NoTransfer) {
        abort_no_transfer(events).await?;
    } else {
        let processing = process_transfer(context, events, file, transfer_type);
        tokio::pin!(processing);

        loop {
            let timeout = super::get_timeout();

            tokio::select! {
                biased;
                result = &mut processing => {
                    if let Err(err) =  result {
                        log::error!("there was an error receiving the file: {err}")
                    }
                    break;
                },
                _ = timeout => {
                    request.ping().await?;
                }
            }
        }
    }

    request.ack().await
}

async fn abort_no_transfer(mut subscription: Subscription) -> anyhow::Result<()> {
    let Some(event) = subscription.next().await else {
        return Ok(());
    };

    match event.message_type()? {
        MessageTypes::QueryTransferType => {
            event.reply(TransferType::NoTransfer).await?;
        }
        ev => {
            log::warn!("Received unexpected event {:?}", ev);
        }
    }

    Ok(())
}

async fn process_transfer(
    context: Context,
    mut subscription: Subscription,
    file: SyncFile,
    transfer_type: TransferType,
) -> anyhow::Result<()> {
    let block_size = super::block_index::get_block_size(file.info.size());
    let mut handle = get_handle_and_prepare_log(&context, &file).await?;

    let mut block_index = match transfer_type {
        TransferType::FullFile => calculate_expected_blocks_for_full_file(&file),
        TransferType::Partial => Default::default(),
        _ => unreachable!(),
    };

    while let Some(request) = subscription.next().await {
        match request.message_type() {
            Ok(MessageTypes::QueryTransferType) => {
                request.reply(transfer_type).await?;
            }
            Ok(MessageTypes::QueryRequiredBlocks) => {
                block_index =
                    process_query_required_blocks(&file, &mut handle, block_size, request).await?;
            }
            Ok(MessageTypes::TransferBlock) => {
                process_transfer_block(&mut handle, &mut block_index, block_size, request).await?;
            }
            Ok(MessageTypes::TransferComplete) => {
                if let Err(err) =
                    process_transfer_complete(&context, &file, &mut handle, &block_index, request)
                        .await
                {
                    log::error!("{err}")
                }
            }
            _ => {}
        }
    }

    Ok(())
}

async fn get_handle_and_prepare_log(
    context: &Context,
    file: &SyncFile,
) -> anyhow::Result<FileHandle> {
    context
        .transaction_log
        .mark_write_pending(&file.storage, &file.path.build_path(), file.info.date())
        .await?;

    context
        .fs
        .open_w(
            file.path
                .absolute(context.config.path(&file.storage)?)?
                .as_path(),
            file.info.size(),
        )
        .await
        .map_err(anyhow::Error::from)
}

async fn get_transfer_type(
    context: &Context,
    ignored_files_cache: Arc<Mutex<IgnoredFilesCache>>,
    file: &SyncFile,
) -> anyhow::Result<TransferType> {
    if is_ignored(context, file, ignored_files_cache).await? {
        return Ok(TransferType::NoTransfer);
    }

    let path_config = context.config.path(&file.storage)?;
    let file_path = file.path.absolute(path_config)?;
    if !context.fs.exists(&file_path).await {
        return Ok(TransferType::FullFile);
    }

    let local_metadata = context.fs.metadata(&file_path).await?;
    if file.info.date() != local_metadata.modified_as_secs()
        || file.info.size() != local_metadata.len()
    {
        Ok(TransferType::Partial)
    } else {
        Ok(TransferType::NoTransfer)
    }
}

async fn is_ignored(
    context: &Context,
    file: &SyncFile,
    ignored_files_cache: Arc<Mutex<IgnoredFilesCache>>,
) -> anyhow::Result<bool> {
    let path_config = context.config.path(&file.storage)?;
    let mut guard = ignored_files_cache.lock().await;

    Ok(guard
        .get(context, path_config)
        .await
        .is_ignored(&file.path.build_path()))
}

async fn process_query_required_blocks(
    file: &SyncFile,
    handle: &mut FileHandle,
    block_size: u64,
    request: RPCMessage,
) -> anyhow::Result<BTreeSet<BlockIndexPosition>> {
    let data = request.data::<QueryRequiredBlocks>()?;

    let file_size = file.info.size();
    // local file size will be the same as the remote
    // file size because we set the file length when opening the file for writing
    let local_file_size = file_size;

    let local_index =
        block_index::get_file_block_index(handle, block_size, file_size, local_file_size).await?;

    let required_blocks = data.sender_block_index.generate_diff(local_index)?;

    request
        .reply(RequiredBlocks {
            required_blocks: required_blocks.clone(),
        })
        .await?;
    Ok(required_blocks)
}

async fn process_transfer_block(
    handle: &mut FileHandle,
    block_index: &mut BTreeSet<BlockIndexPosition>,
    block_size: u64,

    request: RPCMessage,
) -> anyhow::Result<()> {
    let data = request.data::<TransferBlock>()?;
    let position = data.block_index.get_position(block_size);

    if handle.seek(SeekFrom::Start(position)).await? == position {
        handle.write_all(data.block).await?;
    }

    block_index.remove(&data.block_index);
    request.ack().await
}

async fn process_transfer_complete(
    context: &Context,
    file: &SyncFile,
    handle: &mut FileHandle,
    block_index: &BTreeSet<BlockIndexPosition>,
    request: RPCMessage,
) -> anyhow::Result<()> {
    if block_index.is_empty() {
        handle.as_ref().sync_all().await?;

        let modified = file.info.date();
        let permissions = file.info.permissions();
        let storage = file.storage.clone();
        let rel_path = file.path.build_path();
        let path = file.path.absolute(context.config.path(&storage)?)?;

        context
            .fs
            .set_metadata(&path, permissions, modified)
            .await?;
        context
            .transaction_log
            .mark_write_done(&storage, &rel_path, modified)
            .await?;

        request.reply(TransferResult::Success).await
    } else {
        request
            .reply(TransferResult::Failed {
                required_blocks: block_index.clone(),
            })
            .await
    }
}

fn calculate_expected_blocks_for_full_file(file: &SyncFile) -> BTreeSet<BlockIndexPosition> {
    let block_size = super::block_index::get_block_size(file.info.size());
    let file_size = file.info.size();
    let expected_blocks = file_size.div_ceil(block_size);

    (0..expected_blocks).map(Into::into).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_transfer::SyncFile;
    use crate::{
        fs::MetadataB, relative_path::RelativePathBuf, storage::storage_tree::ExistingFileInfo,
    };

    fn make_file(file_size: u64) -> SyncFile {
        let path = RelativePathBuf::from("test.txt");
        let metadata = MetadataB::new()
            .len(file_size)
            .permissions(0o644)
            .modified(1)
            .build();
        let info = ExistingFileInfo::new(path.as_path(), &metadata);
        SyncFile {
            storage: "a".to_string(),
            path,
            info,
        }
    }

    #[test]
    fn expected_blocks_exact_multiple() {
        // file_size is an exact multiple of block_size: must produce exactly N blocks, not N+1
        let transfer = make_file(2048 * 5); // 10240 bytes, block_size will be 2048
        let blocks = calculate_expected_blocks_for_full_file(&transfer);
        assert_eq!(
            blocks.len(),
            5,
            "exact multiple of block_size must yield N blocks, not N+1"
        );
    }

    #[test]
    fn expected_blocks_non_multiple() {
        // file_size is NOT an exact multiple: needs one extra block for the remainder
        let transfer = make_file(2048 * 5 + 1); // one byte over, still block_size=2048
        let blocks = calculate_expected_blocks_for_full_file(&transfer);
        assert_eq!(
            blocks.len(),
            6,
            "remainder bytes must yield a final partial block"
        );
    }

    #[test]
    fn expected_blocks_single_block() {
        // file smaller than block_size: exactly one block
        let transfer = make_file(1024);
        let blocks = calculate_expected_blocks_for_full_file(&transfer);
        assert_eq!(blocks.len(), 1);
    }
}
