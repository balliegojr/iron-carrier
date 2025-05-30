use std::{
    collections::{BTreeSet, HashMap, HashSet},
    io::SeekFrom,
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use tokio::{
    io::{AsyncSeekExt, AsyncWriteExt},
    sync::{OwnedSemaphorePermit, Semaphore, mpsc::Sender},
};
use tokio_stream::StreamExt;

use crate::{
    Context,
    constants::DEFAULT_NETWORK_TIMEOUT,
    fs::FileW,
    ignored_files::IgnoredFilesCache,
    message_types::MessageTypes,
    network::rpc::RPCMessage,
    node_id::NodeId,
    relative_path::RelativePathBuf,
    storage::storage_tree::{FileId, StorageFile},
};

use super::{
    BlockIndexPosition, Transfer,
    block_index::{self},
    events::{
        QueryRequiredBlocks, QueryTransferType, RequiredBlocks, TransferBlock, TransferComplete,
        TransferResult, TransferType,
    },
};

pub async fn receive_files(
    context: Context,
    mut wait_complete_signal_from: HashSet<NodeId>,
) -> anyhow::Result<()> {
    let mut ignored_files_cache = IgnoredFilesCache::default();
    let mut current_transfers: HashMap<FileId, ActiveTransfer> = Default::default();

    let transfers_semaphore = Arc::new(Semaphore::new(
        1.max(context.config.max_parallel_receiving.into()),
    ));

    let (add_current_transfer_tx, mut add_current_transfer_rx) = tokio::sync::mpsc::channel(1);

    let mut events = context
        .rpc
        .subscribe(&[
            MessageTypes::QueryTransferType,
            MessageTypes::QueryRequiredBlocks,
            MessageTypes::TransferBlock,
            MessageTypes::TransferComplete,
            MessageTypes::TransferFilesCompleted,
        ])
        .await?;

    while !wait_complete_signal_from.is_empty() || !current_transfers.is_empty() {
        tokio::select! {
            request = events.next() => {
                let Some(request) = request else { break; };
                match request.type_id() {
                    Ok(MessageTypes::QueryTransferType) => {
                        if let Err(err) = process_query_transfer_type(
                            &context,
                            &mut ignored_files_cache,
                            &add_current_transfer_tx,
                            transfers_semaphore.clone(),
                            request
                        ).await {
                            log::error!("{err}")
                        }
                    }
                    Ok(MessageTypes::QueryRequiredBlocks) => {
                        if let Err(err) = process_query_required_blocks(&mut current_transfers, request).await {
                            log::error!("{err}")
                        }
                    }
                    Ok(MessageTypes::TransferBlock) => {
                        if let Err(err) = process_transfer_block(&mut current_transfers, request).await {
                            log::error!("{err}")
                        }
                    }
                    Ok(MessageTypes::TransferComplete) => {
                        if let Err(err) = process_transfer_complete(&context, &mut current_transfers, request).await {
                            log::error!("{err}")
                        }
                    }
                    Ok(MessageTypes::TransferFilesCompleted) => {
                        wait_complete_signal_from.remove(&request.node_id());
                        request.ack().await?;
                    }
                    _ => {}
                }
            }
            Some(current_transfer) = add_current_transfer_rx.recv() => {
                current_transfers.insert(current_transfer.transfer.file.info.id(), current_transfer);
            }
        }
    }

    Ok(())
}

struct ActiveTransfer {
    transfer: Transfer,
    handle: Pin<Box<dyn FileW>>,
    block_index: BTreeSet<BlockIndexPosition>,
}

async fn process_query_transfer_type(
    context: &Context,
    ignored_files_cache: &mut IgnoredFilesCache,
    add_current_transfer: &Sender<ActiveTransfer>,
    transfers_semaphore: Arc<Semaphore>,
    request: RPCMessage,
) -> anyhow::Result<()> {
    let data = request.data::<QueryTransferType>()?;
    let transfer_type = get_transfer_type(
        context,
        ignored_files_cache,
        &data.file.storage,
        &data.file.path,
        data.file.info.size(),
        data.file.info.date(),
    )
    .await?;

    if matches!(transfer_type, TransferType::NoTransfer) {
        return request.reply(transfer_type).await;
    }

    let context = context.clone();

    let add_current_transfer = add_current_transfer.clone();
    tokio::spawn(async move {
        let permit = acquire_permit(transfers_semaphore, &request).await?;
        let transfer = Transfer::new(data.file, permit)?;
        let handle = context
            .fs
            .open_w(
                transfer
                    .file
                    .path
                    .absolute(context.config.path(&transfer.file.storage)?)?
                    .as_path(),
                transfer.file.info.size(),
            )
            .await?;

        match transfer_type {
            TransferType::FullFile => {
                let block_index = calculate_expected_blocks_for_full_file(&transfer);
                add_current_transfer
                    .send(ActiveTransfer {
                        transfer,
                        handle,
                        block_index,
                    })
                    .await?;
            }
            TransferType::Partial => {
                add_current_transfer
                    .send(ActiveTransfer {
                        transfer,
                        handle,
                        block_index: Default::default(),
                    })
                    .await?;
            }
            TransferType::NoTransfer => unreachable!(),
        }

        request.reply(transfer_type).await
    });

    Ok(())
}

/// Request a transfer permit and extend request timeout until permit is acquired
async fn acquire_permit(
    transfers_semaphore: Arc<Semaphore>,
    request: &RPCMessage,
) -> anyhow::Result<OwnedSemaphorePermit> {
    let (permit_tx, mut permit_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let permit = transfers_semaphore.acquire_owned().await;
        let _ = permit_tx.send(permit);
    });

    loop {
        let timeout = tokio::time::sleep(Duration::from_secs_f32(
            DEFAULT_NETWORK_TIMEOUT as f32 * 0.8,
        ));

        tokio::select! {
            biased;
            permit = &mut permit_rx => {
                return Ok(permit??);
            }
            _ = timeout => {
                request.ping().await?;
            }
        };
    }
}

async fn get_transfer_type(
    context: &Context,
    ignored_files_cache: &mut IgnoredFilesCache,
    storage: &str,
    path: &RelativePathBuf,
    size: u64,
    timestamp: u64,
) -> anyhow::Result<TransferType> {
    let path_config = context.config.path(storage)?;
    if ignored_files_cache
        .get(context, path_config)
        .await
        .is_ignored(&path.build_path())
    {
        return Ok(TransferType::NoTransfer);
    }

    let file_path = path.absolute(path_config)?;
    if !context.fs.exists(&file_path).await {
        return Ok(TransferType::FullFile);
    }

    let local_metadata = context.fs.metadata(&file_path).await?;
    if timestamp != local_metadata.modified_as_secs() || size != local_metadata.len() {
        Ok(TransferType::Partial)
    } else {
        Ok(TransferType::NoTransfer)
    }
}

async fn process_query_required_blocks(
    current_transfers: &mut HashMap<FileId, ActiveTransfer>,
    request: RPCMessage,
) -> anyhow::Result<()> {
    let data = request.data::<QueryRequiredBlocks>()?;

    match current_transfers.get_mut(&data.file_id) {
        Some(active_transfer) => {
            let file_size = active_transfer.transfer.file.info.size();
            // local file size will be the same as the remote
            // file size because we set the file length when opening the file for writing
            let local_file_size = file_size;

            let local_index = block_index::get_file_block_index(
                &mut active_transfer.handle,
                active_transfer.transfer.block_size,
                file_size,
                local_file_size,
            )
            .await?;

            let required_blocks = data.sender_block_index.generate_diff(local_index)?;
            active_transfer.block_index = required_blocks.clone();

            request.reply(RequiredBlocks { required_blocks }).await
        }
        None => {
            let _ = request.cancel().await;
            anyhow::bail!("Received request for invalid transfer");
        }
    }
}

fn calculate_expected_blocks_for_full_file(transfer: &Transfer) -> BTreeSet<BlockIndexPosition> {
    let file_size = transfer.file.info.size();
    let block_size = transfer.block_size;
    let expected_blocks = (file_size / block_size) + 1;

    (0..expected_blocks).map(Into::into).collect()
}

async fn process_transfer_block(
    current_transfers: &mut HashMap<FileId, ActiveTransfer>,
    request: RPCMessage,
) -> anyhow::Result<()> {
    let data = request.data::<TransferBlock>()?;
    match current_transfers.get_mut(&data.file_id) {
        Some(active_transfer) => {
            let position = data
                .block_index
                .get_position(active_transfer.transfer.block_size);

            if active_transfer
                .handle
                .seek(SeekFrom::Start(position))
                .await?
                == position
            {
                active_transfer.handle.write_all(data.block).await?;
            }

            active_transfer.block_index.remove(&data.block_index);
            request.ack().await
        }
        None => {
            let _ = request.cancel().await;
            anyhow::bail!("Received request for invalid transfer");
        }
    }
}

async fn process_transfer_complete(
    context: &Context,
    current_transfers: &mut HashMap<FileId, ActiveTransfer>,
    request: RPCMessage,
) -> anyhow::Result<()> {
    let data = request.data::<TransferComplete>()?;
    match current_transfers.entry(data.file_id) {
        std::collections::hash_map::Entry::Occupied(mut entry) => {
            let active_transfer = entry.get_mut();
            if active_transfer.block_index.is_empty() {
                active_transfer.handle.as_ref().sync_all().await?;

                let modified = active_transfer.transfer.file.info.date();
                let permissions = active_transfer.transfer.file.info.permissions();
                let path = active_transfer.transfer.file.path.absolute(
                    context
                        .config
                        .path(&active_transfer.transfer.file.storage)?,
                )?;

                context
                    .fs
                    .set_metadata(&path, permissions, modified)
                    .await?;

                entry.remove();
                request.reply(TransferResult::Success).await
            } else {
                request
                    .reply(TransferResult::Failed {
                        required_blocks: active_transfer.block_index.clone(),
                    })
                    .await
            }
        }
        std::collections::hash_map::Entry::Vacant(_) => {
            let _ = request.cancel().await;
            anyhow::bail!("Received request for invalid transfer");
        }
    }
}
