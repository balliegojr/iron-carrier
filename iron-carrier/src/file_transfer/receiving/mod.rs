// mod receive_blocks;
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    io::SeekFrom,
    sync::Arc,
};

use tokio::{
    fs::File,
    io::{AsyncSeekExt, AsyncWriteExt},
    sync::{mpsc::Sender, Semaphore},
};
use tokio_stream::StreamExt;

use crate::{
    config::Config, hash_helper, ignored_files::IgnoredFilesCache, network::rpc::RPCMessage,
    node_id::NodeId, storage::FileInfo, SharedState, StateMachineError,
};

use super::{
    block_index::{self},
    events::{
        QueryRequiredBlocks, QueryTransferType, RequiredBlocks, TransferBlock, TransferComplete,
        TransferFilesCompleted, TransferResult, TransferType,
    },
    BlockIndexPosition, Transfer, TransferId,
};

pub async fn receive_files(
    shared_state: SharedState,
    mut wait_complete_signal_from: HashSet<NodeId>,
) -> crate::Result<()> {
    let mut ignored_files_cache = IgnoredFilesCache::default();
    let mut current_transfers: HashMap<TransferId, ActiveTransfer> = Default::default();

    let transfers_semaphore = Arc::new(Semaphore::new(
        shared_state.config.max_parallel_receiving.into(),
    ));

    let (add_current_transfer_tx, mut add_current_transfer_rx) = tokio::sync::mpsc::channel(1);

    let mut query_transfer_type = shared_state
        .rpc
        .consume_events::<QueryTransferType>()
        .await
        .fuse();

    let mut query_required_blocks = shared_state
        .rpc
        .consume_events::<QueryRequiredBlocks>()
        .await
        .fuse();

    let mut transfer_blocks = shared_state
        .rpc
        .consume_events::<TransferBlock>()
        .await
        .fuse();

    let mut transfer_complete = shared_state
        .rpc
        .consume_events::<TransferComplete>()
        .await
        .fuse();

    let mut transfer_files_completed = shared_state
        .rpc
        .consume_events::<TransferFilesCompleted>()
        .await
        .fuse();

    while !wait_complete_signal_from.is_empty() || !current_transfers.is_empty() {
        tokio::select! {
            query_transfer_type_req = query_transfer_type.next() => {
                let request = query_transfer_type_req.ok_or(StateMachineError::Abort)?;
                if let Err(err) = process_query_transfer_type(shared_state.config, &mut ignored_files_cache, &add_current_transfer_tx, transfers_semaphore.clone(), request).await {
                    log::error!("{err}")
                }
            }

            Some(current_transfer) = add_current_transfer_rx.recv() => {
                current_transfers.insert(current_transfer.transfer.transfer_id, current_transfer);
            }

            query_required_blocks_req = query_required_blocks.next() => {
                let request = query_required_blocks_req.ok_or(StateMachineError::Abort)?;
                if let Err(err) = process_query_required_blocks(&mut current_transfers, request).await {
                    log::error!("{err}")
                }
            }

            transfer_block_req = transfer_blocks.next() => {
                let request = transfer_block_req.ok_or(StateMachineError::Abort)?;
                if let Err(err) = process_transfer_block(&mut current_transfers, request).await {
                    log::error!("{err}")
                }
            }

            transfer_complete_req = transfer_complete.next() => {
                let request = transfer_complete_req.ok_or(StateMachineError::Abort)?;
                if let Err(err) = process_transfer_complete(shared_state.config, &mut current_transfers, request).await {
                    log::error!("{err}")
                }
            }

            transfer_files_req = transfer_files_completed.next() => {
                let request = transfer_files_req.ok_or(StateMachineError::Abort)?;
                wait_complete_signal_from.remove(&request.node_id());
                request.ack().await?;
            }
        }
    }

    Ok(())
}

struct ActiveTransfer {
    transfer: Transfer,
    handle: File,
    block_index: BTreeSet<BlockIndexPosition>,
}

async fn process_query_transfer_type(
    config: &'static Config,
    ignored_files_cache: &mut IgnoredFilesCache,
    add_current_transfer: &Sender<ActiveTransfer>,
    transfers_semaphore: Arc<Semaphore>,
    request: RPCMessage,
) -> crate::Result<()> {
    let data = request.data::<QueryTransferType>()?;
    let transfer_type = get_transfer_type(&data.file, config, ignored_files_cache).await?;

    if matches!(transfer_type, TransferType::NoTransfer) {
        return request.reply(transfer_type).await;
    }

    let add_current_transfer = add_current_transfer.clone();
    tokio::spawn(async move {
        let permit = transfers_semaphore.acquire_owned().await?; // FIXME: this will cause a
                                                                 // timeout
        let transfer = Transfer::new(data.file, permit)?;
        let handle =
            crate::storage::file_operations::open_file_for_writing(config, &transfer.file).await?;
        adjust_file_size(&transfer, &handle).await?;

        match transfer_type {
            TransferType::FullFile => {
                let block_index = calculate_expected_blocks_for_full_file(&transfer)?;
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

async fn get_transfer_type(
    remote_file: &FileInfo,
    config: &'static Config,
    ignored_files_cache: &mut IgnoredFilesCache,
) -> crate::Result<TransferType> {
    if let Some(storage_config) = config.storages.get(&remote_file.storage) {
        if ignored_files_cache
            .get(storage_config)
            .await
            .is_ignored(&remote_file.path)
        {
            return Ok(TransferType::NoTransfer);
        }
    }

    let file_path = remote_file.get_absolute_path(config)?;
    if !file_path.exists() {
        return Ok(TransferType::FullFile);
    }

    let local_file = remote_file.get_local_file_info(config)?;
    if hash_helper::calculate_file_hash(remote_file)
        != hash_helper::calculate_file_hash(&local_file)
    {
        Ok(TransferType::Partial)
    } else {
        Ok(TransferType::NoTransfer)
    }
}

async fn process_query_required_blocks(
    current_transfers: &mut HashMap<TransferId, ActiveTransfer>,
    request: RPCMessage,
) -> crate::Result<()> {
    let data = request.data::<QueryRequiredBlocks>()?;

    match current_transfers.get_mut(&data.transfer_id) {
        Some(active_transfer) => {
            let local_file_size = active_transfer.handle.metadata().await?.len();
            let file_size = active_transfer.transfer.file.file_size()?;

            let local_index = block_index::get_file_block_index(
                &mut active_transfer.handle,
                active_transfer.transfer.block_size,
                file_size,
                local_file_size,
            )
            .await?;

            let required_blocks = data.sender_block_index.generate_diff(local_index);
            active_transfer.block_index = required_blocks.clone();

            request.reply(RequiredBlocks { required_blocks }).await
        }
        None => {
            // FIXME: cancel request or send error message
            log::error!("Received request for invalid transfer");
            // FIXME: return proper error
            Ok(())
        }
    }
}

fn calculate_expected_blocks_for_full_file(
    transfer: &Transfer,
) -> crate::Result<BTreeSet<BlockIndexPosition>> {
    let file_size = transfer.file.file_size()?;
    let block_size = transfer.block_size;
    let expected_blocks = (file_size / block_size) + 1;

    Ok((0..expected_blocks).map(Into::into).collect())
}

async fn adjust_file_size(transfer: &Transfer, file_handle: &File) -> crate::Result<()> {
    let file_size = transfer.file.file_size()?;
    if file_size != file_handle.metadata().await?.len() {
        file_handle.set_len(file_size).await?;
        log::trace!("set {:?} len to {file_size}", transfer.file.path);
    }

    Ok(())
}

async fn process_transfer_block(
    current_transfers: &mut HashMap<TransferId, ActiveTransfer>,
    request: RPCMessage,
) -> crate::Result<()> {
    let data = request.data::<TransferBlock>()?;
    match current_transfers.get_mut(&data.transfer_id) {
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
            // FIXME: cancel request or send error message
            log::error!("Received request for invalid transfer");
            // FIXME: return proper error
            Ok(())
        }
    }
}

async fn process_transfer_complete(
    config: &'static Config,
    current_transfers: &mut HashMap<TransferId, ActiveTransfer>,
    request: RPCMessage,
) -> crate::Result<()> {
    let data = request.data::<TransferComplete>()?;
    match current_transfers.entry(data.transfer_id) {
        std::collections::hash_map::Entry::Occupied(mut entry) => {
            let active_transfer = entry.get_mut();
            if active_transfer.block_index.is_empty() {
                active_transfer.handle.sync_all().await?;
                crate::storage::fix_times_and_permissions(&active_transfer.transfer.file, config)?;

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
            // FIXME: cancel request or send error message
            log::error!("Received request for invalid transfer");
            // FIXME: return proper error
            Ok(())
        }
    }
}
