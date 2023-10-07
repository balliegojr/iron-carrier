use std::{
    collections::{BTreeSet, HashMap, HashSet},
    io::SeekFrom,
    sync::Arc,
    time::Duration,
};

use tokio::{
    fs::File,
    io::{AsyncSeekExt, AsyncWriteExt},
    sync::{mpsc::Sender, OwnedSemaphorePermit, Semaphore},
};
use tokio_stream::StreamExt;

use crate::{
    config::Config, constants::DEFAULT_NETWORK_TIMEOUT, hash_helper, hash_type_id::HashTypeId,
    ignored_files::IgnoredFilesCache, network::rpc::RPCMessage, node_id::NodeId, storage::FileInfo,
    SharedState,
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
) -> anyhow::Result<()> {
    let mut ignored_files_cache = IgnoredFilesCache::default();
    let mut current_transfers: HashMap<TransferId, ActiveTransfer> = Default::default();

    let transfers_semaphore = Arc::new(Semaphore::new(
        shared_state.config.max_parallel_receiving.into(),
    ));

    let (add_current_transfer_tx, mut add_current_transfer_rx) = tokio::sync::mpsc::channel(1);

    let mut events = shared_state
        .rpc
        .subscribe_many(vec![
            QueryTransferType::ID,
            QueryRequiredBlocks::ID,
            TransferBlock::ID,
            TransferComplete::ID,
            TransferFilesCompleted::ID,
        ])
        .await?;

    while !wait_complete_signal_from.is_empty() || !current_transfers.is_empty() {
        tokio::select! {
            request = events.next() => {
                let Some(request) = request else { break; };
                match request.type_id() {
                    QueryTransferType::ID => {
                        if let Err(err) = process_query_transfer_type(
                            shared_state.config,
                            &mut ignored_files_cache,
                            &add_current_transfer_tx,
                            transfers_semaphore.clone(),
                            request
                        ).await {
                            log::error!("{err}")
                        }
                    }
                    QueryRequiredBlocks::ID => {
                        if let Err(err) = process_query_required_blocks(&mut current_transfers, request).await {
                            log::error!("{err}")
                        }
                    }
                    TransferBlock::ID => {
                        if let Err(err) = process_transfer_block(&mut current_transfers, request).await {
                            log::error!("{err}")
                        }
                    }
                    TransferComplete::ID => {
                        if let Err(err) = process_transfer_complete(shared_state.config, &mut current_transfers, request).await {
                            log::error!("{err}")
                        }
                    }
                    TransferFilesCompleted::ID => {
                        wait_complete_signal_from.remove(&request.node_id());
                        request.ack().await?;
                    }
                    _ => unreachable!()
                }
            }
            Some(current_transfer) = add_current_transfer_rx.recv() => {
                current_transfers.insert(current_transfer.transfer.transfer_id, current_transfer);
            }
        }
    }

    events.free().await;

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
) -> anyhow::Result<()> {
    let data = request.data::<QueryTransferType>()?;
    let transfer_type = get_transfer_type(&data.file, config, ignored_files_cache).await?;

    if matches!(transfer_type, TransferType::NoTransfer) {
        return request.reply(transfer_type).await;
    }

    let add_current_transfer = add_current_transfer.clone();
    tokio::spawn(async move {
        let permit = acquire_permit(transfers_semaphore, &request).await?;
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
    remote_file: &FileInfo,
    config: &'static Config,
    ignored_files_cache: &mut IgnoredFilesCache,
) -> anyhow::Result<TransferType> {
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
) -> anyhow::Result<()> {
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
) -> anyhow::Result<BTreeSet<BlockIndexPosition>> {
    let file_size = transfer.file.file_size()?;
    let block_size = transfer.block_size;
    let expected_blocks = (file_size / block_size) + 1;

    Ok((0..expected_blocks).map(Into::into).collect())
}

async fn adjust_file_size(transfer: &Transfer, file_handle: &File) -> anyhow::Result<()> {
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
) -> anyhow::Result<()> {
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
) -> anyhow::Result<()> {
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
