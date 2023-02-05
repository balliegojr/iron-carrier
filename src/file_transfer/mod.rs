use std::{collections::HashMap, sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};
use tokio::{
    fs::File,
    io::AsyncReadExt,
    sync::{
        mpsc::{Receiver, Sender},
        Semaphore,
    },
};
use tokio_stream::StreamExt;

use crate::{hash_helper, network_events::NetworkEvents, storage::FileInfo, SharedState};

mod file_receiver;
mod file_sender;

pub use file_receiver::FileReceiver;
pub use file_sender::FileSender;

type BlockIndex = u64;
type BlockHash = u64;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum FileTransfer {
    QueryTransferType {
        file: FileInfo,
        block_size: u64,
    },
    ReplyTransferType {
        transfer_type: Option<TransferType>,
    },

    QueryRequiredBlocks {
        sender_block_index: Vec<BlockHash>,
    },
    ReplyRequiredBlocks {
        required_blocks: Vec<BlockIndex>,
    },

    TransferBlock {
        block_index: BlockIndex,
        block: Arc<Vec<u8>>,
    },
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone, PartialEq, Eq)]
pub enum TransferType {
    Everything,
    PartialTransfer,
}

pub async fn process_transfer_events(
    shared_state: SharedState,
    mut file_transfer_control_events: Receiver<(FileInfo, Vec<u64>)>,
) -> crate::Result<()> {
    // TODO: handle peer disconnection events

    // NOTE: there must be a better way of stopping this task other than a timeout...
    let mut events = Box::pin(
        shared_state
            .connection_handler
            .events_stream()
            .await
            .filter_map(|(peer_id, ev)| match ev {
                NetworkEvents::FileTransfer(transfer_id, file_transfer) => {
                    Some((transfer_id, peer_id, file_transfer))
                }
                _ => None,
            })
            .timeout(Duration::from_secs(1)),
    );

    // TODO: implement max parallel transfer
    // let max_transfer = Arc::new(tokio::sync::Semaphore::new(
    //     shared_state.config.max_parallel_transfers as usize,
    // ));

    let mut active_transfers = HashMap::new();
    loop {
        tokio::select! {
            Some((file, nodes)) = file_transfer_control_events.recv()=> {
                let (transfer_id, sender) = send_file(shared_state, file, nodes).await?;
                active_transfers.insert(transfer_id, sender);
            }
            Some(Ok((transfer_id, node_id, event))) = events.next() => {
                match event {
                    FileTransfer::QueryTransferType { file, block_size } => {
                        if let Some(transfer) = query_transfer_type(shared_state, file, block_size, transfer_id, node_id).await? {
                            let sender = receive_file(
                                shared_state,
                                transfer,
                                transfer_id,
                            )
                            .await?;

                            active_transfers.insert(transfer_id, sender);
                        }
                    }
                    event => {
                        if let Some(sender) = active_transfers.get(&transfer_id) {
                            sender.send((node_id, event)).await?;
                        }
                    }
                }
            }
            else => {
                break;
            }
        }

        active_transfers.drain_filter(|_, channel| channel.is_closed());
    }

    Ok(())
}

async fn query_transfer_type(
    shared_state: SharedState,
    file: FileInfo,
    block_size: u64,
    transfer_id: u64,
    node_id: u64,
) -> crate::Result<Option<FileReceiver>> {
    let transfer_type = file_receiver::get_transfer_type(&file, shared_state.config).await?;
    shared_state
        .connection_handler
        .send_to(
            NetworkEvents::FileTransfer(
                transfer_id,
                FileTransfer::ReplyTransferType { transfer_type },
            ),
            node_id,
        )
        .await?;

    match transfer_type {
        Some(_) => Ok(Some(
            FileReceiver::new(
                shared_state.config,
                shared_state.transaction_log,
                file,
                block_size,
            )
            .await?,
        )),
        None => Ok(None),
    }
}

async fn receive_file(
    shared_state: SharedState,
    transfer: FileReceiver,
    transfer_id: u64,
) -> crate::Result<Sender<(u64, FileTransfer)>> {
    let (tx, rx) = tokio::sync::mpsc::channel(1);

    async fn process_event(
        shared_state: SharedState,
        mut transfer: FileReceiver,
        transfer_id: u64,
        mut rx: Receiver<(u64, FileTransfer)>,
    ) -> crate::Result<()> {
        while let Some((node_id, event)) = rx.recv().await {
            match event {
                FileTransfer::QueryRequiredBlocks { sender_block_index } => {
                    match transfer.get_required_block_index(sender_block_index).await {
                        Ok(required_blocks) => {
                            // Shrinking a file can result into a sync without blocks to
                            // transfer
                            if required_blocks.is_empty() {
                                transfer
                                    .finish(shared_state.config, shared_state.transaction_log)
                                    .await?;
                                break;
                            }

                            shared_state
                                .connection_handler
                                .send_to(
                                    NetworkEvents::FileTransfer(
                                        transfer_id,
                                        FileTransfer::ReplyRequiredBlocks { required_blocks },
                                    ),
                                    node_id,
                                )
                                .await?
                        }
                        Err(err) => {
                            log::error!("Failed to get required blocks {err}");
                            // TODO: send abort transfer event
                        }
                    }
                }
                FileTransfer::TransferBlock { block_index, block } => {
                    match transfer.write_block(block_index, &block).await {
                        Ok(true) => {
                            transfer
                                .finish(shared_state.config, shared_state.transaction_log)
                                .await?;
                            break;
                        }
                        Ok(false) => {}
                        Err(err) => {
                            log::error!("Failed to write file block {err}");
                            // TODO: send abort transfer event
                        }
                    }
                }
                _ => {}
            }
        }

        Ok(())
    }

    tokio::spawn(async move {
        if let Err(err) = process_event(shared_state, transfer, transfer_id, rx).await {
            log::error!("{err}");
        }
    });

    Ok(tx)
}

async fn send_file(
    shared_state: SharedState,
    file: FileInfo,
    nodes: Vec<u64>,
    // transfer_control: Arc<Semaphore>,
) -> crate::Result<(u64, Sender<(u64, FileTransfer)>)> {
    log::trace!("Will send {:?} to {nodes:?}", &file.path);
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);

    let mut transfer = FileSender::new(
        file,
        nodes,
        shared_state.config,
        // transfer_control.acquire_owned().await?,
    )
    .await?;
    let transfer_id = transfer.transfer_id();
    transfer
        .query_transfer_type(shared_state.connection_handler)
        .await?;
    tokio::spawn(async move {
        while let Some((node_id, event)) = rx.recv().await {
            match event {
                FileTransfer::ReplyTransferType { transfer_type } => {
                    transfer
                        .set_transfer_type(shared_state.connection_handler, node_id, transfer_type)
                        .await
                        .expect("burrr");

                    if !transfer.has_participant_nodes() {
                        break;
                    } else if !transfer.pending_information() {
                        if let Err(err) = transfer
                            .transfer_blocks(shared_state.connection_handler)
                            .await
                        {
                            log::error!("Failed to send file {err}");
                        }

                        break;
                    }
                }
                FileTransfer::ReplyRequiredBlocks { required_blocks } => {
                    transfer.set_required_blocks(node_id, required_blocks);
                    if transfer.has_participant_nodes() && !transfer.pending_information() {
                        if let Err(err) = transfer
                            .transfer_blocks(shared_state.connection_handler)
                            .await
                        {
                            log::error!("Failed to send file {err}");
                        }
                        break;
                    }
                }
                _ => {}
            }
        }
    });

    Ok((transfer_id, tx))
}

const MIN_BLOCK_SIZE: u64 = 1024 * 128;
const MAX_BLOCK_SIZE: u64 = 1024 * 1024 * 16;

fn get_block_size(file_size: u64) -> u64 {
    let mut block_size = MIN_BLOCK_SIZE;
    while block_size < MAX_BLOCK_SIZE && file_size / block_size > 2000 {
        block_size *= 2;
    }

    block_size
}

async fn get_file_block_index(
    file: &mut File,
    block_size: u64,
    file_size: u64,
) -> crate::Result<Vec<u64>> {
    if file_size == 0 {
        return Ok(Vec::new());
    }

    let total_blocks = (file_size / block_size) + 1;
    let mut block_index = Vec::with_capacity(total_blocks as usize);

    let mut buf = vec![0u8; block_size as usize];
    let mut position = 0u64;

    while position < file_size {
        let to_read = std::cmp::min(file_size - position, block_size) as usize;
        let actually_read = file.read_exact(&mut buf[..to_read]).await?;
        block_index.push(hash_helper::calculate_checksum(&buf[..actually_read]));
        position += actually_read as u64;
    }

    Ok(block_index)
}
