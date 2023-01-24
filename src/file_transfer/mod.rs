use std::{collections::HashMap, sync::Arc};

use serde::{Deserialize, Serialize};
use tokio::{fs::File, io::AsyncReadExt, sync::mpsc::Receiver};
use tokio_stream::StreamExt;

use crate::{
    config::Config, hash_helper, network::ConnectionHandler, network_events::NetworkEvents,
    storage::FileInfo,
};

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
        transfer_id: u64,
        block_size: u64,
    },
    ReplyTransferType {
        transfer_id: u64,
        transfer_type: Option<TransferType>,
    },

    QueryRequiredBlocks {
        transfer_id: u64,
        sender_block_index: Vec<BlockHash>,
    },
    ReplyRequiredBlocks {
        transfer_id: u64,
        required_blocks: Vec<BlockIndex>,
    },

    TransferBlock {
        transfer_id: u64,
        block_index: BlockIndex,
        block: Arc<Vec<u8>>,
    },
}

// TODO: get rid of this part of the flow, it is causing more problems than solving
#[derive(Debug, Serialize, Deserialize, Copy, Clone, PartialEq, Eq)]
pub enum TransferType {
    Everything,
    PartialTransfer,
}

pub async fn process_transfer_events(
    connection_handler: &'static ConnectionHandler<NetworkEvents>,
    config: &'static Config,
    mut send_file_events: Receiver<(FileInfo, Vec<u64>)>,
) -> crate::Result<()> {
    let mut events =
        connection_handler
            .events_stream()
            .await
            .filter_map(|(peer_id, ev)| match ev {
                NetworkEvents::FileTransfer(file_transfer) => Some((peer_id, file_transfer)),
                _ => None,
            });

    // TODO: implement multiple sending
    // TODO: implement max parallel transfer control
    // TODO: handle peer disconnection events

    let mut receiving = HashMap::new();
    let mut sending: Option<FileSender> = None;

    let mut channel_closed = false;

    while !channel_closed || !receiving.is_empty() || sending.is_some() {
        tokio::select! {
            event = send_file_events.recv(), if sending.is_none() => {
                match event {
                    Some(( file, nodes)) => {
                        log::trace!("Will send {:?} to {nodes:?}", &file.path);
                        let file_send = FileSender::new(file, nodes, config).await?;
                        file_send.query_transfer_type(connection_handler).await?;
                        sending = Some(file_send);
                    }
                    None => channel_closed = true
                }
            }
            Some((node_id, event)) = events.next() => {
                if let Err(err) = process_file_transfer_event(connection_handler, config, node_id, event, &mut sending, &mut receiving).await {
                    log::error!("Error processing file transfer {err}");
                }
            }
        }
    }

    Ok(())
}

async fn process_file_transfer_event(
    connection_handler: &'static ConnectionHandler<NetworkEvents>,
    config: &'static Config,
    node_id: u64,
    event: FileTransfer,
    sending: &mut Option<FileSender>,
    receiving: &mut HashMap<u64, FileReceiver>,
) -> crate::Result<()> {
    match event {
        FileTransfer::QueryTransferType {
            file,
            transfer_id,
            block_size,
        } => {
            let transfer_type = file_receiver::get_transfer_type(&file, config).await?;
            connection_handler
                .send_to(
                    FileTransfer::ReplyTransferType {
                        transfer_id,
                        transfer_type,
                    }
                    .into(),
                    node_id,
                )
                .await?;

            if let Some(transfer_type) = transfer_type {
                let mut receiver = FileReceiver::new(file, block_size, config).await?;

                if transfer_type == TransferType::Everything {
                    receiver.set_full_transfer();
                }

                receiving.insert(transfer_id, receiver);
            }
        }
        FileTransfer::ReplyTransferType {
            transfer_id: _, // NOTE: will be necessary when supporting parallel sending
            transfer_type,
        } => {
            if let Some(transfer) = sending.as_mut() {
                transfer
                    .set_transfer_type(connection_handler, node_id, transfer_type)
                    .await?;

                if !transfer.has_participant_nodes() {
                    *sending = None;
                } else if !transfer.pending_information() {
                    let transfer = sending.take().unwrap();
                    transfer.transfer_blocks(connection_handler).await?;

                    // NOTE: send on a different task?
                    // tokio::spawn(async {
                    //     if let Err(err) = transfer.transfer_blocks(connection_handler).await {
                    //         log::error!("Failed to send file {err}");
                    //     }
                    // });
                }
            };
        }
        FileTransfer::QueryRequiredBlocks {
            transfer_id,
            sender_block_index,
        } => match receiving.entry(transfer_id) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                match entry
                    .get_mut()
                    .get_required_block_index(sender_block_index)
                    .await
                {
                    Ok(required_blocks) => {
                        // Shrinking a file can result into a sync without blocks to
                        // transfer
                        if required_blocks.is_empty() {
                            entry.remove().finish(config).await?;
                        }

                        connection_handler
                            .send_to(
                                FileTransfer::ReplyRequiredBlocks {
                                    transfer_id,
                                    required_blocks,
                                }
                                .into(),
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
            std::collections::hash_map::Entry::Vacant(_) => {
                log::error!("Received event for unknown transfer");
                // TODO: send abort transfer event
            }
        },
        FileTransfer::ReplyRequiredBlocks {
            transfer_id: _,
            required_blocks,
        } => {
            if let Some(transfer) = sending.as_mut() {
                transfer.set_required_blocks(node_id, required_blocks);
                if transfer.has_participant_nodes() && !transfer.pending_information() {
                    let transfer = sending.take().unwrap();
                    transfer.transfer_blocks(connection_handler).await?;

                    // NOTE: send on a different task?
                    // tokio::spawn(async {
                    //     if let Err(err) = transfer.transfer_blocks(connection_handler).await {
                    //         log::error!("Failed to send file {err}");
                    //     }
                    // });
                }
            };
        }
        FileTransfer::TransferBlock {
            transfer_id,
            block_index,
            block,
        } => match receiving.entry(transfer_id) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                let transfer = entry.get_mut();
                match transfer.write_block(block_index, &block).await {
                    Ok(true) => {
                        entry.remove().finish(config).await?;
                    }
                    Ok(false) => {}
                    Err(err) => {
                        // NOTE: remove the entry?
                        // entry.remove_entry();
                        log::error!("Failed to write file block {err}");
                        // TODO: send abort transfer event
                    }
                }
            }
            std::collections::hash_map::Entry::Vacant(_) => {
                log::error!("Received event for unknown transfer");
                // TODO: send abort transfer event
            }
        },
    }

    Ok(())
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
