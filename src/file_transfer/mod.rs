use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use serde::{Deserialize, Serialize};
use tokio::{
    fs::File,
    io::AsyncReadExt,
    sync::mpsc::{Receiver, Sender},
};

use crate::{
    hash_helper,
    network_events::{NetworkEvents, Synchronization},
    state_machine::Step,
    storage::FileInfo,
    SharedState,
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
        block_size: u64,
    },
    ReplyTransferType {
        transfer_type: TransferType,
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
    TransferComplete,
    SendFileTo {
        file: FileInfo,
        nodes: Vec<u64>,
    },
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone, PartialEq, Eq)]
pub enum TransferType {
    FullFile,
    Partial,
    NoTransfer,
}

#[derive(Debug)]
pub struct TransferFiles {
    files_to_send: Vec<(FileInfo, HashSet<u64>)>,
    peers_with_transfers: HashSet<u64>,
}

impl TransferFiles {
    pub fn new(
        files_to_send: Vec<(FileInfo, HashSet<u64>)>,
        peers_with_transfers: HashSet<u64>,
    ) -> Self {
        Self {
            files_to_send,
            peers_with_transfers,
        }
    }

    async fn start_new_transfers(
        &mut self,
        shared_state: &SharedState,
        active_transfers: &mut HashMap<u64, Sender<(u64, FileTransfer)>>,
        when_done_tx: &Sender<u64>,
    ) -> crate::Result<HashSet<u64>> {
        let mut sent_files_to = HashSet::default();

        while !self.files_to_send.is_empty()
            && active_transfers.len() < shared_state.config.max_parallel_transfers as usize
        {
            let (file, nodes) = self.files_to_send.pop().unwrap();
            sent_files_to.extend(nodes.iter().copied());

            let (transfer_id, sender) =
                send_file(*shared_state, file, nodes, when_done_tx.clone()).await?;
            log::debug!("adding active transfer {transfer_id}");
            active_transfers.insert(transfer_id, sender);
        }

        Ok(sent_files_to)
    }
}

impl Step for TransferFiles {
    type Output = ();

    async fn execute(mut self, shared_state: &SharedState) -> crate::Result<Self::Output> {
        let (when_done_tx, mut when_done) = tokio::sync::mpsc::channel(1);
        let mut active_transfers = HashMap::new();
        let mut peers_to_wait = self.peers_with_transfers.clone();

        log::debug!("Starting file transfer");

        self.start_new_transfers(shared_state, &mut active_transfers, &when_done_tx)
            .await?;

        while !peers_to_wait.is_empty()
            || !active_transfers.is_empty()
            || !self.files_to_send.is_empty()
        {
            self.start_new_transfers(shared_state, &mut active_transfers, &when_done_tx)
                .await?;

            tokio::select! {
                Some((peer_id, event)) = shared_state.connection_handler.next_event() => {
                    match event {
                        // TODO: remove active/send transfers related to the disconnected peer
                        NetworkEvents::Disconnected |
                        NetworkEvents::Synchronization(Synchronization::DoneTransferingFiles) => {
                            log::debug!("removing {peer_id} from transfers");
                            peers_to_wait.remove(&peer_id);
                        }
                        NetworkEvents::FileTransfer(transfer_id, file_transfer) => match file_transfer {
                            FileTransfer::QueryTransferType { file, block_size } => {
                                if let Some(transfer) = query_transfer_type(
                                    *shared_state,
                                    file,
                                    block_size,
                                    transfer_id,
                                    peer_id,
                                )
                                .await?
                                {
                                    let sender = receive_file(*shared_state, transfer, transfer_id, when_done_tx.clone()).await?;
                                    log::debug!("adding active transfer {transfer_id}");
                                    active_transfers.insert(transfer_id, sender);
                                }
                            }
                            event => {
                                if let Some(sender) = active_transfers.get(&transfer_id) {
                                    sender.send((peer_id, event)).await?;
                                }
                            }
                        },
                        e => {
                            log::error!("Received unexpected event {e:?}");
                        }
                    }
                }
                Some(transfer_id) = when_done.recv() => {
                    log::debug!("removing active transfer {transfer_id}");
                    active_transfers.remove(&transfer_id);
                    if active_transfers.is_empty() {
                        shared_state.connection_handler.broadcast_to(
                            Synchronization::DoneTransferingFiles.into(),
                            self.peers_with_transfers.iter()
                        ).await?;
                    }
                }
            }

            log::debug!(
                "{} - p{} - a{} - s{}",
                shared_state.config.node_id,
                self.peers_with_transfers.len(),
                active_transfers.len(),
                self.files_to_send.len()
            );
        }

        log::debug!("Finishing file transfer");

        Ok(())
    }
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

    if matches!(transfer_type, TransferType::NoTransfer) {
        Ok(None)
    } else {
        Ok(Some(
            FileReceiver::new(
                shared_state.config,
                shared_state.transaction_log,
                file,
                block_size,
            )
            .await?,
        ))
    }
}

async fn receive_file(
    shared_state: SharedState,
    transfer: FileReceiver,
    transfer_id: u64,
    when_done: Sender<u64>,
) -> crate::Result<Sender<(u64, FileTransfer)>> {
    let (tx, rx) = tokio::sync::mpsc::channel(1);

    async fn receive_file_inner(
        shared_state: SharedState,
        mut transfer: FileReceiver,
        transfer_id: u64,
        mut rx: Receiver<(u64, FileTransfer)>,
        when_done: Sender<u64>,
    ) -> crate::Result<()> {
        while let Some((node_id, event)) = rx.recv().await {
            match event {
                FileTransfer::QueryRequiredBlocks { sender_block_index } => {
                    match transfer.get_required_block_index(sender_block_index).await {
                        Ok(required_blocks) => {
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
                    if let Err(err) = transfer.write_block(block_index, &block).await {
                        log::error!("Failed to write file block {err}");
                        // TODO: send abort transfer event
                    }
                }

                FileTransfer::TransferComplete => {
                    transfer
                        .finish(shared_state.config, shared_state.transaction_log)
                        .await?;

                    when_done.send(transfer_id).await?;
                    break;
                }
                _ => {}
            }
        }

        log::debug!("Done receiving file");
        Ok(())
    }

    tokio::spawn(async move {
        if let Err(err) =
            receive_file_inner(shared_state, transfer, transfer_id, rx, when_done).await
        {
            log::error!("{err}");
        }
    });

    Ok(tx)
}

async fn send_file(
    shared_state: SharedState,
    file: FileInfo,
    nodes: HashSet<u64>,
    when_done: Sender<u64>,
) -> crate::Result<(u64, Sender<(u64, FileTransfer)>)> {
    log::trace!("Will send {:?} to {nodes:?}", &file.path);

    async fn send_file_inner(
        shared_state: SharedState,
        mut transfer: FileSender,
        transfer_id: u64,
        mut rx: Receiver<(u64, FileTransfer)>,
        when_done: Sender<u64>,
    ) -> crate::Result<()> {
        transfer
            .query_transfer_type(shared_state.connection_handler)
            .await?;

        while let Some((node_id, event)) = rx.recv().await {
            match event {
                FileTransfer::ReplyTransferType { transfer_type } => {
                    log::debug!("{transfer_type:?}");
                    let _ = transfer
                        .set_transfer_type(shared_state.connection_handler, node_id, transfer_type)
                        .await;

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
                    log::debug!(
                        "has participants:{} pending_information: {}",
                        transfer.has_participant_nodes(),
                        transfer.pending_information()
                    );
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

        log::debug!("Done sending file");
        when_done.send(transfer_id).await?;

        Ok(())
    }

    let (tx, rx) = tokio::sync::mpsc::channel(1);
    let transfer = FileSender::new(file, nodes, shared_state.config).await?;
    let transfer_id = transfer.transfer_id();
    tokio::spawn(async move {
        if let Err(err) = send_file_inner(shared_state, transfer, transfer_id, rx, when_done).await
        {
            log::error!("{err}");
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
