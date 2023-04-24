use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{Receiver, Sender};

use crate::{
    ignored_files::IgnoredFilesCache,
    network_events::{NetworkEvents, Synchronization},
    state_machine::State,
    storage::FileInfo,
    SharedState,
};

mod block_index;
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

    RemovePeer,
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
    sync_leader: Option<u64>,
}

impl TransferFiles {
    pub fn new(sync_leader: Option<u64>, files_to_send: Vec<(FileInfo, HashSet<u64>)>) -> Self {
        Self {
            files_to_send,
            sync_leader,
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
            active_transfers.insert(transfer_id, sender);
        }

        Ok(sent_files_to)
    }
}

impl State for TransferFiles {
    type Output = ();

    async fn execute(mut self, shared_state: &SharedState) -> crate::Result<Self::Output> {
        let mut nodes = if self.sync_leader.is_none() {
            shared_state
                .connection_handler
                .broadcast(Synchronization::StartTransferingFiles.into())
                .await?
        } else {
            1
        };

        let (when_done_tx, mut when_done) = tokio::sync::mpsc::channel(1);
        let mut active_transfers = HashMap::new();
        let mut ignore_cache = IgnoredFilesCache::default();

        self.start_new_transfers(shared_state, &mut active_transfers, &when_done_tx)
            .await?;

        while nodes > 0 || !active_transfers.is_empty() || !self.files_to_send.is_empty() {
            self.start_new_transfers(shared_state, &mut active_transfers, &when_done_tx)
                .await?;

            tokio::select! {
               Some((peer_id, event)) = shared_state.connection_handler.next_event() => {
                    match event {
                        NetworkEvents::Disconnected => {
                            log::debug!("removing {peer_id} from transfers");
                            nodes -= 1;
                            for transfer in active_transfers.values() {
                                let _ = transfer.send((peer_id, FileTransfer::RemovePeer)).await;
                            }
                        }
                        NetworkEvents::Synchronization(Synchronization::DoneTransferingFiles) => {
                            log::debug!("removing {peer_id} from transfers");
                            nodes -= 1;
                        }
                        NetworkEvents::FileTransfer(transfer_id, file_transfer) => match file_transfer {
                            FileTransfer::QueryTransferType { file, block_size } => {
                                if let Some(transfer) = query_transfer_type(
                                    *shared_state,
                                    file,
                                    block_size,
                                    transfer_id,
                                    peer_id,
                                    &mut ignore_cache
                                )
                                .await?
                                {
                                    let sender = receive_file(*shared_state, transfer, transfer_id, when_done_tx.clone()).await?;
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
                    if let Some(leader) = self.sync_leader && active_transfers.is_empty() {
                        shared_state.connection_handler.send_to(
                            Synchronization::DoneTransferingFiles.into(),
                            leader
                        ).await?;
                    }
                }
            }
        }

        if self.sync_leader.is_none() {
            shared_state
                .connection_handler
                .broadcast(Synchronization::DoneTransferingFiles.into())
                .await?;
        }

        Ok(())
    }
}

async fn query_transfer_type(
    shared_state: SharedState,
    file: FileInfo,
    block_size: u64,
    transfer_id: u64,
    node_id: u64,
    ignored_files_cache: &mut IgnoredFilesCache,
) -> crate::Result<Option<FileReceiver>> {
    let transfer_type =
        file_receiver::get_transfer_type(&file, shared_state.config, ignored_files_cache).await?;
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
                node_id,
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
    ) -> crate::Result<()> {
        while let Some((node_id, event)) = rx.recv().await {
            match event {
                FileTransfer::QueryRequiredBlocks { sender_block_index } => {
                    match transfer.get_required_block_index(sender_block_index).await {
                        Ok(required_blocks) => {
                            let _ = shared_state
                                .connection_handler
                                .send_to(
                                    NetworkEvents::FileTransfer(
                                        transfer_id,
                                        FileTransfer::ReplyRequiredBlocks { required_blocks },
                                    ),
                                    node_id,
                                )
                                .await;
                        }
                        Err(err) => {
                            log::error!("Failed to get required blocks {err}");
                            break;
                        }
                    }
                }
                FileTransfer::TransferBlock { block_index, block } => {
                    if let Err(err) = transfer.write_block(block_index, &block).await {
                        log::error!("Failed to write file block {err}");
                        break;
                    }
                }

                FileTransfer::RemovePeer if transfer.receiving_from() == node_id => {
                    break;
                }
                FileTransfer::TransferComplete => {
                    break;
                }
                _ => {}
            }
        }

        if let Err(err) = transfer
            .finish(shared_state.config, shared_state.transaction_log)
            .await
        {
            log::error!("Failed to finalize file {err}");
        }

        Ok(())
    }

    tokio::spawn(async move {
        let node_id = transfer.receiving_from();
        if let Err(err) = receive_file_inner(shared_state, transfer, transfer_id, rx).await {
            log::error!("{err}");

            let _ = shared_state
                .connection_handler
                .send_to(
                    NetworkEvents::FileTransfer(transfer_id, FileTransfer::RemovePeer),
                    node_id,
                )
                .await;
        }

        let _ = when_done.send(transfer_id).await;
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
        mut rx: Receiver<(u64, FileTransfer)>,
    ) -> crate::Result<()> {
        transfer
            .query_transfer_type(shared_state.connection_handler)
            .await?;

        while let Some((node_id, event)) = rx.recv().await {
            match event {
                FileTransfer::ReplyTransferType { transfer_type } => {
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
                FileTransfer::RemovePeer => {
                    transfer.remove_node(node_id);
                    if !transfer.has_participant_nodes() {
                        break;
                    }
                }
                _ => {}
            }
        }

        Ok(())
    }

    let (tx, rx) = tokio::sync::mpsc::channel(1);
    let transfer = FileSender::new(file, nodes, shared_state.config).await?;
    let transfer_id = transfer.transfer_id();
    tokio::spawn(async move {
        if let Err(err) = send_file_inner(shared_state, transfer, rx).await {
            log::error!("{err}");
        }
        let _ = when_done.send(transfer_id).await;
    });

    Ok((transfer_id, tx))
}
