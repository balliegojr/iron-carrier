use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use serde::{Deserialize, Serialize};
use tokio::sync::{
    mpsc::{Receiver, Sender},
    Mutex,
};

use crate::{
    hash_helper,
    ignored_files::IgnoredFilesCache,
    network_events::{NetworkEvents, Synchronization},
    node_id::NodeId,
    state_machine::{State, StateComposer},
    storage::FileInfo,
    SharedState,
};

mod block_index;
mod query_required_blocks;
mod query_transfer_type;
mod receive_blocks;
mod reply_required_blocks;
mod reply_transfer_type;
mod transfer_blocks;

type BlockIndex = u64;
type BlockHash = u64;

type TransferRecv = Receiver<(NodeId, FileTransfer)>;

#[derive(Debug)]
pub struct Transfer {
    file: FileInfo,
    block_size: u64,
    transfer_id: u64,
}

impl Transfer {
    pub fn new(file: FileInfo) -> crate::Result<Self> {
        let block_size = block_index::get_block_size(file.file_size()?);
        let transfer_id = hash_helper::calculate_file_hash(&file);

        Ok(Self {
            file,
            block_size,
            transfer_id,
        })
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum FileTransfer {
    QueryTransferType {
        file: FileInfo,
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
    TransferSucceeded,
    TransferFailed {
        received_blocks: Vec<u64>,
    },
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
    files_to_send: Vec<(FileInfo, HashSet<NodeId>)>,
    sync_leader_id: Option<NodeId>,
}

impl TransferFiles {
    pub fn new(
        sync_leader_id: Option<NodeId>,
        files_to_send: Vec<(FileInfo, HashSet<NodeId>)>,
    ) -> Self {
        Self {
            files_to_send,
            sync_leader_id,
        }
    }

    async fn start_new_transfers(
        &mut self,
        shared_state: &SharedState,
        active_transfers: &mut HashMap<u64, Sender<(NodeId, FileTransfer)>>,
        active_sending: &mut HashSet<u64>,
        when_done_tx: &Sender<u64>,
    ) -> crate::Result<()> {
        while !self.files_to_send.is_empty()
            && active_transfers.len() < shared_state.config.max_parallel_transfers as usize
        {
            let (file, nodes) = self.files_to_send.pop().unwrap();

            let (transfer_id, sender) =
                send_file(*shared_state, file, nodes, when_done_tx.clone()).await?;
            active_transfers.insert(transfer_id, sender);
            active_sending.insert(transfer_id);
        }

        Ok(())
    }
}

impl State for TransferFiles {
    type Output = ();

    async fn execute(mut self, shared_state: &SharedState) -> crate::Result<Self::Output> {
        // FIXME: control active sending transfers and emit 'done' signal when sending is done

        let mut active_nodes = if self.sync_leader_id.is_none() {
            shared_state
                .connection_handler
                .broadcast(Synchronization::StartTransferingFiles.into())
                .await?
        } else {
            1
        };

        let node_type = if self.sync_leader_id.is_none() {
            "L"
        } else {
            "F"
        };

        log::trace!("[{node_type}] {}", shared_state.config.node_id_hashed);
        log::debug!("[{node_type}] have {active_nodes} active nodes");

        let (when_done_tx, mut when_done) = tokio::sync::mpsc::channel(1);
        let mut active_transfers = HashMap::new();
        let mut active_sending = HashSet::new();
        let ignore_cache = Arc::new(Mutex::new(IgnoredFilesCache::default()));
        let mut sent_done_event = false;

        // self.start_new_transfers(shared_state, &mut active_transfers, &when_done_tx)
        //     .await?;

        while active_nodes > 0 || !active_transfers.is_empty() || !self.files_to_send.is_empty() {
            self.start_new_transfers(
                shared_state,
                &mut active_transfers,
                &mut active_sending,
                &when_done_tx,
            )
            .await?;

            tokio::select! {
               Some((node_id, event)) = shared_state.connection_handler.next_event() => {
                    match event {
                        NetworkEvents::Disconnected => {
                            log::info!("[{node_type}] node {node_id} disconnected, removing transfers");
                            active_nodes = active_nodes.saturating_sub(1);

                            for transfer in active_transfers.values() {
                                let _ = transfer.send((node_id, FileTransfer::RemovePeer)).await;
                            }
                        }
                        NetworkEvents::Synchronization(Synchronization::DoneTransferingFiles) => {
                            log::debug!("[{node_type}] node {node_id} has done transfering files");
                            active_nodes = active_nodes.saturating_sub(1);
                            log::debug!("[{node_type}] have {active_nodes} active nodes");

                            log::debug!(
                                "[{node_type}] {} {:?} {:?}",
                                active_nodes,
                                active_transfers,
                                self.files_to_send
                            );

                        }
                        NetworkEvents::FileTransfer(transfer_id, file_transfer) => match file_transfer {
                            FileTransfer::QueryTransferType { file } => {
                                let transfer = Transfer::new(file.clone())?;
                                let sender = receive_file(*shared_state, node_id, transfer, ignore_cache.clone(), when_done_tx.clone()).await?;
                                sender.send((node_id, FileTransfer::QueryTransferType { file })).await?;
                                active_transfers.insert(transfer_id, sender);
                            }
                            event => {
                                if let Some(sender) = active_transfers.get(&transfer_id) {
                                    sender.send((node_id, event)).await?;
                                }
                            }
                        },
                        e => {
                            log::error!("[file transfer {node_type}] received unexpected event {e:?}");
                        }
                    }
                }
                Some(transfer_id) = when_done.recv() => {
                    log::trace!("[{node_type}] transfer {transfer_id} done");
                    active_transfers.remove(&transfer_id);
                    active_sending.remove(&transfer_id);
                    if !sent_done_event && active_sending.is_empty() && self.files_to_send.is_empty() && let Some(leader) = self.sync_leader_id {
                        log::debug!("[{node_type}] done transfering files X");
                        shared_state.connection_handler.send_to(
                            Synchronization::DoneTransferingFiles.into(),
                            leader
                        ).await?;
                        sent_done_event = true;
                    }
                }
            }
        }

        if self.sync_leader_id.is_none() {
            log::debug!("[{node_type}] done transfering files");
            shared_state
                .connection_handler
                .broadcast(Synchronization::DoneTransferingFiles.into())
                .await?;
        }

        log::debug!("[{node_type}] done",);
        Ok(())
    }
}

async fn receive_file(
    shared_state: SharedState,
    source_node: NodeId,
    transfer: Transfer,
    ignored_files_cache: Arc<Mutex<IgnoredFilesCache>>,
    when_done: Sender<u64>,
) -> crate::Result<Sender<(NodeId, FileTransfer)>> {
    let transfer_id = transfer.transfer_id;
    let (tx, rx) = tokio::sync::mpsc::channel(1);

    tokio::spawn(async move {
        let receive_file_task =
            reply_transfer_type::ReplyTransfer::new(transfer, rx, ignored_files_cache, source_node)
                .and_then(|(source_node, transfer, transfer_chan, transfer_type)| {
                    reply_required_blocks::ReplyRequiredBlocks::new(
                        transfer_chan,
                        transfer,
                        transfer_type,
                        source_node,
                    )
                })
                .and_then(|(transfer, source_node, transfer_chan, expected_blocks)| {
                    receive_blocks::ReceiveBlocks::new(
                        transfer,
                        transfer_chan,
                        expected_blocks,
                        source_node,
                    )
                })
                .execute(&shared_state);

        if let Err(err) = receive_file_task.await {
            log::error!("{err}");

            let _ = shared_state
                .connection_handler
                .send_to(
                    NetworkEvents::FileTransfer(transfer_id, FileTransfer::RemovePeer),
                    source_node,
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
    nodes: HashSet<NodeId>,
    when_done: Sender<u64>,
) -> crate::Result<(u64, Sender<(NodeId, FileTransfer)>)> {
    log::trace!("Will send {:?} to {nodes:?}", &file.path);

    let transfer = Transfer::new(file)?;
    let transfer_id = transfer.transfer_id;

    let (tx, rx) = tokio::sync::mpsc::channel(nodes.len());
    tokio::spawn(async move {
        let transfer_task = query_transfer_type::QueryTransfer::new(transfer, nodes.clone(), rx)
            .and_then(|(transfer, transfer_chan, transfer_types)| {
                query_required_blocks::QueryRequiredBlocks::new(
                    transfer,
                    transfer_chan,
                    transfer_types,
                )
            })
            .and_then(|(transfer, transfer_chan, file_handle, nodes_blocks)| {
                transfer_blocks::TransferBlocks::new(
                    transfer,
                    transfer_chan,
                    file_handle,
                    nodes_blocks,
                )
            })
            .execute(&shared_state);

        if let Err(err) = transfer_task.await {
            log::error!("{err}");

            let _ = shared_state
                .connection_handler
                .broadcast_to(
                    NetworkEvents::FileTransfer(transfer_id, FileTransfer::RemovePeer),
                    nodes.iter(),
                )
                .await;
        }
        let _ = when_done.send(transfer_id).await;
    });

    Ok((transfer_id, tx))
}
