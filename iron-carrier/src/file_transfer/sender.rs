// mod query_required_blocks;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    io::SeekFrom,
    sync::Arc,
};

// pub use query_required_blocks::QueryRequiredBlocks;

// mod query_transfer_type;
// pub use query_transfer_type::QueryTransfer;

// mod transfer_blocks;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
    sync::Semaphore,
};
// pub use transfer_blocks::TransferBlocks;

use crate::{network::rpc::GroupCallResponse, node_id::NodeId, storage::FileInfo, SharedState};

use super::{
    block_index,
    events::{
        self, QueryTransferType, RequiredBlocks, TransferBlock, TransferComplete, TransferResult,
        TransferType,
    },
    BlockIndexPosition, Transfer,
};

pub async fn send_files(
    shared_state: &SharedState,
    files_to_send: Vec<(FileInfo, HashSet<NodeId>)>,
) -> anyhow::Result<()> {
    let sending_limit = Arc::new(Semaphore::new(
        shared_state.config.max_parallel_sending.into(),
    ));

    let tasks: Vec<_> = files_to_send
        .into_iter()
        .map(|(file, nodes)| {
            tokio::spawn(send_file(
                shared_state.clone(),
                file,
                nodes,
                sending_limit.clone(),
            ))
        })
        .collect();

    for task in tasks {
        if let Err(err) = task.await {
            log::error!("{err}");
        }
    }

    Ok(())
}

async fn send_file(
    shared_state: SharedState,
    file: FileInfo,
    nodes: HashSet<NodeId>,
    send_limit: Arc<Semaphore>,
) -> anyhow::Result<()> {
    log::trace!("Waiting slot for file transfer");
    let permit = send_limit.acquire_owned().await.unwrap();
    let transfer = Transfer::new(file, permit).unwrap();

    let transfer_types = query_transfer_type(&shared_state, &transfer, nodes).await?;
    if transfer_types.is_empty() {
        return Ok(());
    }

    let mut file_handle =
        crate::storage::file_operations::open_file_for_reading(shared_state.config, &transfer.file)
            .await?;

    let mut nodes_blocks =
        query_required_blocks(&shared_state, &transfer, &mut file_handle, transfer_types).await?;

    while !nodes_blocks.is_empty() {
        transfer_blocks(
            &shared_state,
            &transfer,
            &mut file_handle,
            &mut nodes_blocks,
        )
        .await?;
    }

    log::info!("{:?} sent to nodes", transfer.file.path);

    Ok(())
}

/// Query `nodes` about the transfer type, returns only Partial or Full transfers
async fn query_transfer_type(
    shared_state: &crate::SharedState,
    transfer: &Transfer,
    nodes: HashSet<NodeId>,
) -> anyhow::Result<HashMap<NodeId, TransferType>> {
    log::debug!("Querying transfer type for {:?}", transfer.file.path);
    shared_state
        .rpc
        .multi_call(
            QueryTransferType {
                // transfer_id: self.transfer.transfer_id,
                file: transfer.file.clone(),
            },
            nodes,
        )
        .result()
        .await
        .and_then(|response| {
            response
                .replies()
                .into_iter()
                .map(|reply| {
                    reply
                        .data()
                        .map(|transfer_type: TransferType| (reply.node_id(), transfer_type))
                })
                .filter(|reply| {
                    matches!(
                        reply,
                        Ok((_, TransferType::FullFile | TransferType::Partial))
                    )
                })
                .collect()
        })
}

async fn query_required_blocks(
    shared_state: &SharedState,
    transfer: &Transfer,
    file_handle: &mut File,
    mut transfer_types: HashMap<NodeId, TransferType>,
) -> anyhow::Result<HashMap<NodeId, BTreeSet<BlockIndexPosition>>> {
    let full_index = block_index::get_file_block_index(
        file_handle,
        transfer.block_size,
        transfer.file.file_size()?,
        file_handle.metadata().await?.len(),
    )
    .await?;

    let nodes: HashSet<NodeId> = transfer_types
        .extract_if(|_, transfer_type| matches!(transfer_type, TransferType::Partial))
        .map(|(node_id, _)| node_id)
        .collect();

    let mut required_blocks: HashMap<NodeId, BTreeSet<BlockIndexPosition>> = if nodes.is_empty() {
        Default::default()
    } else {
        log::debug!("Querying required blocks for {:?}", transfer.file.path);
        shared_state
            .rpc
            .multi_call(
                events::QueryRequiredBlocks {
                    transfer_id: transfer.transfer_id,
                    sender_block_index: full_index.clone(),
                },
                nodes,
            )
            .result()
            .await
            .and_then(|response| {
                response
                    .replies()
                    .into_iter()
                    .map(|reply| {
                        reply.data().map(|required_blocks: RequiredBlocks| {
                            (reply.node_id(), required_blocks.required_blocks)
                        })
                    })
                    .collect()
            })?
    };

    for (node_id, _) in transfer_types {
        required_blocks.insert(node_id, full_index.to_partial());
    }

    Ok(required_blocks)
}

async fn transfer_blocks(
    shared_state: &crate::SharedState,
    transfer: &Transfer,
    file_handle: &mut File,
    nodes_blocks: &mut HashMap<NodeId, BTreeSet<BlockIndexPosition>>,
) -> anyhow::Result<()> {
    log::debug!("Sending {:?} blocks to nodes", transfer.file.path);
    let mut block_nodes: BTreeMap<BlockIndexPosition, HashSet<NodeId>> =
        std::collections::BTreeMap::new();

    for (node, node_blocks) in nodes_blocks.iter() {
        for block in node_blocks {
            block_nodes.entry(*block).or_default().insert(*node);
        }
    }

    let file_size = transfer.file.file_size()?;
    let mut block = vec![0u8; file_size as usize];
    for (block_index, nodes) in block_nodes.into_iter() {
        let position = block_index.get_position(transfer.block_size);
        let bytes_to_read = transfer.block_size.min(file_size - position);

        if file_handle.seek(SeekFrom::Start(position)).await? != position {
            anyhow::bail!("Failed to file from disk");
        }

        file_handle
            .read_exact(&mut block[..bytes_to_read as usize])
            .await?;

        // FIXME: remove nodes missing ack
        // FIXME: back to stream...
        shared_state
            .rpc
            .multi_call(
                TransferBlock {
                    transfer_id: transfer.transfer_id,
                    block_index,
                    block: &block[..bytes_to_read as usize],
                },
                nodes,
            )
            .ack()
            .await?;
    }

    let results = shared_state
        .rpc
        .multi_call(
            TransferComplete {
                transfer_id: transfer.transfer_id,
            },
            nodes_blocks.keys().cloned().collect(),
        )
        .result()
        .await?;

    let (replies, timed_out) = match results {
        GroupCallResponse::Complete(replies) => (replies, Default::default()),
        GroupCallResponse::Partial(replies, timed_out) => (replies, Some(timed_out)),
    };

    for reply in replies {
        match reply.data::<TransferResult>()? {
            TransferResult::Success => {
                nodes_blocks.remove(&reply.node_id());
            }
            TransferResult::Failed { required_blocks } => {
                nodes_blocks
                    .entry(reply.node_id())
                    .and_modify(|e| *e = required_blocks);
            }
        }
    }

    if let Some(timed_out) = timed_out {
        for node_id in timed_out {
            nodes_blocks.remove(&node_id);
        }
    }

    Ok(())
}
