use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    io::SeekFrom,
    pin::Pin,
    sync::Arc,
};

use tokio::{
    io::{AsyncReadExt, AsyncSeekExt},
    sync::Semaphore,
};

use crate::{
    Context,
    file_transfer::SyncFile,
    fs::Metadata,
    network::rpc::{GroupCallResponse, RPCMessage},
    node_id::NodeId,
    states::sync::events::SendFileTo,
    storage::storage_tree::FileId,
};

use super::{
    BlockIndexPosition, block_index,
    events::{
        self, QueryTransferType, RequiredBlocks, TransferBlock, TransferComplete, TransferResult,
        TransferType,
    },
};

pub async fn send_file(
    context: Context,
    request: RPCMessage,
    transfers_semaphore: Arc<Semaphore>,
) -> anyhow::Result<()> {
    log::trace!("Waiting slot for file transfer");

    let _permit = super::acquire_permit(transfers_semaphore, &request).await?;
    let SendFileTo { file, nodes } = request.data()?;
    let context = context.subprocess(FileId::new(&file.path.as_path()).into());

    request.ack().await?;

    let transfer_types = query_transfer_type(&context, &file, nodes).await?;
    if transfer_types.is_empty() {
        return Ok(());
    }

    let storage_config = context.config.path(&file.storage)?;

    let absolute_path = file.path.absolute(storage_config)?;
    let metadata = context.fs.metadata(&absolute_path).await?;
    let mut file_handle = context.fs.open_r(&absolute_path).await?;
    let block_size = super::block_index::get_block_size(file.info.size());

    let mut nodes_blocks = query_required_blocks(
        &context,
        &file,
        &mut file_handle,
        transfer_types,
        metadata,
        block_size,
    )
    .await?;

    while !nodes_blocks.is_empty() {
        transfer_blocks(
            &context,
            &file,
            &mut file_handle,
            &mut nodes_blocks,
            block_size,
        )
        .await?;
    }

    log::info!("{:?} sent to nodes", file.path);

    Ok(())
}

/// Query `nodes` about the transfer type, returns only Partial or Full transfers
async fn query_transfer_type(
    context: &crate::Context,
    file: &SyncFile,
    nodes: HashSet<NodeId>,
) -> anyhow::Result<HashMap<NodeId, TransferType>> {
    log::debug!("Querying transfer type for {:?}", file.path);
    context
        .rpc
        .multi_call(QueryTransferType, nodes)
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
    context: &Context,
    file: &SyncFile,
    file_handle: &mut Pin<Box<dyn crate::fs::FileR>>,
    mut transfer_types: HashMap<NodeId, TransferType>,
    metadata: Metadata,
    block_size: u64,
) -> anyhow::Result<HashMap<NodeId, BTreeSet<BlockIndexPosition>>> {
    let full_index = block_index::get_file_block_index(
        file_handle,
        block_size,
        file.info.size(),
        metadata.len(),
    )
    .await?;

    let nodes: HashSet<NodeId> = transfer_types
        .extract_if(|_, transfer_type| matches!(transfer_type, TransferType::Partial))
        .map(|(node_id, _)| node_id)
        .collect();

    let mut required_blocks: HashMap<NodeId, BTreeSet<BlockIndexPosition>> = if nodes.is_empty() {
        Default::default()
    } else {
        log::debug!("Querying required blocks for {:?}", file.path);
        context
            .rpc
            .multi_call(
                events::QueryRequiredBlocks {
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
    context: &crate::Context,
    file: &SyncFile,
    file_handle: &mut Pin<Box<dyn crate::fs::FileR>>,
    nodes_blocks: &mut HashMap<NodeId, BTreeSet<BlockIndexPosition>>,
    block_size: u64,
) -> anyhow::Result<()> {
    log::debug!("Sending {:?} blocks to nodes", file.path);
    let mut block_nodes: BTreeMap<BlockIndexPosition, HashSet<NodeId>> =
        std::collections::BTreeMap::new();

    for (node, node_blocks) in nodes_blocks.iter() {
        for block in node_blocks {
            block_nodes.entry(*block).or_default().insert(*node);
        }
    }

    let file_size = file.info.size();
    let mut block = vec![0u8; file_size as usize];
    for (block_index, nodes) in block_nodes.into_iter() {
        let position = block_index.get_position(block_size);
        let bytes_to_read = block_size.min(file_size - position);

        if file_handle.seek(SeekFrom::Start(position)).await? != position {
            anyhow::bail!("Failed to file from disk");
        }

        file_handle
            .read_exact(&mut block[..bytes_to_read as usize])
            .await?;

        // FIXME: remove nodes missing ack
        // FIXME: back to stream...
        context
            .rpc
            .multi_call(
                TransferBlock {
                    block_index,
                    block: &block[..bytes_to_read as usize],
                },
                nodes,
            )
            .ack()
            .await?;
    }

    let results = context
        .rpc
        .multi_call(TransferComplete, nodes_blocks.keys().cloned().collect())
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
