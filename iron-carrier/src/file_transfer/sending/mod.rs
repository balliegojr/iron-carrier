mod query_required_blocks;
use std::{collections::HashSet, sync::Arc};

pub use query_required_blocks::QueryRequiredBlocks;

mod query_transfer_type;
pub use query_transfer_type::QueryTransfer;

mod transfer_blocks;
use tokio::sync::Semaphore;
pub use transfer_blocks::TransferBlocks;

use crate::{
    node_id::NodeId,
    state_machine::{State, StateComposer},
    storage::FileInfo,
    SharedState,
};

use super::Transfer;

pub async fn send_files(
    shared_state: &SharedState,
    files_to_send: Vec<(FileInfo, HashSet<NodeId>)>,
) -> crate::Result<()> {
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
) -> crate::Result<()> {
    // log::trace!("Will send {:?} to {nodes:?}", &file.path);

    let permit = send_limit.acquire_owned().await.unwrap();
    let transfer = Transfer::new(file, permit).unwrap();

    // let transfer_types = query_transfer_type(shared_state, &transfer, nodes).await?;

    let transfer_task = QueryTransfer::new(transfer, nodes.clone())
        .and_then(|(transfer, transfer_types)| QueryRequiredBlocks::new(transfer, transfer_types))
        .and_then(|(transfer, file_handle, nodes_blocks)| {
            TransferBlocks::new(transfer, file_handle, nodes_blocks)
        })
        .execute(&shared_state);

    if let Err(err) = transfer_task.await {
        log::error!("{err}");
    }

    Ok(())
}
//
// async fn query_transfer_type(
//     shared_state: SharedState,
//     transfer: &Transfer,
//     nodes: HashSet<NodeId>,
// ) -> crate::Result<HashMap<NodeId, TransferType>> {
//     shared_state
//         .rpc
//         .multi_call(
//             QueryTransferType {
//                 transfer_id: transfer.transfer_id,
//                 file: transfer.file.clone(),
//             },
//             nodes,
//         )
//         .response::<TransferType>()
//         .await
// }
