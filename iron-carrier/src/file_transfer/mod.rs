use std::{sync::Arc, time::Duration};

use crate::{
    constants::DEFAULT_NETWORK_TIMEOUT, network::rpc::RPCMessage, relative_path::RelativePathBuf,
    storage::storage_tree::ExistingFileInfo,
};

mod block_index;
pub use block_index::BlockIndexPosition;

pub mod events;
mod receiver;
mod sender;

use serde::{Deserialize, Serialize};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::Sleep,
};

pub use receiver::receive_file;
pub use sender::send_file;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SyncFile {
    pub storage: String,
    pub path: RelativePathBuf,
    pub info: ExistingFileInfo,
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
        let timeout = get_timeout();
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

fn get_timeout() -> Sleep {
    tokio::time::sleep(Duration::from_secs_f32(
        DEFAULT_NETWORK_TIMEOUT as f32 * 0.8,
    ))
}
