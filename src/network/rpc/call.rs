use std::time::Duration;

use hash_type_id::HashTypeId;
use serde::Serialize;
use tokio::sync::mpsc::Sender;

use crate::{node_id::NodeId, IronCarrierError};

use super::{network_message::NetworkMessage, rpc_reply::RPCReply, OutputMessageType};

#[must_use]
pub struct Call<T> {
    data: T,
    sender: Sender<(NetworkMessage, OutputMessageType)>,
    target: NodeId,
}

impl<T> Call<T>
where
    T: HashTypeId + Serialize,
{
    pub fn new(
        data: T,
        sender: Sender<(NetworkMessage, OutputMessageType)>,
        target: NodeId,
    ) -> Self {
        Self {
            data,
            sender,
            target,
        }
    }

    pub async fn ack(self) -> crate::Result<()> {
        match self.wait_reply().await? {
            Some(reply) if reply.is_ack() => Ok(()),
            _ => Err(IronCarrierError::InvalidReply.into()),
        }
    }

    pub async fn result(self) -> crate::Result<RPCReply> {
        match self.wait_reply().await? {
            Some(reply) if !reply.is_ack() => Ok(reply),
            _ => Err(IronCarrierError::InvalidReply.into()),
        }
    }

    async fn wait_reply(self) -> crate::Result<Option<RPCReply>> {
        let message = NetworkMessage::encode(self.data)?;
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        self.sender
            .send((message, OutputMessageType::SingleNode(self.target, tx)))
            .await?;

        tokio::time::timeout(Duration::from_secs(30), rx.recv())
            .await
            .map_err(|_| IronCarrierError::ReplyTimeOut.into())
    }
}
