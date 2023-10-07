use std::time::Duration;

use crate::hash_type_id::HashTypeId;
use serde::Serialize;
use tokio::sync::mpsc::Sender;

use crate::node_id::NodeId;

use super::{
    message_waiting_reply::ReplyType, network_message::NetworkMessage, rpc_reply::RPCReply,
    OutputMessageType,
};

#[must_use]
pub struct Call<T> {
    data: T,
    sender: Sender<(NetworkMessage, OutputMessageType)>,
    target: NodeId,
    timeout: Duration,
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
            timeout: Duration::from_secs(5),
        }
    }

    pub async fn ack(self) -> anyhow::Result<()> {
        if self.wait_reply().await?.is_ack() {
            Ok(())
        } else {
            anyhow::bail!("Received invalid reply");
        }
    }

    async fn wait_reply(self) -> anyhow::Result<RPCReply> {
        let message = NetworkMessage::encode(self.data)?;
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        self.sender
            .send((
                message,
                OutputMessageType::SingleNode(self.target, tx, self.timeout),
            ))
            .await?;

        if let Some(ReplyType::Message(reply)) = rx.recv().await {
            return Ok(reply);
        }

        anyhow::bail!("Timeout when waiting for replies")
    }
}
