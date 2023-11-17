use std::time::Duration;

use crate::{constants::DEFAULT_NETWORK_TIMEOUT, message_types::MessageType};
use serde::Serialize;
use tokio::sync::mpsc::Sender;

use crate::node_id::NodeId;

use super::{
    message_waiting_reply::ReplyType, network_message::NetworkMessage, rpc_reply::RPCReply,
    OutboundNetworkMessageType,
};

/// Represents a RPC for a single Node
#[must_use]
pub struct Call<T> {
    data: T,
    sender: Sender<(NetworkMessage, OutboundNetworkMessageType)>,
    target: NodeId,
    timeout: Duration,
}

impl<T> Call<T>
where
    T: MessageType + Serialize,
{
    pub fn new(
        data: T,
        sender: Sender<(NetworkMessage, OutboundNetworkMessageType)>,
        target: NodeId,
    ) -> Self {
        Self {
            data,
            sender,
            target,
            timeout: Duration::from_secs(DEFAULT_NETWORK_TIMEOUT),
        }
    }

    /// Wait for the ack for this message.
    ///
    /// The operation may fail if the other Node cancel the request or does't reply in time
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
                OutboundNetworkMessageType::SingleNode(self.target, tx, self.timeout),
            ))
            .await?;

        match rx.recv().await {
            Some(ReplyType::Message(reply)) => Ok(reply),
            Some(ReplyType::Cancel(_)) => anyhow::bail!("Node canceled request"),
            _ => anyhow::bail!("Timeout when waiting for replies"),
        }
    }
}
