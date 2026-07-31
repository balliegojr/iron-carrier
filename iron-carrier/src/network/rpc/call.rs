use std::time::Duration;

use crate::{constants::DEFAULT_NETWORK_TIMEOUT, protocol::Protocol};
use serde::Serialize;
use tokio::sync::mpsc::Sender;

use crate::node_id::NodeId;

use super::{
    OutboundNetworkMessageType, message_waiting_reply::ReplyType, network_message::NetworkMessage,
    rpc_reply::RPCReply,
};

/// Represents a RPC for a single Node
#[must_use]
pub struct Call<T> {
    data: T,
    sender: Sender<(NetworkMessage, OutboundNetworkMessageType)>,
    target: NodeId,
    timeout: Duration,
    sub_process: Option<u64>,
}

impl<T> Call<T>
where
    T: Protocol + Serialize,
{
    pub fn new(
        data: T,
        sender: Sender<(NetworkMessage, OutboundNetworkMessageType)>,
        target: NodeId,
        sub_process: Option<u64>,
    ) -> Self {
        Self {
            data,
            sender,
            target,
            timeout: Duration::from_secs(DEFAULT_NETWORK_TIMEOUT),
            sub_process,
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
        let message = NetworkMessage::new(self.data, self.sub_process)?;
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
