use std::{collections::HashSet, time::Duration};

use crate::{constants::DEFAULT_NETWORK_TIMEOUT, message_types::MessageType};
use serde::Serialize;
use tokio::sync::mpsc::Sender;

use crate::node_id::NodeId;

use super::{network_message::NetworkMessage, rpc_reply::RPCReply, OutboundNetworkMessageType};

/// Represents a RPC for multiple nodes or a broadcast
#[must_use]
pub struct GroupCall<T> {
    data: T,
    sender: Sender<(NetworkMessage, OutboundNetworkMessageType)>,
    targets: Option<HashSet<NodeId>>,
    timeout: Duration,
}

impl<T> GroupCall<T>
where
    T: MessageType + Serialize,
{
    pub fn new(
        data: T,
        sender: Sender<(NetworkMessage, OutboundNetworkMessageType)>,
        targets: Option<HashSet<NodeId>>,
    ) -> Self {
        Self {
            data,
            sender,
            targets,
            timeout: Duration::from_secs(DEFAULT_NETWORK_TIMEOUT),
        }
    }

    /// Wait untill all nodes in the call ack the request. Returns a HashSet of Nodes that acked
    /// the message.
    pub async fn ack(self) -> anyhow::Result<HashSet<NodeId>> {
        self.wait_replies()
            .await
            .and_then(|response| match response {
                GroupCallResponse::Partial(replies, _) | GroupCallResponse::Complete(replies) => {
                    if replies.iter().all(|reply| reply.is_ack()) {
                        Ok(replies.into_iter().map(|r| r.node_id()).collect())
                    } else {
                        anyhow::bail!("Received invalid reply");
                    }
                }
            })
    }

    /// Wait for the execution reply for the nodes involved in this call.
    ///
    /// If any node doesn't reply before the request timeout, returns a partial response
    pub async fn result(self) -> anyhow::Result<GroupCallResponse> {
        self.wait_replies().await
    }

    /// Set the timeout for this call
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    async fn wait_replies(self) -> anyhow::Result<GroupCallResponse> {
        let message = NetworkMessage::encode(self.data)?;
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let output_type = match self.targets {
            Some(nodes) => OutboundNetworkMessageType::MultiNode(nodes, tx, self.timeout),
            None => OutboundNetworkMessageType::Broadcast(tx, self.timeout),
        };

        self.sender.send((message, output_type)).await?;
        let mut replies = Vec::new();
        let mut canceled_nodes = HashSet::new();
        while let Some(reply) = rx.recv().await {
            match reply {
                super::message_waiting_reply::ReplyType::Message(reply) => {
                    replies.push(reply);
                }
                super::message_waiting_reply::ReplyType::Cancel(node_id) => {
                    canceled_nodes.insert(node_id);
                }
                super::message_waiting_reply::ReplyType::Timeout(nodes) => {
                    return Ok(GroupCallResponse::Partial(replies, nodes))
                }
            }
        }

        if canceled_nodes.is_empty() {
            Ok(GroupCallResponse::Complete(replies))
        } else {
            Ok(GroupCallResponse::Partial(replies, canceled_nodes))
        }
    }
}

#[derive(Debug)]
pub enum GroupCallResponse {
    Complete(Vec<RPCReply>),
    Partial(Vec<RPCReply>, HashSet<NodeId>),
}

impl GroupCallResponse {
    /// return the replies, regardless of the type of response
    pub fn replies(self) -> Vec<RPCReply> {
        match self {
            GroupCallResponse::Complete(replies) => replies,
            GroupCallResponse::Partial(replies, _) => replies,
        }
    }
}
