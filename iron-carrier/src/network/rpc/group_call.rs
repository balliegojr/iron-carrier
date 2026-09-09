use std::{collections::HashSet, time::Duration};

use crate::{
    constants::DEFAULT_NETWORK_TIMEOUT,
    protocol::{Protocol, ProtocolAck, ProtocolQuery},
};
use serde::Serialize;
use tokio::sync::mpsc::Sender;

use crate::node_id::NodeId;

use super::{OutboundNetworkMessageType, network_message::NetworkMessage, rpc_reply::RPCReply};

/// Represents a RPC for multiple nodes or a broadcast
#[must_use]
pub struct GroupCall<T> {
    data: T,
    sender: Sender<(NetworkMessage, OutboundNetworkMessageType)>,
    targets: Option<HashSet<NodeId>>,
    timeout: Duration,
    sub_process: Option<u64>,
}

impl<T> GroupCall<T>
where
    T: Protocol + Serialize,
{
    pub fn new(
        data: T,
        sender: Sender<(NetworkMessage, OutboundNetworkMessageType)>,
        targets: Option<HashSet<NodeId>>,
        sub_process: Option<u64>,
    ) -> Self {
        Self {
            data,
            sender,
            targets,
            timeout: Duration::from_secs(DEFAULT_NETWORK_TIMEOUT),
            sub_process,
        }
    }

    /// Set the timeout for this call
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    async fn wait_replies<U>(self) -> anyhow::Result<GroupCallResponse<U>> {
        let message = NetworkMessage::new(self.data, self.sub_process)?;
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
                super::message_waiting_reply::ReplyType::Message(reply, node_id) => {
                    replies.push(RPCReply::new(reply, node_id));
                }
                super::message_waiting_reply::ReplyType::Cancel(node_id) => {
                    canceled_nodes.insert(node_id);
                }
                super::message_waiting_reply::ReplyType::Timeout(nodes) => {
                    return Ok(GroupCallResponse::Partial(replies, nodes));
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

impl<T> GroupCall<T>
where
    T: ProtocolQuery + Serialize,
{
    /// Wait for the execution reply for the nodes involved in this call.
    ///
    /// If any node doesn't reply before the request timeout, returns a partial response
    pub async fn result(self) -> anyhow::Result<GroupCallResponse<T::ResponseType>> {
        self.wait_replies::<T::ResponseType>().await
    }
}

impl<T> GroupCall<T>
where
    T: ProtocolAck + Serialize,
{
    /// Wait until all nodes in the call ack the request. Returns a HashSet of Nodes that acked
    /// the message.
    pub async fn ack(self) -> anyhow::Result<HashSet<NodeId>> {
        self.wait_replies::<T>()
            .await
            .and_then(|response| match response {
                GroupCallResponse::Partial(replies, _) | GroupCallResponse::Complete(replies) => {
                    if replies.iter().all(|reply| reply.is_ack()) {
                        Ok(replies.into_iter().map(|r| r.node_id()).collect())
                    } else {
                        anyhow::bail!("Received invalid reply")
                    }
                }
            })
    }
}

#[derive(Debug)]
pub enum GroupCallResponse<T> {
    Complete(Vec<RPCReply<T>>),
    Partial(Vec<RPCReply<T>>, HashSet<NodeId>),
}

impl<T> GroupCallResponse<T> {
    /// return the replies, regardless of the type of response
    pub fn replies(self) -> Vec<RPCReply<T>> {
        match self {
            GroupCallResponse::Complete(replies) => replies,
            GroupCallResponse::Partial(replies, _) => replies,
        }
    }
}
