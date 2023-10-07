use std::{collections::HashSet, time::Duration};

use crate::hash_type_id::HashTypeId;
use serde::Serialize;
use tokio::sync::mpsc::Sender;

use crate::node_id::NodeId;

use super::{network_message::NetworkMessage, rpc_reply::RPCReply, OutputMessageType};

#[must_use]
pub struct GroupCall<T> {
    data: T,
    sender: Sender<(NetworkMessage, OutputMessageType)>,
    targets: Option<HashSet<NodeId>>,
    timeout: Duration,
}

impl<T> GroupCall<T>
where
    T: HashTypeId + Serialize,
{
    pub fn new(
        data: T,
        sender: Sender<(NetworkMessage, OutputMessageType)>,
        targets: Option<HashSet<NodeId>>,
    ) -> Self {
        Self {
            data,
            sender,
            targets,
            timeout: Duration::from_secs(5),
        }
    }

    /// Wait untill all nodes in the call acknowledge the request
    pub async fn ack(self) -> anyhow::Result<HashSet<NodeId>> {
        // TODO: on timeout, return nodeids that didn't reply
        self.wait_replies()
            .await
            .and_then(|response| match response {
                GroupCallResponse::Complete(replies) => {
                    if replies.iter().all(|reply| reply.is_ack()) {
                        Ok(replies.into_iter().map(|r| r.node_id()).collect())
                    } else {
                        anyhow::bail!("Received invalid reply");
                    }
                }
                GroupCallResponse::Partial(_, _) => anyhow::bail!("Received invalid reply"),
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
            Some(nodes) => OutputMessageType::MultiNode(nodes, tx, self.timeout),
            None => OutputMessageType::Broadcast(tx, self.timeout),
        };

        self.sender.send((message, output_type)).await?;
        let mut replies = Vec::new();
        while let Some(reply) = rx.recv().await {
            match reply {
                super::message_waiting_reply::ReplyType::Message(reply) => {
                    replies.push(reply);
                }
                super::message_waiting_reply::ReplyType::Timeout(nodes) => {
                    return Ok(GroupCallResponse::Partial(replies, nodes))
                }
            }
        }

        Ok(GroupCallResponse::Complete(replies))
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
