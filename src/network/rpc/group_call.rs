use std::{collections::HashSet, time::Duration};

use hash_type_id::HashTypeId;
use serde::Serialize;
use tokio::sync::mpsc::Sender;

use crate::{node_id::NodeId, IronCarrierError};

use super::{network_message::NetworkMessage, rpc_reply::RPCReply, OutputMessageType};

#[must_use]
pub struct GroupCall<T> {
    data: T,
    sender: Sender<(NetworkMessage, OutputMessageType)>,
    targets: Option<HashSet<NodeId>>,
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
        }
    }

    // TODO: on timeout, return nodeids that didn't reply
    pub async fn ack(self) -> crate::Result<HashSet<NodeId>> {
        self.wait_replies().await.and_then(|replies| {
            if replies.iter().all(|reply| reply.is_ack()) {
                Ok(replies.into_iter().map(|r| r.node_id()).collect())
            } else {
                Err(IronCarrierError::InvalidReply.into())
            }
        })
    }

    pub async fn result(self) -> crate::Result<Vec<RPCReply>> {
        self.wait_replies().await
    }

    async fn wait_replies(self) -> crate::Result<Vec<RPCReply>> {
        let message = NetworkMessage::encode(self.data)?;
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let output_type = match self.targets.as_ref() {
            Some(nodes) => OutputMessageType::MultiNode(nodes.clone(), tx),
            None => OutputMessageType::Broadcast(tx),
        };

        self.sender.send((message, output_type)).await?;
        let aggregate_replies = async {
            let mut replies = Vec::new();
            while let Some(reply) = rx.recv().await {
                replies.push(reply);
            }
            Ok(replies)
        };

        tokio::time::timeout(Duration::from_secs(30), aggregate_replies)
            .await
            .map_err(|_| IronCarrierError::ReplyTimeOut)?
    }
}
