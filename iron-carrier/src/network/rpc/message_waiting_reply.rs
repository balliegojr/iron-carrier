use std::{collections::HashSet, time::Duration};
use tokio::sync::mpsc::Sender;

use super::{rpc_reply::RPCReply, Deadline};
use crate::{constants::DEFAULT_NETWORK_TIMEOUT, NodeId};

/// Represents a message that is waiting for replies of one or more nodes.
pub struct InFlightMessage {
    id: u16,
    nodes: HashSet<NodeId>,
    reply_channel: Sender<ReplyType>,
    deadline: Deadline,
}

impl InFlightMessage {
    pub fn new(
        id: u16,
        nodes: HashSet<NodeId>,
        reply_channel: Sender<ReplyType>,
        timeout: Duration,
    ) -> Self {
        Self {
            id,
            nodes,
            reply_channel,
            deadline: Deadline::new(timeout),
        }
    }

    pub async fn process_reply(
        &mut self,
        node_id: NodeId,
        reply: super::network_message::NetworkMessage,
    ) -> anyhow::Result<()> {
        if reply.is_ping() {
            self.deadline.extend(DEFAULT_NETWORK_TIMEOUT);
        } else if self.nodes.remove(&node_id) {
            log::trace!("Message {} received reply from {node_id}", self.id);
            if reply.is_cancel() {
                self.reply_channel.send(ReplyType::Cancel(node_id)).await?;
            } else {
                self.reply_channel
                    .send(ReplyType::Message(RPCReply::new(reply, node_id)))
                    .await?;
            }
        } else {
            log::error!(
                "Message {} received reply from unexpected node {node_id}",
                self.id
            );
        }

        Ok(())
    }

    pub async fn send_timeout(self) -> anyhow::Result<()> {
        log::trace!("Message {} timed out", self.id);
        self.reply_channel
            .send(ReplyType::Timeout(self.nodes))
            .await
            .map_err(anyhow::Error::from)
    }

    pub fn received_all_replies(&self) -> bool {
        self.nodes.is_empty()
    }

    pub fn is_expired(&self) -> bool {
        self.deadline.is_expired()
    }
}

#[derive(Debug)]
pub enum ReplyType {
    Message(RPCReply),
    Cancel(NodeId),
    Timeout(HashSet<NodeId>),
}
