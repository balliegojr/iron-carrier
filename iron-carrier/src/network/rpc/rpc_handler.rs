use std::{collections::HashSet, sync::Arc};

use crate::{
    message_types::{MessageType, MessageTypes},
    network::connection::Connection,
};
use serde::Serialize;
use tokio::sync::{mpsc::Sender, Semaphore};

use crate::node_id::NodeId;

use super::{
    call::Call, group_call::GroupCall, network_message::NetworkMessage, subscription::Subscription,
    Command, CommandTx, OutboundNetworkMessageType,
};

/// Handler to the RPC service. If all copies of this are dropped, the service will shutdown.
#[derive(Debug, Clone)]
pub struct RPCHandler {
    network_output: Sender<(NetworkMessage, OutboundNetworkMessageType)>,
    command_tx: CommandTx,
}

impl RPCHandler {
    pub fn new(
        network_output: Sender<(NetworkMessage, OutboundNetworkMessageType)>,
        command_tx: CommandTx,
    ) -> Self {
        Self {
            network_output,
            command_tx,
        }
    }

    /// Make a RPC to a single `target`
    pub fn call<T>(&self, data: T, target: NodeId) -> Call<T>
    where
        T: MessageType + Serialize,
    {
        Call::new(data, self.network_output.clone(), target)
    }

    /// Make a RPC to multiple `targets`
    pub fn multi_call<T>(&self, data: T, targets: HashSet<NodeId>) -> GroupCall<T>
    where
        T: MessageType + Serialize,
    {
        GroupCall::new(data, self.network_output.clone(), Some(targets))
    }

    /// Make a RPC broadcast to every connected Node
    pub fn broadcast<T>(&self, data: T) -> GroupCall<T>
    where
        T: MessageType + Serialize,
    {
        GroupCall::new(data, self.network_output.clone(), None)
    }

    pub async fn add_connection(&self, connection: Connection) -> anyhow::Result<()> {
        self.command_tx
            .send(Command::AddConnection(connection))
            .await?;
        Ok(())
    }

    pub async fn has_connection_to(&self, node_id: NodeId) -> anyhow::Result<bool> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.command_tx
            .send(Command::QueryIsConnectedTo(node_id, tx))
            .await?;
        rx.await.map_err(anyhow::Error::from)
    }

    /// Subscribe to events from the network. When all connections are closed, the Subscription
    /// event stream will return None.
    pub async fn subscribe(&self, types: &[MessageTypes]) -> anyhow::Result<Subscription> {
        self.send_subscription_command(types, true).await
    }

    /// Subscribe to events from the network.
    pub async fn subscribe_forever(&self, types: &[MessageTypes]) -> anyhow::Result<Subscription> {
        self.send_subscription_command(types, false).await
    }

    async fn send_subscription_command(
        &self,
        types: &[MessageTypes],
        need_connections: bool,
    ) -> anyhow::Result<Subscription> {
        let drop_guard = Arc::new(Semaphore::new(1));
        let permit = drop_guard.clone().acquire_owned().await?;
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        self.command_tx
            .send(Command::AddSubscription {
                message_types: types.to_vec(),
                tx,
                drop_guard,
                need_connections,
            })
            .await?;
        Ok(Subscription::new(rx, permit))
    }
}
