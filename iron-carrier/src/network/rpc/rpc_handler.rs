use std::{collections::HashSet, sync::Arc};

use crate::{
    network::{connection::Connection, rpc::SubscriptionKey},
    protocol::{MessageTypes, Protocol},
};
use serde::Serialize;
use tokio::sync::{Semaphore, mpsc::Sender};

use crate::node_id::NodeId;

use super::{
    Command, CommandTx, OutboundNetworkMessageType, call::Call, group_call::GroupCall,
    network_message::NetworkMessage, subscription::Subscription,
};

/// Handler to the RPC service. If all copies of this are dropped, the service will shutdown.
#[derive(Debug, Clone)]
pub struct RPCHandler {
    network_output: Sender<(NetworkMessage, OutboundNetworkMessageType)>,
    command_tx: CommandTx,
    sub_process: Option<u64>,
}

impl RPCHandler {
    pub fn new(
        network_output: Sender<(NetworkMessage, OutboundNetworkMessageType)>,
        command_tx: CommandTx,
    ) -> Self {
        Self {
            network_output,
            command_tx,
            sub_process: None,
        }
    }

    pub fn create_sub_process(&self, sub_process: u64) -> RPCHandler {
        Self {
            network_output: self.network_output.clone(),
            command_tx: self.command_tx.clone(),
            sub_process: Some(sub_process),
        }
    }

    /// Make a RPC to a single `target`
    pub fn call<T>(&self, data: T, target: NodeId) -> Call<T>
    where
        T: Protocol + Serialize,
    {
        Call::new(data, self.network_output.clone(), target, self.sub_process)
    }

    /// Make a RPC to multiple `targets`
    pub fn multi_call<T>(&self, data: T, targets: HashSet<NodeId>) -> GroupCall<T>
    where
        T: Protocol + Serialize,
    {
        GroupCall::new(
            data,
            self.network_output.clone(),
            Some(targets),
            self.sub_process,
        )
    }

    /// Make a RPC broadcast to every connected Node
    pub fn broadcast<T>(&self, data: T) -> GroupCall<T>
    where
        T: Protocol + Serialize,
    {
        GroupCall::new(data, self.network_output.clone(), None, None)
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
    pub async fn subscribe<const N: usize>(
        &self,
        types: [MessageTypes; N],
    ) -> anyhow::Result<Subscription> {
        self.subscription(types).subscribe().await
    }

    pub fn subscription<const N: usize>(&self, types: [MessageTypes; N]) -> SubscriptionBuilder<N> {
        SubscriptionBuilder::new(types, self.command_tx.clone(), self.sub_process)
    }
}

pub struct SubscriptionBuilder<const N: usize> {
    types: [MessageTypes; N],
    keep_alive: bool,
    sub_process: Option<u64>,
    command_tx: CommandTx,
}

impl<const N: usize> SubscriptionBuilder<N> {
    fn new(types: [MessageTypes; N], command_tx: CommandTx, sub_process: Option<u64>) -> Self {
        Self {
            types,
            keep_alive: false,
            sub_process,
            command_tx,
        }
    }

    pub fn keep_alive(mut self) -> Self {
        self.keep_alive = true;
        self
    }

    pub async fn subscribe(self) -> anyhow::Result<Subscription> {
        let drop_guard = Arc::new(Semaphore::new(1));
        let permit = drop_guard.clone().acquire_owned().await?;
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let subscription_keys = self
            .types
            .into_iter()
            .map(|t| SubscriptionKey::new(t, self.sub_process))
            .collect();

        self.command_tx
            .send(Command::AddSubscription {
                subscription_keys,
                tx,
                drop_guard,
                keep_alive: self.keep_alive,
            })
            .await?;
        Ok(Subscription::new(rx, permit))
    }
}
