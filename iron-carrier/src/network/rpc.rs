use std::{
    collections::{hash_map::Entry, HashMap, HashSet, VecDeque},
    sync::Arc,
    time::Duration,
};

use crate::{constants::DEFAULT_NETWORK_TIMEOUT, message_types::MessageTypes};
use tokio::sync::{
    mpsc::{Receiver, Sender},
    Semaphore,
};
use tokio_stream::StreamExt;

use crate::node_id::NodeId;

use self::{
    deadline::Deadline,
    message_waiting_reply::{InFlightMessage, ReplyType},
    network_event_decoder::NetWorkEventDecoder,
    network_message::NetworkMessage,
};

use super::{
    connection::{Connection, ReadHalf},
    connection_storage::ConnectionStorage,
};

mod call;
mod deadline;
mod group_call;
mod message_waiting_reply;
mod network_event_decoder;
mod network_message;
mod rpc_handler;
mod rpc_message;
mod rpc_reply;
mod subscription;

pub use group_call::GroupCallResponse;
pub use rpc_handler::RPCHandler;
pub use rpc_message::RPCMessage;

pub type CommandRx = Receiver<Command>;
pub type CommandTx = Sender<Command>;

pub enum Command {
    AddConnection(Connection),
    QueryIsConnectedTo(NodeId, tokio::sync::oneshot::Sender<bool>),
    AddSubscription {
        message_types: Vec<MessageTypes>,
        tx: Sender<RPCMessage>,
        drop_guard: Arc<Semaphore>,
        need_connections: bool,
    },
}

pub fn rpc_service() -> RPCHandler {
    let (net_out_tx, net_out_rx) = tokio::sync::mpsc::channel(10);
    let (command_tx, command_rx) = tokio::sync::mpsc::channel(1);

    tokio::spawn(rpc_loop(net_out_rx, net_out_tx.clone(), command_rx));

    RPCHandler::new(net_out_tx, command_tx)
}

#[derive(Default)]
struct RPCState {
    // Holds messages that arrived but didn't had any consumer ready to process it
    waiting_for_consumers: HashMap<MessageTypes, VecDeque<(NodeId, NetworkMessage, Deadline)>>,
    inflight_requests: HashMap<u16, InFlightMessage>,

    subscriptions: HashMap<MessageTypes, (Sender<RPCMessage>, bool)>,

    connections: ConnectionStorage,
}

async fn rpc_loop(
    mut net_out: Receiver<(NetworkMessage, OutboundNetworkMessageType)>,
    net_out_sender: Sender<(NetworkMessage, OutboundNetworkMessageType)>,
    mut command_rx: CommandRx,
) {
    let (net_in_tx, mut net_in_rx) = tokio::sync::mpsc::channel::<IncomingNetworkEvent>(10);
    let (remove_consumers_tx, mut remove_consumers) = tokio::sync::mpsc::channel(1);
    let mut state = RPCState::default();

    let mut cleanup = tokio::time::interval(Duration::from_secs(1));

    loop {
        let has_cleanup = !(state.waiting_for_consumers.is_empty()
            // && state.subscriptions.is_empty()
            && state.inflight_requests.is_empty()
            && state.connections.is_empty());

        tokio::select! {
            biased;
            command = command_rx.recv() => {
                let Some(command) = command else {
                    break;
                };

                match command {
                    Command::AddConnection(connection) => {
                        if let Some(read) = state.connections.insert(connection) {
                            tokio::spawn(read_network_data(read, net_in_tx.clone()));
                        }
                    }
                    Command::QueryIsConnectedTo(node_id, reply_tx) => {
                        let _ = reply_tx.send(state.connections.is_connected(node_id));
                    }
                    Command::AddSubscription{ message_types, tx, drop_guard, need_connections } => {
                        add_new_consumers(
                            message_types,
                            tx,
                            &mut state,
                            &net_out_sender,
                            drop_guard,
                            remove_consumers_tx.clone(),
                            need_connections
                        )
                        .await;
                    }
                }
            }
            request = net_in_rx.recv() => {
                let Some(event) = request else {
                    break;
                };

                match event {
                    IncomingNetworkEvent::Disconnected(node_id) => {
                        state.connections.remove(node_id);
                        if state.connections.is_empty() {
                            cleanup_resources(&mut state).await;
                        }
                    }
                    IncomingNetworkEvent::Message(node_id, message) => {
                        log::trace!("Received message {:?}", message);
                        if message.is_reply() {
                            process_reply(message, node_id, &mut state).await;
                        } else {
                            process_rpc_call(
                                message,
                                node_id,
                                &mut state,
                                &net_out_sender,
                            )
                            .await;
                        }
                    }
                }
            }

            request = net_out.recv() => {
                let Some((message, send_type)) = request else { break; };
                send_outbound_message(message, send_type, &mut state).await;
            }


            request = remove_consumers.recv() => {
                let Some(consumer_types) = request else {
                    break;
                };
                for consumer_type in consumer_types {
                    state.subscriptions.remove(&consumer_type);
                }
            }

            _ = cleanup.tick(), if has_cleanup => {
                cleanup_resources(&mut state).await;
            }
        }
    }

    // It is necessary to ensure that all output messages are sent before exiting
    while let Ok((message, send_type)) = net_out.try_recv() {
        send_outbound_message(message, send_type, &mut state).await;
    }
}

#[derive(Debug)]
enum IncomingNetworkEvent {
    Disconnected(NodeId),
    Message(NodeId, NetworkMessage),
}

#[derive(Debug)]
pub enum OutboundNetworkMessageType {
    Response(NodeId),
    SingleNode(NodeId, Sender<ReplyType>, Duration),
    MultiNode(HashSet<NodeId>, Sender<ReplyType>, Duration),
    Broadcast(Sender<ReplyType>, Duration),
}

async fn process_rpc_call(
    message: NetworkMessage,
    node_id: NodeId,
    state: &mut RPCState,
    net_out_sender: &Sender<(NetworkMessage, OutboundNetworkMessageType)>,
) {
    let Ok(type_id) = message.type_id() else {
        log::error!("Received message without type_id {:?}", message);
        return;
    };

    if let Entry::Occupied(mut entry) = state.subscriptions.entry(type_id) {
        if let Err(err) = entry
            .get_mut()
            .0
            .send(RPCMessage::new(message, node_id, net_out_sender.clone()))
            .await
        {
            let message: NetworkMessage = err.0.into();
            state
                .waiting_for_consumers
                .entry(type_id)
                .or_default()
                .push_back((
                    node_id,
                    message,
                    Deadline::new(Duration::from_secs(DEFAULT_NETWORK_TIMEOUT)),
                ));
            entry.remove_entry();
        }
    } else {
        state
            .waiting_for_consumers
            .entry(type_id)
            .or_default()
            .push_back((
                node_id,
                message,
                Deadline::new(Duration::from_secs(DEFAULT_NETWORK_TIMEOUT)),
            ));
    }
}

async fn process_reply(message: NetworkMessage, node_id: NodeId, state: &mut RPCState) {
    match state.inflight_requests.entry(message.id()) {
        Entry::Occupied(mut entry) => {
            let sent_request = entry.get_mut();
            if let Err(err) = sent_request.process_reply(node_id, message).await {
                log::error!("Failed to send reply {err}");
            }

            if sent_request.received_all_replies() {
                entry.remove_entry();
            }
        }
        Entry::Vacant(_) => {
            log::error!(
                "Received an unexpected message from {node_id}: {:?}",
                message,
            );
        }
    }
}

async fn send_outbound_message(
    message: NetworkMessage,
    send_type: OutboundNetworkMessageType,
    state: &mut RPCState,
) {
    log::trace!("Sending message {:?}", message);

    match send_type {
        OutboundNetworkMessageType::Response(node_id) => {
            if let Err(err) = send_message_to(&message, node_id, &mut state.connections).await {
                log::error!("{err}");
            }
        }
        OutboundNetworkMessageType::SingleNode(node_id, callback, timeout) => {
            if let Err(err) = send_message_to(&message, node_id, &mut state.connections).await {
                log::error!("{err}");
                let _ = callback.send(ReplyType::Cancel(node_id)).await;
            } else {
                state.inflight_requests.insert(
                    message.id(),
                    InFlightMessage::new(message.id(), [node_id].into(), callback, timeout),
                );
            }
        }
        OutboundNetworkMessageType::MultiNode(nodes, callback, timeout) => {
            let mut nodes_sent = HashSet::new();
            for node_id in nodes {
                if let Err(err) = send_message_to(&message, node_id, &mut state.connections).await {
                    log::error!("{err}");
                    let _ = callback.send(ReplyType::Cancel(node_id)).await;
                } else {
                    nodes_sent.insert(node_id);
                }
            }

            state.inflight_requests.insert(
                message.id(),
                InFlightMessage::new(message.id(), nodes_sent, callback, timeout),
            );
        }
        OutboundNetworkMessageType::Broadcast(callback, timeout) => {
            let mut nodes = HashSet::new();
            for node_id in state.connections.connected_nodes().collect::<Vec<_>>() {
                if let Err(err) = send_message_to(&message, node_id, &mut state.connections).await {
                    log::error!("{err}");
                    let _ = callback.send(ReplyType::Cancel(node_id)).await;
                } else {
                    nodes.insert(node_id);
                }
            }

            state.inflight_requests.insert(
                message.id(),
                InFlightMessage::new(message.id(), nodes, callback, timeout),
            );
        }
    }
}

async fn send_message_to(
    message: &NetworkMessage,
    node_id: NodeId,
    connections: &mut ConnectionStorage,
) -> anyhow::Result<()> {
    let connection = connections
        .get_mut(&node_id)
        .ok_or_else(|| anyhow::anyhow!("Unknown node {node_id}"))?;

    if let Err(err) = message.write_into(connection).await {
        connections.remove(node_id);
        anyhow::bail!("Failed to write to connection {err}");
    }

    Ok(())
}

async fn add_new_consumers(
    consumer_types: Vec<MessageTypes>,
    consumer: Sender<RPCMessage>,
    state: &mut RPCState,
    net_out_sender: &Sender<(NetworkMessage, OutboundNetworkMessageType)>,
    drop_guard: Arc<Semaphore>,
    remove_consumers: Sender<Vec<MessageTypes>>,
    need_connections: bool,
) {
    for consumer_type in consumer_types.iter() {
        if let Entry::Occupied(mut entry) = state.waiting_for_consumers.entry(*consumer_type) {
            while let Some((node_id, message, deadline)) = entry.get_mut().pop_front() {
                if let Err(err) = consumer
                    .send(RPCMessage::new(message, node_id, net_out_sender.clone()))
                    .await
                {
                    entry
                        .get_mut()
                        .push_front((node_id, err.0.into(), deadline));
                    return;
                }
            }

            entry.remove_entry();
        }

        state
            .subscriptions
            .insert(*consumer_type, (consumer.clone(), need_connections));
    }

    tokio::spawn(async move {
        let _ = drop_guard.acquire().await;
        let _ = remove_consumers.send(consumer_types).await;
    });
}

async fn cleanup_resources(state: &mut RPCState) {
    state.connections.remove_stale();
    let has_connections = !state.connections.is_empty();

    state
        .subscriptions
        .retain(|_, (consumer, need_connections)| {
            !consumer.is_closed() && (!*need_connections || *need_connections == has_connections)
        });

    for (_, expired_message) in state
        .inflight_requests
        .extract_if(|_, waiting| waiting.is_expired())
    {
        if let Err(err) = expired_message.send_timeout().await {
            log::warn!("Failed to send timeout message {err}");
        }
    }

    state.waiting_for_consumers.retain(|_, messages| {
        messages.retain(|(_, _, deadline)| !deadline.is_expired());
        !messages.is_empty()
    });
}

async fn read_network_data(read_connection: ReadHalf, event_stream: Sender<IncomingNetworkEvent>) {
    let node_id = read_connection.node_id();

    let mut stream = tokio_util::codec::FramedRead::new(read_connection, NetWorkEventDecoder {});
    while let Some(event) = stream.next().await {
        match event {
            Ok(event) => {
                if let Err(err) = event_stream
                    .send(IncomingNetworkEvent::Message(node_id, event))
                    .await
                {
                    log::error!("Error sending event to event stream {err}");
                    break;
                }
            }
            Err(err) => {
                log::error!("error reading from peer {err}");
                break;
            }
        }
    }

    stream.into_inner().set_dropped();

    let _ = event_stream
        .send(IncomingNetworkEvent::Disconnected(node_id))
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        context::Context,
        message_types::MessageTypes,
        states::consensus::{ConsensusReached, StartConsensus},
    };

    #[tokio::test]
    pub async fn ensure_rpc_single_call_times_out() -> anyhow::Result<()> {
        let [zero, one] = crate::context::local_contexts().await?;

        ping_rpc(Duration::from_secs(1), one);

        assert!(zero
            .rpc
            .call(ConsensusReached, 1.into())
            .ack()
            .await
            .is_ok());
        assert!(zero.rpc.call(StartConsensus, 1.into()).ack().await.is_err());

        Ok(())
    }

    #[tokio::test]
    pub async fn ensure_rpc_multi_call_times_out() -> anyhow::Result<()> {
        let [zero, one, two] = crate::context::local_contexts().await?;

        ping_rpc(Duration::from_millis(100), one);
        ping_rpc(Duration::from_secs(3), two);

        assert_eq!(
            zero.rpc
                .multi_call(ConsensusReached, [1.into()].into())
                .timeout(Duration::from_secs(1))
                .ack()
                .await?,
            HashSet::from([NodeId::from(1)])
        );
        assert_eq!(
            zero.rpc
                .multi_call(ConsensusReached, [1.into(), 2.into()].into())
                .timeout(Duration::from_secs(1))
                .ack()
                .await?,
            HashSet::from([NodeId::from(1)])
        );

        assert_eq!(
            zero.rpc
                .multi_call(StartConsensus, [1.into()].into())
                .timeout(Duration::from_secs(1))
                .result()
                .await?
                .replies()
                .len(),
            1
        );

        match zero
            .rpc
            .multi_call(StartConsensus, [1.into()].into())
            .timeout(Duration::from_secs(1))
            .result()
            .await?
        {
            GroupCallResponse::Complete(replies) => assert_eq!(replies.len(), 1),
            GroupCallResponse::Partial(_, _) => unreachable!("Unexpected response"),
        }

        match zero
            .rpc
            .multi_call(ConsensusReached, [1.into(), 2.into()].into())
            .timeout(Duration::from_secs(1))
            .result()
            .await?
        {
            GroupCallResponse::Complete(_) => unreachable!("Unexpected response"),
            GroupCallResponse::Partial(replies, nodes) => {
                assert_eq!(replies.len(), 1);
                assert_eq!(nodes, HashSet::from([NodeId::from(2)]))
            }
        }

        Ok(())
    }

    #[tokio::test]
    pub async fn ensure_rpc_broadcast_times_out() -> anyhow::Result<()> {
        let [zero, one, two] = crate::context::local_contexts().await?;

        ping_rpc(Duration::from_millis(100), one);
        ping_rpc(Duration::from_secs(3), two);

        assert_eq!(
            zero.rpc
                .broadcast(ConsensusReached)
                .timeout(Duration::from_secs(1))
                .ack()
                .await?,
            HashSet::from([NodeId::from(1)])
        );

        match zero
            .rpc
            .broadcast(StartConsensus)
            .timeout(Duration::from_secs(1))
            .result()
            .await?
        {
            GroupCallResponse::Complete(_) => unreachable!("Unexpected response"),
            GroupCallResponse::Partial(replies, nodes) => {
                assert_eq!(replies.len(), 1);
                assert_eq!(nodes, HashSet::from([NodeId::from(2)]))
            }
        }

        Ok(())
    }

    #[tokio::test]
    pub async fn ensure_subscription_aborts_when_no_connections() -> anyhow::Result<()> {
        let [zero, one, two] = crate::context::local_contexts().await?;
        let task = tokio::spawn(async move {
            let context = zero;

            let mut sub = context
                .rpc
                .subscribe(&[MessageTypes::StartConsensus, MessageTypes::ConsensusReached])
                .await
                .unwrap();

            assert!(sub.next().await.is_none());
        });

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            drop(one);
            drop(two);
        });

        Ok(task.await?)
    }

    fn ping_rpc(wait_time: Duration, context: Context) {
        tokio::spawn(async move {
            // Necessary to move the whole context, otherwise it gets dropped
            let context = context;

            let mut sub = context
                .rpc
                .subscribe(&[MessageTypes::StartConsensus, MessageTypes::ConsensusReached])
                .await
                .unwrap();

            while let Some(message) = sub.next().await {
                tokio::spawn(async move {
                    tokio::time::sleep(wait_time).await;

                    match message.type_id() {
                        Ok(MessageTypes::StartConsensus) => {
                            let _ = message.reply(ConsensusReached).await;
                        }
                        Ok(MessageTypes::ConsensusReached) => {
                            let _ = message.ack().await;
                        }
                        _ => unreachable!(),
                    }
                });
            }
        });
    }
}
