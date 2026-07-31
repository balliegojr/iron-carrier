use std::{
    collections::{HashMap, HashSet, VecDeque, hash_map::Entry},
    sync::Arc,
    time::Duration,
};

use crate::{constants::DEFAULT_NETWORK_TIMEOUT, protocol::MessageTypes};
use tokio::sync::{
    Semaphore,
    mpsc::{Receiver, Sender},
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
pub use subscription::Subscription;

pub type CommandRx = Receiver<Command>;
pub type CommandTx = Sender<Command>;

pub enum Command {
    AddConnection(Connection),
    QueryIsConnectedTo(NodeId, tokio::sync::oneshot::Sender<bool>),
    AddSubscription {
        subscription_keys: Vec<SubscriptionKey>,
        tx: Sender<RPCMessage>,
        drop_guard: Arc<Semaphore>,
        keep_alive: bool,
    },
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub enum SubscriptionKey {
    MessageType(MessageTypes),
    SubProcess(MessageTypes, u64),
}

impl SubscriptionKey {
    pub fn new(message_type: MessageTypes, sub_process: Option<u64>) -> Self {
        match sub_process {
            Some(sub_process) => Self::SubProcess(message_type, sub_process),
            None => Self::MessageType(message_type),
        }
    }
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
    dead_letter: HashMap<SubscriptionKey, VecDeque<(NodeId, NetworkMessage, Deadline)>>,
    inflight_requests: HashMap<u16, InFlightMessage>,

    subscriptions: HashMap<SubscriptionKey, (Sender<RPCMessage>, bool)>,

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
        let has_cleanup = !(state.dead_letter.is_empty()
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
                    Command::AddSubscription{ subscription_keys, tx, drop_guard, keep_alive  } => {
                        add_new_subscription(
                            &subscription_keys,
                            tx.clone(),
                            &mut state,
                            drop_guard,
                            remove_consumers_tx.clone(),
                            keep_alive
                        )
                        .await;

                        send_deadletter_messages(&subscription_keys, tx, &mut state, &net_out_sender).await;
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
    let Some(type_id) = message.type_id() else {
        log::error!("Received message without type_id {:?}", message);
        return;
    };

    let subscription_key = SubscriptionKey::new(type_id, message.sub_process());

    if let Some(message) =
        try_message_consumers(state, net_out_sender, message, node_id, subscription_key).await
    {
        state
            .dead_letter
            .entry(subscription_key)
            .or_default()
            .push_back((
                node_id,
                message,
                Deadline::new(Duration::from_secs(DEFAULT_NETWORK_TIMEOUT)),
            ));
    }
}

async fn try_message_consumers(
    state: &mut RPCState,
    net_out_sender: &Sender<(NetworkMessage, OutboundNetworkMessageType)>,
    message: NetworkMessage,
    node_id: NodeId,
    subscription_key: SubscriptionKey,
) -> Option<NetworkMessage> {
    if let Entry::Occupied(mut entry) = state.subscriptions.entry(subscription_key) {
        let consumer = &entry.get_mut().0;
        match consumer
            .send(RPCMessage::new(message, node_id, net_out_sender.clone()))
            .await
        {
            Ok(_) => None,
            Err(err) => {
                entry.remove_entry();
                Some(err.0.into())
            }
        }
    } else {
        log::trace!("No consumers found for subscription {subscription_key:?}");
        Some(message)
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

async fn add_new_subscription(
    subscription_keys: &[SubscriptionKey],
    consumer: Sender<RPCMessage>,
    state: &mut RPCState,
    drop_guard: Arc<Semaphore>,
    remove_consumers: Sender<Vec<SubscriptionKey>>,
    keep_alive: bool,
) {
    for subscription_key in subscription_keys.iter() {
        state
            .subscriptions
            .insert(*subscription_key, (consumer.clone(), keep_alive));
        log::trace!("added subscription to {subscription_key:?}");
    }

    if !keep_alive {
        let subscription_keys = subscription_keys.to_vec();
        tokio::spawn(async move {
            let _ = drop_guard.acquire().await;
            let _ = remove_consumers.send(subscription_keys).await;
        });
    }
}

async fn send_deadletter_messages(
    subscritions: &[SubscriptionKey],
    consumer: Sender<RPCMessage>,
    state: &mut RPCState,
    net_out_sender: &Sender<(NetworkMessage, OutboundNetworkMessageType)>,
) {
    for subscription_key in subscritions.iter() {
        if let Entry::Occupied(mut entry) = state.dead_letter.entry(*subscription_key) {
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
    }
}

async fn cleanup_resources(state: &mut RPCState) {
    state.connections.remove_stale();
    let has_connections = !state.connections.is_empty();

    state.subscriptions.retain(|_, (consumer, keep_alive)| {
        !consumer.is_closed() && (*keep_alive || has_connections)
    });

    for (_, expired_message) in state
        .inflight_requests
        .extract_if(|_, waiting| waiting.is_expired())
    {
        if let Err(err) = expired_message.send_timeout().await {
            log::warn!("Failed to send timeout message {err}");
        }
    }

    state.dead_letter.retain(|_, messages| {
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
        protocol::MessageTypes,
        states::consensus::{ConsensusReached, RequestVote, StartConsensus},
    };

    #[tokio::test]
    pub async fn ensure_rpc_single_call_times_out() -> anyhow::Result<()> {
        let [zero, one] = crate::context::local_contexts().await;

        ping_rpc(Duration::from_secs(1), one);

        assert!(
            zero.rpc
                .call(ConsensusReached, 1.into())
                .ack()
                .await
                .is_ok()
        );
        assert!(zero.rpc.call(StartConsensus, 1.into()).ack().await.is_err());

        Ok(())
    }

    #[tokio::test]
    pub async fn ensure_rpc_multi_call_times_out() -> anyhow::Result<()> {
        let [zero, one, two] = crate::context::local_contexts().await;

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
        let [zero, one, two] = crate::context::local_contexts().await;

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
        let [zero, one, two] = crate::context::local_contexts().await;
        let task = tokio::spawn(async move {
            let context = zero;

            let mut sub = context
                .rpc
                .subscribe([MessageTypes::StartConsensus, MessageTypes::ConsensusReached])
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

    #[tokio::test]
    pub async fn ensure_messages_are_sent_to_subprocess() -> anyhow::Result<()> {
        let [zero, one] = crate::context::local_contexts().await;

        async fn subscribe(context: Context, term: u32) {
            let mut sub = context
                .rpc
                .subscription([MessageTypes::StartConsensus])
                .subscribe()
                .await
                .expect("failed to subscribe");

            while let Some(event) = sub.next().await {
                event
                    .reply(RequestVote { term })
                    .await
                    .expect("failed to reply from process 1");
            }
        }

        async fn call(context: Context, term: u32) {
            let replies = context
                .rpc
                .multi_call(StartConsensus, [0.into()].into())
                .result()
                .await
                .expect("failed to receive result from sub process 1")
                .replies();

            assert_eq!(replies.len(), 1, "received no replies");
            assert_eq!(replies[0].data::<RequestVote>().unwrap().term, term);
        }

        tokio::spawn(subscribe(zero.subprocess(1), 1));
        tokio::spawn(subscribe(zero.subprocess(2), 2));

        let call_one = tokio::spawn(call(one.subprocess(1), 1));
        let call_two = tokio::spawn(call(one.subprocess(2), 2));

        let join = tokio::join!(call_one, call_two);
        assert!(join.0.is_ok());
        assert!(join.1.is_ok());

        Ok(())
    }

    fn ping_rpc(wait_time: Duration, context: Context) {
        tokio::spawn(async move {
            // Necessary to move the whole context, otherwise it gets dropped
            let context = context;

            let mut sub = context
                .rpc
                .subscribe([MessageTypes::StartConsensus, MessageTypes::ConsensusReached])
                .await
                .unwrap();

            while let Some(message) = sub.next().await {
                tokio::spawn(async move {
                    tokio::time::sleep(wait_time).await;

                    match message.message_type() {
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
