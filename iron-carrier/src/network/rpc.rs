use std::{
    collections::{BTreeSet, HashMap, HashSet, VecDeque, hash_map::Entry},
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
    in_flight_message::{InFlightMessage, ReplyType},
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
mod in_flight_message;
mod network_event_decoder;
mod network_message;
mod rpc_handler;
mod rpc_message;
mod rpc_reply;
mod subscription;

pub use group_call::GroupCallResponse;
pub use rpc_handler::RPCHandler;
pub use rpc_message::{RPCEvent, RPCMessage};
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

pub fn rpc_service(id: NodeId) -> RPCHandler {
    let (net_out_tx, net_out_rx) = tokio::sync::mpsc::channel(10);
    let (command_tx, command_rx) = tokio::sync::mpsc::channel(1);

    tokio::spawn(rpc_loop(net_out_rx, net_out_tx.clone(), command_rx, id));

    RPCHandler::new(net_out_tx, command_tx)
}

struct RPCState {
    // Holds messages that arrived but didn't had any consumer ready to process it
    orphan_messages: HashMap<SubscriptionKey, VecDeque<(NodeId, NetworkMessage, Deadline)>>,
    // messages that have been sent and are waiting  for replies
    sent_messages: HashMap<u16, InFlightMessage>,
    // messages expiration is handled in a separate collection, for performance
    sent_messages_expiration: BTreeSet<(Deadline, u16)>,
    // Current subscriptions
    subscriptions: HashMap<SubscriptionKey, (Sender<RPCMessage>, bool)>,

    connections: ConnectionStorage,
    id: NodeId,
}

impl RPCState {
    fn new(id: NodeId) -> Self {
        Self {
            orphan_messages: Default::default(),
            sent_messages: Default::default(),
            sent_messages_expiration: Default::default(),
            subscriptions: Default::default(),
            connections: Default::default(),
            id,
        }
    }
}

async fn rpc_loop(
    mut net_out: Receiver<(NetworkMessage, OutboundNetworkMessageType)>,
    net_out_sender: Sender<(NetworkMessage, OutboundNetworkMessageType)>,
    mut command_rx: CommandRx,
    id: NodeId,
) {
    let (net_in_tx, mut net_in_rx) = tokio::sync::mpsc::channel::<IncomingNetworkEvent>(10);
    let (remove_consumers_tx, mut remove_consumers) = tokio::sync::mpsc::channel(1);
    let mut state = RPCState::new(id);

    let mut cleanup = tokio::time::interval(Duration::from_secs(1));

    loop {
        let has_cleanup = !(state.orphan_messages.is_empty() && state.connections.is_empty());

        let next_expiration = state
            .sent_messages_expiration
            .iter()
            .next()
            .map(|(deadline, _id)| tokio::time::sleep_until(deadline.0));

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

                        send_orphan_messages(&subscription_keys, tx, &mut state, &net_out_sender).await;
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

            request = net_out.recv() => {
                let Some((message, send_type)) = request else { break; };
                send_outbound_message(message, send_type, &mut state, &net_out_sender).await;
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
            _ = async { next_expiration.unwrap().await }, if next_expiration.is_some() => {
                expired_messages(&mut state).await;
            }
        }
    }

    // It is necessary to ensure that all output messages are sent before exiting
    while let Ok((message, send_type)) = net_out.try_recv() {
        send_outbound_message(message, send_type, &mut state, &net_out_sender).await;
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
    if message.is_reply() {
        process_reply(message, node_id, state).await;
        return;
    }

    let Some(type_id) = message.type_id() else {
        log::error!("Received message without type_id {message:?} from {node_id:?}");
        return;
    };

    let subscription_key = SubscriptionKey::new(type_id, message.sub_process());

    if let Some(message) =
        try_message_consumers(state, net_out_sender, message, node_id, subscription_key).await
    {
        log::trace!("failed to message consumers for message {:?}", message);
        state
            .orphan_messages
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
    match state.sent_messages.entry(message.id()) {
        Entry::Occupied(mut entry) => {
            let sent_request = entry.get_mut();

            if message.is_ping() {
                state
                    .sent_messages_expiration
                    .remove(&(sent_request.deadline(), message.id()));
                let new_deadline = sent_request.deadline().extend();
                sent_request.set_deadline(new_deadline);
                state
                    .sent_messages_expiration
                    .insert((new_deadline, message.id()));
            } else {
                if let Err(err) = sent_request.process_reply(node_id, message).await {
                    log::error!("Failed to send reply {err}");
                }

                if sent_request.received_all_replies() {
                    state
                        .sent_messages_expiration
                        .remove(&(sent_request.deadline(), sent_request.id()));
                    entry.remove_entry();
                }
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
    net_out_sender: &Sender<(NetworkMessage, OutboundNetworkMessageType)>,
) {
    log::trace!("Sending message {:?}", message);

    match send_type {
        OutboundNetworkMessageType::Response(node_id) => {
            if let Err(err) = send_message_to(&message, node_id, state, net_out_sender).await {
                log::error!("{err}");
            }
        }
        OutboundNetworkMessageType::SingleNode(node_id, callback, timeout) => {
            if let Err(err) = send_message_to(&message, node_id, state, net_out_sender).await {
                log::error!("{err}");
                let _ = callback.send(ReplyType::Cancel(node_id)).await;
            } else {
                let deadline = Deadline::new(timeout);
                state.sent_messages.insert(
                    message.id(),
                    InFlightMessage::new(message.id(), [node_id].into(), callback, deadline),
                );
                state
                    .sent_messages_expiration
                    .insert((deadline, message.id()));
            }
        }
        OutboundNetworkMessageType::MultiNode(nodes, callback, timeout) => {
            let mut nodes_sent = HashSet::new();
            for node_id in nodes {
                if let Err(err) = send_message_to(&message, node_id, state, net_out_sender).await {
                    log::error!("{err}");
                    let _ = callback.send(ReplyType::Cancel(node_id)).await;
                } else {
                    nodes_sent.insert(node_id);
                }
            }

            let deadline = Deadline::new(timeout);
            state.sent_messages.insert(
                message.id(),
                InFlightMessage::new(message.id(), nodes_sent, callback, deadline),
            );
            state
                .sent_messages_expiration
                .insert((deadline, message.id()));
        }
        OutboundNetworkMessageType::Broadcast(callback, timeout) => {
            let mut nodes = HashSet::new();
            for node_id in state.connections.connected_nodes().collect::<Vec<_>>() {
                if let Err(err) = send_message_to(&message, node_id, state, net_out_sender).await {
                    log::error!("{err}");
                    let _ = callback.send(ReplyType::Cancel(node_id)).await;
                } else {
                    nodes.insert(node_id);
                }
            }

            let deadline = Deadline::new(timeout);
            state.sent_messages.insert(
                message.id(),
                InFlightMessage::new(message.id(), nodes, callback, deadline),
            );
            state
                .sent_messages_expiration
                .insert((deadline, message.id()));
        }
    }
}

async fn send_message_to(
    message: &NetworkMessage,
    node_id: NodeId,
    state: &mut RPCState,
    net_out_sender: &Sender<(NetworkMessage, OutboundNetworkMessageType)>,
) -> anyhow::Result<()> {
    if node_id == state.id {
        process_rpc_call(message.clone(), node_id, state, net_out_sender).await;
    } else {
        let connection = state
            .connections
            .get_mut(&node_id)
            .ok_or_else(|| anyhow::anyhow!("Unknown node {node_id}"))?;

        if let Err(err) = message.write_into(connection).await {
            state.connections.remove(node_id);
            anyhow::bail!("Failed to write to connection {err}");
        }
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

async fn send_orphan_messages(
    subscritions: &[SubscriptionKey],
    consumer: Sender<RPCMessage>,
    state: &mut RPCState,
    net_out_sender: &Sender<(NetworkMessage, OutboundNetworkMessageType)>,
) {
    for subscription_key in subscritions.iter() {
        if let Entry::Occupied(mut entry) = state.orphan_messages.entry(*subscription_key) {
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

async fn expired_messages(state: &mut RPCState) {
    for (_deadline, id) in state
        .sent_messages_expiration
        .extract_if(.., |(deadline, _id)| deadline.is_expired())
    {
        if let Some(message) = state.sent_messages.remove(&id)
            && let Err(err) = message.send_timeout().await
        {
            log::warn!("Failed to send timeout message {err}");
        }
    }
}

async fn cleanup_resources(state: &mut RPCState) {
    state.connections.remove_stale();
    let has_connections = !state.connections.is_empty();

    state.subscriptions.retain(|_, (consumer, keep_alive)| {
        !consumer.is_closed() && (*keep_alive || has_connections)
    });

    state.orphan_messages.retain(|_, messages| {
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
        protocol::MessageTypes::{self},
        states::consensus::{RequestVote, StartConsensus, TermVote},
    };

    #[tokio::test]
    pub async fn ensure_rpc_single_call_is_processed() -> anyhow::Result<()> {
        let [zero, one] = crate::context::local_contexts().await;

        ping_rpc(None, one);
        assert!(zero.rpc.call(StartConsensus, 1.into()).ack().await.is_ok());

        Ok(())
    }

    #[tokio::test]
    pub async fn ensure_rpc_single_call_times_out() -> anyhow::Result<()> {
        let [zero, one] = crate::context::local_contexts().await;

        ping_rpc(Some(Duration::from_millis(100)), one);
        assert!(
            zero.rpc
                .call(StartConsensus, 1.into())
                .timeout(Duration::from_secs(1))
                .ack()
                .await
                .is_ok()
        );
        assert!(
            zero.rpc
                .call(StartConsensus, 1.into())
                .timeout(Duration::from_millis(10))
                .ack()
                .await
                .is_err()
        );

        Ok(())
    }

    #[tokio::test]
    pub async fn ensure_rpc_multi_call_times_out() -> anyhow::Result<()> {
        let [zero, one, two] = crate::context::local_contexts().await;

        ping_rpc(None, one);
        ping_rpc(Some(Duration::from_secs(3)), two);

        assert_eq!(
            zero.rpc
                .multi_call(StartConsensus, [1.into()].into())
                .timeout(Duration::from_millis(100))
                .ack()
                .await?,
            HashSet::from([NodeId::from(1)])
        );

        assert_eq!(
            zero.rpc
                .multi_call(StartConsensus, [1.into(), 2.into()].into())
                .timeout(Duration::from_millis(100))
                .ack()
                .await?,
            HashSet::from([NodeId::from(1)])
        );

        assert_eq!(
            zero.rpc
                .multi_call(RequestVote { term: 1 }, [1.into()].into())
                .timeout(Duration::from_millis(100))
                .result()
                .await?
                .replies()
                .len(),
            1
        );

        match zero
            .rpc
            .multi_call(RequestVote { term: 1 }, [1.into()].into())
            .timeout(Duration::from_millis(100))
            .result()
            .await?
        {
            GroupCallResponse::Complete(replies) => assert_eq!(replies.len(), 1),
            GroupCallResponse::Partial(_, _) => unreachable!("Unexpected response"),
        }

        match zero
            .rpc
            .multi_call(RequestVote { term: 1 }, [1.into(), 2.into()].into())
            .timeout(Duration::from_millis(100))
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

        ping_rpc(None, one);
        ping_rpc(Some(Duration::from_secs(3)), two);

        assert_eq!(
            zero.rpc
                .broadcast(StartConsensus)
                .timeout(Duration::from_millis(100))
                .ack()
                .await?,
            HashSet::from([NodeId::from(1)])
        );

        match zero
            .rpc
            .broadcast(RequestVote { term: 1 })
            .timeout(Duration::from_millis(100))
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
            // tokio::time::sleep(Duration::from_secs(1)).await;
            drop(one);
            drop(two);
        });

        Ok(task.await?)
    }

    #[tokio::test]
    pub async fn ensure_messages_are_sent_to_subprocess() -> anyhow::Result<()> {
        let [zero, one] = crate::context::local_contexts().await;

        async fn subscribe(context: Context, sub_process: u32) {
            let mut sub = context
                .rpc
                .subscription([MessageTypes::RequestVote])
                .subscribe()
                .await
                .expect("failed to subscribe");

            while let Some(event) = sub.next().await {
                let event = event.into_event::<RequestVote>();
                let vote = event.data().unwrap().term == sub_process;
                event
                    .reply(TermVote { vote })
                    .await
                    .expect("failed to reply from process 1");
            }
        }

        async fn call(context: Context, term: u32, vote: bool) {
            let replies = context
                .rpc
                .multi_call(RequestVote { term }, [0.into()].into())
                .result()
                .await
                .expect("failed to receive result from sub process 1")
                .replies();

            assert_eq!(replies.len(), 1, "received no replies");
            assert_eq!(replies[0].data().unwrap().vote, vote);
        }

        tokio::spawn(subscribe(zero.subprocess(1), 1));
        tokio::spawn(subscribe(zero.subprocess(2), 2));

        let call_one = tokio::spawn(call(one.subprocess(1), 1, true));
        let call_two = tokio::spawn(call(one.subprocess(2), 1, false));

        let join = tokio::join!(call_one, call_two);
        assert!(join.0.is_ok());
        assert!(join.1.is_ok());

        Ok(())
    }

    fn ping_rpc(wait_time: Option<Duration>, context: Context) {
        tokio::spawn(async move {
            // Necessary to move the whole context, otherwise it gets dropped
            let context = context;

            let mut sub = context
                .rpc
                .subscribe([MessageTypes::StartConsensus, MessageTypes::RequestVote])
                .await
                .unwrap();

            while let Some(message) = sub.next().await {
                tokio::spawn(async move {
                    if let Some(wait_time) = wait_time {
                        tokio::time::sleep(wait_time).await;
                    }

                    match message.message_type() {
                        Ok(MessageTypes::StartConsensus) => {
                            let event = message.into_event::<StartConsensus>();
                            event.ack().await.expect("failed to ack message");
                        }
                        Ok(MessageTypes::RequestVote) => {
                            let event = message.into_event::<RequestVote>();
                            event
                                .reply(TermVote { vote: true })
                                .await
                                .expect("failed to reply message");
                        }
                        _ => unreachable!(),
                    }
                });
            }
        });
    }
}
