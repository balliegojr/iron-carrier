use std::{collections::HashSet, future, pin::pin, str::FromStr, time::Duration};

use iron_carrier_macros::Protocol;
use serde::{Deserialize, Serialize};
use tokio_stream::StreamExt;

use crate::{
    Context,
    config::Config,
    network::rpc::{RPCEvent, RPCMessage},
    node_id::NodeId,
    protocol::{MessageTypes, ProtocolAck},
    state_machine::{Result, State, StateComposer, StateMachineError},
    states::consensus::{ConsensusResult, StartConsensus},
    stream,
    sync_options::SyncOptions,
};

use super::{ConnectAllPeers, Consensus, DiscoverPeers, SetSyncRole};

#[derive(Default, Debug)]
pub struct Daemon {}

impl State for Daemon {
    type Output = ();

    async fn execute(self, context: &Context) -> Result<Self::Output> {
        loop {
            let event = wait_event(context).await?;
            if let Err(err) = execute_event(context, event).await {
                log::error!("{err}")
            }
        }
    }
}

async fn wait_event(context: &Context) -> Result<DaemonEvent> {
    let (watcher_events_sender, watcher_events) = tokio::sync::mpsc::channel(1);
    let _watcher = crate::storage::file_watcher::get_file_watcher(context, watcher_events_sender)?;

    let mut watcher_events = stream::fold_timeout(
        watcher_events,
        Duration::from_secs(context.config.delay_watcher_events),
    );
    let mut full_sync_deadline = pin!(next_cron_schedule(context.config));
    let mut events = context
        .rpc
        .subscription([MessageTypes::StartConsensus, MessageTypes::Follow])
        .keep_alive()
        .subscribe()
        .await?;

    async fn process_event(event: Option<RPCMessage>) -> Result<DaemonEvent> {
        let request = event.ok_or(StateMachineError::Abort)?;
        match request.message_type()? {
            MessageTypes::StartConsensus => {
                Ok(DaemonEvent::SyncWithConsensus(request.into_event()))
            }
            MessageTypes::Follow => Ok(DaemonEvent::BecomeFollower(request.into_event())),
            _ => unreachable!(),
        }
    }

    tokio::select! {
        event = events.next() => {
            process_event(event).await
        }
        Some(to_sync) = watcher_events.recv() => {
             Ok(DaemonEvent::SyncWithoutConsensus(SyncOptions::new(to_sync)))
        }
        _ = &mut full_sync_deadline => {
            Ok(DaemonEvent::SyncWithoutConsensus(Default::default()))
        }
    }
}

#[derive(Debug)]
enum DaemonEvent {
    SyncWithoutConsensus(SyncOptions),
    SyncWithConsensus(RPCEvent<StartConsensus>),
    BecomeFollower(RPCEvent<Follow>),
}

async fn execute_event(context: &Context, event: DaemonEvent) -> Result<()> {
    match event {
        DaemonEvent::SyncWithConsensus(request) => {
            DiscoverPeers::default()
                .and_then(ConnectAllPeers::new)
                .and_then(|nodes| AckRequest { nodes, request })
                .and_then(Consensus::new)
                .and_then(|consensus_result| SetSyncRole::new(consensus_result, Default::default()))
                .execute(context)
                .await
        }
        DaemonEvent::SyncWithoutConsensus(sync_options) => {
            DiscoverPeers::default()
                .and_then(ConnectAllPeers::new)
                .and_then(BypassConsensus::new)
                .and_then(|consensus_result| {
                    SetSyncRole::new(Some(consensus_result), Some(sync_options))
                })
                .execute(context)
                .await
        }

        DaemonEvent::BecomeFollower(request) => {
            DiscoverPeers::default()
                .and_then(ConnectAllPeers::new)
                .and_then(|nodes| AckRequest { nodes, request })
                .and_then(|_| SetSyncRole::new(None, Default::default()))
                .execute(context)
                .await
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Protocol)]
pub struct Follow;

#[derive(Debug)]
struct BypassConsensus {
    nodes: HashSet<NodeId>,
}

impl BypassConsensus {
    fn new(nodes: HashSet<NodeId>) -> Self {
        Self { nodes }
    }
}

impl State for BypassConsensus {
    type Output = ConsensusResult;

    async fn execute(self, context: &Context) -> Result<Self::Output> {
        context
            .rpc
            .multi_call(Follow, self.nodes.clone())
            .timeout(Duration::from_secs(30))
            .ack()
            .await?;

        Ok(ConsensusResult {
            participants: self.nodes,
        })
    }
}

#[derive(Debug)]
struct AckRequest<T> {
    request: RPCEvent<T>,
    nodes: HashSet<NodeId>,
}

impl<T> State for AckRequest<T>
where
    T: ProtocolAck + std::fmt::Debug,
{
    type Output = HashSet<NodeId>;

    async fn execute(self, _context: &Context) -> Result<Self::Output> {
        self.request.ack().await?;
        Ok(self.nodes)
    }
}

async fn next_cron_schedule(config: &Config) {
    let cron_deadline = config.schedule_sync.as_ref().and_then(|schedule_cron| {
        let schedule = cron::Schedule::from_str(schedule_cron).unwrap();

        schedule
            .upcoming(chrono::Local)
            .take(1)
            .next()
            .map(|event| {
                let deadline = std::time::Instant::now()
                    + event
                        .signed_duration_since(chrono::Local::now())
                        .to_std()
                        .unwrap();
                tokio::time::sleep_until(deadline.into())
            })
    });

    match cron_deadline {
        Some(deadline) => deadline.await,
        None => future::pending().await,
    }
}
