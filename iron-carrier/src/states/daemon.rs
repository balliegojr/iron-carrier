use std::{future, pin::pin, str::FromStr, time::Duration};

use tokio_stream::StreamExt;

use crate::{
    config::Config,
    hash_type_id::HashTypeId,
    node_id::NodeId,
    state_machine::{Result, State, StateComposer, StateMachineError},
    stream,
    sync_options::SyncOptions,
    Context,
};

use super::{
    consensus::{ConsensusReached, StartConsensus},
    ConnectAllPeers, Consensus, DiscoverPeers, SetSyncRole,
};

#[derive(Default, Debug)]
pub struct Daemon {}

impl State for Daemon {
    type Output = ();

    async fn execute(self, context: &Context) -> Result<Self::Output> {
        let _service_discovery =
            crate::network::service_discovery::get_service_discovery(context.config).await?;

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
    let _watcher = crate::storage::file_watcher::get_file_watcher(
        context.config,
        context.transaction_log.clone(),
        watcher_events_sender,
    )?;

    let mut watcher_events = stream::fold_timeout(
        watcher_events,
        Duration::from_secs(context.config.delay_watcher_events),
    );

    let mut full_sync_deadline = pin!(next_cron_schedule(context.config));
    let mut events = context
        .rpc
        .subscribe(&[StartConsensus::ID, ConsensusReached::ID])
        .await?;

    let event = tokio::select! {
        event = events.next() => {
            let request = event.ok_or(StateMachineError::Abort)?;
            match request.type_id() {
                StartConsensus::ID => {
                    DiscoverPeers::default()
                       .and_then(ConnectAllPeers::new)
                       .execute(context).await?;

                    request.ack().await?;
                    Ok(DaemonEvent::SyncWithConsensus)

                }
                ConsensusReached::ID => {
                    let leader = request.node_id();
                    request.ack().await?;

                    Ok(DaemonEvent::BecomeFollower(leader))
                }
                _ => unreachable!()
            }
        }
        Some(to_sync) = watcher_events.recv() => {
             Ok(DaemonEvent::SyncWithoutConsensus(SyncOptions::new(to_sync)))
        }
        _ = &mut full_sync_deadline => {
            Ok(DaemonEvent::SyncWithoutConsensus(Default::default()))
        }
    };

    event
}

#[derive(Debug)]
enum DaemonEvent {
    SyncWithoutConsensus(SyncOptions),
    SyncWithConsensus,
    BecomeFollower(NodeId),
}

async fn execute_event(context: &Context, event: DaemonEvent) -> Result<()> {
    match event {
        DaemonEvent::SyncWithConsensus => {
            Consensus::default()
                .and_then(|leader| SetSyncRole::new(leader, Default::default()))
                .execute(context)
                .await
        }
        DaemonEvent::SyncWithoutConsensus(sync_options) => {
            DiscoverPeers::default()
                .and_then(ConnectAllPeers::new)
                .and_then(|_| BypassConsensus)
                .and_then(|leader_id| SetSyncRole::new(leader_id, sync_options))
                .execute(context)
                .await
        }

        DaemonEvent::BecomeFollower(leader_id) => {
            DiscoverPeers::default()
                .and_then(ConnectAllPeers::new)
                .and_then(|_| SetSyncRole::new(leader_id, Default::default()))
                .execute(context)
                .await
        }
    }
}

#[derive(Debug)]
struct BypassConsensus;

impl State for BypassConsensus {
    type Output = NodeId;

    async fn execute(self, context: &Context) -> Result<Self::Output> {
        context.rpc.broadcast(ConsensusReached).ack().await?;
        Ok(context.config.node_id_hashed)
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
