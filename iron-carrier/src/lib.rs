//! Keep your files in sync!
//!
//! Synchronize your files in differents machines on the same network
#![allow(incomplete_features)]
#![feature(hash_extract_if)]
#![feature(btree_extract_if)]
#![feature(let_chains)]
#![feature(result_option_inspect)]
#![feature(is_sorted)]
#![feature(async_fn_in_trait)]

use network::{rpc::RPCHandler, ConnectionHandler};
use node_id::NodeId;
use state_machine::{State, StateComposer};
use states::SetSyncRole;
use sync_options::SyncOptions;
use tokio::sync::mpsc::Sender;
use transaction_log::TransactionLog;
use validation::Validated;

pub mod config;
use config::Config;

pub mod hash_type_id;

pub mod constants;
mod hash_helper;
mod ignored_files;
mod node_id;
pub mod relative_path;
mod stream;
mod sync_options;
mod time;

pub mod leak;

mod network;
mod storage;

mod file_transfer;
mod state_machine;
pub use state_machine::StateMachineError;
mod states;

pub mod transaction_log;
pub mod validation;

#[derive(Clone)]
pub struct SharedState {
    config: &'static Validated<Config>,
    connection_handler: ConnectionHandler,
    rpc: RPCHandler,
    transaction_log: TransactionLog,
    when_done: Option<Sender<()>>,
}

// TODO: implement client mode if the daemon is running (basic a cli that shows sync status)
// TODO: add sync information to the transaction log (when it was last synched and what nodes
// participated

pub async fn run_full_sync(
    config: &'static validation::Validated<config::Config>,
) -> anyhow::Result<()> {
    let (connection_handler, rpc) = network::start_network_service(config).await?;
    let transaction_log = transaction_log::TransactionLog::load(&config.log_path)?;

    let shared_state = SharedState {
        config,
        connection_handler,
        rpc,
        transaction_log,
        when_done: None,
    };

    states::DiscoverPeers::default()
        .and_then(states::ConnectAllPeers::new)
        .and::<states::Consensus>()
        .and_then(|leader_id| SetSyncRole::new(leader_id, SyncOptions::default()))
        .execute(&shared_state)
        .await?;

    Ok(())
}

pub async fn start_daemon(
    config: &'static validation::Validated<config::Config>,
    when_done: Option<Sender<()>>,
) -> anyhow::Result<()> {
    log::trace!("My id is {}", config.node_id);

    let (connection_handler, rpc) = network::start_network_service(config).await?;
    let transaction_log = transaction_log::TransactionLog::load(&config.log_path)?;

    let shared_state = SharedState {
        config,
        connection_handler,
        rpc,
        transaction_log,
        when_done,
    };

    states::DiscoverPeers::default()
        .and_then(states::ConnectAllPeers::new)
        .and::<states::Consensus>()
        .and_then(|leader_id| SetSyncRole::new(leader_id, SyncOptions::default()))
        .then_default_to(states::Daemon::default)
        .execute(&shared_state)
        .await?;

    Ok(())
}
