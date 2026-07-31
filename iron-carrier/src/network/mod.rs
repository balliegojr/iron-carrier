use std::time::Duration;

use crate::config::Config;

mod connection;
mod connection_handler;
pub use connection_handler::ConnectionHandler;

mod connection_storage;
pub mod rpc;
pub mod service_discovery;
pub use rpc::Subscription;

pub fn get_network_service(config: &'static Config) -> (ConnectionHandler, rpc::RPCHandler) {
    let rpc_handler = rpc::rpc_service();
    let connection_handler = ConnectionHandler::new(config, rpc_handler.clone());

    (connection_handler, rpc_handler)
}

async fn backoff_retry<I, E, Fn, Fut>(timeout: u64, operation: Fn) -> Result<I, E>
where
    Fn: FnMut() -> Fut,
    Fut: Future<Output = Result<I, backoff::Error<E>>>,
{
    let backoff = backoff::ExponentialBackoffBuilder::new()
        .with_max_elapsed_time(Some(Duration::from_secs(timeout)))
        .build();

    backoff::future::retry(backoff, operation).await
}
