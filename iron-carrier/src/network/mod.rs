use crate::config::Config;

mod connection;
mod connection_handler;
pub use connection_handler::ConnectionHandler;

mod connection_storage;
pub mod rpc;
pub mod service_discovery;

pub fn get_network_service(config: &'static Config) -> (ConnectionHandler, rpc::RPCHandler) {
    let (on_connect, new_connection) = tokio::sync::mpsc::channel(10);
    let connection_handler = ConnectionHandler::new(config, on_connect);
    let rpc_handler = rpc::rpc_service(new_connection);

    (connection_handler, rpc_handler)
}
