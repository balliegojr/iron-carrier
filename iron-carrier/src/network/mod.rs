use crate::config::Config;

mod connection;
mod connection_handler;
pub use connection_handler::ConnectionHandler;

mod connection_storage;
pub mod rpc;
pub mod service_discovery;

pub fn get_network_service(config: &'static Config) -> (ConnectionHandler, rpc::RPCHandler) {
    let rpc_handler = rpc::rpc_service();
    let connection_handler = ConnectionHandler::new(config, rpc_handler.clone());

    (connection_handler, rpc_handler)
}
