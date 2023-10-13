use crate::{
    config::Config,
    network::{rpc::RPCHandler, ConnectionHandler},
    transaction_log::TransactionLog,
    validation::Validated,
};
use tokio::sync::mpsc::Sender;

#[derive(Clone)]
pub struct Context {
    pub config: &'static Validated<Config>,
    pub rpc: RPCHandler,
    pub connection_handler: ConnectionHandler,
    pub transaction_log: TransactionLog,
    pub when_done: Option<Sender<()>>,
}

impl Context {
    pub fn new(
        config: &'static Validated<Config>,
        connection_handler: ConnectionHandler,
        rpc: RPCHandler,
        transaction_log: TransactionLog,
    ) -> Self {
        Self {
            config,
            connection_handler,
            rpc,
            transaction_log,
            when_done: None,
        }
    }

    pub fn with_output_channel(mut self, when_done: Sender<()>) -> Self {
        self.when_done = Some(when_done);
        self
    }
}

#[cfg(test)]
pub async fn local_contexts<const LENGTH: usize>() -> anyhow::Result<[Context; LENGTH]> {
    let mut contexts = vec![testing_context(0)?];
    for i in 1..LENGTH {
        let context = testing_context(i as u64)?;
        for c in contexts.iter() {
            c.connection_handler.connect_context(&context).await;
        }

        contexts.push(context);
    }

    Ok(contexts.try_into().unwrap_or_else(|v: Vec<Context>| {
        panic!("Expected a Vec of length {} but it was {}", LENGTH, v.len())
    }))
}

#[cfg(test)]
fn testing_context(id: u64) -> anyhow::Result<Context> {
    let config = crate::leak::Leak::leak(Validated::new(Config {
        node_id_hashed: id.into(),
        ..Default::default()
    }));

    let transaction_log = crate::transaction_log::TransactionLog::memory()?;
    let (connection_handler, rpc) = crate::network::get_network_service(config);

    Ok(Context {
        config,
        rpc,
        connection_handler,
        transaction_log,
        when_done: None,
    })
}
