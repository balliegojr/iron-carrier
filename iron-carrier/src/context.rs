use crate::{
    config::Config,
    fs::FS,
    leak::Leak,
    network::{ConnectionHandler, rpc::RPCHandler},
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
    pub fs: &'static (dyn FS + Send + Sync),
}

impl Context {
    pub fn new(
        config: &'static Validated<Config>,
        connection_handler: ConnectionHandler,
        rpc: RPCHandler,
        transaction_log: TransactionLog,
    ) -> Self {
        let fs = crate::fs::TokioFS.leak();

        Self {
            config,
            connection_handler,
            rpc,
            transaction_log,
            when_done: None,
            fs,
        }
    }

    pub fn subprocess(&self, id: u64) -> Context {
        let mut context = self.clone();
        context.rpc = context.rpc.create_sub_process(id);
        context
    }

    pub fn with_output_channel(mut self, when_done: Sender<()>) -> Self {
        self.when_done = Some(when_done);
        self
    }
}

#[cfg(test)]
pub async fn local_contexts<const LENGTH: usize>() -> [Context; LENGTH] {
    use std::str::FromStr;

    fn config(id: u64) -> &'static Validated<Config> {
        Validated::new(Config {
            node_id_hashed: id.into(),
            storages: [(
                "a".to_string(),
                crate::config::PathConfig::from_str("/").unwrap(),
            )]
            .into(),
            ..Default::default()
        })
        .leak()
    }

    let mut contexts = vec![test_context(config(0), crate::fs::MemFS::empty().leak())];
    for i in 1..LENGTH {
        let context = test_context(config(i as u64), crate::fs::MemFS::empty().leak());
        for c in contexts.iter() {
            c.connection_handler.connect_context(&context).await;
        }

        contexts.push(context);
    }

    contexts.try_into().unwrap_or_else(|v: Vec<Context>| {
        panic!("Expected a Vec of length {} but it was {}", LENGTH, v.len())
    })
}

#[cfg(test)]
pub fn test_context(config: &'static Validated<Config>, fs: &'static dyn FS) -> Context {
    let transaction_log =
        crate::transaction_log::TransactionLog::memory().expect("Failed to start transaction log");
    let (connection_handler, rpc) = crate::network::get_network_service(config);

    Context {
        config,
        rpc,
        connection_handler,
        transaction_log,
        when_done: None,
        fs,
    }
}
