use tokio::sync::mpsc::{Receiver, Sender};

use crate::hash_type_id::TypeId;

use super::RPCMessage;

pin_project_lite::pin_project! {
    pub struct Subscriber {
        subscriptions: Vec<TypeId>,
        remove_subscriptions: Sender<Vec<TypeId>>,
        #[pin]
        rx: Receiver<RPCMessage>,
    }
}

impl Subscriber {
    pub fn new(
        subscriptions: Vec<TypeId>,
        rx: Receiver<RPCMessage>,
        remove_subscriptions: Sender<Vec<TypeId>>,
    ) -> Self {
        Self {
            subscriptions,
            rx,
            remove_subscriptions,
        }
    }

    pub async fn free(self) {
        if let Err(err) = self.remove_subscriptions.send(self.subscriptions).await {
            log::error!("Failed to free subscriptions {err}");
        }
    }
}

impl tokio_stream::Stream for Subscriber {
    type Item = RPCMessage;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let mut me = self.project();
        me.rx.poll_recv(cx)
    }
}
