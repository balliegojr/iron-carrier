use tokio::sync::{mpsc::Receiver, OwnedSemaphorePermit};

use super::RPCMessage;

pin_project_lite::pin_project! {
    /// Events subscription.
    pub struct Subscription {
        permit: OwnedSemaphorePermit,
        #[pin]
        rx: Receiver<RPCMessage>,
    }
}

impl Subscription {
    pub fn new(rx: Receiver<RPCMessage>, permit: OwnedSemaphorePermit) -> Self {
        Self { rx, permit }
    }
}

impl tokio_stream::Stream for Subscription {
    type Item = RPCMessage;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let mut me = self.project();
        me.rx.poll_recv(cx)
    }
}
