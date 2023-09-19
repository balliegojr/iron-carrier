use std::{
    pin::Pin,
    sync::{atomic::AtomicU64, Arc},
    time::SystemTime,
};

use tokio::io::AsyncRead;

use crate::{node_id::NodeId, time::system_time_to_secs};

pin_project_lite::pin_project! {
    pub struct ReadHalf {
        #[pin]
        inner: Pin<Box<dyn AsyncRead + Send>>,
        node_id: NodeId,
        last_access: Arc<AtomicU64>
    }
}

impl ReadHalf {
    pub fn new(
        inner: Pin<Box<dyn AsyncRead + Send>>,
        node_id: NodeId,
        last_access: Arc<AtomicU64>,
    ) -> Self {
        Self {
            inner,
            node_id,
            last_access,
        }
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    fn touch(self: Pin<&mut Self>) {
        self.last_access.store(
            system_time_to_secs(SystemTime::now()),
            std::sync::atomic::Ordering::SeqCst,
        );
    }
}

impl AsyncRead for ReadHalf {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let me = self.as_mut().project();
        match me.inner.poll_read(cx, buf) {
            ev @ std::task::Poll::Ready(_) => {
                self.touch();
                ev
            }
            ev => ev,
        }
    }
}
