use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU64},
        Arc,
    },
    time::SystemTime,
};

use tokio::io::AsyncRead;

use crate::{node_id::NodeId, time::system_time_to_secs};

pin_project_lite::pin_project! {
    pub struct ReadHalf {
        #[pin]
        inner: Pin<Box<dyn AsyncRead + Send>>,
        node_id: NodeId,
        last_access: Arc<AtomicU64>,
        is_dropped: Arc<AtomicBool>
    }
}

impl std::fmt::Debug for ReadHalf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadHalf")
            .field("node_id", &self.node_id)
            .field("last_access", &self.last_access)
            .field("is_dropped", &self.is_dropped)
            .finish()
    }
}

impl ReadHalf {
    pub fn new(
        inner: Pin<Box<dyn AsyncRead + Send>>,
        node_id: NodeId,
        last_access: Arc<AtomicU64>,
        is_dropped: Arc<AtomicBool>,
    ) -> Self {
        Self {
            inner,
            node_id,
            last_access,
            is_dropped,
        }
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn set_dropped(&self) {
        self.is_dropped
            .store(true, std::sync::atomic::Ordering::SeqCst)
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
