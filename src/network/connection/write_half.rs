use std::{
    pin::Pin,
    sync::{atomic::AtomicU64, Arc},
    time::SystemTime,
};

use tokio::io::AsyncWrite;

use crate::{
    constants::PEER_STALE_CONNECTION, network::connection::ConnectionId, time::system_time_to_secs,
};

pin_project_lite::pin_project! {
    pub struct WriteHalf {
        #[pin]
        inner: Pin<Box<dyn AsyncWrite + Send + Sync>>,
        pub connection_id: ConnectionId,
        last_access: Arc<AtomicU64>
    }
}

impl WriteHalf {
    pub fn new(
        inner: Pin<Box<dyn AsyncWrite + Send + Sync>>,
        connection_id: ConnectionId,
        last_access: Arc<AtomicU64>,
    ) -> Self {
        Self {
            inner,
            connection_id,
            last_access,
        }
    }

    fn touch(self: Pin<&mut Self>) {
        self.last_access.store(
            system_time_to_secs(SystemTime::now()),
            std::sync::atomic::Ordering::SeqCst,
        );
    }

    pub fn is_stale(&self) -> bool {
        let last_access = self.last_access.load(std::sync::atomic::Ordering::SeqCst);
        let now = system_time_to_secs(SystemTime::now());
        (now - last_access) > PEER_STALE_CONNECTION
    }
}

impl AsyncWrite for WriteHalf {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        match self.as_mut().project().inner.poll_write(cx, buf) {
            ev @ std::task::Poll::Ready(_) => {
                self.touch();
                ev
            }
            ev => ev,
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        match self.as_mut().project().inner.poll_flush(cx) {
            ev @ std::task::Poll::Ready(_) => {
                self.touch();
                ev
            }
            ev => ev,
        }
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        self.project().inner.poll_shutdown(cx)
    }
}
