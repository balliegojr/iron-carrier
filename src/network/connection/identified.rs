use std::{
    ops::{Deref, DerefMut},
    sync::{atomic::AtomicU64, Arc},
    time::SystemTime,
};

use crate::node_id::NodeId;

use super::{Connection, ReadHalf, WriteHalf};

pub struct Identified<T> {
    inner: T,
    node_id: NodeId,
}

impl<T> Identified<T> {
    pub fn new(inner: T, node_id: NodeId) -> Self {
        Self { inner, node_id }
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn into_inner(self) -> T {
        self.inner
    }
}

impl<T> Deref for Identified<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> DerefMut for Identified<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Identified<Connection> {
    pub fn split(self) -> (Identified<WriteHalf>, Identified<ReadHalf>) {
        let node_id = self.node_id;
        let connection_id = self.connection_id;
        let last_access = Arc::new(AtomicU64::new(crate::time::system_time_to_secs(
            SystemTime::now(),
        )));

        (
            Identified::new(
                WriteHalf::new(self.inner.write_half, connection_id, last_access.clone()),
                node_id,
            ),
            Identified::new(
                ReadHalf::new(self.inner.read_half, connection_id, last_access),
                node_id,
            ),
        )
    }
}
