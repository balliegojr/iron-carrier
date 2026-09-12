use crate::relative_path::RelativePathBuf;

use super::Metadata;

pub struct ReadDir {
    inner: Box<dyn Iterator<Item = anyhow::Result<DirEntry>> + Send>,
}

impl ReadDir {
    pub fn new(inner: Box<dyn Iterator<Item = anyhow::Result<DirEntry>> + Send>) -> Self {
        Self { inner }
    }
}

impl Iterator for ReadDir {
    type Item = anyhow::Result<DirEntry>;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

#[derive(Debug, Clone)]
pub struct DirEntry {
    path: RelativePathBuf,
    metadata: Metadata,
}

impl DirEntry {
    pub fn new(path: RelativePathBuf, metadata: Metadata) -> Self {
        Self { path, metadata }
    }

    pub fn path(&self) -> &RelativePathBuf {
        &self.path
    }

    pub fn into_path(self) -> RelativePathBuf {
        self.path
    }

    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }
}
