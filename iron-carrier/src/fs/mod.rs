use async_trait::async_trait;
use std::{path::Path, pin::Pin};
use tokio::io::{self, AsyncRead, AsyncSeek, AsyncWrite};

use crate::{config::PathConfig, ignored_files::IgnoredFiles, relative_path::RelativePathBuf};

mod metadata;
pub use metadata::Metadata;
#[cfg(test)]
pub use metadata::MetadataB;

mod read_dir;
use read_dir::{DirEntry, ReadDir};

#[cfg(test)]
mod mem_fs;
#[cfg(test)]
pub use mem_fs::MemFS;

mod tokio_fs;
pub use tokio_fs::TokioFS;

#[async_trait]
pub trait FS: Send + Sync {
    async fn read_dir(
        &self,
        c: &'static PathConfig,
        p: &RelativePathBuf,
        i: &IgnoredFiles,
    ) -> anyhow::Result<ReadDir>;

    async fn remove(&self, p: &Path) -> io::Result<()>;
    async fn rename(&self, old: &Path, new: &Path) -> io::Result<()>;

    async fn exists(&self, p: &Path) -> bool;
    async fn metadata(&self, p: &Path) -> io::Result<Metadata>;

    async fn read_to_string(&self, p: &Path) -> io::Result<String>;
    async fn open_r(&self, p: &Path) -> io::Result<Pin<Box<dyn FileR>>>;
    async fn open_w(&self, p: &Path, desired_size: u64) -> io::Result<Pin<Box<dyn FileW>>>;

    async fn set_metadata(&self, p: &Path, permissions: u32, modified: u64) -> anyhow::Result<()>;
}

pub trait FileR: AsyncRead + AsyncSeek + Send + Sync {}

#[async_trait]
pub trait FileW: AsyncWrite + FileR {
    async fn sync_all(mut self: Pin<&Self>) -> io::Result<()>;
}
