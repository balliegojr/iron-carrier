use async_trait::async_trait;
use std::{
    path::Path,
    pin::Pin,
    time::{Duration, SystemTime},
};
use tokio::{
    fs::OpenOptions,
    io::{self},
};

use crate::{config::PathConfig, ignored_files::IgnoredFiles, relative_path::RelativePathBuf};

use super::{DirEntry, FS, FileR, FileW, Metadata, ReadDir, metadata};

impl FileR for tokio::fs::File {}

#[async_trait]
impl FileW for tokio::fs::File {
    async fn sync_all(mut self: Pin<&Self>) -> io::Result<()> {
        (*self).sync_data().await
    }
}

pub struct TokioFS;

#[async_trait]
impl FS for TokioFS {
    async fn read_dir(
        &self,
        c: &'static PathConfig,
        p: &RelativePathBuf,
        i: &IgnoredFiles,
    ) -> anyhow::Result<ReadDir> {
        std::fs::read_dir(p.absolute(c)?.as_path())
            .map(|read_dir| {
                let inner: Vec<_> = read_dir
                    .filter_map(|entry| {
                        let entry = entry.ok()?;
                        let path = RelativePathBuf::new(c, entry.path()).ok()?;
                        if i.is_ignored(&path.build_path()) {
                            return None;
                        }

                        let metadata = Metadata::new(entry.metadata().ok()?);
                        Some(Ok(DirEntry::new(path, metadata)))
                    })
                    .collect();

                ReadDir::new(Box::new(inner.into_iter()))
            })
            .map_err(anyhow::Error::from)
    }

    async fn remove(&self, p: &Path) -> io::Result<()> {
        if p.is_dir() {
            tokio::fs::remove_dir_all(p).await
        } else {
            tokio::fs::remove_file(p).await
        }
    }

    async fn rename(&self, old: &Path, new: &Path) -> io::Result<()> {
        if !old.exists() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("source does not exist: {old:?}"),
            ));
        }

        if let Some(parent) = new.parent()
            && !parent.exists()
        {
            log::debug!("creating folders {parent:?}");
            tokio::fs::create_dir_all(parent).await?;
        }

        tokio::fs::rename(old, new).await
    }

    async fn exists(&self, p: &Path) -> bool {
        tokio::fs::try_exists(p).await.unwrap_or_default()
    }

    async fn read_to_string(&self, p: &Path) -> io::Result<String> {
        tokio::fs::read_to_string(p).await
    }

    async fn metadata(&self, p: &Path) -> io::Result<Metadata> {
        tokio::fs::metadata(p).await.map(Metadata::new)
    }

    async fn open_r(&self, p: &Path) -> io::Result<Pin<Box<dyn FileR>>> {
        let file = tokio::fs::File::open(p).await?;
        Ok(Box::pin(file))
    }

    async fn open_w(&self, p: &Path, desired_size: u64) -> io::Result<Pin<Box<dyn FileW>>> {
        if let Some(parent) = p.parent()
            && !parent.exists()
        {
            log::debug!("creating folders {parent:?}");
            tokio::fs::create_dir_all(parent).await?;
        }

        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .read(true)
            .open(p)
            .await?;

        let metadata = file.metadata().await?;
        if metadata.len() != desired_size {
            file.set_len(desired_size).await?;
        }

        Ok(Box::pin(file))
    }

    async fn set_metadata(&self, p: &Path, permissions: u32, modified: u64) -> anyhow::Result<()> {
        if permissions > 0 {
            metadata::set_file_permissions(p, permissions)?;
        }

        let mod_time = filetime::FileTime::from_system_time(
            SystemTime::UNIX_EPOCH + Duration::from_secs(modified),
        );
        log::trace!("setting {p:?} modification time to {mod_time}");
        filetime::set_file_mtime(p, mod_time)?;

        Ok(())
    }
}
