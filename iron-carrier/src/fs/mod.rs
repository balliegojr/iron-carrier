use async_trait::async_trait;
use std::{
    path::Path,
    pin::Pin,
    time::{Duration, SystemTime},
};
use tokio::{
    fs::OpenOptions,
    io::{self, AsyncRead, AsyncSeek, AsyncWrite},
};

use crate::{config::PathConfig, relative_path::RelativePathBuf};

mod metadata;
pub use metadata::Metadata;
#[cfg(test)]
pub use metadata::MetadataB;

mod read_dir;
use read_dir::{DirEntry, ReadDir};

#[async_trait]
pub trait FS: Send + Sync {
    async fn read_dir(
        &self,
        c: &'static PathConfig,
        p: &RelativePathBuf,
    ) -> anyhow::Result<ReadDir>;

    async fn remove(&self, p: &Path) -> io::Result<()>;
    async fn rename(&self, old: &Path, new: &Path) -> io::Result<()>;

    async fn exists(&self, p: &Path) -> bool;
    async fn metadata(&self, p: &Path) -> io::Result<Metadata>;

    async fn read_to_string(&self, p: &Path) -> io::Result<String>;
    async fn open_r(&self, p: &Path) -> io::Result<Pin<Box<dyn FileR>>>;
    async fn open_w(&self, p: &Path, desired_size: u64) -> io::Result<Pin<Box<dyn FileW>>>;

    fn set_metadata(&self, p: &Path, permissions: u32, modified: u64) -> anyhow::Result<()>;
}

pub trait FileR: AsyncRead + AsyncSeek + Send + Sync {}

#[async_trait]
pub trait FileW: AsyncWrite + FileR {
    async fn sync_all(mut self: Pin<&Self>) -> io::Result<()>;
}

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
    ) -> anyhow::Result<ReadDir> {
        std::fs::read_dir(p.absolute(c)?.as_path())
            .map(|read_dir| {
                let inner = Box::new(read_dir.map(|entry| {
                    let entry = entry?;
                    let path = RelativePathBuf::new(c, entry.path())?;
                    let metadata = Metadata::new(entry.metadata().map_err(anyhow::Error::from)?);

                    Ok(DirEntry::new(path, metadata))
                }));

                ReadDir::new(inner)
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
                format!("source does not exist: {:?}", old),
            ));
        }

        if let Some(parent) = new.parent() {
            if !parent.exists() {
                log::debug!("creating folders {:?}", parent);
                tokio::fs::create_dir_all(parent).await?;
            }
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
        if let Some(parent) = p.parent() {
            if !parent.exists() {
                log::debug!("creating folders {:?}", parent);
                tokio::fs::create_dir_all(parent).await?;
            }
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

    fn set_metadata(&self, p: &Path, permissions: u32, modified: u64) -> anyhow::Result<()> {
        if permissions > 0 {
            metadata::set_file_permissions(p, permissions)?;
        }

        let mod_time = filetime::FileTime::from_system_time(
            SystemTime::UNIX_EPOCH + Duration::from_secs(modified),
        );
        log::trace!("setting {:?} modification time to {mod_time}", p);
        filetime::set_file_mtime(p, mod_time)?;

        Ok(())
    }
}

#[cfg(test)]
pub struct MemFS {
    files: std::collections::HashMap<std::path::PathBuf, (Vec<u8>, Metadata)>,
}

#[cfg(test)]
impl MemFS {
    pub fn empty() -> Self {
        Self {
            files: std::collections::HashMap::new(),
        }
    }

    pub fn new<'a>(iter: impl Iterator<Item = (&'a str, Vec<u8>, Metadata)>) -> Self {
        Self {
            files: iter
                .map(|(key, bytes, metadata)| (Path::new(key).to_owned(), (bytes, metadata)))
                .collect(),
        }
    }
}

#[cfg(test)]
#[async_trait]
impl FS for MemFS {
    async fn read_dir(
        &self,
        c: &'static PathConfig,
        p: &RelativePathBuf,
    ) -> anyhow::Result<ReadDir> {
        let path = p
            .absolute(c)?
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("failed to get absolute path"))?
            .to_owned();

        let entries: Vec<anyhow::Result<DirEntry>> = self
            .files
            .iter()
            .filter(move |(key, _)| {
                key.strip_prefix(&path)
                    .is_ok_and(|p| p.components().count() == 1)
            })
            .map(|(key, (_data, metadata))| {
                let path = RelativePathBuf::new(c, key.clone())?;
                let metadata = metadata.clone();

                Ok(DirEntry::new(path, metadata))
            })
            .collect();

        Ok(ReadDir::new(Box::new(entries.into_iter())))
    }
    async fn remove(&self, p: &Path) -> io::Result<()> {
        todo!()
    }

    async fn exists(&self, p: &Path) -> bool {
        self.files.contains_key(p)
    }

    async fn read_to_string(&self, p: &Path) -> io::Result<String> {
        self.files
            .get(p)
            .map(|(data, _)| String::from_utf8_lossy(data).to_string())
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "File not found"))
    }

    async fn metadata(&self, p: &Path) -> io::Result<Metadata> {
        self.files
            .get(p)
            .map(|(_data, metadata)| metadata.clone())
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "File not found"))
    }

    async fn open_r(&self, p: &Path) -> io::Result<Pin<Box<dyn FileR>>> {
        todo!()
    }

    async fn open_w(&self, p: &Path, desired_size: u64) -> io::Result<Pin<Box<dyn FileW>>> {
        todo!()
    }

    async fn rename(&self, _old: &Path, _new: &Path) -> io::Result<()> {
        todo!()
    }

    fn set_metadata(&self, _p: &Path, _permissions: u32, _modified: u64) -> anyhow::Result<()> {
        todo!()
    }
}
