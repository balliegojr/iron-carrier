use async_trait::async_trait;
use std::{io::Cursor, path::Path, pin::Pin, sync::Arc};
use tokio::{
    io::{self, AsyncReadExt},
    sync::{Mutex, OwnedMutexGuard, RwLock},
};

use crate::{config::PathConfig, fs::MetadataB, relative_path::RelativePathBuf};

use super::{DirEntry, FS, FileR, FileW, Metadata, ReadDir};

pub struct MemFS {
    files: RwLock<std::collections::HashMap<std::path::PathBuf, VirtualFile>>,
}

impl MemFS {
    pub fn empty() -> Self {
        Self {
            files: Default::default(),
        }
    }

    pub fn new<'a>(iter: impl Iterator<Item = (&'a str, Vec<u8>, Metadata)>) -> Self {
        let files = iter
            .map(|(key, data, metadata)| {
                (Path::new(key).to_owned(), VirtualFile::new(data, metadata))
            })
            .collect();

        Self {
            files: RwLock::new(files),
        }
    }
}

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

        let path_metadata: Vec<_> = self
            .files
            .read()
            .await
            .iter()
            .filter(move |(key, _)| {
                key.strip_prefix(&path)
                    .is_ok_and(|p| p.components().count() == 1)
            })
            .map(|(key, virtual_file)| {
                let path = RelativePathBuf::new(c, key.clone()).expect("failed to create path");
                let metadata = virtual_file.metadata.clone();

                (path, metadata)
            })
            .collect();

        let mut entries: Vec<anyhow::Result<DirEntry>> = Default::default();
        for (path, metadata) in path_metadata {
            let entry = DirEntry::new(path, metadata.lock().await.clone());
            entries.push(Ok(entry));
        }

        Ok(ReadDir::new(Box::new(entries.into_iter())))
    }
    async fn remove(&self, p: &Path) -> io::Result<()> {
        self.files
            .write()
            .await
            .remove(p)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "File not found"))?;

        Ok(())
    }

    async fn exists(&self, p: &Path) -> bool {
        self.files.read().await.contains_key(p)
    }

    async fn read_to_string(&self, p: &Path) -> io::Result<String> {
        let virtual_file = self
            .files
            .read()
            .await
            .get(p)
            .cloned()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "File not found"))?;

        let mut data = String::new();
        virtual_file
            .data
            .lock()
            .await
            .read_to_string(&mut data)
            .await?;

        Ok(data)
    }

    async fn metadata(&self, p: &Path) -> io::Result<Metadata> {
        let virtual_file = self
            .files
            .read()
            .await
            .get(p)
            .cloned()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "File not found"))?;

        let metadata = virtual_file.metadata.lock().await;
        Ok(metadata.clone())
    }

    async fn open_r(&self, p: &Path) -> io::Result<Pin<Box<dyn FileR>>> {
        let virtual_file = self
            .files
            .read()
            .await
            .get(p)
            .cloned()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "File not found"))?;

        let mut data = virtual_file.data.lock_owned().await;
        data.set_position(0);

        Ok(Box::pin(VirtualFileGuard {
            data: Pin::new(data),
        }))
    }

    async fn open_w(&self, p: &Path, desired_size: u64) -> io::Result<Pin<Box<dyn FileW>>> {
        let mut files = self.files.write().await;
        let virtual_file = match files.entry(p.to_path_buf()) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.get().clone(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(Default::default()).clone()
            }
        };

        let mut data = virtual_file.data.lock_owned().await;
        data.get_mut().resize(desired_size as usize, 0);
        data.set_position(0);

        let mut metadata = virtual_file.metadata.lock().await;
        *metadata = MetadataB::new().len(desired_size).build();

        Ok(Box::pin(VirtualFileGuard {
            data: Pin::new(data),
        }))
    }

    async fn rename(&self, old: &Path, new: &Path) -> io::Result<()> {
        let mut files = self.files.write().await;
        let file = files
            .remove(old)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Source file not found"))?;

        files.insert(new.to_path_buf(), file);

        Ok(())
    }

    async fn set_metadata(&self, p: &Path, permissions: u32, modified: u64) -> anyhow::Result<()> {
        let virtual_file = self
            .files
            .read()
            .await
            .get(p)
            .cloned()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "File not found"))?;

        let mut metadata = virtual_file.metadata.lock().await;
        *metadata = MetadataB::new()
            .len(metadata.len())
            .permissions(permissions)
            .modified(modified)
            .build();

        Ok(())
    }
}

#[derive(Clone, Default)]
struct VirtualFile {
    data: Arc<Mutex<Cursor<Vec<u8>>>>,
    metadata: Arc<Mutex<Metadata>>,
}

pin_project_lite::pin_project! {
    struct VirtualFileGuard {
        #[pin]
        data: Pin<OwnedMutexGuard<Cursor<Vec<u8>>>>,
    }
}

impl VirtualFile {
    fn new(data: Vec<u8>, metadata: Metadata) -> Self {
        Self {
            data: Arc::new(Mutex::new(Cursor::new(data))),
            metadata: Arc::new(Mutex::new(metadata)),
        }
    }
}

impl FileR for VirtualFileGuard {}

#[async_trait]
impl FileW for VirtualFileGuard {
    async fn sync_all(mut self: Pin<&Self>) -> io::Result<()> {
        Ok(())
    }
}

impl tokio::io::AsyncSeek for VirtualFileGuard {
    fn start_seek(self: Pin<&mut Self>, pos: io::SeekFrom) -> io::Result<()> {
        let me = self.project();
        me.data.start_seek(pos)
    }

    fn poll_complete(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<u64>> {
        let me = self.project();
        me.data.poll_complete(cx)
    }
}

impl tokio::io::AsyncRead for VirtualFileGuard {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let me = self.project();
        me.data.poll_read(cx, buf)
    }
}

impl tokio::io::AsyncWrite for VirtualFileGuard {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        let me = self.project();
        me.data.poll_write(cx, buf)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        let me = self.project();
        me.data.poll_flush(cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        let me = self.project();
        me.data.poll_shutdown(cx)
    }
}
