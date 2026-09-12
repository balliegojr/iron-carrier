use async_trait::async_trait;
use std::{io::Cursor, path::Path, pin::Pin, sync::Arc};
use tokio::{
    io::{self, AsyncReadExt},
    sync::{Mutex, OwnedMutexGuard, RwLock},
};

use crate::{
    config::PathConfig, fs::MetadataB, ignored_files::IgnoredFiles, relative_path::RelativePathBuf,
};

use super::{DirEntry, FS, FileR, FileW, Metadata, ReadDir};

pub struct MemFS {
    files: RwLock<std::collections::HashMap<RelativePathBuf, VirtualFile>>,
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
                (
                    RelativePathBuf::from(key)
                        .without_leading_slash()
                        .to_owned(),
                    VirtualFile::new(data, metadata),
                )
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
        _c: &'static PathConfig,
        p: &RelativePathBuf,
        i: &IgnoredFiles,
    ) -> anyhow::Result<ReadDir> {
        let files = self.files.read().await;

        let path_metadata: Vec<_> = files
            .iter()
            .filter(move |(key, _)| !i.is_ignored(key.build_path().as_path()) && key.has_parent(p))
            .map(|(key, virtual_file)| {
                let path = key.clone();
                let metadata = virtual_file.metadata.clone();

                (path, metadata)
            })
            .collect();

        let mut entries: Vec<anyhow::Result<DirEntry>> = Default::default();
        for (path, metadata) in path_metadata {
            // if path has the same parent as p, add it to the entries
            if path.parent().is_some_and(|parent| parent == p.as_path()) {
                let entry = DirEntry::new(path, metadata.lock().await.clone());
                entries.push(Ok(entry));
            } else {
                let mut path = path;
                while let Some(parent) = path.parent() {
                    if parent == p.as_path() {
                        let entry = DirEntry::new(path, MetadataB::dir().build());
                        entries.push(Ok(entry));
                        break;
                    }
                    path = parent.to_owned();
                }
            }
        }

        Ok(ReadDir::new(Box::new(entries.into_iter())))
    }
    async fn remove(&self, p: &Path) -> io::Result<()> {
        let path = RelativePathBuf::from(p).without_leading_slash().to_owned();
        self.files
            .write()
            .await
            .remove(&path)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "File not found"))?;

        Ok(())
    }

    async fn exists(&self, p: &Path) -> bool {
        let path = RelativePathBuf::from(p).without_leading_slash().to_owned();
        self.files.read().await.contains_key(&path)
    }

    async fn read_to_string(&self, p: &Path) -> io::Result<String> {
        let path = RelativePathBuf::from(p).without_leading_slash().to_owned();
        let virtual_file = self
            .files
            .read()
            .await
            .get(&path)
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
        let path = RelativePathBuf::from(p).without_leading_slash().to_owned();
        let virtual_file = self
            .files
            .read()
            .await
            .get(&path)
            .cloned()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "File not found"))?;

        let metadata = virtual_file.metadata.lock().await;
        Ok(metadata.clone())
    }

    async fn open_r(&self, p: &Path) -> io::Result<Pin<Box<dyn FileR>>> {
        let path = RelativePathBuf::from(p).without_leading_slash().to_owned();
        let virtual_file = self
            .files
            .read()
            .await
            .get(&path)
            .cloned()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "File not found"))?;

        let mut data = virtual_file.data.lock_owned().await;
        data.set_position(0);

        Ok(Box::pin(VirtualFileGuard {
            data: Pin::new(data),
        }))
    }

    async fn open_w(&self, p: &Path, desired_size: u64) -> io::Result<Pin<Box<dyn FileW>>> {
        let path = RelativePathBuf::from(p).without_leading_slash().to_owned();
        let mut files = self.files.write().await;
        let virtual_file = match files.entry(path) {
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
        let old = RelativePathBuf::from(old)
            .without_leading_slash()
            .to_owned();
        let file = files
            .remove(&old)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Source file not found"))?;

        let new = RelativePathBuf::from(new)
            .without_leading_slash()
            .to_owned();
        files.insert(new, file);

        Ok(())
    }

    async fn set_metadata(&self, p: &Path, permissions: u32, modified: u64) -> anyhow::Result<()> {
        let path = RelativePathBuf::from(p).without_leading_slash().to_owned();
        let virtual_file = self
            .files
            .read()
            .await
            .get(&path)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::PathConfig, ignored_files::IgnoredFiles, leak::Leak, relative_path::RelativePathBuf,
    };
    use std::str::FromStr;

    #[tokio::test]
    async fn test_read_dir_hierarchy() {
        let config = &PathConfig::from_str("/").unwrap().leak();
        let ignored = IgnoredFiles::empty();

        // Create test data
        let test_data = vec![(
            "a/b/file.txt",
            vec![0u8; 10],
            MetadataB::new().len(10).build(),
        )];
        let mem_fs = MemFS::new(test_data.into_iter());

        // Read root directory
        let mut root_entries = mem_fs
            .read_dir(config, &RelativePathBuf::root(), &ignored)
            .await
            .unwrap();
        let entry = root_entries
            .next()
            .expect("Root directory should have entries")
            .unwrap();

        assert_eq!(entry.path(), &RelativePathBuf::from("a"));
        assert!(entry.metadata().is_dir());
        assert!(
            root_entries.next().is_none(),
            "Root directory should not have more than one entry"
        );

        // Read 'a' directory
        let mut a_entries = mem_fs
            .read_dir(config, &RelativePathBuf::from("a"), &ignored)
            .await
            .unwrap();
        let a_entry = a_entries
            .next()
            .expect("A directory should have entries")
            .unwrap();
        assert_eq!(a_entry.path(), &RelativePathBuf::from("a/b"));
        assert!(a_entry.metadata().is_dir());
        assert!(
            a_entries.next().is_none(),
            "A directory should not have more than one entry"
        );

        // Read 'a/b' directory
        let mut b_entries = mem_fs
            .read_dir(config, &RelativePathBuf::from("a/b"), &ignored)
            .await
            .unwrap();
        let b_entry = b_entries
            .next()
            .expect("B directory should have entries")
            .unwrap();
        assert_eq!(b_entry.path(), &RelativePathBuf::from("a/b/file.txt"));
        assert!(!b_entry.metadata().is_dir());
        assert_eq!(b_entry.metadata().len(), 10, "File length should match");
        assert!(
            b_entries.next().is_none(),
            "B directory should not have more than one entry"
        );
    }
}
