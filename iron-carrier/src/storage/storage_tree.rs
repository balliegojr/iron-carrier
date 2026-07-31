use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, hash_map::Entry},
    hash::Hash,
};

use crate::{
    fs::Metadata,
    relative_path::{RelativePath, RelativePathBuf},
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct StorageTree<T>
where
    T: StorageFile,
{
    directories: HashMap<DirId, Directory>,
    files: HashMap<FileId, T>,
}

impl<T> StorageTree<T>
where
    T: StorageFile,
{
    pub fn new() -> Self {
        Self {
            directories: HashMap::new(),
            files: HashMap::new(),
        }
    }

    pub fn insert(&mut self, file: T, path: Option<RelativePath>) {
        if file.parent() != DirId(0)
            && !self.directories.contains_key(&file.parent())
            && let Some(path) = path
        {
            self.create_dir(path);
        }

        self.files.insert(file.id(), file);
    }

    fn create_dir(&mut self, path: RelativePath) {
        let dir_id = DirId::new(&path);
        if let Entry::Vacant(entry) = self.directories.entry(dir_id) {
            let name = path.name().to_string();
            if let Some(parent) = path.parent() {
                entry.insert(Directory {
                    name,
                    parent: DirId::new(&parent),
                });

                self.create_dir(parent);
            }
        }
    }

    pub fn files(&self) -> impl Iterator<Item = &T> {
        self.files.values()
    }

    pub fn ids(&self) -> impl Iterator<Item = FileId> {
        self.files.keys().copied()
    }

    pub fn contains_id(&self, id: FileId) -> bool {
        self.files.contains_key(&id)
    }

    pub fn get(&self, id: FileId) -> Option<&T> {
        self.files.get(&id)
    }

    pub fn get_path(&self, id: FileId) -> Option<RelativePathBuf> {
        self.files.get(&id).map(|f| self.build_path(f))
    }

    pub fn build_path(&self, file: &T) -> RelativePathBuf {
        let mut components = vec![file.name().to_string()];
        let mut parent_id = file.parent();
        while let Some(parent) = self.directories.get(&parent_id) {
            components.push(parent.name.clone());
            parent_id = parent.parent
        }

        components.into_iter().rev().collect()
    }

    pub fn remove(&mut self, id: FileId) {
        self.files.remove(&id);
    }

    pub fn move_file(&mut self, id: FileId, dst_path: &RelativePathBuf) {
        if let Some(mut file) = self.files.remove(&id) {
            file.update_location(dst_path.as_path());
            self.insert(file, dst_path.parent());
        }
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.files.len()
    }

    pub fn clear(&mut self) {
        self.files.clear();
        self.directories.clear();
    }

    pub fn drain(&mut self) -> impl Iterator<Item = T> {
        self.files.drain().map(|(_, f)| f)
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Serialize, Deserialize, PartialOrd, Ord)]
pub struct DirId(u64);

impl DirId {
    pub fn new(path: &RelativePath) -> Self {
        Self(path.hash())
    }

    pub const fn to_le_bytes(self) -> [u8; size_of::<u64>()] {
        self.0.to_le_bytes()
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Serialize, Deserialize, PartialOrd, Ord)]
pub struct FileId(u64);

impl From<u64> for FileId {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<FileId> for u64 {
    fn from(value: FileId) -> Self {
        value.0
    }
}

impl FileId {
    pub fn new(path: &RelativePath) -> Self {
        Self(path.hash())
    }

    pub const fn to_le_bytes(self) -> [u8; size_of::<u64>()] {
        self.0.to_le_bytes()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Directory {
    name: String,
    parent: DirId,
}

pub trait StorageFile {
    fn id(&self) -> FileId;
    fn parent(&self) -> DirId;
    fn date(&self) -> u64;
    fn name(&self) -> &str;
    fn update_location(&mut self, new_location: RelativePath);
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct ExistingFileInfo {
    id: FileId,
    parent: DirId,
    name: String,
    modified_at: u64,
    created_at: u64,
    size: u64,
    permissions: u32,
}

impl ExistingFileInfo {
    pub fn new(path: RelativePath, metadata: &Metadata) -> Self {
        let id = FileId::new(&path);
        let name = path.name().to_string();
        let parent = path.parent().map(|p| p.hash()).unwrap_or_default();

        let permissions = metadata.permissions();
        let created_at = metadata.created_as_secs();
        let modified_at = metadata.modified_as_secs();
        let size = metadata.len();

        Self {
            id,
            parent: DirId(parent),
            name,
            modified_at,
            created_at,
            size,
            permissions,
        }
    }

    pub fn needs_sync(&self, other: &Self) -> bool {
        self.modified_at != other.modified_at || self.size != other.size
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn permissions(&self) -> u32 {
        self.permissions
    }
}

impl Hash for ExistingFileInfo {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl StorageFile for ExistingFileInfo {
    fn id(&self) -> FileId {
        self.id
    }

    fn date(&self) -> u64 {
        self.modified_at
    }

    fn parent(&self) -> DirId {
        self.parent
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn update_location(&mut self, new_location: RelativePath) {
        let id = FileId::new(&new_location);
        let name = new_location.name().to_string();
        let parent = new_location.parent().map(|p| p.hash()).unwrap_or_default();

        self.id = id;
        self.name = name;
        self.parent = DirId(parent);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct DeletedFileInfo {
    id: FileId,
    parent: DirId,
    name: String,
    deleted_at: u64,
}

impl DeletedFileInfo {
    pub fn new(path: RelativePath, deleted_at: u64) -> Self {
        let id = FileId::new(&path);
        let name = path.name().to_string();
        let parent = path.parent().map(|p| p.hash()).unwrap_or_default();

        Self {
            id,
            parent: DirId(parent),
            name,
            deleted_at,
        }
    }
}

impl Hash for DeletedFileInfo {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl StorageFile for DeletedFileInfo {
    fn id(&self) -> FileId {
        self.id
    }

    fn date(&self) -> u64 {
        self.deleted_at
    }

    fn parent(&self) -> DirId {
        self.parent
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn update_location(&mut self, new_location: RelativePath) {
        let id = FileId::new(&new_location);
        let name = new_location.name().to_string();
        let parent = new_location.parent().map(|p| p.hash()).unwrap_or_default();

        self.id = id;
        self.name = name;
        self.parent = DirId(parent);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct MovedFileInfo {
    pub id: FileId,
    pub parent: DirId,
    pub name: String,
    pub old_path: RelativePathBuf,
    pub moved_at: u64,
}

impl MovedFileInfo {
    pub fn new(path: RelativePath, old_path: RelativePathBuf, moved_at: u64) -> Self {
        let id = FileId::new(&path);
        let name = path.name().to_string();
        let parent = path.parent().map(|p| p.hash()).unwrap_or_default();

        Self {
            id,
            parent: DirId(parent),
            name,
            old_path,
            moved_at,
        }
    }

    pub fn as_deleted(&self) -> DeletedFileInfo {
        DeletedFileInfo::new(self.old_path.as_path(), self.moved_at)
    }
}

impl Hash for MovedFileInfo {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl StorageFile for MovedFileInfo {
    fn id(&self) -> FileId {
        self.id
    }

    fn date(&self) -> u64 {
        self.moved_at
    }

    fn parent(&self) -> DirId {
        self.parent
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn update_location(&mut self, new_location: RelativePath) {
        let id = FileId::new(&new_location);
        let name = new_location.name().to_string();
        let parent = new_location.parent().map(|p| p.hash()).unwrap_or_default();

        self.id = id;
        self.name = name;
        self.parent = DirId(parent);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relative_path::RelativePathBuf;

    #[test]
    fn get_path_root_file() {
        let mut tree = StorageTree::<DeletedFileInfo>::new();
        let path = RelativePathBuf::from("file.txt");
        let file = DeletedFileInfo::new(path.as_path(), 0);

        tree.insert(file.clone(), path.parent());

        let path = tree.get_path(file.id()).unwrap();
        assert_eq!(path, RelativePathBuf::from("file.txt"));
    }

    #[test]
    fn get_path_nested_file() {
        let mut tree = StorageTree::<DeletedFileInfo>::new();

        let path = RelativePathBuf::from("subdir/file.txt");
        let file = DeletedFileInfo::new(path.as_path(), 0);
        tree.insert(file.clone(), path.parent());

        let path = tree.get_path(file.id()).unwrap();
        assert_eq!(path, RelativePathBuf::from("subdir/file.txt"));
    }

    #[test]
    fn get_path_multi_level() {
        let mut tree = StorageTree::<DeletedFileInfo>::new();
        let path = RelativePathBuf::from("subdir/deep/file.txt");
        let file = DeletedFileInfo::new(path.as_path(), 0);
        tree.insert(file.clone(), path.parent());

        let path = tree.get_path(file.id()).unwrap();
        assert_eq!(path, RelativePathBuf::from("subdir/deep/file.txt"));
    }

    #[test]
    fn get_path_nonexistent_file() {
        let tree = StorageTree::<DeletedFileInfo>::new();
        assert!(tree.get_path(FileId(999)).is_none());
    }

    #[test]
    fn get_path_multiple_files_same_subfolder() {
        let mut tree = StorageTree::<DeletedFileInfo>::new();
        let file1_path = RelativePathBuf::from("subdir/file1.txt");
        let file2_path = RelativePathBuf::from("subdir/file2.txt");
        let file1 = DeletedFileInfo::new(file1_path.as_path(), 0);
        let file2 = DeletedFileInfo::new(file2_path.as_path(), 0);
        tree.insert(file1.clone(), file1_path.parent());
        tree.insert(file2.clone(), file2_path.parent());

        let path1 = tree.get_path(file1.id()).unwrap();
        let path2 = tree.get_path(file2.id()).unwrap();
        assert_eq!(path1, file1_path);
        assert_eq!(path2, file2_path);
    }
}
