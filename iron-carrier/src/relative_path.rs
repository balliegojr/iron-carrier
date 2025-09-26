use std::{
    hash::Hash,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::{config::PathConfig, hash_helper::HASHER};

/// Represents a relative Path starting from the root of the storage.
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RelativePathBuf {
    components: Vec<String>,
}

impl RelativePathBuf {
    pub fn root() -> Self {
        Self {
            components: Vec::new(),
        }
    }

    /// Returns true if this path is the root path (empty path)
    pub fn is_root(&self) -> bool {
        self.components.is_empty()
    }

    pub fn new(path_config: &PathConfig, mut path: PathBuf) -> anyhow::Result<Self> {
        if !path.has_root() {
            path = path.canonicalize()?;
        }

        let inner = path.strip_prefix(&path_config.path.canonicalize()?)?;

        let parts = inner
            .components()
            .map(|c| {
                c.as_os_str()
                    .to_str()
                    .map(|s| s.to_string())
                    .ok_or_else(|| anyhow::anyhow!("failed to convert path"))
            })
            .collect::<anyhow::Result<Vec<String>>>()?;

        Ok(Self { components: parts })
    }

    /// Returns the absolute for the given [PathConfig]
    pub fn absolute(&self, path_config: &PathConfig) -> anyhow::Result<PathBuf> {
        let root_path = path_config.path.canonicalize()?;
        Ok(root_path.join(self.build_path()))
    }

    pub fn build_path(&self) -> PathBuf {
        self.components.iter().collect()
    }

    pub fn as_path(&self) -> RelativePath<'_> {
        RelativePath {
            components: &self.components,
        }
    }

    pub fn name(&self) -> &str {
        name(&self.components)
    }

    pub fn parent(&self) -> Option<RelativePath<'_>> {
        parent(&self.components)
    }

    pub fn without_leading_slash(&self) -> RelativePath<'_> {
        without_leading_slash(&self.components)
    }

    pub fn has_parent(&self, other: &RelativePathBuf) -> bool {
        // check if self has all of other components
        if other.components.len() >= self.components.len() {
            return false;
        }
        if other.components.is_empty() {
            return true; // empty path is always a parent
        }
        if self.components.is_empty() {
            return false; // empty path cannot have a parent
        }

        for (i, component) in other.components.iter().enumerate() {
            if self.components.get(i) != Some(component) {
                return false; // mismatch found
            }
        }

        true // all components match
    }
}

impl From<&Path> for RelativePathBuf {
    fn from(path: &Path) -> Self {
        Self {
            components: path
                .components()
                .map(|c| c.as_os_str().to_str().map(|c| c.to_string()).unwrap())
                .collect(),
        }
    }
}

impl From<&str> for RelativePathBuf {
    fn from(value: &str) -> Self {
        let path: PathBuf = value.into();
        path.as_path().into()
    }
}

impl FromIterator<String> for RelativePathBuf {
    fn from_iter<T: IntoIterator<Item = String>>(iter: T) -> Self {
        Self {
            components: iter.into_iter().collect(),
        }
    }
}

impl std::fmt::Debug for RelativePathBuf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.build_path().fmt(f)
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub struct RelativePath<'a> {
    components: &'a [String],
}

impl<'a> RelativePath<'a> {
    pub fn parent(&self) -> Option<RelativePath<'_>> {
        parent(self.components)
    }

    pub fn name(&self) -> &str {
        name(self.components)
    }

    pub fn build_path(&self) -> PathBuf {
        self.components.iter().collect()
    }

    pub fn hash(&self) -> u64 {
        if self.components.is_empty() {
            return 0;
        }

        let mut digest = HASHER.digest();
        for component in self.components {
            digest.update(component.as_bytes());
        }
        digest.finalize()
    }

    pub fn without_leading_slash(&self) -> RelativePath<'_> {
        without_leading_slash(self.components)
    }
}

impl<'a> std::fmt::Debug for RelativePath<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.build_path().fmt(f)
    }
}

impl<'a> RelativePath<'a> {
    pub fn to_owned(&self) -> RelativePathBuf {
        RelativePathBuf {
            components: self.components.to_vec(),
        }
    }
}

fn name(components: &[String]) -> &str {
    if components.is_empty() {
        ""
    } else {
        components.last().unwrap()
    }
}

fn parent(components: &[String]) -> Option<RelativePath<'_>> {
    if components.is_empty() {
        None
    } else {
        Some(RelativePath {
            components: &components[..components.len() - 1],
        })
    }
}

fn without_leading_slash(components: &[String]) -> RelativePath<'_> {
    if components.first().map(|c| c == "/").unwrap_or_default() {
        RelativePath {
            components: &components[1..],
        }
    } else {
        RelativePath { components }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_path_is_root() {
        let root_path = RelativePathBuf::root();
        assert!(
            root_path.is_root(),
            "Root path should be identified as root"
        );
    }

    #[test]
    fn test_nested_path_is_not_root() {
        let path = RelativePathBuf::from("some/path");
        assert!(
            !path.is_root(),
            "Nested path should not be identified as root"
        );
    }

    #[test]
    fn test_single_file_is_not_root() {
        let path = RelativePathBuf::from("file.txt");
        assert!(
            !path.is_root(),
            "Single file path should not be identified as root"
        );
    }

    #[test]
    fn test_directory_is_not_root() {
        let path = RelativePathBuf::from("dir/");
        assert!(
            !path.is_root(),
            "Directory path should not be identified as root"
        );
    }

    #[test]
    fn test_deep_path_is_not_root() {
        let path = RelativePathBuf::from("a/very/deep/path/structure");
        assert!(
            !path.is_root(),
            "Deep path structure should not be identified as root"
        );
    }

    #[test]
    fn test_hash_empty_path() {
        let path = RelativePathBuf::root();
        let hash1 = path.as_path().hash();
        assert_eq!(hash1, 0, "Hash should be deterministic for empty path");
    }

    #[test]
    fn test_hash_same_paths() {
        let path1 = RelativePathBuf::from("some/test/path");
        let path2 = RelativePathBuf::from("some/test/path");
        assert_eq!(
            path1.as_path().hash(),
            path2.as_path().hash(),
            "Same paths should have same hash"
        );
    }

    #[test]
    fn test_hash_different_paths() {
        let path1 = RelativePathBuf::from("some/test/path1");
        let path2 = RelativePathBuf::from("some/test/path2");
        assert_ne!(
            path1.as_path().hash(),
            path2.as_path().hash(),
            "Different paths should have different hashes"
        );
    }

    #[test]
    fn test_hash_is_order_dependent() {
        let path1 = RelativePathBuf::from("a/b/c");
        let path2 = RelativePathBuf::from("c/b/a");
        assert_ne!(
            path1.as_path().hash(),
            path2.as_path().hash(),
            "Different path orders should have different hashes"
        );
    }
}
