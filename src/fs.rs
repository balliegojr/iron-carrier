use std::{cmp::Ord, collections::HashMap, hash::Hash, path::{Path, PathBuf}, time::SystemTime};
use serde::{Serialize, Deserialize };
use tokio::fs;

use crate::{config::Config, IronCarrierError};

/// Represents a local file
#[derive(Debug, Serialize, Deserialize)]
pub struct FileInfo {
    pub alias: String,
    pub path: PathBuf,
    
    pub modified_at: Option<SystemTime>,
    pub created_at: Option<SystemTime>,
    pub size: Option<u64>
}

impl FileInfo {
    /// constructs a [FileInfo] from a relative [PathBuf]
    pub fn new(alias: String, relative_path: PathBuf, metadata: Option<std::fs::Metadata>) -> Self {
        match metadata {
            Some(metadata) => {
                FileInfo{
                    alias,
                    path: relative_path,
                    created_at: metadata.created().ok(),
                    modified_at: metadata.modified().ok(),
                    size: Some(metadata.len())
                }
            }
            None => {
                FileInfo{
                    alias,
                    path: relative_path,
                    created_at: None,
                    modified_at: None,
                    size: None
                }
            }
        }
    }

    pub fn to_local_file(&self, config: &Config) -> Option<FileInfo> {
        let path = self.get_absolute_path(config).ok()?;
        let metadata = path.metadata().ok();

        Some(Self::new(self.alias.clone(), self.path.clone(), metadata))
    }

    pub fn get_absolute_path(&self, config: &Config) -> crate::Result<PathBuf> {
        let root_path = config.paths.get(&self.alias)
            .and_then(|p| p.canonicalize().ok())
            .ok_or_else(|| IronCarrierError::AliasNotAvailable(self.alias.to_owned()))?;

        Ok(root_path.join(&self.path))
    }
}

impl Hash for FileInfo {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.alias.hash(state);
        self.path.hash(state);
        self.modified_at.hash(state);
        self.size.hash(state);
    }
}

impl Eq for FileInfo {}

impl PartialEq for FileInfo {
    fn eq(&self, other: &Self) -> bool {
        self.path.eq(&other.path)
    }
}

impl PartialOrd for FileInfo {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.path.partial_cmp(&other.path)
    }
}

impl Ord for FileInfo {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.path.cmp(&other.path)
    }
}


pub async fn walk_path<'a>(root_path: &Path, alias: &'a str) -> crate::Result<Vec<FileInfo>> {
    let mut paths = vec![root_path.to_owned()];
    let mut files = Vec::new();
    
    
    while let Some(path) = paths.pop() {
        let mut entries = fs::read_dir(path).await
            .map_err(|_| IronCarrierError::IOReadingError)?;
        while let Some(entry) = entries.next_entry().await.map_err(|_| IronCarrierError::IOReadingError)? {
            let path = entry.path();
            
            if is_temp_file(&path) { continue; }
            
            if path.is_dir() {
                paths.push(path);
                continue;
            }

            let metadata = path.metadata().ok();

            // let absolute_path = path.canonicalize()?;
            // let relative_path = path.strip_prefix(root_path)?.to_owned();
            files.push(FileInfo::new(
                alias.to_owned(), 
                path.strip_prefix(root_path).map_err(|_| IronCarrierError::IOReadingError)?.to_owned(), 
                metadata
            ));
        }
    }

    files.sort();
    
    return Ok(files);
}

pub async fn get_files_with_hash<'a>(path: &Path, alias: &'a str) -> crate::Result<(u64, Vec<FileInfo>)>{
    let files = walk_path(path, alias).await?;
    let hash = crate::crypto::calculate_hash(&files);

    return Ok((hash, files));
}

pub async fn get_hash_for_alias(alias_path: &HashMap<String, PathBuf>) -> crate::Result<HashMap<String, u64>> {
    let mut result = HashMap::new();
    
    for (alias, path) in alias_path {
        let (hash, _) = get_files_with_hash(path.as_path(), alias).await?;
        result.insert(alias.to_string(), hash);
    }

    Ok(result)
}

pub fn is_temp_file(path: &Path) -> bool {
    match path.extension() {
        Some(ext) => { ext == "iron-carrier" }
        None => { false }
    }
}

pub fn is_local_file_newer_than_remote(local_file: &FileInfo, remote_file: &FileInfo) -> bool {
    local_file.modified_at > remote_file.modified_at
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::calculate_hash;

    #[tokio::test]
    async fn can_read_local_files() {
        let files = walk_path(&PathBuf::from("./samples"), "a").await.unwrap();

        assert_eq!(files[0].path.to_str(), Some("config_peer_a.toml"));
        assert_eq!(files[1].path.to_str(), Some("config_peer_b.toml"));

        assert_eq!(files[2].path.to_str(), Some("peer_a/sample_file_a"));
        assert_eq!(files[3].path.to_str(), Some("peer_b/sample_file_b"));
        
    }

    #[test]
    fn calc_hash() {
        let file = FileInfo{
            alias: "a".to_owned(),
            created_at: Some(SystemTime::UNIX_EPOCH),
            modified_at: Some(SystemTime::UNIX_EPOCH),
            path: Path::new("./samples/peer_a/sample_file_a").to_owned(),
            size: Some(100)
        };
        let files = vec![file];
        assert_eq!(calculate_hash(&files), 1185603756799400450);
    }

    #[test]
    fn test_is_temp_file() {
        assert!(!is_temp_file(Path::new("some_file.txt")));
        assert!(is_temp_file(Path::new("some_file.iron-carrier")));
    }
}