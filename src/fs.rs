use std::{cmp::Ord, collections::HashMap, hash::Hash, path::{PathBuf, Path}, time::SystemTime};
use serde::{Serialize, Deserialize };
use tokio::fs;

use crate::RSyncError;


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FileInfo {
    pub path: PathBuf,
    pub modified_at: SystemTime,
    pub created_at: SystemTime,
    pub size: u64
}

impl Hash for FileInfo {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
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

pub async fn walk_path<'a >(root_path: &'a str) -> Result<Vec<FileInfo>, Box<dyn std::error::Error>> {
    let root_path = Path::new(root_path).to_path_buf();
    let mut paths = vec![root_path.clone()];
    let mut files = Vec::new();
    
    
    while let Some(path) = paths.pop() {
        let mut entries = fs::read_dir(path).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            let metadata = entry.metadata().await?;

            if path.is_dir() {
                paths.push(path);
                continue;
            }

            files.push(FileInfo{
                path: path.strip_prefix(&root_path).unwrap().to_path_buf(),
                created_at: metadata.created()?,
                modified_at: metadata.modified()?,
                size: metadata.len()
            })
        }
    }

    files.sort();
    
    return Ok(files);
}

pub async fn get_files_with_hash(path: &str) -> Result<(u64, Vec<FileInfo>), RSyncError>{
    let files = walk_path(path).await.map_err(|_| RSyncError::ErrorReadingLocalFiles)?;
    let hash = crate::crypto::calculate_hash(&files);

    return Ok((hash, files));
}

pub async fn get_hash_for_alias(alias_path: &HashMap<String, String>) -> Result<HashMap<String, u64>, RSyncError> {
    let mut result = HashMap::new();
    
    for (alias, path) in alias_path {
        let (hash, _) = get_files_with_hash(path).await?;
        result.insert(alias.to_string(), hash);
    }

    Ok(result)
}


#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn can_read_local_files() {
        let files = walk_path("./samples").await.unwrap();
        

        assert_eq!(files[0].path.to_str(), Some("config_peer_a.toml"));
        assert_eq!(files[1].path.to_str(), Some("config_peer_b.toml"));

        assert_eq!(files[2].path.to_str(), Some("peer_a/sample_file_a"));
        assert_eq!(files[3].path.to_str(), Some("peer_b/sample_file_b"));
        
    }
}