use std::{cmp::Ord, collections::HashMap, hash::Hash, path::{Path, PathBuf}, time::SystemTime};
use serde::{Serialize, Deserialize };
use tokio::fs;

use crate::RSyncError;


#[derive(Debug)]
pub struct LocalFile {
    pub absolute_path: PathBuf,
    pub relative_path: PathBuf,
    
    pub modified_at: SystemTime,
    pub created_at: SystemTime,
    pub size: u64
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RemoteFile {
    pub path: PathBuf,

    pub modified_at: SystemTime,
    pub created_at: SystemTime,
    pub size: u64
}


impl LocalFile {
    /// constructs a [FileInfo] from a relative [PathBuf]
    pub fn new(absolute_path: PathBuf, relative_path: PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        let metadata = absolute_path.metadata()?;
        
        Ok(LocalFile{
            absolute_path: absolute_path,
            relative_path: relative_path,

            created_at: metadata.created()?,
            modified_at: metadata.modified()?,
            size: metadata.len()
        })
    }
}

impl  Hash for LocalFile {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.relative_path.hash(state);
        self.modified_at.hash(state);
        self.size.hash(state);
    }
}

impl Hash for RemoteFile {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.path.hash(state);
        self.modified_at.hash(state);
        self.size.hash(state);
    }
}

impl  Eq for LocalFile {}

impl  PartialEq for LocalFile {
    fn eq(&self, other: &Self) -> bool {
        self.relative_path.eq(&other.relative_path)
    }
}

impl  PartialOrd for LocalFile {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.relative_path.partial_cmp(&other.relative_path)
    }
}

impl  Ord for LocalFile  {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.relative_path.cmp(&other.relative_path)
    }
}


impl  Eq for RemoteFile {}

impl  PartialEq for RemoteFile {
    fn eq(&self, other: &Self) -> bool {
        self.path.eq(&other.path)
    }
}

impl  PartialOrd for RemoteFile {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.path.partial_cmp(&other.path)
    }
}

impl  Ord for RemoteFile  {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.path.cmp(&other.path)
    }
}

impl From<&LocalFile> for RemoteFile {
    fn from(lf: &LocalFile) -> Self {
        RemoteFile {
            created_at: lf.created_at,
            modified_at: lf.modified_at,
            size: lf.size,
            path: lf.relative_path.to_owned()
        }
    }
}

pub async fn walk_path(root_path: &Path) -> Result<Vec<LocalFile>, Box<dyn std::error::Error>> {
    let mut paths = vec![root_path.to_owned()];
    let mut files = Vec::new();
    
    
    while let Some(path) = paths.pop() {
        let mut entries = fs::read_dir(path).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            
            if path.is_dir() {
                paths.push(path);
                continue;
            }

            let absolute_path = path.canonicalize()?;
            let relative_path = path.strip_prefix(root_path)?.to_owned();
            files.push(LocalFile::new(absolute_path, relative_path)?);
        }
    }

    files.sort();
    
    return Ok(files);
}

pub async fn get_files_with_hash(path: &Path) -> Result<(u64, Vec<LocalFile>), RSyncError>{
    let files = walk_path(path).await.map_err(|_| RSyncError::ErrorReadingLocalFiles)?;
    let hash = crate::crypto::calculate_hash(&files);

    return Ok((hash, files));
}

pub async fn get_hash_for_alias(alias_path: &HashMap<String, PathBuf>) -> Result<HashMap<String, u64>, RSyncError> {
    let mut result = HashMap::new();
    
    for (alias, path) in alias_path {
        let (hash, _) = get_files_with_hash(path.as_path()).await?;
        result.insert(alias.to_string(), hash);
    }

    Ok(result)
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::calculate_hash;

    #[tokio::test]
    async fn can_read_local_files() {
        let files = walk_path(&PathBuf::from("./samples")).await.unwrap();

        assert_eq!(files[0].relative_path.to_str(), Some("config_peer_a.toml"));
        assert_eq!(files[1].relative_path.to_str(), Some("config_peer_b.toml"));

        assert_eq!(files[2].relative_path.to_str(), Some("peer_a/sample_file_a"));
        assert_eq!(files[3].relative_path.to_str(), Some("peer_b/sample_file_b"));
        
    }

    #[test]
    fn calc_hash() {
        
        let file = LocalFile::new("./samples/peer_a/sample_file_a".into(), PathBuf::from("sample_file_a")).unwrap();
        let files = vec![file];
        assert_eq!(calculate_hash(&files), 17626586277459498626);
    }
}