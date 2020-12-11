use std::{cmp::Ord, collections::HashMap, hash::Hash, path::{Path, PathBuf}, time::SystemTime};
use serde::{Serialize, Deserialize };
use tokio::{fs::{self, File}, io::{AsyncRead, AsyncWrite, AsyncWriteExt}};

use crate::{IronCarrierError, config::Config, deletion_tracker::DeletionTracker, network::streaming::DirectStream};

/// Represents a local file
#[derive(Debug, Serialize, Deserialize)]
pub struct FileInfo {
    pub alias: String,
    pub path: PathBuf,
    
    pub modified_at: Option<SystemTime>,
    pub created_at: Option<SystemTime>,
    pub deleted_at: Option<SystemTime>,
    pub size: Option<u64>
}

impl FileInfo {
    /// constructs a [FileInfo] from a relative [PathBuf]
    pub fn new(alias: String, relative_path: PathBuf, metadata: std::fs::Metadata) -> Self {
        FileInfo {
            alias,
            path: relative_path,
            created_at: metadata.created().ok(),
            modified_at: metadata.modified().ok(),
            size: Some(metadata.len()),
            deleted_at: None
        }
                
    }
    
    pub fn new_deleted(alias: String, relative_path: PathBuf, deleted_at: Option<SystemTime>) -> Self {
        
        FileInfo{
            alias,
            path: relative_path,
            created_at: None,
            modified_at: None,
            size: None,
            deleted_at: deleted_at.or_else(|| Some(SystemTime::now()))
        }
    }

    pub fn to_local_file(&self, config: &Config) -> Option<FileInfo> {
        let path = self.get_absolute_path(config).ok()?;
        let metadata = path.metadata().ok()?;

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
    
    let deletion_tracker = DeletionTracker::new(root_path);
    let mut files: Vec<FileInfo> = deletion_tracker.get_files().await?
        .into_iter()
        .map(|(k,v)| FileInfo::new_deleted(
            alias.to_owned(), k, Some(v))
        )
        .collect();
        

    while let Some(path) = paths.pop() {
        let mut entries = fs::read_dir(path).await
            .map_err(|_| IronCarrierError::IOReadingError)?;
        while let Some(entry) = entries.next_entry().await.map_err(|_| IronCarrierError::IOReadingError)? {
            let path = entry.path();
            
            if is_special_file(&path) { continue; }
            
            if path.is_dir() {
                paths.push(path);
                continue;
            }

            let metadata = path.metadata().map_err(|_| IronCarrierError::IOReadingError)?;

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

pub async fn delete_file(file_info: &FileInfo, config: &Config) -> crate::Result<()> {
    let path = file_info.get_absolute_path(config)?;
    if !path.exists() {
        return Ok(())
    } else if path.is_dir() {
        tokio::fs::remove_dir(path).await
            .map_err(|_| IronCarrierError::IOWritingError)
    } else {
        tokio::fs::remove_file(path).await
            .map_err(|_| IronCarrierError::IOWritingError)
    }
}

pub async fn move_file<'b>(src_file: &'b FileInfo, dest_file: &'b FileInfo, config: &Config) -> crate::Result<()> {
    let src_path = src_file.get_absolute_path(config)?;
    let dest_path = dest_file.get_absolute_path(config)?;

    tokio::fs::rename(src_path, dest_path).await
        .map_err(|_| IronCarrierError::IOWritingError)
}

pub async fn write_file<T : AsyncRead + AsyncWrite + Unpin>(file_info: &FileInfo, config: &Config, direct_stream: &mut DirectStream<T>) -> crate::Result<()> {
    let final_path = file_info.get_absolute_path(config)?;
    let mut temp_path = final_path.clone();
    
    temp_path.set_extension("ironcarrier");

    if let Some(parent) = temp_path.parent() {
        if !parent.exists() {
            tokio::fs::create_dir_all(parent).await.map_err(|_| IronCarrierError::IOWritingError)?;
        }
    }

    let mut file = File::create(&temp_path).await.map_err(|_| IronCarrierError::IOWritingError)?;
    direct_stream.read_stream(file_info.size.unwrap() as usize, &mut file).await.map_err(|_| IronCarrierError::NetworkIOReadingError)?;
    
    file.flush().await.map_err(|_| IronCarrierError::IOWritingError)?;

    tokio::fs::rename(&temp_path, &final_path).await.map_err(|_| IronCarrierError::IOWritingError)?;
    filetime::set_file_mtime(&final_path, filetime::FileTime::from_system_time(file_info.modified_at.unwrap())).map_err(|_| IronCarrierError::IOWritingError)?;

    Ok(())
}

pub fn is_special_file(path: &Path) -> bool {
    path.file_name()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.ends_with("ironcarrier"))
        .unwrap_or_default()
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
            size: Some(100),
            deleted_at: None
        };
        let files = vec![file];
        assert_eq!(calculate_hash(&files), 1185603756799400450);
    }

    #[test]
    fn test_is_temp_file() {
        assert!(!is_special_file(Path::new("some_file.txt")));
        assert!(is_special_file(Path::new("some_file.ironcarrier")));
        assert!(is_special_file(Path::new(".ironcarrier")));
    }
}