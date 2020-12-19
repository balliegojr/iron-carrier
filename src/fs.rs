//! This module is responsible for handling file system operations

use std::{time::Duration, cmp::Ord, collections::HashMap, hash::Hash, path::{Path, PathBuf}, time::SystemTime};
use serde::{Serialize, Deserialize };
use tokio::{fs::{self, File}, io::{AsyncRead, AsyncWrite, AsyncWriteExt}};

use crate::{IronCarrierError, config::Config, deletion_tracker::DeletionTracker, network::streaming::DirectStream};

/// Holds the information for a file inside a mapped folder  
///
/// If the file exists, `modified_at`, `created_at` and `size` will be [Some]  
/// Otherwise, only `deleted_at` will be [Some]
/// 
/// The `path` will always be relative to the alias root folder
#[derive(Debug, Serialize, Deserialize)]
pub struct FileInfo {
    /// Root folder alias
    pub alias: String,
    /// File path, it is always relative to the alias root  
    /// The relative path will always be the same, no matter the machine
    pub path: PathBuf,
    
    pub modified_at: Option<u64>,
    pub created_at: Option<u64>,
    pub deleted_at: Option<u64>,
    pub size: Option<u64>
}

fn system_time_to_secs(time: SystemTime) -> Option<u64> {
    time.duration_since(SystemTime::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .ok()
}

impl FileInfo {
    pub fn new(alias: String, relative_path: PathBuf, metadata: std::fs::Metadata) -> Self {
        FileInfo {
            alias,
            path: relative_path,
            created_at: metadata.created().ok().and_then(system_time_to_secs),
            modified_at: metadata.modified().ok().and_then(system_time_to_secs),
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
            deleted_at: deleted_at.or_else(|| Some(SystemTime::now())).and_then(system_time_to_secs)
        }
    }

    pub fn is_local_file_newer(&self, config: &Config) -> bool {
        if self.deleted_at.is_some() {
            true
        } else {
            self.get_absolute_path(config).ok()
                .and_then(|path| path.metadata().ok())
                .and_then(|metadata| metadata.created().ok())
                .and_then(system_time_to_secs)
                .and_then(|created_at| Some( created_at > self.created_at.unwrap()))
                .unwrap_or(false)
        }
    }

    /// Returns the absolute path of the file for this file system  
    /// Using the provided root path for the alias in [Config]
    pub fn get_absolute_path(&self, config: &Config) -> crate::Result<PathBuf> {
        let mut root_path = config.paths.get(&self.alias)
            .and_then(|p| p.canonicalize().ok())
            .ok_or_else(|| IronCarrierError::AliasNotAvailable(self.alias.to_owned()))?;

        root_path.extend(self.path.components());

        Ok(root_path)
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

/// Returns a sorted vector with the entire folder structure for the given path
///
/// This function will look for deletes files in the [DeletionTracker] log and append all entries to the return list  
/// files with name or extension `.ironcarrier` will be ignored
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

/// This function returns the result of [walk_path] along with the hash for the file list
pub async fn get_files_with_hash<'a>(path: &Path, alias: &'a str) -> crate::Result<(u64, Vec<FileInfo>)>{
    let files = walk_path(path, alias).await?;
    let hash = crate::crypto::calculate_hash(&files);

    return Ok((hash, files));
}

/// This function will return a [HashMap] containing the alias as key and the hash as value
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

/// Reads file content from [DirectStream] and writes it to [FileInfo] path
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

    let mod_time = SystemTime::UNIX_EPOCH + Duration::from_secs(file_info.modified_at.unwrap());
    filetime::set_file_mtime(&final_path, filetime::FileTime::from_system_time(mod_time)).map_err(|_| IronCarrierError::IOWritingError)?;
    

    Ok(())
}

/// Returns true if `path` name or extension are .ironcarrier
pub fn is_special_file(path: &Path) -> bool {
    path.file_name()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.ends_with("ironcarrier"))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::calculate_hash;

    #[tokio::test]
    async fn can_read_local_files() -> Result<(), Box<dyn std::error::Error>> {
        fs::create_dir_all("./tmp/fs_read_local_files").await?;
        File::create("./tmp/fs_read_local_files/file_1").await?;
        File::create("./tmp/fs_read_local_files/file_2").await?;

        let files = walk_path(&PathBuf::from("./tmp/fs_read_local_files"), "a").await.unwrap();

        assert_eq!(files[0].path.to_str(), Some("file_1"));
        assert_eq!(files[1].path.to_str(), Some("file_2"));
        
        fs::remove_dir_all("./tmp/fs_read_local_files").await?;

        Ok(())
    }

    #[test]
    fn calc_hash() {
        let file = FileInfo{
            alias: "a".to_owned(),
            created_at: Some(0),
            modified_at: Some(0),
            path: Path::new("./some_file_path").to_owned(),
            size: Some(100),
            deleted_at: None
        };
        
        let files = vec![file];
        assert_eq!(calculate_hash(&files), 1762848629165523426);
    }

    #[test]
    fn test_is_special_file() {
        assert!(!is_special_file(Path::new("some_file.txt")));
        assert!(is_special_file(Path::new("some_file.ironcarrier")));
        assert!(is_special_file(Path::new(".ironcarrier")));
    }
}