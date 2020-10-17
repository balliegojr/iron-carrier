use std::{cmp::Ord, time::SystemTime};
use std::path::{ PathBuf, Path };
use std::hash::{Hash};
use serde::{Serialize, Deserialize };

#[derive(Hash, Debug, Serialize, Deserialize, Clone)]
pub struct FileInfo {
    pub path: PathBuf,
    pub modified_at: SystemTime,
    pub created_at: SystemTime,
    pub size: u64
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

pub fn walk_path<'a >(root_path: &'a str) -> Result<Vec<FileInfo>, Box<dyn std::error::Error>> {
    let root_path = Path::new(root_path).to_path_buf();
    let mut paths = vec![root_path.clone()];
    let mut files = Vec::new();
    
    while let Some(path) = paths.pop() {
        let entries = path.read_dir()?;
        for entry in entries {
            let entry = entry?;

            let path = entry.path();
            let metadata = entry.metadata()?;

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

pub fn get_files_with_hash(path: &str) -> Result<(u64, Vec<FileInfo>), Box<dyn std::error::Error>>{
    let files = walk_path(path)?;
    let hash = crate::crypto::calculate_hash(&files);

    return Ok((hash, files));
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_read_local_files() {
        let files = walk_path("./samples").unwrap();
        assert_eq!(files[0].path.to_str(), Some("config_peer_a.toml"));
        assert_eq!(files[1].path.to_str(), Some("config_peer_b.toml"));

        assert_eq!(files[2].path.to_str(), Some("peer_a/sample_file_a"));
        assert_eq!(files[3].path.to_str(), Some("peer_b/sample_file_b"));
        
    }
}