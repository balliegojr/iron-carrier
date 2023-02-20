use crate::{config::PathConfig, constants::IGNORE_FILE_NAME, relative_path::RelativePathBuf};
use globset::{Glob, GlobSet, GlobSetBuilder};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

#[derive(Default)]
pub struct IgnoredFilesCache {
    loaded: HashMap<PathBuf, IgnoredFiles>,
}

impl IgnoredFilesCache {
    pub async fn get(&mut self, path_config: &PathConfig) -> &IgnoredFiles {
        if !self.loaded.contains_key(&path_config.path) {
            let ignored_files = IgnoredFiles::new(path_config).await;
            self.loaded.insert(path_config.path.clone(), ignored_files);
        }

        self.loaded.get(&path_config.path).unwrap()
    }
}

/// Handle ignored files
#[derive(Debug)]
pub struct IgnoredFiles {
    ignore_sets: Option<GlobSet>,
}

impl IgnoredFiles {
    pub async fn new(path_config: &PathConfig) -> Self {
        IgnoredFiles {
            ignore_sets: get_glob_set(&path_config.path).await,
        }
    }

    #[cfg(test)]
    pub fn empty() -> Self {
        IgnoredFiles { ignore_sets: None }
    }

    pub fn is_ignored(&self, path: &RelativePathBuf) -> bool {
        self.ignore_sets
            .as_ref()
            .map(|set| set.is_match(&**path))
            .unwrap_or_default()
    }
}

async fn get_glob_set(path: impl AsRef<Path>) -> Option<GlobSet> {
    let patterns = get_ignore_patterns(path.as_ref()).await?;

    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        let glob = match Glob::new(&pattern) {
            Ok(glob) => glob,
            Err(err) => {
                log::error!(
                    "Found invalid glob pattern {} at {:?}: {err}",
                    pattern,
                    path.as_ref()
                );
                continue;
            }
        };

        builder.add(glob);
    }

    match builder.build() {
        Ok(set) => Some(set),
        Err(err) => {
            log::error!("Failed to build glob set: {err}");
            None
        }
    }
}

async fn get_ignore_patterns(path: impl AsRef<Path>) -> Option<Vec<String>> {
    let ignore_file_path = path.as_ref().join(IGNORE_FILE_NAME);
    if !ignore_file_path.exists() {
        return None;
    }

    let content = match tokio::fs::read_to_string(ignore_file_path).await {
        Ok(content) => content,
        Err(_) => {
            log::error!("Failed to read ignore file at {:?}", path.as_ref());
            return None;
        }
    };
    Some(content.lines().map(|s| s.to_string()).collect())
}
