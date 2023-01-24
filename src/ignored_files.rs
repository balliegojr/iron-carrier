use crate::constants::IGNORE_FILE_NAME;
use globset::{Glob, GlobSet, GlobSetBuilder};
use std::path::Path;

/// Handle ignored files
#[derive(Debug)]
pub struct IgnoredFiles {
    ignore_sets: Option<GlobSet>,
}

impl IgnoredFiles {
    pub fn is_ignored(&self, path: impl AsRef<Path>) -> bool {
        self.ignore_sets
            .as_ref()
            .map(|set| set.is_match(path))
            .unwrap_or_default()
    }
}

pub async fn load_ignored_file_pattern(path: impl AsRef<Path>) -> IgnoredFiles {
    IgnoredFiles {
        ignore_sets: get_glob_set(path.as_ref()).await,
    }
}

pub fn empty_ignored_files() -> IgnoredFiles {
    IgnoredFiles { ignore_sets: None }
}

async fn get_glob_set<P: AsRef<Path>>(path: P) -> Option<GlobSet> {
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
