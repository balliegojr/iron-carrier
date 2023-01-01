use std::{collections::HashMap, path::Path, sync::Mutex};

use globset::{Glob, GlobSet, GlobSetBuilder};

use crate::{config::Config, constants::IGNORE_FILE_NAME};

/// Handle ignored files
pub struct IgnoredFiles {
    config: &'static Config,
    ignore_sets: Mutex<HashMap<String, Option<GlobSet>>>,
}

impl IgnoredFiles {
    pub fn new(config: &'static Config) -> Self {
        Self {
            ignore_sets: HashMap::new().into(),
            config,
        }
    }

    pub fn clear(&self) {
        self.ignore_sets.lock().unwrap().clear();
    }

    pub fn is_ignored<P: AsRef<Path>>(&self, storage: &str, path: P) -> bool {
        let mut ignore_sets = self.ignore_sets.lock().unwrap();
        let glob_set = ignore_sets
            .entry(storage.to_string())
            .or_insert_with(|| get_glob_set(&self.config.storages[storage].path));

        glob_set
            .as_ref()
            .map(|set| set.is_match(path))
            .unwrap_or_default()
    }
}

fn get_glob_set<P: AsRef<Path>>(path: P) -> Option<GlobSet> {
    let patterns = get_ignore_patterns(path.as_ref())?;

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

fn get_ignore_patterns<P: AsRef<Path>>(path: P) -> Option<Vec<String>> {
    let ignore_file_path = path.as_ref().join(IGNORE_FILE_NAME);
    if !ignore_file_path.exists() {
        return None;
    }

    let content = match std::fs::read_to_string(ignore_file_path) {
        Ok(content) => content,
        Err(_) => {
            log::error!("Failed to read ignore file at {:?}", path.as_ref());
            return None;
        }
    };
    Some(content.lines().map(|s| s.to_string()).collect())
}
