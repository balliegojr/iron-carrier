use serde::Deserialize;
use std::{collections::HashMap, fs::read_to_string, path::PathBuf};
use toml::from_str;

use crate::RSyncError;

fn default_port() -> u32 { 8090 }
fn default_enable_watcher() -> bool { true }
fn default_watcher_debounce() -> u64 { 10 }

#[derive(Deserialize)]
pub struct Config {
    pub paths: HashMap<String, PathBuf>,
    pub peers: Vec<String>,
    
    #[serde(default="default_port")]
    pub port: u32,

    #[serde(default="default_enable_watcher")]
    pub enable_file_watcher: bool,

    #[serde(default="default_watcher_debounce")]
    pub debounce_watcher_events: u64
}

impl Config {
    pub fn new(config_path: String) -> Result<Self, RSyncError> {
        match read_to_string(config_path) {
            Ok(config_content) => Config::parse_content(config_content),
            Err(_) => Err(RSyncError::InvalidConfigPath)
        }
    }

    pub(crate) fn parse_content(content: String) -> Result<Self, RSyncError> {
        match from_str(&content) {
            Ok(config) => Ok(config),
            Err(_) => Err(RSyncError::InvalidConfigFile)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_parse_config() -> Result<(), RSyncError>{
        let config_content = "
        peers = [
            \"127.0.0.1:8888\"
        ]

        [paths]
        a = \"some/path\"
        b = \"some/other/path\"
        ".to_owned();

        let config = Config::parse_content(config_content)?;
        let peers = config.peers;

        assert_eq!(1, peers.len());
        assert_eq!("127.0.0.1:8888", peers[0]);

        let paths = config.paths;
        assert_eq!(2, paths.len());
        
        assert_eq!(PathBuf::from("some/path"), paths["a"]);
        assert_eq!(PathBuf::from("some/other/path"), paths["b"]);

        Ok(())

    }
}