//! Handles configuration

use serde::Deserialize;
use std::{collections::HashMap, fs::read_to_string, path::PathBuf};

use crate::IronCarrierError;

fn default_port() -> u32 { 8090 }
fn default_enable_watcher() -> bool { true }
fn default_watcher_debounce() -> u64 { 10 }

/// Represents the configuration for the current machine
#[derive(Deserialize)]
pub struct Config {
    /// Contains the folder that will be watched for synchronization  
    /// **Key** is the path alias  
    /// **Value** is the path itself  
    pub paths: HashMap<String, PathBuf>,
    /// contains the address for the other peers  
    /// in the format IPV4:PORT (**192.168.1.1:9090**)
    pub peers: Vec<String>,
    
    /// Port to listen to connections, defaults to 8090
    #[serde(default="default_port")]
    pub port: u32,

    /// Enable file watchers for real time syncs, defaults to true
    #[serde(default="default_enable_watcher")]
    pub enable_file_watcher: bool,

    /// Seconds to debounce file events, defaults to 10 seconds
    #[serde(default="default_watcher_debounce")]
    pub debounce_events_seconds: u64
}

impl Config {
    /// creates a new [Config] reading the contents from the given path
    ///
    /// [Ok]`(`[Config]`)` if successful  
    /// [IronCarrierError::ConfigFileNotFound] if the provided path doesn't exists   
    /// [IronCarrierError::ConfigFileIsInvalid] if the provided configuration is not valid   
    pub fn new(config_path: String) -> crate::Result<Self> {
        match read_to_string(config_path) {
            Ok(config_content) => Config::parse_content(config_content),
            Err(_) => Err(IronCarrierError::ConfigFileNotFound)
        }
    }

    /// Parses the given content into [Config]
    pub(crate) fn parse_content(content: String) -> crate::Result<Self> {
        match toml::from_str(&content) {
            Ok(config) => Ok(config),
            Err(_) => Err(IronCarrierError::ConfigFileIsInvalid)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_parse_config() -> crate::Result<()>{
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