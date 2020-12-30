//! Handles configuration

use serde::Deserialize;
use std::{collections::HashMap, fs::read_to_string, path::PathBuf};

use crate::IronCarrierError;

fn default_port() -> u32 { 8090 }
fn default_enable_watcher() -> bool { true }
fn default_watcher_debounce() -> u64 { 10 }

const MAX_PORT: u32 = 65535;
/// Represents the configuration for the current machine
#[derive(Deserialize)]
pub struct Config {
    /// Contains the folder that will be watched for synchronization  
    /// **Key** is the path alias  
    /// **Value** is the path itself  
    pub paths: HashMap<String, PathBuf>,
    /// contains the address for the other peers  
    /// in the format IPV4:PORT (**192.168.1.1:9090**)
    pub peers: Option<Vec<String>>,
    
    /// Port to listen to connections, defaults to 8090
    #[serde(default="default_port")]
    pub port: u32,

    /// Enable file watchers for real time syncs, defaults to true
    #[serde(default="default_enable_watcher")]
    pub enable_file_watcher: bool,

    /// Seconds to debounce file events, defaults to 10 seconds
    #[serde(default="default_watcher_debounce")]
    pub delay_watcher_events: u64
}

impl Config {
    /// creates a new [Config] reading the contents from the given path
    ///
    /// [Ok]`(`[Config]`)` if successful  
    /// [IronCarrierError::ConfigFileNotFound] if the provided path doesn't exists   
    /// [IronCarrierError::ConfigFileIsInvalid] if the provided configuration is not valid   
    pub fn new(config_path: &str) -> crate::Result<Self> {
        log::debug!("reading config file {}", config_path);

        Config::parse_content(read_to_string(config_path)?)
    }

    /// Parses the given content into [Config]
    pub(crate) fn parse_content(content: String) -> crate::Result<Self> {
        toml::from_str::<Config>(&content)?.validate()
    }

    fn validate(self) -> crate::Result<Self> {
        if 0 == self.port || self.port > MAX_PORT { 
            log::error!("Invalid port number");
            return Err(IronCarrierError::ConfigFileIsInvalid("invalid port number".into()).into()) 
        }

        for (alias, path) in &self.paths {
            if !path.exists() { 
                log::info!("creating directory for alias {}", alias);
                std::fs::create_dir_all(path)?; 
            }
            if !path.is_dir() { 
                log::error!("provided path for alias {} is invalid", alias);
                return Err(IronCarrierError::ConfigFileIsInvalid(format!("invalid path: {}", alias)).into()) 
            }
        }

        Ok(self)
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
        a = \"./tmp\"
        ".to_owned();

        let config = Config::parse_content(config_content)?;
        let peers = config.peers.unwrap();

        assert_eq!(1, peers.len());
        assert_eq!("127.0.0.1:8888", peers[0]);

        let paths = config.paths;
        assert_eq!(1, paths.len());
        assert_eq!(PathBuf::from("./tmp"), paths["a"]);
        

        Ok(())

    }
}