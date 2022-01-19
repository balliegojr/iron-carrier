//! Handles configuration

use serde::Deserialize;
use std::{
    collections::HashMap,
    fs::read_to_string,
    path::{Path, PathBuf},
};

use crate::{hash_helper, IronCarrierError};
fn default_port() -> u16 {
    25230
}
fn default_enable_watcher() -> bool {
    true
}
fn default_watcher_debounce() -> u64 {
    5
}
fn default_enable_service_discovery() -> bool {
    true
}
fn default_log_path() -> PathBuf {
    let mut log_path = dirs::config_dir().expect("Can't access home folder");
    log_path.push("iron-carrier/iron-carrier.log");
    log_path
}
fn empty_string() -> String {
    String::new()
}

/// Represents the configuration for the current machine
#[derive(Debug, Deserialize)]
pub struct Config {
    #[serde(default = "empty_string")]
    pub node_id: String,
    /// Contains the folder that will be watched for synchronization  
    /// **Key** is the path alias  
    /// **Value** is the path itself  
    pub paths: HashMap<String, PathBuf>,
    /// contains the address for the other peers  
    /// in the format IPV4:PORT (**192.168.1.1:25230**)
    pub peers: Option<Vec<String>>,

    /// Port to listen to connections, defaults to 25230
    #[serde(default = "default_port")]
    pub port: u16,

    /// Enable file watchers for real time syncs, defaults to true
    #[serde(default = "default_enable_watcher")]
    pub enable_file_watcher: bool,

    /// Seconds to debounce file events, defaults to 10 seconds
    #[serde(default = "default_watcher_debounce")]
    pub delay_watcher_events: u64,

    /// Enable service discovery
    #[serde(default = "default_enable_service_discovery")]
    pub enable_service_discovery: bool,

    /// path to the log file
    #[serde(default = "default_log_path")]
    pub log_path: PathBuf,
}

impl Config {
    /// creates a new [Config] reading the contents from the given path
    ///
    /// [Ok]`(`[Config]`)` if successful  
    /// [IronCarrierError::ConfigFileNotFound] if the provided path doesn't exists   
    /// [IronCarrierError::ConfigFileIsInvalid] if the provided configuration is not valid   
    pub fn new(config_path: &Path) -> crate::Result<Self> {
        log::debug!("reading config file {:?}", config_path);

        match read_to_string(config_path) {
            Ok(config_content) => Config::new_from_str(config_content),
            Err(_) => Err(Box::new(IronCarrierError::ConfigFileNotFound)),
        }
    }

    /// Parses the given content into [Config]
    pub fn new_from_str(content: String) -> crate::Result<Self> {
        toml::from_str::<Config>(&content)?
            .fill_node_id()
            .validate()
    }

    fn fill_node_id(mut self) -> Self {
        if self.node_id.is_empty() {
            self.node_id = hash_helper::get_node_id(self.port)
                .to_string()
                .chars()
                .take(10)
                .collect();
        }

        self
    }

    fn validate(mut self) -> crate::Result<Self> {
        if 0 == self.port {
            log::error!("Invalid port number");
            return Err(IronCarrierError::ConfigFileIsInvalid("invalid port number".into()).into());
        }

        if self.paths.is_empty() {
            return Err(IronCarrierError::ConfigFileIsInvalid(
                "At least one storage path must be provided".into(),
            )
            .into());
        }

        for (alias, path) in &self.paths {
            if !path.exists() {
                log::info!("creating directory for alias {}", alias);
                std::fs::create_dir_all(path)?;
            }
            if !path.is_dir() {
                log::error!("provided path for alias {} is invalid", alias);
                return Err(IronCarrierError::ConfigFileIsInvalid(format!(
                    "invalid path: {}",
                    alias
                ))
                .into());
            }
        }

        if self.log_path.exists() && self.log_path.is_dir() {
            self.log_path.push("iron-carrier.log");
        }

        Ok(self)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn can_parse_config() -> crate::Result<()> {
        let config_content = "
        peers = [
            \"127.0.0.1:8888\"
        ]

        [paths]
        a = \"./tmp\"
        "
        .to_owned();

        let config = Config::new_from_str(config_content)?;
        let peers = config.peers.unwrap();

        assert_eq!(1, peers.len());
        assert_eq!("127.0.0.1:8888", peers[0]);

        let paths = config.paths;
        assert_eq!(1, paths.len());
        assert_eq!(PathBuf::from("./tmp"), paths["a"]);

        Ok(())
    }
}
