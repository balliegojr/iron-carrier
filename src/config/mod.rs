//! Handles configuration
use std::{
    collections::HashMap,
    error::Error,
    fs::read_to_string,
    path::{Path, PathBuf},
    str::FromStr,
};

use crate::{hash_helper, validation::Unvalidated, IronCarrierError};
use serde::Deserialize;
use serde_with::{serde_as, DisplayFromStr, PickFirst};

mod path_config;
pub use path_config::PathConfig;

#[derive(Debug, Deserialize, Default)]
pub enum OperationMode {
    #[default]
    Auto,
    Manual,
}

/// Represents the configuration for the current machine
#[serde_as]
#[derive(Debug, Deserialize)]
pub struct Config {
    #[serde(default = "String::default")]
    pub node_id: String,

    #[serde(skip)]
    pub node_id_hashed: u64,

    /// Option group of nodes to sync
    pub group: Option<String>,

    /// Contains the folder that will be watched for synchronization  
    /// **Key** is the path alias  
    /// **Value** is the path itself  
    #[serde_as(as = "HashMap<_, PickFirst<(_, DisplayFromStr)>>")]
    pub storages: HashMap<String, PathConfig>,
    /// contains the address for the other peers  
    /// in the format IPV4:PORT (**192.168.1.1:25230**)
    pub peers: Option<Vec<String>>,

    /// Port to listen to connections, defaults to 25230
    #[serde(default = "defaults::port")]
    pub port: u16,

    /// Enable file watchers for real time syncs, defaults to true
    #[serde(default = "defaults::watcher_enabled")]
    pub enable_file_watcher: bool,

    /// Seconds to debounce file events, defaults to 10 seconds
    #[serde(default = "defaults::watcher_debounce")]
    pub delay_watcher_events: u64,

    /// Enable service discovery
    #[serde(default = "defaults::service_discovery_enabled")]
    pub enable_service_discovery: bool,

    #[serde(default)]
    pub operation_mode: OperationMode,

    /// path to the log file
    #[serde(default = "defaults::log_path")]
    pub log_path: PathBuf,

    pub encryption_key: Option<String>,

    #[serde(default = "defaults::max_parallel_transfers")]
    pub max_parallel_transfers: u8,

    pub schedule_sync: Option<String>,
}

impl Config {
    /// creates a new [Config] reading the contents from the given path
    ///
    /// [Ok]`(`[Config]`)` if successful  
    /// [IronCarrierError::ConfigFileNotFound] if the provided path doesn't exists   
    /// [IronCarrierError::ConfigFileIsInvalid] if the provided configuration is not valid   
    pub fn new(config_path: &Path) -> crate::Result<Unvalidated<Self>> {
        log::debug!("reading config file {:?}", config_path);

        read_to_string(config_path)?.parse()
    }

    fn with_node_id(mut self) -> Self {
        if self.node_id.is_empty() {
            self.node_id = hash_helper::get_node_id(self.port)
                .to_string()
                .chars()
                .take(20)
                .collect();
        }

        self.node_id_hashed = hash_helper::calculate_checksum(self.node_id.as_bytes());

        self
    }

    fn with_correct_log_path(mut self) -> Self {
        if self.log_path.exists() && self.log_path.is_dir() {
            self.log_path.push("iron-carrier.log");
        }

        self
    }

    fn with_non_empty_group(mut self) -> Self {
        if let Some(group) = &self.group && group.is_empty() {
            self.group = None;
        }

        self
    }
}

impl Unvalidated<Config> {}

impl FromStr for Unvalidated<Config> {
    type Err = Box<dyn Error + Send + Sync>;

    fn from_str(content: &str) -> Result<Self, Self::Err> {
        Ok(toml::from_str::<Config>(content).map(|c| {
            Unvalidated::new(
                c.with_node_id()
                    .with_non_empty_group()
                    .with_correct_log_path(),
            )
        })?)
    }
}

impl crate::validation::Verifiable for Config {
    fn is_valid(&self) -> crate::Result<()> {
        if 0 == self.port {
            log::error!("Invalid port number");
            return Err(IronCarrierError::ConfigFileIsInvalid("invalid port number".into()).into());
        }

        if self.storages.is_empty() {
            return Err(IronCarrierError::ConfigFileIsInvalid(
                "At least one storage storage must be provided".into(),
            )
            .into());
        }

        for (alias, path) in &self.storages {
            if !path.path.exists() {
                log::info!("creating directory for storage {}", alias);
                std::fs::create_dir_all(&path.path)?;
            }
            if !path.path.is_dir() {
                log::error!("provided path for alias {} is invalid", alias);
                return Err(IronCarrierError::ConfigFileIsInvalid(format!(
                    "invalid path: {alias}",
                ))
                .into());
            }
        }

        if let Some(group) = &self.group && group.len() > 255 {
            return Err(
                IronCarrierError::ConfigFileIsInvalid("Group name is too long".into()).into(),
            );
        }

        if let Some(schedule_cron) = self.schedule_sync.as_ref() {
            if cron::Schedule::from_str(schedule_cron).is_err() {
                return Err(IronCarrierError::ConfigFileIsInvalid(
                    "schedule_sync contains an invalid cron".into(),
                )
                .into());
            }
        }

        Ok(())
    }
}

mod defaults {
    use std::path::PathBuf;

    pub fn port() -> u16 {
        25230
    }
    pub fn watcher_enabled() -> bool {
        true
    }
    pub fn watcher_debounce() -> u64 {
        4
    }
    pub fn service_discovery_enabled() -> bool {
        true
    }
    pub fn log_path() -> PathBuf {
        let mut log_path = dirs::config_dir().expect("Can't access home folder");
        log_path.push("iron-carrier/iron-carrier.db");
        log_path
    }

    pub fn max_parallel_transfers() -> u8 {
        4
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn can_parse_config() -> crate::Result<()> {
        let config_content = r#"
        peers = [
            "127.0.0.1:8888"
        ]

        group = ""
        log_path = "./tmp"

        [storages]
        str_path = "./tmp"
        struct_path = { path = "./extended_path", enable_watcher = false }
        "#
        .to_owned();

        let config: Unvalidated<Config> = config_content.parse()?;
        assert_eq!(config.peers, Some(vec!["127.0.0.1:8888".to_owned()]));

        let storages = &config.storages;
        assert_eq!(2, storages.len());
        assert_eq!(PathBuf::from("./tmp"), storages["str_path"].path);
        assert_eq!(
            PathBuf::from("./extended_path"),
            storages["struct_path"].path
        );

        assert!(!config.node_id.is_empty());
        assert!(config.group.is_none());
        assert_eq!(
            config.log_path,
            PathBuf::from_str("./tmp/iron-carrier.log")?
        );

        Ok(())
    }
}
