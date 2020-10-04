mod config;

#[derive(Debug)]
pub enum RSyncError {
    InvalidConfigPath,
    InvalidConfigFile
}