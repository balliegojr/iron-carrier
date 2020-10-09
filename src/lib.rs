mod config;
mod fs;

#[derive(Debug)]
pub enum RSyncError {
    InvalidConfigPath,
    InvalidConfigFile,
    ErrorReadingLocalFiles
}