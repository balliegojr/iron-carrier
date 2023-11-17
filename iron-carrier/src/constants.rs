pub const VERSION: &str = env!("CARGO_PKG_VERSION");

pub const DEFAULT_NETWORK_TIMEOUT: u64 = 30;
pub const PEER_STALE_CONNECTION: u64 = 120;
pub const IGNORE_FILE_NAME: &str = ".ignore";

pub const MAX_ELECTION_TERMS: u32 = 100;

#[cfg(not(windows))]
pub const LINE_ENDING: &str = "\n";
#[cfg(windows)]
pub const LINE_ENDING: &str = "\r\n";
