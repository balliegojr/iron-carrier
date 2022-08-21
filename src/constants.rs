pub const VERSION: &str = "0.1";

pub const PEER_IDENTIFICATION_TIMEOUT: u64 = 1;
pub const PEER_STALE_CONNECTION: u64 = 120;
pub const PING_CONNECTIONS: u64 = 60;
pub const CLEAR_TIMEOUT: u64 = 60;

pub const START_CONNECTIONS_RETRIES: u8 = 3;
pub const START_CONNECTIONS_AFTER_SECONDS: u64 = 3;
pub const START_CONNECTIONS_RETRY_WAIT: u8 = 10;

pub const START_NEGOTIATION_MIN: f64 = 0.250;
pub const START_NEGOTIATION_MAX: f64 = 0.5;

pub const DNS_RESOURCE_CACHE: u32 = 3600;

pub const IGNORE_FILE_NAME: &str = ".ignore";

#[cfg(not(windows))]
pub const LINE_ENDING: &str = "\n";
#[cfg(windows)]
pub const LINE_ENDING: &str = "\r\n";
