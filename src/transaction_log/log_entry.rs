use super::{EntryStatus, EntryType};

#[derive(Debug)]
pub struct LogEntry {
    pub timestamp: u64,
    pub event_type: EntryType,
    pub event_status: EntryStatus,
}

impl LogEntry {
    pub fn new(event_type: EntryType, event_status: EntryStatus, timestamp: u64) -> Self {
        Self {
            timestamp,
            event_type,
            event_status,
        }
    }
}
