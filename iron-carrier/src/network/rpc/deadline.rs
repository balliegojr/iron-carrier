use std::time::Duration;

use tokio::time::Instant;

/// Message deadline. This is a convenience wrapper around an Instant
pub struct Deadline {
    deadline: Instant,
}

impl Deadline {
    pub fn new(timeout: Duration) -> Self {
        let deadline = Instant::now() + timeout;
        Self { deadline }
    }

    pub fn is_expired(&self) -> bool {
        Instant::now() > self.deadline
    }

    pub fn extend(&mut self, secs: u64) {
        self.deadline = Instant::now() + Duration::from_secs(secs)
    }
}
