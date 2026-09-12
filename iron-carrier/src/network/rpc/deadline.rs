use std::time::Duration;
use tokio::time::Instant;

/// Message deadline. This is a convenience wrapper around an Instant
#[derive(Clone, Copy, Eq)]
pub struct Deadline(pub Instant, Duration);

impl std::hash::Hash for Deadline {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl Ord for Deadline {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}
impl PartialOrd for Deadline {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Deadline {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Deadline {
    pub fn new(timeout: Duration) -> Self {
        let deadline = Instant::now() + timeout;
        Self(deadline, timeout)
    }

    pub fn extend(self) -> Self {
        Self::new(self.1)
    }

    pub fn is_expired(&self) -> bool {
        Instant::now() > self.0
    }
}
