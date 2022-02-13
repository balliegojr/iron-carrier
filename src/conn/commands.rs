use serde::{Deserialize, Serialize};
use std::fmt::Display;

use crate::sync::{FileHandlerEvent, SyncEvent, WatcherEvent};

use super::HandlerEvent;

#[derive(Debug, Deserialize, Serialize)]
pub enum Commands {
    Command(SyncEvent),
    FileHandler(FileHandlerEvent),
    Stream(Vec<u8>),
    Watcher(WatcherEvent),
    Clear,
}

impl From<SyncEvent> for Commands {
    fn from(v: SyncEvent) -> Self {
        Commands::Command(v)
    }
}

impl From<FileHandlerEvent> for Commands {
    fn from(v: FileHandlerEvent) -> Self {
        Commands::FileHandler(v)
    }
}

impl From<WatcherEvent> for Commands {
    fn from(v: WatcherEvent) -> Self {
        Commands::Watcher(v)
    }
}

impl Display for Commands {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            Commands::Command(event) => write!(f, "command {}", event),
            Commands::FileHandler(event) => write!(f, "file event {}", event),
            Commands::Stream(_) => write!(f, "Stream"),
            Commands::Watcher(event) => write!(f, "watcher {}", event),
            Commands::Clear => write!(f, "Clear"),
        }
    }
}

impl<T: Into<Commands>> From<T> for HandlerEvent {
    fn from(v: T) -> Self {
        HandlerEvent::Command(v.into())
    }
}
