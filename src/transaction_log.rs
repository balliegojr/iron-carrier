use crate::sync::SyncType;
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
    str::FromStr,
    time::SystemTime,
};
use thiserror::Error;

const TRANSACTION_KEEP_LIMIT_SECS: u64 = 30 * 24 * 60 * 60;

#[cfg(not(windows))]
const LINE_ENDING: &str = "\n";
#[cfg(windows)]
const LINE_ENDING: &str = "\r\n";

pub(crate) fn get_log_writer(log_path: &Path) -> std::io::Result<TransactionLogWriter<File>> {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .write(true)
        .open(log_path)?;

    Ok(TransactionLogWriter::new(file))
}

pub(crate) fn get_log_reader(log_path: &Path) -> std::io::Result<TransactionLogReader<File>> {
    let file = File::open(log_path)?;
    Ok(TransactionLogReader::new(file))
}

pub(crate) struct TransactionLogWriter<T: Write> {
    log_stream: T,
}

impl<T: Write> TransactionLogWriter<T> {
    pub fn new(log_stream: T) -> Self {
        Self { log_stream }
    }

    pub fn append(
        &mut self,
        event_type: EventType,
        event_status: EventStatus,
    ) -> Result<(), TransactionLogError> {
        let event = LogEvent {
            timestamp: SystemTime::UNIX_EPOCH.elapsed().unwrap().as_secs(),
            event_type,
            event_status,
        };
        self.log_stream
            .write_all(format!("{}{}", event, LINE_ENDING).as_bytes())?;

        Ok(())
    }
}

pub(crate) struct TransactionLogReader<T: Read> {
    log_stream: T,
}

impl<T: Read> TransactionLogReader<T> {
    pub fn new(log_stream: T) -> Self {
        Self { log_stream }
    }
    pub fn get_log_events(self) -> TransactionLogIter<T> {
        TransactionLogIter::new(self.log_stream)
    }
}

pub(crate) struct TransactionLogIter<T: Read> {
    stream: BufReader<T>,
}

impl<T: Read> TransactionLogIter<T> {
    pub fn new(stream: T) -> Self {
        Self {
            stream: BufReader::new(stream),
        }
    }
}

impl<T: Read> Iterator for TransactionLogIter<T> {
    type Item = Result<LogEvent, TransactionLogError>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut line = String::new();
        match self.stream.read_line(&mut line) {
            Ok(size) if size > 0 => Some(line.strip_suffix(LINE_ENDING)?.parse()),
            Ok(_) => None,
            Err(err) => Some(Err(TransactionLogError::IoError(err))),
        }
    }
}

#[derive(Error, Debug)]
pub enum TransactionLogError {
    #[error("There is a invalid string in the log line")]
    InvalidStringFormat,
    #[error("There is a invalid timestamp in the log line")]
    InvalidTimestampFormat(#[from] std::num::ParseIntError),
    #[error("There was an IO error: {0}")]
    IoError(#[from] std::io::Error),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct LogEvent {
    pub timestamp: u64,
    pub event_type: EventType,
    pub event_status: EventStatus,
}

impl std::fmt::Display for LogEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{},{},{}",
            self.timestamp, self.event_type, self.event_status
        )
    }
}

impl FromStr for LogEvent {
    type Err = TransactionLogError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split(',');
        let mut next_or = || parts.next().ok_or(TransactionLogError::InvalidStringFormat);

        let timestamp = next_or()?.parse()?;
        let event_type = next_or()?.parse()?;
        let event_status = next_or()?.parse()?;

        Ok(LogEvent {
            timestamp,
            event_type,
            event_status,
        })
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum EventType {
    Sync(SyncType, u64),
    FileDelete(String, PathBuf),
    FileCreate(String, PathBuf),
    FileWrite(String, PathBuf),
    FileMove(String, PathBuf, PathBuf),
}

impl std::fmt::Display for EventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventType::Sync(sync_type, peer_id) => write!(f, "Sync:{:?}:{}", sync_type, peer_id),
            EventType::FileDelete(storage, file_path) => {
                write!(
                    f,
                    "FileDelete:{}:{}",
                    storage,
                    file_path.to_str().ok_or(std::fmt::Error)?
                )
            }
            EventType::FileCreate(storage, file_path) => {
                write!(
                    f,
                    "FileCreate:{}:{}",
                    storage,
                    file_path.to_str().ok_or(std::fmt::Error)?
                )
            }
            EventType::FileWrite(storage, file_path) => {
                write!(
                    f,
                    "FileWrite:{}:{}",
                    storage,
                    file_path.to_str().ok_or(std::fmt::Error)?
                )
            }
            EventType::FileMove(storage, source_path, destinnation_path) => {
                write!(
                    f,
                    "FileMove:{}:{}:{}",
                    storage,
                    source_path.to_str().ok_or(std::fmt::Error)?,
                    destinnation_path.to_str().ok_or(std::fmt::Error)?
                )
            }
        }
    }
}

impl FromStr for EventType {
    type Err = TransactionLogError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split(':');
        let mut next_or = || parts.next().ok_or(TransactionLogError::InvalidStringFormat);

        match next_or()? {
            "Sync" => {
                let sync_type = match next_or()? {
                    "Full" => SyncType::Full,
                    "Partial" => SyncType::Partial,
                    _ => return Err(TransactionLogError::InvalidStringFormat),
                };

                Ok(EventType::Sync(sync_type, next_or()?.parse()?))
            }
            "FileDelete" => Ok(EventType::FileDelete(next_or()?.into(), next_or()?.into())),
            "FileCreate" => Ok(EventType::FileCreate(next_or()?.into(), next_or()?.into())),
            "FileWrite" => Ok(EventType::FileWrite(next_or()?.into(), next_or()?.into())),
            "FileMove" => Ok(EventType::FileMove(
                next_or()?.into(),
                next_or()?.into(),
                next_or()?.into(),
            )),
            _ => Err(TransactionLogError::InvalidStringFormat),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum EventStatus {
    Started,
    Finished,
    Failed,
}

impl std::fmt::Display for EventStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl FromStr for EventStatus {
    type Err = TransactionLogError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Started" => Ok(EventStatus::Started),
            "Finished" => Ok(EventStatus::Finished),
            "Failed" => Ok(EventStatus::Failed),
            _ => Err(TransactionLogError::InvalidStringFormat),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::{BufRead, Cursor};

    use super::*;

    fn sample_log_stream() -> Cursor<Vec<u8>> {
        let bytes = br#"10,Sync:Full:1,Started
12,FileDelete:books:some/file/deleted,Finished
13,FileCreate:books:some/file/created,Finished
14,FileWrite:books:some/file/update,Started
15,FileWrite:books:some/file/update,Finished
16,FileMove:books:some/file/source:some/file/destination,Finished
"#
        .to_vec();
        Cursor::new(bytes)
    }

    #[test]
    pub fn test_append_event() -> Result<(), TransactionLogError> {
        let mut stream = Cursor::new(Vec::new());
        {
            let mut log = TransactionLogWriter::new(&mut stream);

            log.append(EventType::Sync(SyncType::Full, 1), EventStatus::Started)?;
            log.append(
                EventType::FileCreate("books".into(), "some/path".into()),
                EventStatus::Finished,
            )?;
            log.append(
                EventType::FileDelete("books".into(), "some/path".into()),
                EventStatus::Finished,
            )?;
            log.append(
                EventType::FileWrite("books".into(), "some/path".into()),
                EventStatus::Finished,
            )?;
            log.append(
                EventType::FileMove("books".into(), "source/path".into(), "dest/path".into()),
                EventStatus::Finished,
            )?;
        }

        stream.set_position(0);
        let lines: Vec<String> = stream.lines().map(|x| x.unwrap()).collect();
        assert_eq!(5, lines.len());
        assert!(lines[0].ends_with("Sync:Full:1,Started"));
        assert!(lines[1].ends_with("FileCreate:books:some/path,Finished"));
        assert!(lines[2].ends_with("FileDelete:books:some/path,Finished"));
        assert!(lines[3].ends_with("FileWrite:books:some/path,Finished"));
        assert!(lines[4].ends_with("FileMove:books:source/path:dest/path,Finished"));
        Ok(())
    }

    #[test]
    pub fn test_can_parse_events() {
        let log = TransactionLogReader::new(sample_log_stream());
        let events: Vec<LogEvent> = log.get_log_events().filter_map(|ev| ev.ok()).collect();

        assert_eq!(
            LogEvent {
                timestamp: 10,
                event_type: EventType::Sync(SyncType::Full, 1),
                event_status: EventStatus::Started
            },
            events[0]
        );
        assert_eq!(
            LogEvent {
                timestamp: 12,
                event_type: EventType::FileDelete("books".to_string(), "some/file/deleted".into()),
                event_status: EventStatus::Finished
            },
            events[1]
        );
        assert_eq!(
            LogEvent {
                timestamp: 13,
                event_type: EventType::FileCreate("books".to_string(), "some/file/created".into()),
                event_status: EventStatus::Finished
            },
            events[2]
        );
        assert_eq!(
            LogEvent {
                timestamp: 14,
                event_type: EventType::FileWrite("books".to_string(), "some/file/update".into()),
                event_status: EventStatus::Started
            },
            events[3]
        );
        assert_eq!(
            LogEvent {
                timestamp: 15,
                event_type: EventType::FileWrite("books".to_string(), "some/file/update".into()),
                event_status: EventStatus::Finished
            },
            events[4]
        );
        assert_eq!(
            LogEvent {
                timestamp: 16,
                event_type: EventType::FileMove(
                    "books".to_string(),
                    "some/file/source".into(),
                    "some/file/destination".into()
                ),
                event_status: EventStatus::Finished
            },
            events[5]
        );
    }

    #[test]
    pub fn test_compact_log() {
        todo!()
    }

    #[test]
    pub fn test_last_sync_was_successfull() {
        todo!()
    }
}
