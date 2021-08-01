use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
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
        storage: String,
        event_type: EventType,
        event_status: EventStatus,
    ) -> Result<(), TransactionLogError> {
        let event = LogEvent {
            timestamp: SystemTime::UNIX_EPOCH.elapsed().unwrap().as_secs(),
            event_type,
            event_status,
            storage,
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
    pub fn get_failed_events(self, storage: String) -> impl Iterator<Item = PathBuf> {
        let mut events = HashSet::new();

        let stream = self
            .get_log_events()
            .filter_map(|ev| ev.ok())
            .filter(move |ev| {
                ev.storage == storage && matches!(ev.event_type, EventType::Write(_))
            });

        for event in stream {
            match event.event_status {
                EventStatus::Started | EventStatus::Failed => {
                    events.insert(event.event_type);
                }
                EventStatus::Finished => {
                    events.remove(&event.event_type);
                }
            }
        }

        events.into_iter().map(|e| match e {
            EventType::Write(path) => path,
            _ => unreachable!(),
        })
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
    pub storage: String,
    pub event_type: EventType,
    pub event_status: EventStatus,
}

impl std::fmt::Display for LogEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{},{},{},{}",
            self.timestamp, self.storage, self.event_type, self.event_status
        )
    }
}

impl FromStr for LogEvent {
    type Err = TransactionLogError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split(',');
        let mut next_or = || parts.next().ok_or(TransactionLogError::InvalidStringFormat);

        let timestamp = next_or()?.parse()?;
        let storage = next_or()?.into();
        let event_type = next_or()?.parse()?;
        let event_status = next_or()?.parse()?;

        Ok(LogEvent {
            timestamp,
            storage,
            event_type,
            event_status,
        })
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(crate) enum EventType {
    Delete(PathBuf),
    Write(PathBuf),
    Move(PathBuf, PathBuf),
}

impl std::fmt::Display for EventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventType::Delete(file_path) => {
                write!(f, "Delete:{}", file_path.to_str().ok_or(std::fmt::Error)?)
            }
            EventType::Write(file_path) => {
                write!(f, "Write:{}", file_path.to_str().ok_or(std::fmt::Error)?)
            }
            EventType::Move(source_path, destinnation_path) => {
                write!(
                    f,
                    "Move:{}:{}",
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
            "Delete" => Ok(EventType::Delete(next_or()?.into())),
            "Write" => Ok(EventType::Write(next_or()?.into())),
            "Move" => Ok(EventType::Move(next_or()?.into(), next_or()?.into())),
            _ => Err(TransactionLogError::InvalidStringFormat),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
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

    #[test]
    pub fn test_append_event() -> Result<(), TransactionLogError> {
        let mut stream = Cursor::new(Vec::new());
        {
            let mut log = TransactionLogWriter::new(&mut stream);
            log.append(
                "books".into(),
                EventType::Delete("some/path".into()),
                EventStatus::Finished,
            )?;
            log.append(
                "books".into(),
                EventType::Write("some/path".into()),
                EventStatus::Finished,
            )?;
            log.append(
                "books".into(),
                EventType::Move("source/path".into(), "dest/path".into()),
                EventStatus::Finished,
            )?;
        }

        stream.set_position(0);
        let lines: Vec<String> = stream.lines().map(|x| x.unwrap()).collect();
        assert_eq!(3, lines.len());
        assert!(lines[0].ends_with("books,Delete:some/path,Finished"));
        assert!(lines[1].ends_with("books,Write:some/path,Finished"));
        assert!(lines[2].ends_with("books,Move:source/path:dest/path,Finished"));
        Ok(())
    }

    #[test]
    pub fn test_can_parse_events() {
        let log = TransactionLogReader::new({
            let bytes = br#"
12,books,Delete:some/file/deleted,Finished
14,books,Write:some/file/update,Started
15,books,Write:some/file/update,Finished
16,books,Move:some/file/source:some/file/destination,Finished
"#
            .to_vec();
            Cursor::new(bytes)
        });
        let mut events = log.get_log_events().filter_map(|ev| ev.ok());

        assert_eq!(
            LogEvent {
                timestamp: 12,
                storage: "books".into(),
                event_type: EventType::Delete("some/file/deleted".into()),
                event_status: EventStatus::Finished
            },
            events.next().unwrap()
        );
        assert_eq!(
            LogEvent {
                timestamp: 14,
                storage: "books".into(),
                event_type: EventType::Write("some/file/update".into()),
                event_status: EventStatus::Started
            },
            events.next().unwrap()
        );
        assert_eq!(
            LogEvent {
                timestamp: 15,
                storage: "books".into(),
                event_type: EventType::Write("some/file/update".into()),
                event_status: EventStatus::Finished
            },
            events.next().unwrap()
        );
        assert_eq!(
            LogEvent {
                timestamp: 16,
                storage: "books".into(),
                event_type: EventType::Move(
                    "some/file/source".into(),
                    "some/file/destination".into()
                ),
                event_status: EventStatus::Finished
            },
            events.next().unwrap()
        );
    }

    #[test]
    pub fn test_compact_log() {
        todo!()
    }

    #[test]
    pub fn test_get_failed_writes() {
        let log = TransactionLogReader::new({
            let bytes = br#"
12,books,Delete:some/file/deleted,Finished
14,books,Write:some/file/update,Started
15,books,Write:some/file/update,Finished
16,books,Move:some/file/source:some/file/destination,Finished
17,books,Write:file_one,Started
18,books,Write:file_two,Started
18,books,Write:file_two,Finished
18,books,Write:file_one,Finished
19,books,Write:file_two,Started
20,books,Write:file_one,Started
21,books,Write:file_three,Started
"#
            .to_vec();
            Cursor::new(bytes)
        });

        let mut events: Vec<PathBuf> = log.get_failed_events("books".into()).collect();
        events.sort();
        assert_eq!(PathBuf::from("file_one"), events[0]);
        assert_eq!(PathBuf::from("file_three"), events[1]);
        assert_eq!(PathBuf::from("file_two"), events[2]);
    }
}
