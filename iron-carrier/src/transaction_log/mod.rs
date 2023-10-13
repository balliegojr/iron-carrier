mod entry_status;
mod entry_type;
mod log_entry;

pub use entry_status::EntryStatus;
pub use entry_type::EntryType;
pub use log_entry::LogEntry;

use rusqlite::{params, Connection, OpenFlags};
use tokio::sync::Mutex;

use std::{collections::HashSet, path::Path, sync::Arc, time::Duration};

use crate::relative_path::RelativePathBuf;

const TRANSACTION_KEEP_LIMIT_SECS: u64 = 30 * 24 * 60 * 60;

#[derive(Debug, Clone)]
pub struct TransactionLog {
    storage: Arc<Mutex<Connection>>,
}

impl TransactionLog {
    pub fn load(log_path: &Path) -> anyhow::Result<Self> {
        get_storage(log_path)
            .map(|storage| Self {
                storage: Arc::new(Mutex::new(storage)),
            })
            .map_err(anyhow::Error::from)
    }

    #[cfg(test)]
    pub fn memory() -> anyhow::Result<Self> {
        get_memory_storage()
            .map(|storage| Self {
                storage: Arc::new(Mutex::new(storage)),
            })
            .map_err(anyhow::Error::from)
    }

    pub async fn append_entry(
        &self,
        storage: &str,
        path: &RelativePathBuf,
        old_path: Option<&RelativePathBuf>,
        entry: LogEntry,
    ) -> anyhow::Result<()> {
        self.storage.lock().await
            .execute("INSERT OR REPLACE INTO LogEntry (storage, path, old_path, entry_type, status, timestamp) VALUES (?,?,?,?,?,?)", params![
                storage,
                path.to_str(),
                old_path.map(|p| p.to_str()),
                &entry.event_type.to_string(),
                &entry.event_status.to_string(),
                entry.timestamp as i64
            ])?;

        Ok(())
    }

    pub async fn flush(&self) -> anyhow::Result<()> {
        let limit = (tokio::time::Instant::now()
            - Duration::from_secs(TRANSACTION_KEEP_LIMIT_SECS))
        .elapsed()
        .as_secs();

        self.storage
            .lock()
            .await
            .execute("DELETE FROM LogEntry where timestamp < ?", [limit as i64])?;

        Ok(())
    }

    pub async fn get_deleted(&self, storage: &str) -> anyhow::Result<Vec<(RelativePathBuf, u64)>> {
        let conn = self.storage.lock().await;
        let mut stmt = conn
            .prepare("SELECT path, timestamp FROM LogEntry WHERE storage = ? and entry_type = ?")?;

        stmt.query_map(params![storage, EntryType::Delete.to_string()], |row| {
            let path = row.get_ref("path")?;
            let timestamp: u64 = row.get("timestamp")?;

            Ok((path.as_str()?.into(), timestamp))
        })
        .and_then(|it| it.collect())
        .map_err(anyhow::Error::from)
    }

    pub async fn get_moved(
        &self,
        storage: &str,
    ) -> anyhow::Result<Vec<(RelativePathBuf, RelativePathBuf, u64)>> {
        let conn = self.storage.lock().await;
        let mut stmt = conn.prepare(
            "SELECT path, old_path, timestamp FROM LogEntry WHERE storage = ? and entry_type = ?",
        )?;

        stmt.query_map(params![storage, EntryType::Move.to_string()], |row| {
            let path = row.get_ref("path")?;
            let old_path = row.get_ref("old_path")?;
            let timestamp: u64 = row.get("timestamp")?;

            Ok((path.as_str()?.into(), old_path.as_str()?.into(), timestamp))
        })
        .and_then(|it| it.collect())
        .map_err(anyhow::Error::from)
    }

    pub async fn get_failed_writes(
        &self,
        storage: &str,
    ) -> anyhow::Result<HashSet<RelativePathBuf>> {
        let conn = self.storage.lock().await;
        let mut stmt = conn.prepare(
            "SELECT path FROM LogEntry WHERE storage = ? and entry_type = ? and status in ('fail', 'pending')"        
        )?;

        stmt.query_map(params![storage, EntryType::Write.to_string()], |row| {
            let path = row.get_ref("path")?;

            Ok(path.as_str()?.into())
        })
        .and_then(|it| it.collect())
        .map_err(anyhow::Error::from)
    }
}

fn get_storage(log_path: &Path) -> rusqlite::Result<Connection> {
    Connection::open_with_flags(
        log_path,
        OpenFlags::SQLITE_OPEN_CREATE | OpenFlags::SQLITE_OPEN_READ_WRITE,
    )
    .and_then(setup)
}

#[cfg(test)]
fn get_memory_storage() -> rusqlite::Result<Connection> {
    Connection::open_in_memory().and_then(setup)
}

fn setup(conn: Connection) -> rusqlite::Result<Connection> {
    conn.execute(
        r#"
            CREATE TABLE IF NOT EXISTS LogEntry (
                storage TEXT NOT NULL,
                path TEXT NOT NULL,
                old_path TEXT,
                entry_type TEXT NOT NULL check (entry_type in ('delete', 'write', 'move')),
                status TEXT NOT NULL check (status in ('pending', 'done', 'fail')),
                timestamp INTEGER NOT NULL,

                PRIMARY KEY (storage, path)
            )
        "#,
        (),
    )
    .map(|_| conn)
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use crate::config::PathConfig;

    use super::*;

    #[tokio::test]
    pub async fn test_can_run_migrations() -> anyhow::Result<()> {
        get_memory_storage()?;
        Ok(())
    }

    #[tokio::test]
    pub async fn test_can_retrieve_entries() -> anyhow::Result<()> {
        let log = TransactionLog::memory()?;
        let path_config = PathConfig {
            path: "/tmp".into(),
            ..Default::default()
        };

        let file_path = RelativePathBuf::new(&path_config, "/tmp/file".into())?;
        let entry_time = SystemTime::UNIX_EPOCH.elapsed().unwrap().as_secs() + 5;

        log.append_entry(
            "storage_0",
            &file_path,
            None,
            LogEntry {
                timestamp: entry_time,
                event_type: EntryType::Delete,
                event_status: EntryStatus::Done,
            },
        )
        .await?;

        let deleted = log.get_deleted("storage_0").await?;
        assert_eq!(deleted, vec![(file_path, entry_time)]);

        Ok(())
    }
}
