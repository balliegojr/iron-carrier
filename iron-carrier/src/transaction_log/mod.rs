mod entry_status;
mod entry_type;
mod log_entry;
mod migrate;
mod sync_entry;
mod sync_status;

pub use {
    entry_status::EntryStatus, entry_type::EntryType, log_entry::LogEntry, sync_entry::SyncEntry,
    sync_status::SyncStatus,
};

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

    pub fn memory() -> anyhow::Result<Self> {
        get_memory_storage()
            .map(|storage| Self {
                storage: Arc::new(Mutex::new(storage)),
            })
            .map_err(anyhow::Error::from)
    }

    pub async fn append_log_entry(
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

    pub async fn get_deleted_files(
        &self,
        storage: &str,
    ) -> anyhow::Result<Vec<(RelativePathBuf, u64)>> {
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

    /// get all the moved files in the log and returns a vec of (path, old path, timestamp)
    pub async fn get_moved_files(
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

    pub async fn save_sync_status(
        &self,
        node: &str,
        storage: &str,
        status: SyncStatus,
    ) -> anyhow::Result<()> {
        let timestamp = crate::time::system_time_to_secs(std::time::SystemTime::now());

        self.storage.lock().await.execute(
            "INSERT OR REPLACE INTO SyncEntry (node, storage, status, timestamp) VALUES (?,?,?,?)",
            params![node, storage, status.to_string(), timestamp],
        )?;

        Ok(())
    }

    pub async fn get_sync_entries(&self) -> anyhow::Result<Vec<SyncEntry>> {
        let conn = self.storage.lock().await;
        let mut stmt = conn.prepare("SELECT * FROM SyncEntry order by storage, node")?;

        stmt.query_map([], |row| {
            let storage = row.get_ref("storage")?.as_str()?.to_owned();
            let node = row.get_ref("node")?.as_str()?.to_owned();
            let sync_status = row
                .get_ref("status")?
                .as_str()?
                .parse()
                .map_err(|_| rusqlite::Error::InvalidQuery)?;
            let timestamp: u64 = row.get("timestamp")?;

            Ok(SyncEntry::new(storage, timestamp, sync_status, node))
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
    .and_then(migrate::migrate)
}

fn get_memory_storage() -> rusqlite::Result<Connection> {
    Connection::open_in_memory().and_then(migrate::migrate)
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

        log.append_log_entry(
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

        let deleted = log.get_deleted_files("storage_0").await?;
        assert_eq!(deleted, vec![(file_path, entry_time)]);

        Ok(())
    }

    #[tokio::test]
    pub async fn test_can_retried_sync_entries() -> anyhow::Result<()> {
        let log = TransactionLog::memory()?;

        log.save_sync_status("node_1", "storage_0", SyncStatus::Done)
            .await?;

        let entries = log.get_sync_entries().await?;
        assert_eq!(1, entries.len());
        assert_eq!("node_1", entries[0].node);
        assert_eq!("storage_0", entries[0].storage);
        assert_eq!(SyncStatus::Done, entries[0].sync_status);

        Ok(())
    }
}
