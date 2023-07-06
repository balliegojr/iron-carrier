mod entry_status;
mod entry_type;
mod log_entry;

pub use entry_status::EntryStatus;
pub use entry_type::EntryType;
pub use log_entry::LogEntry;

use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    Row, SqlitePool,
};

use std::{collections::HashSet, path::Path, time::Duration};
use thiserror::Error;

use crate::relative_path::RelativePathBuf;

const TRANSACTION_KEEP_LIMIT_SECS: u64 = 30 * 24 * 60 * 60;

pub struct TransactionLog {
    storage: SqlitePool,
}

impl TransactionLog {
    pub async fn load(log_path: &Path) -> crate::Result<Self> {
        get_storage(log_path).await.map(|storage| Self { storage })
    }

    #[cfg(test)]
    pub async fn memory() -> crate::Result<Self> {
        get_memory_storage().await.map(|storage| Self { storage })
    }

    pub async fn append_entry(
        &self,
        storage: &str,
        path: &RelativePathBuf,
        old_path: Option<&RelativePathBuf>,
        entry: LogEntry,
    ) -> crate::Result<()> {
        sqlx::query("INSERT OR REPLACE INTO LogEntry (storage, path, old_path, entry_type, status, timestamp) VALUES (?,?,?,?,?,?)")
            .bind(storage)
            .bind(path.to_str())
            .bind(old_path.map(|p| p.to_str()))
            .bind(&entry.event_type.to_string())
            .bind(&entry.event_status.to_string())
            .bind(entry.timestamp as i64)
            .execute(&self.storage).await?;

        Ok(())
    }

    pub async fn flush(&self) -> crate::Result<()> {
        let limit = (tokio::time::Instant::now()
            - Duration::from_secs(TRANSACTION_KEEP_LIMIT_SECS))
        .elapsed()
        .as_secs();

        sqlx::query("DELETE FROM LogEntry where timestamp < ?")
            .bind(limit as i64)
            .execute(&self.storage)
            .await?;

        Ok(())
    }

    pub async fn get_deleted(&self, storage: &str) -> crate::Result<Vec<(RelativePathBuf, u64)>> {
        sqlx::query("SELECT path, timestamp FROM LogEntry WHERE storage = ? and entry_type = ?")
            .bind(storage)
            .bind(EntryType::Delete.to_string())
            .map(|row| {
                (
                    row.get::<&str, &str>("path").into(),
                    row.get::<i64, &str>("timestamp") as u64,
                )
            })
            .fetch_all(&self.storage)
            .await
            .map_err(Box::from)
    }

    pub async fn get_moved(
        &self,
        storage: &str,
    ) -> crate::Result<Vec<(RelativePathBuf, RelativePathBuf, u64)>> {
        sqlx::query(
            "SELECT path, old_path, timestamp FROM LogEntry WHERE storage = ? and entry_type = ?",
        )
        .bind(storage)
        .bind(EntryType::Move.to_string())
        .map(|row| {
            (
                row.get::<&str, &str>("path").into(),
                row.get::<&str, &str>("old_path").into(),
                row.get::<i64, &str>("timestamp") as u64,
            )
        })
        .fetch_all(&self.storage)
        .await
        .map_err(Box::from)
    }

    pub async fn get_failed_writes(
        &self,
        storage: &str,
    ) -> crate::Result<HashSet<RelativePathBuf>> {
        sqlx::query("SELECT path FROM LogEntry WHERE storage = ? and entry_type = ? and status in ('fail', 'pending')")
            .bind(storage)
            .bind(EntryType::Write.to_string())
            .map(|row| row.get::<&str, &str>("path").into())
            .fetch_all(&self.storage)
            .await
            .map(HashSet::from_iter)
            .map_err(Box::from)
    }
}

pub async fn get_storage(log_path: &Path) -> crate::Result<SqlitePool> {
    let options = SqliteConnectOptions::new()
        .create_if_missing(true)
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
        .filename(log_path);

    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await?;

    sqlx::migrate!().run(&pool).await?;

    Ok(pool)
}

#[cfg(test)]
pub async fn get_memory_storage() -> crate::Result<SqlitePool> {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(":memory:")
        .await?;

    sqlx::migrate!().run(&pool).await?;

    Ok(pool)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    pub async fn test_can_run_migrations() -> crate::Result<()> {
        get_memory_storage().await?;
        Ok(())
    }
}

#[derive(Error, Debug)]
pub enum TransactionLogError {
    #[error("There is a invalid string in the log line")]
    InvalidStringFormat,
    #[error("There was an IO error: {0}")]
    IoError(#[from] std::io::Error),
}
