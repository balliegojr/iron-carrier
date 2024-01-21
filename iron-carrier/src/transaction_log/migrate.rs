use rusqlite::Connection;

pub fn migrate(conn: Connection) -> rusqlite::Result<Connection> {
    conn.execute_batch(
        r#"
            BEGIN;

            CREATE TABLE IF NOT EXISTS LogEntry (
                storage TEXT NOT NULL,
                path TEXT NOT NULL,
                old_path TEXT,
                entry_type TEXT NOT NULL check (entry_type in ('delete', 'write', 'move')),
                status TEXT NOT NULL check (status in ('pending', 'done', 'fail')),
                timestamp INTEGER NOT NULL,

                PRIMARY KEY (storage, path)
            );

            CREATE TABLE IF NOT EXISTS SyncEntry (
                node TEXT NOT NULL,
                storage TEXT NOT NULL,
                status TEXT NOT NULL check (status in ('started', 'done', 'fail')),
                timestamp INTEGER NOT NULL,

                PRIMARY KEY (node, storage)
            );

            COMMIT;
        "#,
    )?;

    Ok(conn)
}
