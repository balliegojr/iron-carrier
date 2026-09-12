use rusqlite::Connection;

pub fn migrate(conn: Connection) -> rusqlite::Result<Connection> {
    conn.execute_batch(include_str!("database_setup.sql"))?;

    Ok(conn)
}
