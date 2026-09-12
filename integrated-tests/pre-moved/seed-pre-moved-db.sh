#!/bin/bash
set -e
DB_PATH="$1"
SQL_SETUP="$2"
MOVE_LIST="$3"
TIMESTAMP=$(date +%s)

sqlite3 "$DB_PATH" < "$SQL_SETUP"

while IFS=' ' read -r old_path new_path; do
    [ -z "$old_path" ] && continue
    sqlite3 "$DB_PATH" \
        "INSERT OR REPLACE INTO LogEntry (storage, path, old_path, entry_type, status, timestamp) \
         VALUES ('scripts', '${old_path}', NULL, 'delete', 'done', ${TIMESTAMP});"
    sqlite3 "$DB_PATH" \
        "INSERT OR REPLACE INTO LogEntry (storage, path, old_path, entry_type, status, timestamp) \
         VALUES ('scripts', '${new_path}', '${old_path}', 'move', 'done', ${TIMESTAMP});"
done < "$MOVE_LIST"
