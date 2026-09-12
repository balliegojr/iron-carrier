#!/bin/bash
set -e
DB_PATH="$1"
SQL_SETUP="$2"
FILES_LIST="$3"
TIMESTAMP=$(date +%s)

sqlite3 "$DB_PATH" < "$SQL_SETUP"

while IFS= read -r filepath; do
    [ -z "$filepath" ] && continue
    sqlite3 "$DB_PATH" \
        "INSERT OR REPLACE INTO LogEntry (storage, path, old_path, entry_type, status, timestamp) \
         VALUES ('scripts', '${filepath}', NULL, 'delete', 'done', ${TIMESTAMP});"
done < "$FILES_LIST"
