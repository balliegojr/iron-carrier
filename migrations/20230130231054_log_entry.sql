CREATE TABLE LogEntry (
    storage TEXT NOT NULL,
    path TEXT NOT NULL,
    new_path TEXT,
    entry_type TEXT NOT NULL check (entry_type in ('delete', 'write', 'move')),
    status TEXT NOT NULL check (status in ('pending', 'done', 'fail')),
    timestamp INTEGER NOT NULL,

    PRIMARY KEY (storage, path)
)
