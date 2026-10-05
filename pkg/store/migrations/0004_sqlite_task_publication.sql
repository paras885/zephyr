CREATE TABLE IF NOT EXISTS task_publication_outbox (
    task_id TEXT PRIMARY KEY,
    work_item TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    available_at INTEGER NOT NULL,
    claimed_until INTEGER,
    claim_token TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    published_at INTEGER
);

CREATE INDEX IF NOT EXISTS task_publication_outbox_pending_idx
    ON task_publication_outbox (available_at, created_at, task_id)
    WHERE published_at IS NULL;