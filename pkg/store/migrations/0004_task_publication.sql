CREATE TABLE IF NOT EXISTS task_publication_outbox (
    task_id TEXT PRIMARY KEY,
    work_item JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    claimed_until TIMESTAMPTZ,
    claim_token TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    published_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS task_publication_outbox_pending_idx
    ON task_publication_outbox (available_at, created_at, task_id)
    WHERE published_at IS NULL;