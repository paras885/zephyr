CREATE TABLE IF NOT EXISTS workflow_event_outbox (
    workflow_id TEXT NOT NULL,
    sequence BIGINT NOT NULL CHECK (sequence > 0),
    event JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    claimed_until TIMESTAMPTZ,
    claim_token TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    delivered_at TIMESTAMPTZ,
    PRIMARY KEY (workflow_id, sequence),
    FOREIGN KEY (workflow_id, sequence)
        REFERENCES workflow_events (workflow_id, sequence) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS workflow_event_outbox_pending_idx
    ON workflow_event_outbox (available_at, created_at)
    WHERE delivered_at IS NULL;