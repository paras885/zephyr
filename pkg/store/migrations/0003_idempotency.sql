CREATE TABLE IF NOT EXISTS workflow_idempotency_keys (
    workflow_name TEXT NOT NULL,
    definition_version INTEGER NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    workflow_id TEXT NOT NULL REFERENCES workflow_executions (id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (workflow_name, definition_version, idempotency_key)
);