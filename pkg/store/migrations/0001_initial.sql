CREATE TABLE IF NOT EXISTS workflow_definitions (
    workflow_name TEXT NOT NULL,
    version INTEGER NOT NULL CHECK (version > 0),
    definition JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (workflow_name, version)
);

CREATE TABLE IF NOT EXISTS workflow_executions (
    id TEXT PRIMARY KEY,
    workflow_name TEXT NOT NULL,
    definition_version INTEGER NOT NULL,
    status TEXT NOT NULL,
    context JSONB NOT NULL,
    snapshot JSONB NOT NULL,
    version BIGINT NOT NULL DEFAULT 1 CHECK (version > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (workflow_name, definition_version)
        REFERENCES workflow_definitions (workflow_name, version)
);

CREATE TABLE IF NOT EXISTS workflow_events (
    workflow_id TEXT NOT NULL REFERENCES workflow_executions (id) ON DELETE CASCADE,
    sequence BIGINT NOT NULL CHECK (sequence > 0),
    event_type TEXT NOT NULL,
    task_id TEXT,
    node_id TEXT,
    payload JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (workflow_id, sequence)
);

CREATE TABLE IF NOT EXISTS task_executions (
    workflow_id TEXT NOT NULL REFERENCES workflow_executions (id) ON DELETE CASCADE,
    node_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    task_name TEXT NOT NULL,
    status TEXT NOT NULL,
    result JSONB,
    error TEXT NOT NULL DEFAULT '',
    lease_token BIGINT NOT NULL DEFAULT 0,
    completed_at TIMESTAMPTZ,
    PRIMARY KEY (workflow_id, node_id),
    UNIQUE (workflow_id, task_id)
);

CREATE INDEX IF NOT EXISTS task_executions_ready_idx
    ON task_executions (workflow_id, node_id)
    WHERE status = 'READY';

CREATE INDEX IF NOT EXISTS workflow_executions_status_idx
    ON workflow_executions (status, updated_at);