CREATE SEQUENCE IF NOT EXISTS task_lease_fencing_token_seq AS BIGINT START WITH 1;

CREATE TABLE IF NOT EXISTS task_leases (
    lease_id TEXT PRIMARY KEY,
    delivery_id TEXT NOT NULL,
    workflow_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    work_item JSONB NOT NULL,
    fencing_token BIGINT NOT NULL UNIQUE CHECK (fencing_token > 0),
    expires_at TIMESTAMPTZ NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('ACTIVE', 'EXPIRED', 'COMPLETED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS task_leases_task_active_idx
    ON task_leases (workflow_id, task_id)
    WHERE state IN ('ACTIVE', 'EXPIRED');

CREATE INDEX IF NOT EXISTS task_leases_expiry_idx
    ON task_leases (expires_at, lease_id)
    WHERE state = 'ACTIVE';