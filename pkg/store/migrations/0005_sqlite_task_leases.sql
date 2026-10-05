CREATE TABLE IF NOT EXISTS task_leases (
    fencing_token INTEGER PRIMARY KEY AUTOINCREMENT,
    lease_id TEXT NOT NULL UNIQUE,
    delivery_id TEXT NOT NULL,
    workflow_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    work_item TEXT NOT NULL,
    expires_at_ns INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('ACTIVE', 'EXPIRED', 'COMPLETED')),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS task_leases_task_active_idx
    ON task_leases (workflow_id, task_id)
    WHERE state IN ('ACTIVE', 'EXPIRED');

CREATE INDEX IF NOT EXISTS task_leases_expiry_idx
    ON task_leases (expires_at_ns, lease_id)
    WHERE state = 'ACTIVE';