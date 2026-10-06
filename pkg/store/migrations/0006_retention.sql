CREATE INDEX IF NOT EXISTS workflow_event_outbox_delivered_idx
    ON workflow_event_outbox (delivered_at, workflow_id, sequence)
    WHERE delivered_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS task_publication_outbox_published_idx
    ON task_publication_outbox (published_at, task_id)
    WHERE published_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS task_leases_completed_updated_idx
    ON task_leases (updated_at, lease_id)
    WHERE state = 'COMPLETED';