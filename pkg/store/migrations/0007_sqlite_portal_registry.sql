ALTER TABLE workflow_definitions ADD COLUMN source TEXT;
CREATE TABLE portal_sessions (
    id TEXT PRIMARY KEY,
    data TEXT NOT NULL,
    expires_at INTEGER NOT NULL
);
CREATE INDEX portal_sessions_expiry ON portal_sessions (expires_at);
