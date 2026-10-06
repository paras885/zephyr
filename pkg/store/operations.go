package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

func (store *PostgresStore) OperationalMetrics(ctx context.Context) (OperationalMetrics, error) {
	metrics := OperationalMetrics{}
	var err error
	metrics.WorkflowStates, err = readPostgresStateCounts(ctx, store.db, `SELECT status, COUNT(*) FROM workflow_executions GROUP BY status`)
	if err != nil {
		return OperationalMetrics{}, fmt.Errorf("read workflow states: %w", err)
	}
	metrics.TaskStates, err = readPostgresStateCounts(ctx, store.db, `SELECT status, COUNT(*) FROM task_executions GROUP BY status`)
	if err != nil {
		return OperationalMetrics{}, fmt.Errorf("read task states: %w", err)
	}
	metrics.LeaseStates, err = readPostgresStateCounts(ctx, store.db, `SELECT state, COUNT(*) FROM task_leases GROUP BY state`)
	if err != nil {
		return OperationalMetrics{}, fmt.Errorf("read lease states: %w", err)
	}
	if err := readPostgresOutboxMetrics(ctx, store.db, "workflow_event_outbox", "delivered_at", &metrics.WorkflowEventOutboxPending, &metrics.WorkflowEventOutboxOldestAge, &metrics.WorkflowEventOutboxRetries); err != nil {
		return OperationalMetrics{}, fmt.Errorf("read workflow event outbox metrics: %w", err)
	}
	if err := readPostgresOutboxMetrics(ctx, store.db, "task_publication_outbox", "published_at", &metrics.TaskPublicationPending, &metrics.TaskPublicationOldestAge, &metrics.TaskPublicationRetries); err != nil {
		return OperationalMetrics{}, fmt.Errorf("read task publication outbox metrics: %w", err)
	}
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM task_leases WHERE state = 'ACTIVE' AND expires_at <= NOW()`).Scan(&metrics.ExpiredLeaseBacklog); err != nil {
		return OperationalMetrics{}, fmt.Errorf("read expired lease backlog: %w", err)
	}
	return metrics, nil
}

func readPostgresStateCounts(ctx context.Context, db *sql.DB, query string) (map[string]int64, error) {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	counts := make(map[string]int64)
	for rows.Next() {
		var state string
		var count int64
		if err := rows.Scan(&state, &count); err != nil {
			return nil, err
		}
		counts[state] = count
	}
	return counts, rows.Err()
}

func readPostgresOutboxMetrics(ctx context.Context, db *sql.DB, table, deliveredColumn string, count *int64, oldestAge *time.Duration, retries *int64) error {
	query := fmt.Sprintf(`SELECT COUNT(*), MIN(created_at), COALESCE(SUM(attempts), 0) FROM %s WHERE %s IS NULL`, table, deliveredColumn)
	var oldest sql.NullTime
	if err := db.QueryRowContext(ctx, query).Scan(count, &oldest, retries); err != nil {
		return err
	}
	if oldest.Valid {
		*oldestAge = time.Since(oldest.Time)
		if *oldestAge < 0 {
			*oldestAge = 0
		}
	}
	return nil
}

func (store *SQLiteStore) OperationalMetrics(ctx context.Context) (OperationalMetrics, error) {
	metrics := OperationalMetrics{}
	var err error
	metrics.WorkflowStates, err = readSQLiteStateCounts(ctx, store.db, `SELECT status, COUNT(*) FROM workflow_executions GROUP BY status`)
	if err != nil {
		return OperationalMetrics{}, fmt.Errorf("read SQLite workflow states: %w", err)
	}
	metrics.TaskStates, err = readSQLiteStateCounts(ctx, store.db, `SELECT status, COUNT(*) FROM task_executions GROUP BY status`)
	if err != nil {
		return OperationalMetrics{}, fmt.Errorf("read SQLite task states: %w", err)
	}
	metrics.LeaseStates, err = readSQLiteStateCounts(ctx, store.db, `SELECT state, COUNT(*) FROM task_leases GROUP BY state`)
	if err != nil {
		return OperationalMetrics{}, fmt.Errorf("read SQLite lease states: %w", err)
	}
	if err := readSQLiteOutboxMetrics(ctx, store.db, "workflow_event_outbox", "delivered_at", &metrics.WorkflowEventOutboxPending, &metrics.WorkflowEventOutboxOldestAge, &metrics.WorkflowEventOutboxRetries); err != nil {
		return OperationalMetrics{}, fmt.Errorf("read SQLite workflow event outbox metrics: %w", err)
	}
	if err := readSQLiteOutboxMetrics(ctx, store.db, "task_publication_outbox", "published_at", &metrics.TaskPublicationPending, &metrics.TaskPublicationOldestAge, &metrics.TaskPublicationRetries); err != nil {
		return OperationalMetrics{}, fmt.Errorf("read SQLite task publication outbox metrics: %w", err)
	}
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM task_leases WHERE state = 'ACTIVE' AND expires_at_ns <= ?`, time.Now().UnixNano()).Scan(&metrics.ExpiredLeaseBacklog); err != nil {
		return OperationalMetrics{}, fmt.Errorf("read SQLite expired lease backlog: %w", err)
	}
	return metrics, nil
}

func readSQLiteStateCounts(ctx context.Context, db *sql.DB, query string) (map[string]int64, error) {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	counts := make(map[string]int64)
	for rows.Next() {
		var state string
		var count int64
		if err := rows.Scan(&state, &count); err != nil {
			return nil, err
		}
		counts[state] = count
	}
	return counts, rows.Err()
}

func readSQLiteOutboxMetrics(ctx context.Context, db *sql.DB, table, deliveredColumn string, count *int64, oldestAge *time.Duration, retries *int64) error {
	query := fmt.Sprintf(`SELECT COUNT(*), MIN(created_at), COALESCE(SUM(attempts), 0) FROM %s WHERE %s IS NULL`, table, deliveredColumn)
	var oldest sql.NullInt64
	if err := db.QueryRowContext(ctx, query).Scan(count, &oldest, retries); err != nil {
		return err
	}
	if oldest.Valid {
		*oldestAge = time.Since(time.Unix(0, oldest.Int64))
		if *oldestAge < 0 {
			*oldestAge = 0
		}
	}
	return nil
}

func (store *PostgresStore) CleanupRetention(ctx context.Context, deliveredBefore time.Time, batchSize int) (RetentionCleanupResult, error) {
	if err := validateRetentionCleanup(deliveredBefore, batchSize); err != nil {
		return RetentionCleanupResult{}, err
	}
	transaction, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return RetentionCleanupResult{}, fmt.Errorf("begin retention cleanup: %w", err)
	}
	defer transaction.Rollback()
	var result RetentionCleanupResult
	result.WorkflowEventOutboxDeleted, err = deletePostgresRetentionBatch(ctx, transaction, `WITH candidates AS (
		SELECT workflow_id, sequence FROM workflow_event_outbox
		WHERE delivered_at IS NOT NULL AND delivered_at < $1
		ORDER BY delivered_at, workflow_id, sequence FOR UPDATE SKIP LOCKED LIMIT $2
	)
	DELETE FROM workflow_event_outbox AS outbox USING candidates
	WHERE outbox.workflow_id = candidates.workflow_id AND outbox.sequence = candidates.sequence`, deliveredBefore, batchSize)
	if err != nil {
		return RetentionCleanupResult{}, fmt.Errorf("clean workflow event outbox: %w", err)
	}
	result.TaskPublicationDeleted, err = deletePostgresRetentionBatch(ctx, transaction, `WITH candidates AS (
		SELECT task_id FROM task_publication_outbox
		WHERE published_at IS NOT NULL AND published_at < $1
		ORDER BY published_at, task_id FOR UPDATE SKIP LOCKED LIMIT $2
	)
	DELETE FROM task_publication_outbox AS outbox USING candidates WHERE outbox.task_id = candidates.task_id`, deliveredBefore, batchSize)
	if err != nil {
		return RetentionCleanupResult{}, fmt.Errorf("clean task publication outbox: %w", err)
	}
	result.CompletedLeasesDeleted, err = deletePostgresRetentionBatch(ctx, transaction, `WITH candidates AS (
		SELECT lease_id FROM task_leases
		WHERE state = 'COMPLETED' AND updated_at < $1
		ORDER BY updated_at, lease_id FOR UPDATE SKIP LOCKED LIMIT $2
	)
	DELETE FROM task_leases AS leases USING candidates WHERE leases.lease_id = candidates.lease_id`, deliveredBefore, batchSize)
	if err != nil {
		return RetentionCleanupResult{}, fmt.Errorf("clean completed leases: %w", err)
	}
	if err := transaction.Commit(); err != nil {
		return RetentionCleanupResult{}, fmt.Errorf("commit retention cleanup: %w", err)
	}
	return result, nil
}

func deletePostgresRetentionBatch(ctx context.Context, transaction *sql.Tx, query string, cutoff time.Time, batchSize int) (int64, error) {
	result, err := transaction.ExecContext(ctx, query, cutoff, batchSize)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (store *SQLiteStore) CleanupRetention(ctx context.Context, deliveredBefore time.Time, batchSize int) (RetentionCleanupResult, error) {
	if err := validateRetentionCleanup(deliveredBefore, batchSize); err != nil {
		return RetentionCleanupResult{}, err
	}
	transaction, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return RetentionCleanupResult{}, fmt.Errorf("begin SQLite retention cleanup: %w", err)
	}
	defer transaction.Rollback()
	cutoff := deliveredBefore.UnixNano()
	var result RetentionCleanupResult
	result.WorkflowEventOutboxDeleted, err = deleteSQLiteRetentionBatch(ctx, transaction, `DELETE FROM workflow_event_outbox
		WHERE rowid IN (SELECT rowid FROM workflow_event_outbox WHERE delivered_at IS NOT NULL AND delivered_at < ? ORDER BY delivered_at LIMIT ?)`, cutoff, batchSize)
	if err != nil {
		return RetentionCleanupResult{}, fmt.Errorf("clean SQLite workflow event outbox: %w", err)
	}
	result.TaskPublicationDeleted, err = deleteSQLiteRetentionBatch(ctx, transaction, `DELETE FROM task_publication_outbox
		WHERE rowid IN (SELECT rowid FROM task_publication_outbox WHERE published_at IS NOT NULL AND published_at < ? ORDER BY published_at LIMIT ?)`, cutoff, batchSize)
	if err != nil {
		return RetentionCleanupResult{}, fmt.Errorf("clean SQLite task publication outbox: %w", err)
	}
	result.CompletedLeasesDeleted, err = deleteSQLiteRetentionBatch(ctx, transaction, `DELETE FROM task_leases
		WHERE rowid IN (SELECT rowid FROM task_leases WHERE state = 'COMPLETED' AND updated_at < ? ORDER BY updated_at LIMIT ?)`, deliveredBefore.UTC().Format("2006-01-02 15:04:05"), batchSize)
	if err != nil {
		return RetentionCleanupResult{}, fmt.Errorf("clean SQLite completed leases: %w", err)
	}
	if err := transaction.Commit(); err != nil {
		return RetentionCleanupResult{}, fmt.Errorf("commit SQLite retention cleanup: %w", err)
	}
	return result, nil
}

func deleteSQLiteRetentionBatch(ctx context.Context, transaction *sql.Tx, query string, cutoff any, batchSize int) (int64, error) {
	result, err := transaction.ExecContext(ctx, query, cutoff, batchSize)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func validateRetentionCleanup(cutoff time.Time, batchSize int) error {
	if cutoff.IsZero() || batchSize < 1 {
		return fmt.Errorf("retention cutoff and positive batch size are required")
	}
	return nil
}
