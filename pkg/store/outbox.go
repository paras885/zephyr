package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/zephyr-workflow/zephyr/pkg/domain"
	"github.com/zephyr-workflow/zephyr/pkg/queue"
)

func taskPublicationFromEvent(event domain.Event) (TaskPublicationRecord, bool, error) {
	if event.Type != domain.EventTaskScheduled && event.Type != domain.EventCompensationScheduled {
		return TaskPublicationRecord{}, false, nil
	}
	taskName, _ := event.Payload["task_name"].(string)
	if event.WorkflowID == "" || event.TaskID == "" || event.NodeID == "" || taskName == "" {
		return TaskPublicationRecord{}, false, fmt.Errorf("scheduled task publication requires workflow, task, node, and task names")
	}
	input, _ := event.Payload["input"].(map[string]any)
	return TaskPublicationRecord{Item: queue.WorkItem{
		ID: event.TaskID, WorkflowID: event.WorkflowID, TaskID: event.TaskID,
		NodeID: event.NodeID, TaskName: taskName, Payload: copyMap(input),
	}}, true, nil
}

func validateTaskPublicationRecord(record TaskPublicationRecord) error {
	if record.Item.ID == "" || record.Item.TaskID == "" || record.Item.ID != record.Item.TaskID || record.ClaimToken == "" {
		return fmt.Errorf("task publication identity and claim token are required")
	}
	return nil
}

func (store *PostgresStore) ClaimTaskPublications(ctx context.Context, limit int, leaseDuration time.Duration) ([]TaskPublicationRecord, error) {
	if limit < 1 || leaseDuration <= 0 {
		return nil, fmt.Errorf("task publication claim limit and lease duration must be positive")
	}
	leaseMicros := leaseDuration.Microseconds()
	if leaseMicros < 1 {
		leaseMicros = 1
	}
	claimToken := domain.NewID("task-publication-claim")
	rows, err := store.db.QueryContext(ctx, `WITH candidates AS (
		SELECT task_id FROM task_publication_outbox
		WHERE published_at IS NULL AND available_at <= NOW()
			AND (claimed_until IS NULL OR claimed_until <= NOW())
		ORDER BY created_at, task_id
		FOR UPDATE SKIP LOCKED
		LIMIT $1
	)
	UPDATE task_publication_outbox AS outbox
	SET claim_token = $2, claimed_until = NOW() + ($3 * INTERVAL '1 microsecond'), attempts = attempts + 1
	FROM candidates
	WHERE outbox.task_id = candidates.task_id
	RETURNING outbox.work_item`, limit, claimToken, leaseMicros)
	if err != nil {
		return nil, fmt.Errorf("claim PostgreSQL task publications: %w", err)
	}
	defer rows.Close()
	return scanTaskPublicationRows(rows, claimToken)
}

func (store *PostgresStore) MarkTaskPublished(ctx context.Context, record TaskPublicationRecord) error {
	if err := validateTaskPublicationRecord(record); err != nil {
		return err
	}
	result, err := store.db.ExecContext(ctx, `UPDATE task_publication_outbox
		SET published_at = NOW(), claimed_until = NULL, claim_token = NULL
		WHERE task_id = $1 AND claim_token = $2 AND published_at IS NULL`, record.Item.TaskID, record.ClaimToken)
	return taskPublicationUpdateResult(result, err)
}

func (store *PostgresStore) RetryTaskPublication(ctx context.Context, record TaskPublicationRecord, delay time.Duration) error {
	if err := validateTaskPublicationRecord(record); err != nil {
		return err
	}
	if delay < 0 {
		return fmt.Errorf("task publication retry delay cannot be negative")
	}
	delayMicros := delay.Microseconds()
	if delay > 0 && delayMicros < 1 {
		delayMicros = 1
	}
	result, err := store.db.ExecContext(ctx, `UPDATE task_publication_outbox
		SET available_at = NOW() + ($3 * INTERVAL '1 microsecond'), claimed_until = NULL, claim_token = NULL
		WHERE task_id = $1 AND claim_token = $2 AND published_at IS NULL`, record.Item.TaskID, record.ClaimToken, delayMicros)
	return taskPublicationUpdateResult(result, err)
}

func (store *SQLiteStore) ClaimTaskPublications(ctx context.Context, limit int, leaseDuration time.Duration) ([]TaskPublicationRecord, error) {
	if limit < 1 || leaseDuration <= 0 {
		return nil, fmt.Errorf("task publication claim limit and lease duration must be positive")
	}
	claimToken := domain.NewID("task-publication-claim")
	now := time.Now()
	rows, err := store.db.QueryContext(ctx, `UPDATE task_publication_outbox
		SET claim_token = ?, claimed_until = ?, attempts = attempts + 1
		WHERE task_id IN (
			SELECT task_id FROM task_publication_outbox
			WHERE published_at IS NULL AND available_at <= ?
				AND (claimed_until IS NULL OR claimed_until <= ?)
			ORDER BY created_at, task_id LIMIT ?
		)
		RETURNING work_item`, claimToken, now.Add(leaseDuration).UnixNano(), now.UnixNano(), now.UnixNano(), limit)
	if err != nil {
		return nil, fmt.Errorf("claim SQLite task publications: %w", err)
	}
	defer rows.Close()
	return scanTaskPublicationRows(rows, claimToken)
}

func (store *SQLiteStore) MarkTaskPublished(ctx context.Context, record TaskPublicationRecord) error {
	if err := validateTaskPublicationRecord(record); err != nil {
		return err
	}
	result, err := store.db.ExecContext(ctx, `UPDATE task_publication_outbox
		SET published_at = ?, claimed_until = NULL, claim_token = NULL
		WHERE task_id = ? AND claim_token = ? AND published_at IS NULL`, time.Now().UnixNano(), record.Item.TaskID, record.ClaimToken)
	return taskPublicationUpdateResult(result, err)
}

func (store *SQLiteStore) RetryTaskPublication(ctx context.Context, record TaskPublicationRecord, delay time.Duration) error {
	if err := validateTaskPublicationRecord(record); err != nil {
		return err
	}
	if delay < 0 {
		return fmt.Errorf("task publication retry delay cannot be negative")
	}
	result, err := store.db.ExecContext(ctx, `UPDATE task_publication_outbox
		SET available_at = ?, claimed_until = NULL, claim_token = NULL
		WHERE task_id = ? AND claim_token = ? AND published_at IS NULL`, time.Now().Add(delay).UnixNano(), record.Item.TaskID, record.ClaimToken)
	return taskPublicationUpdateResult(result, err)
}

func scanTaskPublicationRows(rows *sql.Rows, claimToken string) ([]TaskPublicationRecord, error) {
	var records []TaskPublicationRecord
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			return nil, err
		}
		var item queue.WorkItem
		if err := json.Unmarshal(payload, &item); err != nil {
			return nil, err
		}
		records = append(records, TaskPublicationRecord{Item: item, ClaimToken: claimToken})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

func insertTaskPublication(ctx context.Context, transaction *sql.Tx, event domain.Event, query string, args ...any) error {
	record, scheduled, err := taskPublicationFromEvent(event)
	if err != nil || !scheduled {
		return err
	}
	workItem, err := json.Marshal(record.Item)
	if err != nil {
		return fmt.Errorf("encode task publication: %w", err)
	}
	if _, err := transaction.ExecContext(ctx, query, append([]any{record.Item.TaskID, workItem}, args...)...); err != nil {
		return fmt.Errorf("enqueue task publication: %w", err)
	}
	return nil
}

func taskPublicationUpdateResult(result sql.Result, err error) error {
	if err != nil {
		return fmt.Errorf("update task publication outbox: %w", err)
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("check task publication outbox update: %w", err)
	}
	if updated == 0 {
		return ErrConflict
	}
	return nil
}

func (store *PostgresStore) ClaimOutbox(ctx context.Context, limit int, leaseDuration time.Duration) ([]OutboxRecord, error) {
	if limit < 1 || leaseDuration <= 0 {
		return nil, fmt.Errorf("outbox claim limit and lease duration must be positive")
	}
	leaseMicros := leaseDuration.Microseconds()
	if leaseMicros < 1 {
		leaseMicros = 1
	}
	claimToken := domain.NewID("outbox-claim")
	rows, err := store.db.QueryContext(ctx, `WITH candidates AS (
		SELECT workflow_id, sequence FROM workflow_event_outbox
		WHERE delivered_at IS NULL AND available_at <= NOW()
			AND (claimed_until IS NULL OR claimed_until <= NOW())
		ORDER BY created_at, workflow_id, sequence
		FOR UPDATE SKIP LOCKED
		LIMIT $1
	)
	UPDATE workflow_event_outbox AS outbox
	SET claim_token = $2, claimed_until = NOW() + ($3 * INTERVAL '1 microsecond'), attempts = attempts + 1
	FROM candidates
	WHERE outbox.workflow_id = candidates.workflow_id AND outbox.sequence = candidates.sequence
	RETURNING outbox.event`, limit, claimToken, leaseMicros)
	if err != nil {
		return nil, fmt.Errorf("claim PostgreSQL workflow events: %w", err)
	}
	defer rows.Close()
	records, err := scanOutboxRows(rows, claimToken)
	if err != nil {
		return nil, fmt.Errorf("decode claimed PostgreSQL workflow events: %w", err)
	}
	return records, nil
}

func (store *PostgresStore) MarkOutboxDelivered(ctx context.Context, record OutboxRecord) error {
	if err := validateOutboxRecord(record); err != nil {
		return err
	}
	result, err := store.db.ExecContext(ctx, `UPDATE workflow_event_outbox
		SET delivered_at = NOW(), claimed_until = NULL, claim_token = NULL
		WHERE workflow_id = $1 AND sequence = $2 AND claim_token = $3 AND delivered_at IS NULL`,
		record.Event.WorkflowID, int64(record.Event.Sequence), record.ClaimToken)
	return markOutboxResult(result, err)
}

func (store *PostgresStore) RetryOutbox(ctx context.Context, record OutboxRecord, delay time.Duration) error {
	if err := validateOutboxRecord(record); err != nil {
		return err
	}
	if delay < 0 {
		return fmt.Errorf("outbox retry delay cannot be negative")
	}
	delayMicros := delay.Microseconds()
	if delay > 0 && delayMicros < 1 {
		delayMicros = 1
	}
	result, err := store.db.ExecContext(ctx, `UPDATE workflow_event_outbox
		SET available_at = NOW() + ($4 * INTERVAL '1 microsecond'), claimed_until = NULL, claim_token = NULL
		WHERE workflow_id = $1 AND sequence = $2 AND claim_token = $3 AND delivered_at IS NULL`,
		record.Event.WorkflowID, int64(record.Event.Sequence), record.ClaimToken, delayMicros)
	return markOutboxResult(result, err)
}

func (store *SQLiteStore) ClaimOutbox(ctx context.Context, limit int, leaseDuration time.Duration) ([]OutboxRecord, error) {
	if limit < 1 || leaseDuration <= 0 {
		return nil, fmt.Errorf("outbox claim limit and lease duration must be positive")
	}
	claimToken := domain.NewID("outbox-claim")
	now := time.Now()
	rows, err := store.db.QueryContext(ctx, `UPDATE workflow_event_outbox
		SET claim_token = ?, claimed_until = ?, attempts = attempts + 1
		WHERE (workflow_id, sequence) IN (
			SELECT workflow_id, sequence FROM workflow_event_outbox
			WHERE delivered_at IS NULL AND available_at <= ?
				AND (claimed_until IS NULL OR claimed_until <= ?)
			ORDER BY created_at, workflow_id, sequence LIMIT ?
		)
		RETURNING event`, claimToken, now.Add(leaseDuration).UnixNano(), now.UnixNano(), now.UnixNano(), limit)
	if err != nil {
		return nil, fmt.Errorf("claim SQLite workflow events: %w", err)
	}
	defer rows.Close()
	return scanOutboxRows(rows, claimToken)
}

func (store *SQLiteStore) MarkOutboxDelivered(ctx context.Context, record OutboxRecord) error {
	if err := validateOutboxRecord(record); err != nil {
		return err
	}
	result, err := store.db.ExecContext(ctx, `UPDATE workflow_event_outbox
		SET delivered_at = ?, claimed_until = NULL, claim_token = NULL
		WHERE workflow_id = ? AND sequence = ? AND claim_token = ? AND delivered_at IS NULL`,
		time.Now().UnixNano(), record.Event.WorkflowID, int64(record.Event.Sequence), record.ClaimToken)
	return markOutboxResult(result, err)
}

func (store *SQLiteStore) RetryOutbox(ctx context.Context, record OutboxRecord, delay time.Duration) error {
	if err := validateOutboxRecord(record); err != nil {
		return err
	}
	if delay < 0 {
		return fmt.Errorf("outbox retry delay cannot be negative")
	}
	result, err := store.db.ExecContext(ctx, `UPDATE workflow_event_outbox
		SET available_at = ?, claimed_until = NULL, claim_token = NULL
		WHERE workflow_id = ? AND sequence = ? AND claim_token = ? AND delivered_at IS NULL`,
		time.Now().Add(delay).UnixNano(), record.Event.WorkflowID, int64(record.Event.Sequence), record.ClaimToken)
	return markOutboxResult(result, err)
}

func scanOutboxRows(rows *sql.Rows, claimToken string) ([]OutboxRecord, error) {
	var records []OutboxRecord
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			return nil, err
		}
		var event domain.Event
		if err := json.Unmarshal(payload, &event); err != nil {
			return nil, err
		}
		records = append(records, OutboxRecord{Event: event, ClaimToken: claimToken})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

func validateOutboxRecord(record OutboxRecord) error {
	if record.Event.WorkflowID == "" || record.Event.Sequence == 0 || record.ClaimToken == "" {
		return fmt.Errorf("outbox event identity and claim token are required")
	}
	return nil
}

func markOutboxResult(result sql.Result, err error) error {
	if err != nil {
		return fmt.Errorf("update workflow event outbox: %w", err)
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("check workflow event outbox update: %w", err)
	}
	if updated == 0 {
		return ErrConflict
	}
	return nil
}

var _ OutboxStore = (*PostgresStore)(nil)
var _ OutboxStore = (*SQLiteStore)(nil)
var _ TaskPublicationStore = (*PostgresStore)(nil)
var _ TaskPublicationStore = (*SQLiteStore)(nil)
