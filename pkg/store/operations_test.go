package store

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/zephyr-workflow/zephyr/pkg/domain"
	"github.com/zephyr-workflow/zephyr/pkg/lease"
	"github.com/zephyr-workflow/zephyr/pkg/queue"
)

func TestSQLiteOperationalMetricsAndBoundedRetentionCleanup(t *testing.T) {
	ctx := t.Context()
	local, err := OpenSQLiteStore(ctx, filepath.Join(t.TempDir(), "operations.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer local.Close()
	instance := testInstance(t)
	if err := local.Create(instance); err != nil {
		t.Fatal(err)
	}
	if err := local.Append(instance.ID, domain.Event{WorkflowID: instance.ID, Type: domain.EventWorkflowStarted}); err != nil {
		t.Fatal(err)
	}
	appendTaskPublicationEvents(t, func(event domain.Event) error { return local.Append(instance.ID, event) }, instance.ID)
	eventRecords, err := local.ClaimOutbox(ctx, 10, time.Minute)
	if err != nil || len(eventRecords) != 3 {
		t.Fatalf("event outbox claim = %d records, err=%v", len(eventRecords), err)
	}
	for _, record := range eventRecords[:2] {
		if err := local.MarkOutboxDelivered(ctx, record); err != nil {
			t.Fatal(err)
		}
	}
	taskRecords, err := local.ClaimTaskPublications(ctx, 10, time.Minute)
	if err != nil || len(taskRecords) != 2 {
		t.Fatalf("task publication claim = %d records, err=%v", len(taskRecords), err)
	}
	if err := local.MarkTaskPublished(ctx, taskRecords[0]); err != nil {
		t.Fatal(err)
	}

	completed, err := local.Claim(ctx, lease.Lease{
		ID: "completed-retention-lease", DeliveryID: "completed-retention-delivery",
		Item: queue.WorkItem{WorkflowID: instance.ID, TaskID: "completed-retention-task", NodeID: "task"},
	}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := local.CompleteWith(ctx, completed.ID, completed.Token, nil); err != nil {
		t.Fatal(err)
	}
	active, err := local.Claim(ctx, lease.Lease{
		ID: "active-retention-lease", DeliveryID: "active-retention-delivery",
		Item: queue.WorkItem{WorkflowID: instance.ID, TaskID: "active-retention-task", NodeID: "task"},
	}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	cutoff := time.Now().Add(-90 * 24 * time.Hour).UTC().Truncate(time.Second)
	old := cutoff.Add(-time.Hour).UnixNano()
	for _, record := range eventRecords[:2] {
		if _, err := local.db.ExecContext(ctx, `UPDATE workflow_event_outbox SET created_at = ?, delivered_at = ? WHERE workflow_id = ? AND sequence = ?`, old, old, record.Event.WorkflowID, record.Event.Sequence); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := local.db.ExecContext(ctx, `UPDATE task_publication_outbox SET created_at = ?, published_at = ? WHERE task_id = ?`, old, old, taskRecords[0].Item.TaskID); err != nil {
		t.Fatal(err)
	}
	if _, err := local.db.ExecContext(ctx, `UPDATE workflow_event_outbox SET created_at = ? WHERE workflow_id = ? AND sequence = ?`, old, eventRecords[2].Event.WorkflowID, eventRecords[2].Event.Sequence); err != nil {
		t.Fatal(err)
	}
	if _, err := local.db.ExecContext(ctx, `UPDATE task_publication_outbox SET created_at = ? WHERE task_id = ?`, old, taskRecords[1].Item.TaskID); err != nil {
		t.Fatal(err)
	}
	if _, err := local.db.ExecContext(ctx, `UPDATE task_leases SET updated_at = ? WHERE lease_id = ?`, cutoff.Add(-time.Hour).Format("2006-01-02 15:04:05"), completed.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := local.db.ExecContext(ctx, `UPDATE task_leases SET expires_at_ns = ? WHERE lease_id = ?`, time.Now().Add(-time.Minute).UnixNano(), active.ID); err != nil {
		t.Fatal(err)
	}
	idempotent := testInstance(t)
	if _, _, err := local.CreateIdempotent(ctx, idempotent, "retain-key", "retain-hash"); err != nil {
		t.Fatal(err)
	}

	metrics, err := local.OperationalMetrics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if metrics.WorkflowEventOutboxPending != 1 || metrics.TaskPublicationPending != 1 || metrics.ExpiredLeaseBacklog != 1 {
		t.Fatalf("operational backlog metrics = %#v", metrics)
	}
	if metrics.WorkflowEventOutboxRetries == 0 || metrics.TaskPublicationRetries == 0 || metrics.WorkflowEventOutboxOldestAge < 90*24*time.Hour-time.Minute {
		t.Fatalf("outbox age/retry metrics = %#v", metrics)
	}

	cleaned, err := local.CleanupRetention(ctx, cutoff, 1)
	if err != nil {
		t.Fatal(err)
	}
	if cleaned.WorkflowEventOutboxDeleted != 1 || cleaned.TaskPublicationDeleted != 1 || cleaned.CompletedLeasesDeleted != 1 {
		t.Fatalf("first bounded cleanup result = %#v", cleaned)
	}
	var deliveredEvents, taskPublications, completedLeases, activeLeases, idempotencyKeys int
	for query, destination := range map[string]*int{
		`SELECT COUNT(*) FROM workflow_event_outbox WHERE delivered_at IS NOT NULL`: &deliveredEvents,
		`SELECT COUNT(*) FROM task_publication_outbox`:                              &taskPublications,
		`SELECT COUNT(*) FROM task_leases WHERE state = 'COMPLETED'`:                &completedLeases,
		`SELECT COUNT(*) FROM task_leases WHERE lease_id = ? AND state = 'ACTIVE'`:  &activeLeases,
		`SELECT COUNT(*) FROM workflow_idempotency_keys`:                            &idempotencyKeys,
	} {
		var queryErr error
		if query == `SELECT COUNT(*) FROM task_leases WHERE lease_id = ? AND state = 'ACTIVE'` {
			queryErr = local.db.QueryRowContext(ctx, query, active.ID).Scan(destination)
		} else {
			queryErr = local.db.QueryRowContext(ctx, query).Scan(destination)
		}
		if queryErr != nil {
			t.Fatal(queryErr)
		}
	}
	if deliveredEvents != 1 || taskPublications != 1 || completedLeases != 0 || activeLeases != 1 || idempotencyKeys != 1 {
		t.Fatalf("retention deleted protected or pending data: eventOutbox=%d taskOutbox=%d completedLeases=%d activeLeases=%d idempotency=%d", deliveredEvents, taskPublications, completedLeases, activeLeases, idempotencyKeys)
	}
	if _, err := local.Get(instance.ID); err != nil {
		t.Fatalf("cleanup deleted workflow execution/history: %v", err)
	}
}
