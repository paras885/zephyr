package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/zephyr-workflow/zephyr/pkg/domain"
	"github.com/zephyr-workflow/zephyr/pkg/lease"
	"github.com/zephyr-workflow/zephyr/pkg/queue"
	"github.com/zephyr-workflow/zephyr/pkg/store"
)

func TestPostgresOperationalMetricsAndBoundedRetentionCleanup(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "postgres:16-alpine",
			Env:          map[string]string{"POSTGRES_USER": "zephyr", "POSTGRES_PASSWORD": "zephyr-test", "POSTGRES_DB": "zephyr"},
			ExposedPorts: []string{"5432/tcp"},
			// ForListeningPort alone is insufficient: Postgres opens its TCP
			// listener for a temporary initdb server before restarting into
			// the real server, so "ready to accept connections" is logged
			// twice before it's actually ready for use.
			WaitingFor: wait.ForAll(
				wait.ForListeningPort("5432/tcp"),
				wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
			),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start PostgreSQL Testcontainer: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if err := container.Terminate(cleanupCtx); err != nil {
			t.Errorf("terminate PostgreSQL Testcontainer: %v", err)
		}
	})
	host, err := container.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := container.MappedPort(ctx, "5432/tcp")
	if err != nil {
		t.Fatal(err)
	}
	dataSource := fmt.Sprintf("postgres://zephyr:zephyr-test@%s:%s/zephyr?sslmode=disable", host, port.Port())
	executions, err := store.OpenPostgresStore(ctx, dataSource)
	if err != nil {
		t.Fatal(err)
	}
	defer executions.Close()
	if err := executions.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	queryDB, err := sql.Open("pgx", dataSource)
	if err != nil {
		t.Fatal(err)
	}
	defer queryDB.Close()

	instance, err := domain.NewWorkflowInstance(domain.WorkflowDef{
		Name: "retention-workflow", Version: 1,
		Nodes: map[string]domain.NodeDefinition{"task": {ID: "task", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "run"}}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := executions.Create(instance); err != nil {
		t.Fatal(err)
	}
	for _, event := range []domain.Event{
		{WorkflowID: instance.ID, Type: domain.EventWorkflowStarted},
		{WorkflowID: instance.ID, TaskID: "task-publication", NodeID: "task", Type: domain.EventTaskScheduled, Payload: map[string]any{"task_name": "run", "input": map[string]any{}}},
		{WorkflowID: instance.ID, TaskID: "compensation-publication", NodeID: "compensation", Type: domain.EventCompensationScheduled, Payload: map[string]any{"task_name": "undo", "input": map[string]any{}}},
	} {
		if err := executions.Append(instance.ID, event); err != nil {
			t.Fatal(err)
		}
	}
	eventRecords, err := executions.ClaimOutbox(ctx, 10, time.Minute)
	if err != nil || len(eventRecords) != 3 {
		t.Fatalf("event outbox claim = %d records, err=%v", len(eventRecords), err)
	}
	for _, record := range eventRecords[:2] {
		if err := executions.MarkOutboxDelivered(ctx, record); err != nil {
			t.Fatal(err)
		}
	}
	taskRecords, err := executions.ClaimTaskPublications(ctx, 10, time.Minute)
	if err != nil || len(taskRecords) != 2 {
		t.Fatalf("task publication claim = %d records, err=%v", len(taskRecords), err)
	}
	if err := executions.MarkTaskPublished(ctx, taskRecords[0]); err != nil {
		t.Fatal(err)
	}

	completed, err := executions.Claim(ctx, lease.Lease{
		ID: "completed-retention-lease", DeliveryID: "completed-retention-delivery",
		Item: queue.WorkItem{WorkflowID: instance.ID, TaskID: "completed-retention-task", NodeID: "task"},
	}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executions.CompleteWith(ctx, completed.ID, completed.Token, nil); err != nil {
		t.Fatal(err)
	}
	active, err := executions.Claim(ctx, lease.Lease{
		ID: "active-retention-lease", DeliveryID: "active-retention-delivery",
		Item: queue.WorkItem{WorkflowID: instance.ID, TaskID: "active-retention-task", NodeID: "task"},
	}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	cutoff := time.Now().Add(-90 * 24 * time.Hour).UTC()
	old := cutoff.Add(-time.Hour)
	for _, record := range eventRecords[:2] {
		if _, err := queryDB.ExecContext(ctx, `UPDATE workflow_event_outbox SET created_at = $1, delivered_at = $1 WHERE workflow_id = $2 AND sequence = $3`, old, record.Event.WorkflowID, record.Event.Sequence); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := queryDB.ExecContext(ctx, `UPDATE task_publication_outbox SET created_at = $1, published_at = $1 WHERE task_id = $2`, old, taskRecords[0].Item.TaskID); err != nil {
		t.Fatal(err)
	}
	if _, err := queryDB.ExecContext(ctx, `UPDATE workflow_event_outbox SET created_at = $1 WHERE workflow_id = $2 AND sequence = $3`, old, eventRecords[2].Event.WorkflowID, eventRecords[2].Event.Sequence); err != nil {
		t.Fatal(err)
	}
	if _, err := queryDB.ExecContext(ctx, `UPDATE task_publication_outbox SET created_at = $1 WHERE task_id = $2`, old, taskRecords[1].Item.TaskID); err != nil {
		t.Fatal(err)
	}
	if _, err := queryDB.ExecContext(ctx, `UPDATE task_leases SET updated_at = $1 WHERE lease_id = $2`, old, completed.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := queryDB.ExecContext(ctx, `UPDATE task_leases SET expires_at = NOW() - INTERVAL '1 minute' WHERE lease_id = $1`, active.ID); err != nil {
		t.Fatal(err)
	}
	idempotent, err := domain.NewWorkflowInstance(domain.WorkflowDef{
		Name: "idempotent-retention", Version: 1,
		Nodes: map[string]domain.NodeDefinition{"task": {ID: "task", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "run"}}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := executions.CreateIdempotent(ctx, idempotent, "retain-key", "retain-hash"); err != nil {
		t.Fatal(err)
	}

	metrics, err := executions.OperationalMetrics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if metrics.WorkflowEventOutboxPending != 1 || metrics.TaskPublicationPending != 1 || metrics.ExpiredLeaseBacklog != 1 || metrics.WorkflowEventOutboxRetries == 0 || metrics.TaskPublicationRetries == 0 {
		t.Fatalf("operational metrics = %#v", metrics)
	}
	cleaned, err := executions.CleanupRetention(ctx, cutoff, 1)
	if err != nil {
		t.Fatal(err)
	}
	if cleaned.WorkflowEventOutboxDeleted != 1 || cleaned.TaskPublicationDeleted != 1 || cleaned.CompletedLeasesDeleted != 1 {
		t.Fatalf("bounded cleanup result = %#v", cleaned)
	}
	var deliveredEvents, taskPublications, completedLeases, activeLeases, idempotencyKeys, workflowEvents int
	for query, destination := range map[string]*int{
		`SELECT COUNT(*) FROM workflow_event_outbox WHERE delivered_at IS NOT NULL`: &deliveredEvents,
		`SELECT COUNT(*) FROM task_publication_outbox`:                              &taskPublications,
		`SELECT COUNT(*) FROM task_leases WHERE state = 'COMPLETED'`:                &completedLeases,
		`SELECT COUNT(*) FROM task_leases WHERE lease_id = $1 AND state = 'ACTIVE'`: &activeLeases,
		`SELECT COUNT(*) FROM workflow_idempotency_keys`:                            &idempotencyKeys,
		`SELECT COUNT(*) FROM workflow_events WHERE workflow_id = $1`:               &workflowEvents,
	} {
		var queryErr error
		switch query {
		case `SELECT COUNT(*) FROM task_leases WHERE lease_id = $1 AND state = 'ACTIVE'`:
			queryErr = queryDB.QueryRowContext(ctx, query, active.ID).Scan(destination)
		case `SELECT COUNT(*) FROM workflow_events WHERE workflow_id = $1`:
			queryErr = queryDB.QueryRowContext(ctx, query, instance.ID).Scan(destination)
		default:
			queryErr = queryDB.QueryRowContext(ctx, query).Scan(destination)
		}
		if queryErr != nil {
			t.Fatal(queryErr)
		}
	}
	if deliveredEvents != 1 || taskPublications != 1 || completedLeases != 0 || activeLeases != 1 || idempotencyKeys != 1 || workflowEvents != 3 {
		t.Fatalf("cleanup removed protected or pending records: events=%d taskOutbox=%d completedLeases=%d activeLeases=%d idempotency=%d history=%d", deliveredEvents, taskPublications, completedLeases, activeLeases, idempotencyKeys, workflowEvents)
	}
}
