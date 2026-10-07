package integration_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/zephyr-workflow/zephyr/pkg/domain"
	"github.com/zephyr-workflow/zephyr/pkg/lease"
	"github.com/zephyr-workflow/zephyr/pkg/queue"
	"github.com/zephyr-workflow/zephyr/pkg/store"
)

func TestPostgresAtomicLeaseTransitions(t *testing.T) {
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
		t.Fatalf("open PostgreSQL execution store: %v", err)
	}
	defer executions.Close()
	if err := executions.Migrate(ctx); err != nil {
		t.Fatalf("migrate PostgreSQL schema: %v", err)
	}
	queryDB, err := sql.Open("pgx", dataSource)
	if err != nil {
		t.Fatal(err)
	}
	defer queryDB.Close()

	t.Run("rollback_and_response_loss_replay_keep_one_transition_and_publication", func(t *testing.T) {
		instance, active := createPostgresAtomicTask(t, ctx, executions, queryDB, "atomic-completion", 1, time.Minute)
		taskID := instance.ID + "-task"
		downstreamTaskID := instance.ID + "-downstream"
		transition := []domain.Event{
			{WorkflowID: instance.ID, TaskID: taskID, NodeID: "run", Type: domain.EventTaskCompleted, Payload: map[string]any{"result": map[string]any{"ok": true}}},
			{WorkflowID: instance.ID, TaskID: downstreamTaskID, NodeID: "next", Type: domain.EventTaskScheduled, Payload: map[string]any{"task_name": "next", "attempt": 1}},
		}
		injectedFailure := errors.New("injected failure before commit")
		if _, err := executions.CompleteWith(ctx, active.ID, active.Token, func(_ lease.Lease, mutation lease.MutationStore) error {
			if err := mutation.AppendMany(instance.ID, transition); err != nil {
				return err
			}
			return injectedFailure
		}); !errors.Is(err, injectedFailure) {
			t.Fatalf("injected pre-commit error = %v", err)
		}
		assertPostgresAtomicState(t, ctx, executions, queryDB, instance.ID, domain.TaskReady, 2, "ACTIVE", 0)

		apply := func(_ lease.Lease, mutation lease.MutationStore) error {
			current, err := mutation.Get(instance.ID)
			if err != nil {
				return err
			}
			if current.Tasks["run"].Status == domain.TaskCompleted {
				return nil
			}
			return mutation.AppendMany(instance.ID, transition)
		}
		if _, err := executions.CompleteWith(ctx, active.ID, active.Token, apply); err != nil {
			t.Fatalf("commit completion: %v", err)
		}
		if _, err := executions.CompleteWith(ctx, active.ID, active.Token, apply); err != nil {
			t.Fatalf("replay after simulated response loss: %v", err)
		}
		assertPostgresAtomicState(t, ctx, executions, queryDB, instance.ID, domain.TaskCompleted, 4, "COMPLETED", 1)
		if _, err := executions.CompleteWith(ctx, active.ID, active.Token+1, apply); !errors.Is(err, lease.ErrStaleToken) {
			t.Fatalf("stale fencing token error = %v, want ErrStaleToken", err)
		}
		assertPostgresAtomicState(t, ctx, executions, queryDB, instance.ID, domain.TaskCompleted, 4, "COMPLETED", 1)
	})

	t.Run("expiry_retries_with_task_attempt_and_outbox_in_one_commit", func(t *testing.T) {
		instance, active := createPostgresAtomicTask(t, ctx, executions, queryDB, "atomic-expiry-retry", 1, time.Minute)
		taskID := instance.ID + "-task"
		if _, err := queryDB.ExecContext(ctx, `UPDATE task_leases SET expires_at = clock_timestamp() - INTERVAL '1 second' WHERE lease_id = $1`, active.ID); err != nil {
			t.Fatal(err)
		}
		retryAt := time.Now().Add(time.Minute)
		_, expired, err := executions.ExpireWith(ctx, active.ID, active.Token, func(_ lease.Lease, mutation lease.MutationStore) error {
			return mutation.AppendMany(instance.ID, []domain.Event{
				{WorkflowID: instance.ID, TaskID: taskID, NodeID: "run", Type: domain.EventTaskFailed, Payload: map[string]any{"error": "task lease expired"}},
				{WorkflowID: instance.ID, TaskID: taskID, NodeID: "run", Type: domain.EventTaskRetryScheduled, Payload: map[string]any{"error": "task lease expired", "attempt": 2, "retry_at": retryAt}},
			})
		})
		if err != nil || !expired {
			t.Fatalf("expire lease = %t, error = %v", expired, err)
		}
		updated, err := executions.Get(instance.ID)
		if err != nil {
			t.Fatal(err)
		}
		if task := updated.Tasks["run"]; task.Status != domain.TaskRetrying || task.Attempt != 2 || !task.RetryAt.Equal(retryAt) {
			t.Fatalf("retry task projection = %#v", task)
		}
		var state string
		if err := queryDB.QueryRowContext(ctx, `SELECT state FROM task_leases WHERE lease_id = $1`, active.ID).Scan(&state); err != nil {
			t.Fatal(err)
		}
		if state != "EXPIRED" {
			t.Fatalf("lease state = %q, want EXPIRED", state)
		}
		if _, expired, err := executions.ExpireWith(ctx, active.ID, active.Token, nil); err == nil || expired {
			t.Fatalf("duplicate expiry = %t, %v; expected expired lease to be rejected", expired, err)
		}
	})

	t.Run("completion_races_expiry_with_exactly_one_winner", func(t *testing.T) {
		instance, active := createPostgresAtomicTask(t, ctx, executions, queryDB, "atomic-expiry-race", 1, time.Minute)
		taskID := instance.ID + "-task"
		if _, err := queryDB.ExecContext(ctx, `UPDATE task_leases SET expires_at = clock_timestamp() - INTERVAL '1 second' WHERE lease_id = $1`, active.ID); err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		var group sync.WaitGroup
		var completionErr error
		var expirationErr error
		var expired bool
		group.Add(2)
		go func() {
			defer group.Done()
			<-start
			_, completionErr = executions.CompleteWith(ctx, active.ID, active.Token, func(_ lease.Lease, mutation lease.MutationStore) error {
				return mutation.Append(instance.ID, domain.Event{WorkflowID: instance.ID, TaskID: taskID, NodeID: "run", Type: domain.EventTaskCompleted})
			})
		}()
		go func() {
			defer group.Done()
			<-start
			_, expired, expirationErr = executions.ExpireWith(ctx, active.ID, active.Token, func(_ lease.Lease, mutation lease.MutationStore) error {
				return mutation.AppendMany(instance.ID, []domain.Event{
					{WorkflowID: instance.ID, TaskID: taskID, NodeID: "run", Type: domain.EventTaskFailed, Payload: map[string]any{"error": "task lease expired"}},
					{WorkflowID: instance.ID, Type: domain.EventWorkflowFailureRequested, Payload: map[string]any{"error": "task lease expired"}},
				})
			})
		}()
		close(start)
		group.Wait()
		if !errors.Is(completionErr, lease.ErrLeaseExpired) || expirationErr != nil || !expired {
			t.Fatalf("completion/expiry outcomes: completion=%v expiry=%t/%v", completionErr, expired, expirationErr)
		}
		assertPostgresAtomicState(t, ctx, executions, queryDB, instance.ID, domain.TaskFailed, 4, "EXPIRED", 0)
	})

	t.Run("heartbeat_waits_for_row_lock_and_observes_postgres_deadline", func(t *testing.T) {
		instance, active := createPostgresAtomicTask(t, ctx, executions, queryDB, "atomic-heartbeat-race", 0, time.Minute)
		blocker, err := queryDB.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer blocker.Rollback()
		var lockedLease string
		if err := blocker.QueryRowContext(ctx, `SELECT lease_id FROM task_leases WHERE lease_id = $1 FOR UPDATE`, active.ID).Scan(&lockedLease); err != nil {
			t.Fatal(err)
		}
		heartbeatDone := make(chan error, 1)
		go func() {
			_, err := executions.Renew(ctx, active.ID, active.Token, time.Minute)
			heartbeatDone <- err
		}()
		if _, err := blocker.ExecContext(ctx, `UPDATE task_leases SET expires_at = clock_timestamp() - INTERVAL '1 second' WHERE lease_id = $1`, active.ID); err != nil {
			t.Fatal(err)
		}
		if err := blocker.Commit(); err != nil {
			t.Fatal(err)
		}
		if err := <-heartbeatDone; !errors.Is(err, lease.ErrLeaseExpired) {
			t.Fatalf("heartbeat after locked deadline change = %v, want ErrLeaseExpired", err)
		}
		var expiry time.Time
		if err := queryDB.QueryRowContext(ctx, `SELECT expires_at FROM task_leases WHERE lease_id = $1`, active.ID).Scan(&expiry); err != nil {
			t.Fatal(err)
		}
		if expiry.After(time.Now()) {
			t.Fatalf("heartbeat extended lease despite database deadline: %s", expiry)
		}
		if _, err := executions.Get(instance.ID); err != nil {
			t.Fatal(err)
		}
	})
}

func createPostgresAtomicTask(t *testing.T, ctx context.Context, executions *store.PostgresStore, queryDB *sql.DB, name string, retries int, duration time.Duration) (*domain.WorkflowInstance, lease.Lease) {
	t.Helper()
	instance, err := domain.NewWorkflowInstance(domain.WorkflowDef{
		Name: name, Version: 1,
		Nodes: map[string]domain.NodeDefinition{
			"run": {ID: "run", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "run", Retries: retries}},
		},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := executions.Create(instance); err != nil {
		t.Fatal(err)
	}
	taskID := instance.ID + "-task"
	if err := executions.AppendMany(instance.ID, []domain.Event{
		{WorkflowID: instance.ID, Type: domain.EventWorkflowStarted},
		{WorkflowID: instance.ID, TaskID: taskID, NodeID: "run", Type: domain.EventTaskScheduled, Payload: map[string]any{"task_name": "run", "attempt": 1, "retry_limit": retries}},
	}); err != nil {
		t.Fatal(err)
	}
	initial, err := executions.ClaimTaskPublications(ctx, 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range initial {
		if record.Item.TaskID != taskID {
			continue
		}
		if err := executions.MarkTaskPublished(ctx, record); err != nil {
			t.Fatal(err)
		}
	}
	active, err := executions.Claim(ctx, lease.Lease{
		ID: "lease-" + name, DeliveryID: "delivery-" + name,
		Item: queue.WorkItem{ID: "work-" + name, WorkflowID: instance.ID, TaskID: taskID, NodeID: "run", TaskName: "run"},
	}, duration)
	if err != nil {
		t.Fatal(err)
	}
	var persisted string
	if err := queryDB.QueryRowContext(ctx, `SELECT state FROM task_leases WHERE lease_id = $1`, active.ID).Scan(&persisted); err != nil {
		t.Fatal(err)
	}
	if persisted != "ACTIVE" {
		t.Fatalf("new lease state = %q, want ACTIVE", persisted)
	}
	return instance, active
}

func assertPostgresAtomicState(t *testing.T, ctx context.Context, executions *store.PostgresStore, queryDB *sql.DB, workflowID string, taskStatus domain.TaskStatus, eventCount int, leaseState string, publicationCount int) {
	t.Helper()
	instance, err := executions.Get(workflowID)
	if err != nil {
		t.Fatal(err)
	}
	if instance.Tasks["run"].Status != taskStatus || len(instance.Events) != eventCount {
		t.Fatalf("workflow state = %s with %d events, want %s with %d", instance.Tasks["run"].Status, len(instance.Events), taskStatus, eventCount)
	}
	var storedLeaseState string
	if err := queryDB.QueryRowContext(ctx, `SELECT state FROM task_leases WHERE workflow_id = $1`, workflowID).Scan(&storedLeaseState); err != nil {
		t.Fatal(err)
	}
	if storedLeaseState != leaseState {
		t.Fatalf("stored lease state = %q, want %q", storedLeaseState, leaseState)
	}
	var actualPublications int
	if err := queryDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM task_publication_outbox AS publication
		JOIN workflow_events AS event ON event.task_id = publication.task_id
		WHERE event.workflow_id = $1 AND event.event_type = $2 AND publication.published_at IS NULL`, workflowID, string(domain.EventTaskScheduled)).Scan(&actualPublications); err != nil {
		t.Fatal(err)
	}
	if actualPublications != publicationCount {
		t.Fatalf("publication outbox count = %d, want %d", actualPublications, publicationCount)
	}
}
