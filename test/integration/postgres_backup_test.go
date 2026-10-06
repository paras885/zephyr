package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/zephyr-workflow/zephyr/pkg/domain"
	"github.com/zephyr-workflow/zephyr/pkg/lease"
	"github.com/zephyr-workflow/zephyr/pkg/queue"
	"github.com/zephyr-workflow/zephyr/pkg/store"
)

func TestPostgresBackupRestorePreservesWorkflowAndOutboxConsistency(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "postgres:16-alpine",
			Env:          map[string]string{"POSTGRES_USER": "zephyr", "POSTGRES_PASSWORD": "zephyr-test", "POSTGRES_DB": "zephyr"},
			ExposedPorts: []string{"5432/tcp"},
			WaitingFor:   wait.ForListeningPort("5432/tcp"),
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
	source, err := store.OpenPostgresStore(ctx, dataSource)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	if err := source.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	definition := domain.WorkflowDef{
		Name: "backup-checkout", Version: 1,
		Nodes: map[string]domain.NodeDefinition{"charge": {ID: "charge", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "Charge"}}},
	}
	instance, err := domain.NewWorkflowInstance(definition, map[string]any{"order_id": "restore-42"})
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Create(instance); err != nil {
		t.Fatal(err)
	}
	if err := source.AppendMany(instance.ID, []domain.Event{
		{WorkflowID: instance.ID, Type: domain.EventWorkflowStarted},
		{WorkflowID: instance.ID, TaskID: "backup-task", NodeID: "charge", Type: domain.EventTaskScheduled, Payload: map[string]any{"task_name": "Charge", "input": map[string]any{"order_id": "restore-42"}}},
	}); err != nil {
		t.Fatal(err)
	}
	active, err := source.Claim(ctx, lease.Lease{
		ID: "backup-lease", DeliveryID: "backup-delivery",
		Item: queue.WorkItem{WorkflowID: instance.ID, TaskID: "backup-task", NodeID: "charge", TaskName: "Charge"},
	}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Append(instance.ID, domain.Event{
		WorkflowID: instance.ID, TaskID: active.Item.TaskID, NodeID: active.Item.NodeID,
		Type: domain.EventTaskStarted, Payload: map[string]any{"lease_token": active.Token},
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := source.CreateIdempotent(ctx, mustBackupInstance(t), "backup-key", "backup-hash"); err != nil {
		t.Fatal(err)
	}

	backupCode, backupOutput, err := container.Exec(ctx, []string{"pg_dump", "-Fc", "--no-owner", "--no-acl", "-U", "zephyr", "-d", "zephyr"}, tcexec.Multiplexed())
	if err != nil {
		t.Fatalf("start pg_dump: %v", err)
	}
	backup, err := io.ReadAll(backupOutput)
	if err != nil || backupCode != 0 || len(backup) == 0 {
		t.Fatalf("pg_dump exit=%d bytes=%d err=%v", backupCode, len(backup), err)
	}
	backupPath := filepath.Join(t.TempDir(), "zephyr.dump")
	if err := os.WriteFile(backupPath, backup, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := container.CopyFileToContainer(ctx, backupPath, "/tmp/zephyr.dump", 0o600); err != nil {
		t.Fatalf("copy backup into restore container: %v", err)
	}
	if err := runPostgresContainerCommand(t, ctx, container, []string{"createdb", "-U", "zephyr", "zephyr_restore"}); err != nil {
		t.Fatal(err)
	}
	if err := runPostgresContainerCommand(t, ctx, container, []string{"pg_restore", "-U", "zephyr", "-d", "zephyr_restore", "--no-owner", "--no-acl", "/tmp/zephyr.dump"}); err != nil {
		t.Fatal(err)
	}

	restoreURL := fmt.Sprintf("postgres://zephyr:zephyr-test@%s:%s/zephyr_restore?sslmode=disable", host, port.Port())
	restored, err := store.OpenPostgresStore(ctx, restoreURL)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	restoreDB, err := sql.Open("pgx", restoreURL)
	if err != nil {
		t.Fatal(err)
	}
	defer restoreDB.Close()
	loaded, err := restored.Get(instance.ID)
	if err != nil {
		t.Fatalf("read restored workflow: %v", err)
	}
	if loaded.Context["order_id"] != "restore-42" || loaded.NextSequence != 3 || loaded.Tasks["charge"].LeaseToken != active.Token {
		t.Fatalf("restored workflow snapshot mismatch: %#v", loaded)
	}
	var restoredEvents, restoredEventOutbox, restoredTaskOutbox, restoredLeases, restoredKeys int
	for query, destination := range map[string]*int{
		`SELECT COUNT(*) FROM workflow_events`:           &restoredEvents,
		`SELECT COUNT(*) FROM workflow_event_outbox`:     &restoredEventOutbox,
		`SELECT COUNT(*) FROM task_publication_outbox`:   &restoredTaskOutbox,
		`SELECT COUNT(*) FROM task_leases`:               &restoredLeases,
		`SELECT COUNT(*) FROM workflow_idempotency_keys`: &restoredKeys,
	} {
		if err := restoreDB.QueryRowContext(ctx, query).Scan(destination); err != nil {
			t.Fatalf("query restored consistency: %v", err)
		}
	}
	if restoredEvents != 3 || restoredEventOutbox != 3 || restoredTaskOutbox != 1 || restoredLeases != 1 || restoredKeys != 1 {
		t.Fatalf("restored table counts events=%d eventOutbox=%d taskOutbox=%d leases=%d idempotency=%d", restoredEvents, restoredEventOutbox, restoredTaskOutbox, restoredLeases, restoredKeys)
	}
	metrics, err := restored.OperationalMetrics(ctx)
	if err != nil || metrics.WorkflowEventOutboxPending != 3 || metrics.TaskPublicationPending != 1 {
		t.Fatalf("restored operational metrics = %#v, err=%v", metrics, err)
	}
}

func mustBackupInstance(t *testing.T) *domain.WorkflowInstance {
	t.Helper()
	instance, err := domain.NewWorkflowInstance(domain.WorkflowDef{
		Name: "backup-idempotent", Version: 1,
		Nodes: map[string]domain.NodeDefinition{"task": {ID: "task", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "run"}}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return instance
}

func runPostgresContainerCommand(t *testing.T, ctx context.Context, container testcontainers.Container, command []string) error {
	t.Helper()
	exitCode, output, err := container.Exec(ctx, command, tcexec.Multiplexed())
	if err != nil {
		return fmt.Errorf("run %q: %w", command, err)
	}
	message, readErr := io.ReadAll(output)
	if readErr != nil {
		return fmt.Errorf("read %q output: %w", command, readErr)
	}
	if exitCode != 0 {
		return fmt.Errorf("%q exited with %d: %s", command, exitCode, message)
	}
	return nil
}
