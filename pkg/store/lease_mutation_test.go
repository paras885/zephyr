package store

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/zephyr-workflow/zephyr/pkg/domain"
	"github.com/zephyr-workflow/zephyr/pkg/lease"
	"github.com/zephyr-workflow/zephyr/pkg/queue"
)

type leaseMutationContractStore interface {
	ExecutionStore
	lease.StateStore
	TaskPublicationStore
}

func TestMemoryLeaseMutationAtomicityAndReplay(t *testing.T) {
	verifyLeaseMutationContract(t, NewMemoryStore())
}

func TestSQLiteLeaseMutationAtomicityAndReplay(t *testing.T) {
	sqliteStore, err := OpenSQLiteStore(t.Context(), filepath.Join(t.TempDir(), "lease-mutation.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer sqliteStore.Close()
	verifyLeaseMutationContract(t, sqliteStore)
}

func verifyLeaseMutationContract(t *testing.T, executions leaseMutationContractStore) {
	t.Helper()
	ctx := context.Background()
	instance := testInstance(t)
	if err := executions.Create(instance); err != nil {
		t.Fatal(err)
	}
	if err := executions.AppendMany(instance.ID, []domain.Event{
		{WorkflowID: instance.ID, Type: domain.EventWorkflowStarted},
		{WorkflowID: instance.ID, TaskID: "task-1", NodeID: "task", Type: domain.EventTaskScheduled, Payload: map[string]any{"task_name": "run", "attempt": 1}},
	}); err != nil {
		t.Fatal(err)
	}
	initialPublications, err := executions.ClaimTaskPublications(ctx, 10, time.Minute)
	if err != nil || len(initialPublications) != 1 {
		t.Fatalf("initial task publication = %#v, error = %v", initialPublications, err)
	}
	if err := executions.MarkTaskPublished(ctx, initialPublications[0]); err != nil {
		t.Fatal(err)
	}
	active, err := executions.Claim(ctx, lease.Lease{
		ID: "lease-1", DeliveryID: "delivery-1",
		Item: queue.WorkItem{WorkflowID: instance.ID, TaskID: "task-1", NodeID: "task", TaskName: "run"},
	}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	transition := []domain.Event{
		{WorkflowID: instance.ID, TaskID: "task-1", NodeID: "task", Type: domain.EventTaskCompleted, Payload: map[string]any{"result": map[string]any{"ok": true}}},
		{WorkflowID: instance.ID, TaskID: "task-2", NodeID: "next", Type: domain.EventTaskScheduled, Payload: map[string]any{"task_name": "next", "attempt": 1}},
	}
	injectedFailure := errors.New("injected failure before commit")
	if _, err := executions.CompleteWith(ctx, active.ID, active.Token, func(_ lease.Lease, mutation lease.MutationStore) error {
		if err := mutation.AppendMany(instance.ID, transition); err != nil {
			return err
		}
		return injectedFailure
	}); !errors.Is(err, injectedFailure) {
		t.Fatalf("pre-commit injected failure = %v, want injected error", err)
	}
	unchanged, err := executions.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if unchanged.Tasks["task"].Status != domain.TaskReady || len(unchanged.Events) != 2 {
		t.Fatalf("workflow changed after rolled-back callback: task=%s events=%d", unchanged.Tasks["task"].Status, len(unchanged.Events))
	}
	pending, err := executions.ClaimTaskPublications(ctx, 10, time.Minute)
	if err != nil || len(pending) != 0 {
		t.Fatalf("publication outbox after rollback = %#v, error = %v", pending, err)
	}

	applyTransition := func(_ lease.Lease, mutation lease.MutationStore) error {
		current, err := mutation.Get(instance.ID)
		if err != nil {
			return err
		}
		if current.Tasks["task"].Status == domain.TaskCompleted {
			return nil
		}
		return mutation.AppendMany(instance.ID, transition)
	}
	if _, err := executions.CompleteWith(ctx, active.ID, active.Token, applyTransition); err != nil {
		t.Fatalf("commit task completion: %v", err)
	}
	if _, err := executions.CompleteWith(ctx, active.ID, active.Token, applyTransition); err != nil {
		t.Fatalf("replay task completion after lost response: %v", err)
	}
	updated, err := executions.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Tasks["task"].Status != domain.TaskCompleted || len(updated.Events) != 4 {
		t.Fatalf("workflow after completion replay: task=%s events=%d", updated.Tasks["task"].Status, len(updated.Events))
	}
	publications, err := executions.ClaimTaskPublications(ctx, 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(publications) != 1 || publications[0].Item.TaskID != "task-2" {
		t.Fatalf("publication outbox after completion replay = %#v, want one task-2 publication", publications)
	}
}

func TestMemoryLeaseExpiryRacesCompletionAtomically(t *testing.T) {
	memory := NewMemoryStore()
	verifyLeaseExpiryRace(t, memory)
}

func TestSQLiteLeaseExpiryRacesCompletionAtomically(t *testing.T) {
	sqliteStore, err := OpenSQLiteStore(t.Context(), filepath.Join(t.TempDir(), "lease-expiry-race.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer sqliteStore.Close()
	verifyLeaseExpiryRace(t, sqliteStore)
}

func verifyLeaseExpiryRace(t *testing.T, executions leaseMutationContractStore) {
	t.Helper()
	ctx := context.Background()
	instance := testInstance(t)
	if err := executions.Create(instance); err != nil {
		t.Fatal(err)
	}
	if err := executions.AppendMany(instance.ID, []domain.Event{
		{WorkflowID: instance.ID, Type: domain.EventWorkflowStarted},
		{WorkflowID: instance.ID, TaskID: "task-expiring", NodeID: "task", Type: domain.EventTaskScheduled, Payload: map[string]any{"task_name": "run", "attempt": 1}},
	}); err != nil {
		t.Fatal(err)
	}
	active, err := executions.Claim(ctx, lease.Lease{
		ID: "lease-expiring", DeliveryID: "delivery-expiring",
		Item: queue.WorkItem{WorkflowID: instance.ID, TaskID: "task-expiring", NodeID: "task", TaskName: "run"},
	}, 5*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	start := make(chan struct{})
	var group sync.WaitGroup
	var completeErr error
	var expireErr error
	var expired bool
	group.Add(2)
	go func() {
		defer group.Done()
		<-start
		_, completeErr = executions.CompleteWith(ctx, active.ID, active.Token, func(_ lease.Lease, mutation lease.MutationStore) error {
			return mutation.Append(instance.ID, domain.Event{WorkflowID: instance.ID, TaskID: "task-expiring", NodeID: "task", Type: domain.EventTaskCompleted})
		})
	}()
	go func() {
		defer group.Done()
		<-start
		_, expired, expireErr = executions.ExpireWith(ctx, active.ID, active.Token, func(_ lease.Lease, mutation lease.MutationStore) error {
			return mutation.AppendMany(instance.ID, []domain.Event{
				{WorkflowID: instance.ID, TaskID: "task-expiring", NodeID: "task", Type: domain.EventTaskFailed, Payload: map[string]any{"error": "task lease expired"}},
				{WorkflowID: instance.ID, TaskID: "task-expiring", NodeID: "task", Type: domain.EventTaskRetryScheduled, Payload: map[string]any{"error": "task lease expired", "attempt": 2, "retry_at": time.Now().Add(time.Second)}},
			})
		})
	}()
	close(start)
	group.Wait()
	if !errors.Is(completeErr, lease.ErrLeaseExpired) {
		t.Fatalf("completion racing an expired lease error = %v, want ErrLeaseExpired", completeErr)
	}
	if expireErr != nil || !expired {
		t.Fatalf("expiry result = %t, error = %v", expired, expireErr)
	}
	updated, err := executions.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if task := updated.Tasks["task"]; task.Status != domain.TaskRetrying || task.Attempt != 2 || len(updated.Events) != 4 {
		t.Fatalf("expired task transition = status %s attempt %d events %d", task.Status, task.Attempt, len(updated.Events))
	}
}
