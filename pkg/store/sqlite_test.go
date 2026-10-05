package store

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/zephyr-workflow/zephyr/pkg/domain"
)

func TestSQLiteStorePersistsExecutionAcrossReopen(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "zephyr.db")
	local, err := OpenSQLiteStore(t.Context(), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	definition := domain.WorkflowDef{
		Name:    "local-checkout",
		Version: 1,
		Nodes: map[string]domain.NodeDefinition{
			"charge": {ID: "charge", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "ChargePayment"}},
		},
	}
	instance, err := domain.NewWorkflowInstance(definition, map[string]any{"order_id": "local-1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := local.Create(instance); err != nil {
		t.Fatal(err)
	}
	appendEvent := func(event domain.Event) {
		t.Helper()
		if err := local.Append(instance.ID, event); err != nil {
			t.Fatalf("append %s: %v", event.Type, err)
		}
	}
	appendEvent(domain.Event{WorkflowID: instance.ID, Type: domain.EventWorkflowStarted})
	appendEvent(domain.Event{WorkflowID: instance.ID, Type: domain.EventTaskScheduled, TaskID: "task-local", NodeID: "charge", Payload: map[string]any{"task_name": "ChargePayment"}})
	appendEvent(domain.Event{WorkflowID: instance.ID, Type: domain.EventTaskStarted, TaskID: "task-local", NodeID: "charge", Payload: map[string]any{"lease_token": uint64(11)}})
	appendEvent(domain.Event{WorkflowID: instance.ID, Type: domain.EventTaskCompleted, TaskID: "task-local", NodeID: "charge", Payload: map[string]any{"result": map[string]any{"status": "paid"}}})
	if err := local.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenSQLiteStore(t.Context(), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	loaded, err := reopened.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.NextSequence != 4 || loaded.Status != domain.WorkflowRunning || loaded.Context["order_id"] != "local-1" {
		t.Fatalf("reopened workflow = %#v", loaded)
	}
	if task := loaded.Tasks["charge"]; task.Status != domain.TaskCompleted || task.LeaseToken != 11 || task.Result["status"] != "paid" {
		t.Fatalf("reopened task = %#v", task)
	}
	listed, total, err := reopened.ListExecutions(t.Context(), ExecutionFilter{WorkflowName: "local-checkout", Status: domain.WorkflowRunning, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 || len(listed) != 1 || listed[0].ID != instance.ID {
		t.Fatalf("listed SQLite executions/total = %#v/%d", listed, total)
	}
	if len(loaded.Events) != 4 {
		t.Fatalf("reopened event count = %d, want 4", len(loaded.Events))
	}
	page, err := reopened.ListEvents(t.Context(), instance.ID, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 2 || page[0].Sequence != 2 || page[1].Sequence != 3 {
		t.Fatalf("event history page = %#v, want sequences 2 and 3", page)
	}
	if _, err := reopened.ListEvents(t.Context(), "missing", 0, 10); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing event history error = %v, want ErrNotFound", err)
	}
	if err := reopened.Create(instance); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("duplicate creation error = %v, want ErrAlreadyExists", err)
	}
	if _, err := reopened.Get("missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing workflow error = %v, want ErrNotFound", err)
	}
}

func TestSQLiteIdempotencyClaimsPersistAndRejectChangedRequests(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "idempotency.db")
	local, err := OpenSQLiteStore(t.Context(), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	first := testInstance(t)
	canceledContext, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := local.CreateIdempotent(canceledContext, first, "cancelled", "hash-cancelled"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled idempotent create error = %v, want context.Canceled", err)
	}
	created, workflowID, err := local.CreateIdempotent(t.Context(), first, "request-1", "hash-1")
	if err != nil || !created || workflowID != first.ID {
		t.Fatalf("first idempotent create = %t/%q/%v", created, workflowID, err)
	}
	duplicate := testInstance(t)
	created, workflowID, err = local.CreateIdempotent(t.Context(), duplicate, "request-1", "hash-1")
	if err != nil || created || workflowID != first.ID {
		t.Fatalf("duplicate idempotent create = %t/%q/%v", created, workflowID, err)
	}
	if _, _, err := local.CreateIdempotent(t.Context(), testInstance(t), "request-1", "hash-2"); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("changed request error = %v, want ErrIdempotencyConflict", err)
	}
	if err := local.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenSQLiteStore(t.Context(), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	created, workflowID, err = reopened.CreateIdempotent(t.Context(), testInstance(t), "request-1", "hash-1")
	if err != nil || created || workflowID != first.ID {
		t.Fatalf("reopened idempotent create = %t/%q/%v", created, workflowID, err)
	}
	_, total, err := reopened.ListExecutions(t.Context(), ExecutionFilter{Limit: 10})
	if err != nil || total != 1 {
		t.Fatalf("persisted idempotent run count = %d, error = %v", total, err)
	}
}

func TestSQLiteStoreSerializesConcurrentAppends(t *testing.T) {
	local, err := OpenSQLiteStore(t.Context(), filepath.Join(t.TempDir(), "concurrent.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer local.Close()
	instance := testInstance(t)
	if err := local.Create(instance); err != nil {
		t.Fatal(err)
	}
	const appendCount = 24
	var group sync.WaitGroup
	errorsFound := make(chan error, appendCount)
	for range appendCount {
		group.Add(1)
		go func() {
			defer group.Done()
			errorsFound <- local.Append(instance.ID, domain.Event{WorkflowID: instance.ID, Type: domain.EventWorkflowStarted})
		}()
	}
	group.Wait()
	close(errorsFound)
	for err := range errorsFound {
		if err != nil {
			t.Fatal(err)
		}
	}
	loaded, err := local.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.NextSequence != appendCount || len(loaded.Events) != appendCount {
		t.Fatalf("events/sequence = %d/%d, want %d/%d", len(loaded.Events), loaded.NextSequence, appendCount, appendCount)
	}
}

func TestSQLiteOutboxClaimsRetriesAndMarksDelivery(t *testing.T) {
	local, err := OpenSQLiteStore(t.Context(), filepath.Join(t.TempDir(), "outbox.db"))
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
	ctx := context.Background()
	claimed, err := local.ClaimOutbox(ctx, 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 || claimed[0].Event.Sequence != 1 || claimed[0].ClaimToken == "" {
		t.Fatalf("claimed outbox records = %#v", claimed)
	}
	secondClaim, err := local.ClaimOutbox(ctx, 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(secondClaim) != 0 {
		t.Fatalf("active claim was delivered twice: %#v", secondClaim)
	}
	if err := local.RetryOutbox(ctx, claimed[0], 0); err != nil {
		t.Fatal(err)
	}
	retried, err := local.ClaimOutbox(ctx, 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(retried) != 1 || retried[0].Event.Sequence != claimed[0].Event.Sequence || retried[0].ClaimToken == claimed[0].ClaimToken {
		t.Fatalf("retried claim = %#v", retried)
	}
	if err := local.MarkOutboxDelivered(ctx, retried[0]); err != nil {
		t.Fatal(err)
	}
	remaining, err := local.ClaimOutbox(ctx, 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(remaining) != 0 {
		t.Fatalf("delivered event remained claimable: %#v", remaining)
	}
	if err := local.MarkOutboxDelivered(ctx, retried[0]); !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate delivery mark error = %v, want ErrConflict", err)
	}
}

func TestSQLiteTaskPublicationOutboxPersistsAndClaims(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "task-publications.db")
	local, err := OpenSQLiteStore(t.Context(), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	instance := testInstance(t)
	if err := local.Create(instance); err != nil {
		t.Fatal(err)
	}
	appendTaskPublicationEvents(t, func(event domain.Event) error {
		return local.Append(instance.ID, event)
	}, instance.ID)
	if err := local.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenSQLiteStore(t.Context(), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	claimed, err := reopened.ClaimTaskPublications(t.Context(), 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	assertTaskPublicationItems(t, claimed)
	if secondClaim, err := reopened.ClaimTaskPublications(t.Context(), 10, time.Minute); err != nil || len(secondClaim) != 0 {
		t.Fatalf("second claim = %#v, error = %v; want no active claims", secondClaim, err)
	}
	byTaskID := make(map[string]TaskPublicationRecord, len(claimed))
	for _, record := range claimed {
		byTaskID[record.Item.TaskID] = record
	}
	if err := reopened.RetryTaskPublication(t.Context(), byTaskID["task-publication"], 0); err != nil {
		t.Fatal(err)
	}
	if err := reopened.MarkTaskPublished(t.Context(), byTaskID["compensation-publication"]); err != nil {
		t.Fatal(err)
	}
	if err := reopened.MarkTaskPublished(t.Context(), byTaskID["task-publication"]); !errors.Is(err, ErrConflict) {
		t.Fatalf("mark after retry error = %v, want ErrConflict", err)
	}
	retried, err := reopened.ClaimTaskPublications(t.Context(), 10, time.Minute)
	if err != nil || len(retried) != 1 || retried[0].Item.TaskID != "task-publication" || retried[0].ClaimToken == byTaskID["task-publication"].ClaimToken {
		t.Fatalf("retried task publication = %#v, error = %v", retried, err)
	}
	if err := reopened.MarkTaskPublished(t.Context(), retried[0]); err != nil {
		t.Fatal(err)
	}
	if remaining, err := reopened.ClaimTaskPublications(t.Context(), 10, time.Minute); err != nil || len(remaining) != 0 {
		t.Fatalf("remaining task publications = %#v, error = %v", remaining, err)
	}
}
