package store

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/zephyr-workflow/zephyr/pkg/domain"
)

func TestMemoryStoreAppendsEventsAndIsolatesReads(t *testing.T) {
	definition := domain.WorkflowDef{
		Name:    "checkout",
		Version: 1,
		Start:   []string{"charge"},
		Nodes: map[string]domain.NodeDefinition{
			"charge": {ID: "charge", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "ChargePayment"}},
		},
	}
	instance, err := domain.NewWorkflowInstance(definition, nil)
	if err != nil {
		t.Fatal(err)
	}
	memory := NewMemoryStore()
	if err := memory.Create(instance); err != nil {
		t.Fatal(err)
	}
	if err := memory.Append(instance.ID, domain.Event{
		WorkflowID: instance.ID,
		Type:       domain.EventWorkflowStarted,
	}); err != nil {
		t.Fatal(err)
	}

	read, err := memory.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	read.Context["mutated"] = true
	read.Events[0].Payload = map[string]any{"mutated": true}

	again, err := memory.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if again.Status != domain.WorkflowRunning {
		t.Fatalf("status = %s, want %s", again.Status, domain.WorkflowRunning)
	}
	if _, exists := again.Context["mutated"]; exists {
		t.Fatal("store exposed mutable context")
	}
	if again.Events[0].Payload != nil {
		t.Fatal("store exposed mutable event payload")
	}
}

func TestMemoryStoreRejectsDuplicateAndMissingWorkflows(t *testing.T) {
	memory := NewMemoryStore()
	instance := testInstance(t)
	if err := memory.Create(instance); err != nil {
		t.Fatal(err)
	}
	if err := memory.Create(instance); !errors.Is(err, ErrAlreadyExists) {
		t.Fatal("Create accepted a duplicate workflow")
	}
	if _, err := memory.Get("missing"); !errors.Is(err, ErrNotFound) {
		t.Fatal("Get accepted a missing workflow")
	}
	if err := memory.Append("missing", domain.Event{}); !errors.Is(err, ErrNotFound) {
		t.Fatal("Append accepted a missing workflow")
	}
}

func TestMemoryStoreSerializesConcurrentAppends(t *testing.T) {
	memory := NewMemoryStore()
	instance := testInstance(t)
	if err := memory.Create(instance); err != nil {
		t.Fatal(err)
	}
	var waitGroup sync.WaitGroup
	errors := make(chan error, 32)
	for index := 0; index < 32; index++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			errors <- memory.Append(instance.ID, domain.Event{WorkflowID: instance.ID, Type: domain.EventWorkflowStarted})
		}()
	}
	waitGroup.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	read, err := memory.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(read.Events) != 32 || read.NextSequence != 32 {
		t.Fatalf("events = %d, next sequence = %d; want 32, 32", len(read.Events), read.NextSequence)
	}
	for index, event := range read.Events {
		wantSequence := uint64(index + 1)
		if event.Sequence != wantSequence {
			t.Fatalf("event %d sequence = %d, want %d", index, event.Sequence, wantSequence)
		}
	}
}

func TestMemoryStoreListsExecutionsByWorkflowStatusAndPage(t *testing.T) {
	memory := NewMemoryStore()
	first := testInstance(t)
	first.ID = "run-1"
	if err := memory.Create(first); err != nil {
		t.Fatal(err)
	}
	if err := memory.Append(first.ID, domain.Event{WorkflowID: first.ID, Type: domain.EventWorkflowStarted, OccurredAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}); err != nil {
		t.Fatal(err)
	}
	second := testInstance(t)
	second.ID = "run-2"
	if err := memory.Create(second); err != nil {
		t.Fatal(err)
	}
	if err := memory.Append(second.ID, domain.Event{WorkflowID: second.ID, Type: domain.EventWorkflowStarted, OccurredAt: time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)}); err != nil {
		t.Fatal(err)
	}
	if err := memory.Append(second.ID, domain.Event{WorkflowID: second.ID, Type: domain.EventWorkflowCompleted}); err != nil {
		t.Fatal(err)
	}

	page, total, err := memory.ListExecutions(context.Background(), ExecutionFilter{WorkflowName: "test", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if total != 2 || len(page) != 1 || page[0].ID != "run-2" {
		t.Fatalf("newest page/total = %#v/%d", page, total)
	}
	completed, total, err := memory.ListExecutions(context.Background(), ExecutionFilter{Status: domain.WorkflowCompleted, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 || len(completed) != 1 || completed[0].ID != "run-2" {
		t.Fatalf("completed executions/total = %#v/%d", completed, total)
	}
	page, total, err = memory.ListExecutions(context.Background(), ExecutionFilter{Limit: 1, Offset: 8})
	if err != nil || total != 2 || len(page) != 0 {
		t.Fatalf("out-of-range page/total/error = %#v/%d/%v", page, total, err)
	}
}

func TestMemoryStoreIsolatesRuntimeFanOutState(t *testing.T) {
	instance := testInstance(t)
	instance.RuntimeNodes = map[string]domain.NodeDefinition{
		"fan_out_1:0:task": {ID: "fan_out_1:0:task", Type: domain.NodeTask, RuntimeScope: map[string]any{"item": map[string]any{"id": 10}}},
	}
	instance.FanOuts = map[string]domain.FanOutExecution{
		"fan_out_1": {Expanded: true, Items: []domain.FanOutItemExecution{{Index: 0, NodeIDs: []string{"fan_out_1:0:task"}, Leaves: []string{"fan_out_1:0:task"}}}},
	}
	memory := NewMemoryStore()
	if err := memory.Create(instance); err != nil {
		t.Fatal(err)
	}
	read, err := memory.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	node := read.RuntimeNodes["fan_out_1:0:task"]
	node.RuntimeScope["item"].(map[string]any)["id"] = 99
	read.RuntimeNodes[node.ID] = node
	state := read.FanOuts["fan_out_1"]
	state.Items[0].Leaves[0] = "mutated"
	read.FanOuts["fan_out_1"] = state

	again, err := memory.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if again.RuntimeNodes[node.ID].RuntimeScope["item"].(map[string]any)["id"] != 10 || again.FanOuts["fan_out_1"].Items[0].Leaves[0] != node.ID {
		t.Fatalf("store exposed mutable fan-out state: %#v / %#v", again.RuntimeNodes, again.FanOuts)
	}
}

func TestMemoryTaskPublicationClaimsRetryAndMark(t *testing.T) {
	memory := NewMemoryStore()
	instance := testInstance(t)
	if err := memory.Create(instance); err != nil {
		t.Fatal(err)
	}
	appendTaskPublicationEvents(t, func(event domain.Event) error {
		return memory.Append(instance.ID, event)
	}, instance.ID)

	claimed, err := memory.ClaimTaskPublications(t.Context(), 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	assertTaskPublicationItems(t, claimed)
	if secondClaim, err := memory.ClaimTaskPublications(t.Context(), 10, time.Minute); err != nil || len(secondClaim) != 0 {
		t.Fatalf("second claim = %#v, error = %v; want no active claims", secondClaim, err)
	}
	byTaskID := make(map[string]TaskPublicationRecord, len(claimed))
	for _, record := range claimed {
		byTaskID[record.Item.TaskID] = record
	}
	if err := memory.RetryTaskPublication(t.Context(), byTaskID["task-publication"], 0); err != nil {
		t.Fatal(err)
	}
	if err := memory.MarkTaskPublished(t.Context(), byTaskID["compensation-publication"]); err != nil {
		t.Fatal(err)
	}
	if err := memory.MarkTaskPublished(t.Context(), byTaskID["task-publication"]); !errors.Is(err, ErrConflict) {
		t.Fatalf("mark after retry error = %v, want ErrConflict", err)
	}
	retried, err := memory.ClaimTaskPublications(t.Context(), 10, time.Minute)
	if err != nil || len(retried) != 1 || retried[0].Item.TaskID != "task-publication" || retried[0].ClaimToken == byTaskID["task-publication"].ClaimToken {
		t.Fatalf("retried task publication = %#v, error = %v", retried, err)
	}
	if err := memory.MarkTaskPublished(t.Context(), retried[0]); err != nil {
		t.Fatal(err)
	}
	if remaining, err := memory.ClaimTaskPublications(t.Context(), 10, time.Minute); err != nil || len(remaining) != 0 {
		t.Fatalf("remaining task publications = %#v, error = %v", remaining, err)
	}
}

func TestMemoryStoreCreatesInitialTaskPublications(t *testing.T) {
	instance := testInstance(t)
	appendTaskPublicationEvents(t, instance.Append, instance.ID)
	memory := NewMemoryStore()
	if err := memory.Create(instance); err != nil {
		t.Fatal(err)
	}
	records, err := memory.ClaimTaskPublications(t.Context(), 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	assertTaskPublicationItems(t, records)
}

func TestMemoryStoreAppendManyIsAtomic(t *testing.T) {
	memory := NewMemoryStore()
	instance := testInstance(t)
	if err := memory.Create(instance); err != nil {
		t.Fatal(err)
	}
	err := memory.AppendMany(instance.ID, []domain.Event{
		{WorkflowID: instance.ID, Type: domain.EventWorkflowStarted},
		{WorkflowID: instance.ID, TaskID: "missing-name", NodeID: "charge", Type: domain.EventTaskScheduled},
	})
	if err == nil {
		t.Fatal("AppendMany accepted an invalid scheduled task")
	}
	current, err := memory.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(current.Events) != 0 || current.Status != domain.WorkflowPending {
		t.Fatalf("failed event batch partially changed workflow: status=%s events=%#v", current.Status, current.Events)
	}
	if err := memory.AppendMany(instance.ID, []domain.Event{
		{WorkflowID: instance.ID, Type: domain.EventWorkflowStarted},
		{WorkflowID: instance.ID, Type: domain.EventWorkflowFailureRequested, Payload: map[string]any{"error": "invalid input"}},
	}); err != nil {
		t.Fatal(err)
	}
	current, err = memory.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(current.Events) != 2 || current.Events[0].Sequence != 1 || current.Events[1].Sequence != 2 || current.FailureReason != "invalid input" {
		t.Fatalf("committed event batch = %#v", current.Events)
	}
}

func appendTaskPublicationEvents(t *testing.T, appendEvent func(domain.Event) error, workflowID string) {
	t.Helper()
	for _, event := range []domain.Event{
		{
			WorkflowID: workflowID,
			TaskID:     "task-publication",
			NodeID:     "task-node",
			Type:       domain.EventTaskScheduled,
			Payload:    map[string]any{"task_name": "run", "input": map[string]any{"order_id": "order-1"}},
		},
		{
			WorkflowID: workflowID,
			TaskID:     "compensation-publication",
			NodeID:     "compensation-node",
			Type:       domain.EventCompensationScheduled,
			Payload:    map[string]any{"task_name": "undo", "input": map[string]any{"order_id": "order-1"}},
		},
	} {
		if err := appendEvent(event); err != nil {
			t.Fatalf("append %s: %v", event.Type, err)
		}
	}
}

func assertTaskPublicationItems(t *testing.T, records []TaskPublicationRecord) {
	t.Helper()
	if len(records) != 2 {
		t.Fatalf("task publication count = %d, want 2: %#v", len(records), records)
	}
	want := map[string]taskPublicationExpectation{
		"task-publication":         {nodeID: "task-node", taskName: "run"},
		"compensation-publication": {nodeID: "compensation-node", taskName: "undo"},
	}
	for _, record := range records {
		expected, exists := want[record.Item.TaskID]
		if !exists || record.Item.ID != record.Item.TaskID || record.Item.WorkflowID == "" || record.Item.NodeID != expected.nodeID || record.Item.TaskName != expected.taskName || record.Item.Payload["order_id"] != "order-1" || record.ClaimToken == "" {
			t.Fatalf("unexpected task publication record: %#v", record)
		}
		delete(want, record.Item.TaskID)
	}
	if len(want) != 0 {
		t.Fatalf("missing task publication records: %#v", want)
	}
}

type taskPublicationExpectation struct {
	nodeID   string
	taskName string
}

func testInstance(t *testing.T) *domain.WorkflowInstance {
	t.Helper()
	instance, err := domain.NewWorkflowInstance(domain.WorkflowDef{
		Name:    "test",
		Version: 1,
		Nodes: map[string]domain.NodeDefinition{
			"task": {ID: "task", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "run"}},
		},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return instance
}
