package gateway

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zephyr-workflow/zephyr/pkg/domain"
	"github.com/zephyr-workflow/zephyr/pkg/queue"
	"github.com/zephyr-workflow/zephyr/pkg/store"
)

type failOnceTaskPublicationMark struct {
	store.TaskPublicationStore
	calls atomic.Int32
}

func (outbox *failOnceTaskPublicationMark) MarkTaskPublished(ctx context.Context, record store.TaskPublicationRecord) error {
	if outbox.calls.Add(1) == 1 {
		return errors.New("temporary outbox update failure")
	}
	return outbox.TaskPublicationStore.MarkTaskPublished(ctx, record)
}

func TestTaskPublicationDispatcherDeduplicatesAfterMarkFailure(t *testing.T) {
	ctx := context.Background()
	memory := store.NewMemoryStore()
	definition := domain.WorkflowDef{
		Name: "mailbox-test", Version: 1,
		Nodes: map[string]domain.NodeDefinition{
			"run": {ID: "run", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "Run"}},
		},
	}
	instance, err := domain.NewWorkflowInstance(definition, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := memory.Create(instance); err != nil {
		t.Fatal(err)
	}
	if err := memory.Append(instance.ID, domain.Event{
		WorkflowID: instance.ID, TaskID: "task-stable", NodeID: "run", Type: domain.EventTaskScheduled,
		Payload: map[string]any{"task_name": "Run", "input": map[string]any{"value": "once"}},
	}); err != nil {
		t.Fatal(err)
	}
	outbox := &failOnceTaskPublicationMark{TaskPublicationStore: memory}
	workQueue := queue.NewMemoryQueue(4)
	defer workQueue.Close()
	dispatcher, err := NewTaskPublicationDispatcher(outbox, workQueue, TaskPublicationDispatcherConfig{
		ClaimDuration: 20 * time.Millisecond,
		PollInterval:  2 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if count, err := dispatcher.DispatchOnce(ctx); err == nil || count != 0 {
		t.Fatalf("first dispatch = %d/%v, want mark failure", count, err)
	}

	runContext, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- dispatcher.Run(runContext) }()
	deadline := time.After(time.Second)
	for outbox.calls.Load() < 2 {
		select {
		case <-time.After(time.Millisecond):
		case <-deadline:
			cancel()
			<-done
			t.Fatal("dispatcher did not reclaim and mark the task publication")
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("dispatcher run = %v", err)
	}

	delivery, err := workQueue.Receive(ctx, "worker-a")
	if err != nil {
		t.Fatal(err)
	}
	if delivery.Item.TaskID != "task-stable" {
		t.Fatalf("mailbox task ID = %q, want stable task ID", delivery.Item.TaskID)
	}
	if err := workQueue.Ack(ctx, delivery.ID); err != nil {
		t.Fatal(err)
	}
	duplicateContext, cancelDuplicate := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancelDuplicate()
	if _, err := workQueue.Receive(duplicateContext, "worker-a"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("worker received a duplicate task after ack: %v", err)
	}
}
