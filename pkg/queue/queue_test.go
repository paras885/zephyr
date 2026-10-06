package queue

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestMemoryQueueDeliversOneItemToExactlyOneWorker(t *testing.T) {
	queue := NewMemoryQueue(4)
	defer queue.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	item := WorkItem{WorkflowID: "workflow-1", TaskID: "task-1", NodeID: "charge", TaskName: "ChargePayment"}
	if err := queue.Publish(ctx, item); err != nil {
		t.Fatal(err)
	}

	var waitGroup sync.WaitGroup
	deliveries := make(chan Delivery, 2)
	errorsFound := make(chan error, 2)
	for _, workerID := range []string{"worker-a", "worker-b"} {
		waitGroup.Add(1)
		go func(workerID string) {
			defer waitGroup.Done()
			delivery, err := queue.Receive(ctx, workerID)
			if err != nil {
				errorsFound <- err
				return
			}
			deliveries <- delivery
		}(workerID)
	}
	waitGroup.Wait()
	close(deliveries)
	close(errorsFound)
	if len(errorsFound) != 1 {
		t.Fatalf("receive errors = %d, want one timed-out competing consumer", len(errorsFound))
	}
	for err := range errorsFound {
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("receive error = %v, want deadline exceeded", err)
		}
	}
	if len(deliveries) != 1 {
		t.Fatalf("deliveries = %d, want 1", len(deliveries))
	}
	ackContext := context.Background()
	for delivery := range deliveries {
		if err := queue.Ack(ackContext, delivery.ID); err != nil {
			t.Fatal(err)
		}
		if err := queue.Ack(ackContext, delivery.ID); !errors.Is(err, ErrNotFound) {
			t.Fatalf("second Ack() error = %v, want %v", err, ErrNotFound)
		}
	}
}

func TestMemoryQueueDepthTracksReadyItems(t *testing.T) {
	workQueue := NewMemoryQueue(2)
	defer workQueue.Close()
	for _, id := range []string{"depth-1", "depth-2"} {
		if err := workQueue.Publish(context.Background(), WorkItem{ID: id}); err != nil {
			t.Fatal(err)
		}
	}
	depth, err := workQueue.QueueDepth(context.Background())
	if err != nil || depth != 2 {
		t.Fatalf("queued depth = %d, err=%v", depth, err)
	}
	if _, err := workQueue.Receive(context.Background(), "worker"); err != nil {
		t.Fatal(err)
	}
	depth, err = workQueue.QueueDepth(context.Background())
	if err != nil || depth != 1 {
		t.Fatalf("depth after receive = %d, err=%v", depth, err)
	}
}

func TestMemoryQueueRejectRequeuesItem(t *testing.T) {
	queue := NewMemoryQueue(1)
	defer queue.Close()
	ctx := context.Background()
	if err := queue.Publish(ctx, WorkItem{ID: "work-1", TaskName: "Run"}); err != nil {
		t.Fatal(err)
	}
	first, err := queue.Receive(ctx, "worker-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Reject(ctx, first.ID, true); err != nil {
		t.Fatal(err)
	}
	second, err := queue.Receive(ctx, "worker-b")
	if err != nil {
		t.Fatal(err)
	}
	if second.Item.ID != "work-1" {
		t.Fatalf("requeued item ID = %q, want work-1", second.Item.ID)
	}
	if err := queue.Ack(ctx, second.ID); err != nil {
		t.Fatal(err)
	}
}

func TestMemoryQueueDeduplicatesWorkIDsUntilAck(t *testing.T) {
	queue := NewMemoryQueue(2)
	defer queue.Close()
	ctx := context.Background()
	original := WorkItem{ID: "work-deduplicate", TaskName: "Run", Payload: map[string]any{"version": "original"}}
	duplicate := WorkItem{ID: original.ID, TaskName: "Run", Payload: map[string]any{"version": "duplicate"}}
	if err := queue.Publish(ctx, original); err != nil {
		t.Fatal(err)
	}
	if err := queue.Publish(ctx, duplicate); err != nil {
		t.Fatal(err)
	}

	first, err := queue.Receive(ctx, "worker-a")
	if err != nil {
		t.Fatal(err)
	}
	if first.Item.Payload["version"] != "original" {
		t.Fatalf("first payload version = %v, want original", first.Item.Payload["version"])
	}
	if err := queue.Publish(ctx, duplicate); err != nil {
		t.Fatal(err)
	}
	if _, err := receiveWithDeadline(t, queue, "worker-b"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Receive() while item is in flight = %v, want deadline exceeded", err)
	}

	if err := queue.Reject(ctx, first.ID, true); err != nil {
		t.Fatal(err)
	}
	requeued, err := queue.Receive(ctx, "worker-b")
	if err != nil {
		t.Fatal(err)
	}
	if requeued.Item.Payload["version"] != "original" {
		t.Fatalf("requeued payload version = %v, want original", requeued.Item.Payload["version"])
	}
	if _, err := receiveWithDeadline(t, queue, "worker-a"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Receive() after requeue = %v, want deadline exceeded", err)
	}
	if err := queue.Ack(ctx, requeued.ID); err != nil {
		t.Fatal(err)
	}

	if err := queue.Publish(ctx, duplicate); err != nil {
		t.Fatal(err)
	}
	if _, err := receiveWithDeadline(t, queue, "worker-a"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Receive() after acknowledged duplicate publish = %v, want deadline exceeded", err)
	}
}

func TestMemoryQueueSuppressesPermanentlyRejectedWorkID(t *testing.T) {
	queue := NewMemoryQueue(1)
	defer queue.Close()
	ctx := context.Background()
	item := WorkItem{ID: "work-rejected", TaskName: "Run"}
	if err := queue.Publish(ctx, item); err != nil {
		t.Fatal(err)
	}
	delivery, err := queue.Receive(ctx, "worker-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Reject(ctx, delivery.ID, false); err != nil {
		t.Fatal(err)
	}
	if err := queue.Publish(ctx, item); err != nil {
		t.Fatal(err)
	}
	if _, err := receiveWithDeadline(t, queue, "worker-a"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Receive() after permanent rejection and duplicate publish = %v, want deadline exceeded", err)
	}
}

func receiveWithDeadline(t *testing.T, queue *MemoryQueue, workerID string) (Delivery, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	return queue.Receive(ctx, workerID)
}

func TestMemoryQueueCloseUnblocksReceive(t *testing.T) {
	queue := NewMemoryQueue(1)
	result := make(chan error, 1)
	go func() {
		_, err := queue.Receive(context.Background(), "worker-a")
		result <- err
	}()
	if err := queue.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-result; !errors.Is(err, ErrClosed) {
		t.Fatalf("Receive() error = %v, want %v", err, ErrClosed)
	}
}

func TestFencingTokensAreMonotonic(t *testing.T) {
	generator := FencingTokenGenerator{}
	first := generator.Next()
	second := generator.Next()
	if first != 1 || second != 2 {
		t.Fatalf("tokens = %d, %d; want 1, 2", first, second)
	}
	if err := ValidateFencingToken(first, first); err != nil {
		t.Fatal(err)
	}
	if err := ValidateFencingToken(first, second); err == nil {
		t.Fatal("ValidateFencingToken accepted a stale token")
	}
}
