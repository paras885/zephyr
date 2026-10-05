package lease

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/zephyr-workflow/zephyr/pkg/queue"
	"github.com/zephyr-workflow/zephyr/pkg/timer"
)

func TestManagerHeartbeatAndCompletionFenceLease(t *testing.T) {
	workQueue := queue.NewMemoryQueue(1)
	timers := timer.NewService(2)
	manager, err := NewManager(workQueue, timers, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		manager.Close()
		timers.Close()
		workQueue.Close()
	}()
	ctx := context.Background()
	if err := workQueue.Publish(ctx, queue.WorkItem{ID: "work-1", TaskID: "task-1"}); err != nil {
		t.Fatal(err)
	}
	delivery, err := workQueue.Receive(ctx, "worker-a")
	if err != nil {
		t.Fatal(err)
	}
	lease, err := manager.Acquire(ctx, delivery, 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Token == 0 || lease.Item.LeaseToken != lease.Token {
		t.Fatalf("lease token = %d, item token = %d", lease.Token, lease.Item.LeaseToken)
	}
	if err := manager.Heartbeat(ctx, lease.ID, lease.Token+1, time.Second); !errors.Is(err, ErrStaleToken) {
		t.Fatalf("stale heartbeat error = %v, want %v", err, ErrStaleToken)
	}
	if err := manager.Heartbeat(ctx, lease.ID, lease.Token, time.Second); err != nil {
		t.Fatal(err)
	}
	if err := manager.Complete(ctx, lease.ID, lease.Token); err != nil {
		t.Fatal(err)
	}
	if err := manager.Complete(ctx, lease.ID, lease.Token); !errors.Is(err, ErrLeaseNotFound) {
		t.Fatalf("second completion error = %v, want %v", err, ErrLeaseNotFound)
	}
}

func TestManagerReclaimsExpiredLeaseAndRejectsZombie(t *testing.T) {
	workQueue := queue.NewMemoryQueue(1)
	timers := timer.NewService(2)
	manager, err := NewManager(workQueue, timers, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		manager.Close()
		timers.Close()
		workQueue.Close()
	}()
	ctx := context.Background()
	if err := workQueue.Publish(ctx, queue.WorkItem{ID: "work-2", TaskID: "task-2"}); err != nil {
		t.Fatal(err)
	}
	delivery, err := workQueue.Receive(ctx, "worker-a")
	if err != nil {
		t.Fatal(err)
	}
	lease, err := manager.Acquire(ctx, delivery, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case expiration := <-manager.Expirations():
		if expiration.Lease.ID != lease.ID {
			t.Fatalf("expired lease = %q, want %q", expiration.Lease.ID, lease.ID)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("lease did not expire")
	}
	if err := manager.Complete(ctx, lease.ID, lease.Token); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("zombie completion error = %v, want %v", err, ErrLeaseExpired)
	}
	requeued, err := workQueue.Receive(ctx, "worker-b")
	if err != nil {
		t.Fatal(err)
	}
	if requeued.Item.ID != "work-2" {
		t.Fatalf("requeued work ID = %q, want work-2", requeued.Item.ID)
	}
	if err := workQueue.Ack(ctx, requeued.ID); err != nil {
		t.Fatal(err)
	}
}
