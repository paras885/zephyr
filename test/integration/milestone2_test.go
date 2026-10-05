package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/zephyr-workflow/zephyr/pkg/decider"
	"github.com/zephyr-workflow/zephyr/pkg/discovery"
	"github.com/zephyr-workflow/zephyr/pkg/domain"
	"github.com/zephyr-workflow/zephyr/pkg/lease"
	"github.com/zephyr-workflow/zephyr/pkg/queue"
	"github.com/zephyr-workflow/zephyr/pkg/store"
	"github.com/zephyr-workflow/zephyr/pkg/timer"
)

func TestMilestone2WorkerQueueAndFencingEndToEnd(t *testing.T) {
	workQueue := queue.NewMemoryQueue(2)
	defer workQueue.Close()
	ctx := context.Background()
	generator := queue.FencingTokenGenerator{}
	leaseToken := generator.Next()
	item := queue.WorkItem{
		ID:         "work-1",
		WorkflowID: "workflow-1",
		TaskID:     "task-1",
		NodeID:     "charge",
		TaskName:   "ChargePayment",
		LeaseToken: leaseToken,
	}
	if err := workQueue.Publish(ctx, item); err != nil {
		t.Fatal(err)
	}
	delivery, err := workQueue.Receive(ctx, "worker-a")
	if err != nil {
		t.Fatal(err)
	}
	if delivery.Item.WorkflowID != item.WorkflowID || delivery.Item.TaskID != item.TaskID {
		t.Fatalf("delivery item = %#v, want workflow/task %s/%s", delivery.Item, item.WorkflowID, item.TaskID)
	}
	if err := queue.ValidateFencingToken(delivery.Item.LeaseToken, leaseToken); err != nil {
		t.Fatal(err)
	}
	if err := queue.ValidateFencingToken(delivery.Item.LeaseToken, generator.Next()); err == nil {
		t.Fatal("stale worker token was accepted")
	}
	if err := workQueue.Ack(ctx, delivery.ID); err != nil {
		t.Fatal(err)
	}
	if err := workQueue.Ack(ctx, delivery.ID); !errors.Is(err, queue.ErrNotFound) {
		t.Fatalf("second acknowledgement error = %v, want %v", err, queue.ErrNotFound)
	}
}

func TestMilestone2LeaseTimerEndToEnd(t *testing.T) {
	service := timer.NewService(2)
	defer service.Close()
	if err := service.Schedule(timer.Deadline{ID: "lease-task-1", At: time.Now().Add(10 * time.Millisecond), Payload: "expire"}); err != nil {
		t.Fatal(err)
	}
	select {
	case deadline := <-service.Events():
		if deadline.ID != "lease-task-1" || deadline.Payload != "expire" {
			t.Fatalf("deadline = %#v, want lease expiration", deadline)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("lease deadline did not reach the timer consumer")
	}
}

func TestMilestone2LeaseReclamationAndWorkerDiscoveryEndToEnd(t *testing.T) {
	workQueue := queue.NewMemoryQueue(2)
	timers := timer.NewService(2)
	manager, err := lease.NewManager(workQueue, timers, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		manager.Close()
		timers.Close()
		workQueue.Close()
	}()
	ctx := context.Background()
	if err := workQueue.Publish(ctx, queue.WorkItem{ID: "work-reclaim", TaskID: "task-reclaim"}); err != nil {
		t.Fatal(err)
	}
	delivery, err := workQueue.Receive(ctx, "worker-a")
	if err != nil {
		t.Fatal(err)
	}
	activeLease, err := manager.Acquire(ctx, delivery, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-manager.Expirations():
	case <-time.After(250 * time.Millisecond):
		t.Fatal("lease was not reclaimed")
	}
	if err := manager.Complete(ctx, activeLease.ID, activeLease.Token); !errors.Is(err, lease.ErrLeaseExpired) {
		t.Fatalf("stale completion error = %v, want %v", err, lease.ErrLeaseExpired)
	}
	reclaimed, err := workQueue.Receive(ctx, "worker-b")
	if err != nil {
		t.Fatal(err)
	}
	if err := workQueue.Ack(ctx, reclaimed.ID); err != nil {
		t.Fatal(err)
	}

	resolver, err := discovery.NewRoundRobin(discovery.StaticResolver{Endpoints: []discovery.Endpoint{
		{Scheme: "http", Host: "engine-a", Port: 8080},
		{Scheme: "http", Host: "engine-b", Port: 8080},
	}})
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := resolver.Next(ctx, "zephyr-engine")
	if err != nil {
		t.Fatal(err)
	}
	if endpoint.Address() != "engine-a:8080" {
		t.Fatalf("discovered endpoint = %q, want engine-a:8080", endpoint.Address())
	}
}

func TestMilestone2DelayNodeEndToEnd(t *testing.T) {
	definition := domain.WorkflowDef{
		Name:    "delayed-notification",
		Version: 1,
		Nodes: map[string]domain.NodeDefinition{
			"wait":   {ID: "wait", Type: domain.NodeDelay, Delay: 15 * time.Millisecond},
			"notify": {ID: "notify", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "Notify"}, DependsOn: []string{"wait"}},
		},
	}
	memory := store.NewMemoryStore()
	timers := timer.NewService(2)
	engine := decider.NewWithTimer(memory, timers)
	defer func() {
		engine.Close()
		timers.Close()
	}()
	instance, err := engine.Start(definition, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, scheduled := instance.Tasks["notify"]; scheduled {
		t.Fatal("notify was scheduled before delay completion")
	}
	deadline := time.After(250 * time.Millisecond)
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			current, getErr := memory.Get(instance.ID)
			if getErr != nil {
				t.Fatal(getErr)
			}
			if current.Tasks["notify"].Status == domain.TaskReady {
				if err := engine.CompleteTask(instance.ID, "notify", nil); err != nil {
					t.Fatal(err)
				}
				completed, getErr := memory.Get(instance.ID)
				if getErr != nil {
					t.Fatal(getErr)
				}
				if completed.Status != domain.WorkflowCompleted {
					t.Fatalf("workflow status = %s, want %s", completed.Status, domain.WorkflowCompleted)
				}
				return
			}
		case <-deadline:
			t.Fatal("delay node did not release downstream work")
		}
	}
}
