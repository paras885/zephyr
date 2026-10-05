package eventbus

import (
	"context"
	"testing"
	"time"

	"github.com/zephyr-workflow/zephyr/pkg/domain"
)

func TestMemoryPublishesIndependentEventCopies(t *testing.T) {
	bus := NewMemory()
	defer bus.Close()
	first, err := bus.Subscribe(1)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := bus.Subscribe(1)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	event := domain.Event{WorkflowID: "workflow-1", Sequence: 3, Type: domain.EventTaskCompleted, Payload: map[string]any{"result": map[string]any{"status": "paid"}}}
	if err := bus.Publish(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	firstEvent := <-first.Events()
	firstEvent.Payload["result"].(map[string]any)["status"] = "mutated"
	secondEvent := <-second.Events()
	if secondEvent.Payload["result"].(map[string]any)["status"] != "paid" {
		t.Fatalf("subscriber event was mutable across subscribers: %#v", secondEvent.Payload)
	}
}

func TestMemoryPublishHonorsBackpressureContext(t *testing.T) {
	bus := NewMemory()
	defer bus.Close()
	subscription, err := bus.Subscribe(0)
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err = bus.Publish(ctx, domain.Event{WorkflowID: "workflow-1", Sequence: 1})
	if err != context.DeadlineExceeded {
		t.Fatalf("Publish() error = %v, want deadline exceeded", err)
	}
}
