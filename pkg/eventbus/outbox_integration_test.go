package eventbus

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zephyr-workflow/zephyr/pkg/domain"
	"github.com/zephyr-workflow/zephyr/pkg/store"
)

func TestDurableOutboxRetriesWebhookDelivery(t *testing.T) {
	executions, err := store.OpenSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "events.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer executions.Close()
	instance, err := domain.NewWorkflowInstance(domain.WorkflowDef{
		Name:    "event-delivery",
		Version: 1,
		Nodes: map[string]domain.NodeDefinition{
			"task": {ID: "task", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "Run"}},
		},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := executions.Create(instance); err != nil {
		t.Fatal(err)
	}
	if err := executions.Append(instance.ID, domain.Event{WorkflowID: instance.ID, Type: domain.EventWorkflowStarted}); err != nil {
		t.Fatal(err)
	}
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Idempotency-Key") != instance.ID+":1" {
			t.Errorf("idempotency key = %q", request.Header.Get("Idempotency-Key"))
		}
		if attempts.Add(1) == 1 {
			response.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		response.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	webhook, err := NewWebhook(WebhookConfig{Endpoint: server.URL, HTTPClient: server.Client(), MaxAttempts: 1})
	if err != nil {
		t.Fatal(err)
	}
	dispatcher, err := NewOutboxDispatcher(executions, webhook, OutboxDispatcherConfig{RetryDelay: time.Nanosecond})
	if err != nil {
		t.Fatal(err)
	}
	firstDelivered, firstErr := dispatcher.DispatchOnce(context.Background())
	if firstDelivered != 0 || firstErr == nil {
		t.Fatalf("first dispatch = %d, %v; want failure without delivery", firstDelivered, firstErr)
	}
	secondDelivered, secondErr := dispatcher.DispatchOnce(context.Background())
	if secondErr != nil || secondDelivered != 1 {
		t.Fatalf("retry dispatch = %d, %v; want one delivery", secondDelivered, secondErr)
	}
	if attempts.Load() != 2 {
		t.Fatalf("webhook attempts = %d, want 2", attempts.Load())
	}
	remaining, err := executions.ClaimOutbox(context.Background(), 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(remaining) != 0 {
		t.Fatalf("successfully delivered event remained in outbox: %#v", remaining)
	}
}
