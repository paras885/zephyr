package eventbus

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zephyr-workflow/zephyr/pkg/domain"
)

func TestWebhookRetriesAndUsesEventIdentity(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.Header.Get("Authorization") != "Bearer secret" || request.Header.Get("Idempotency-Key") != "workflow-8:4" {
			t.Errorf("unexpected webhook request: %#v", request)
		}
		var event domain.Event
		if err := json.NewDecoder(request.Body).Decode(&event); err != nil {
			t.Errorf("decode webhook event: %v", err)
		}
		if event.WorkflowID != "workflow-8" || event.Sequence != 4 {
			t.Errorf("webhook event = %#v", event)
		}
		if attempts.Add(1) == 1 {
			response.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		response.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	webhook, err := NewWebhook(WebhookConfig{Endpoint: server.URL, Token: "secret", HTTPClient: server.Client(), MaxAttempts: 2, RetryDelay: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if err := webhook.Publish(context.Background(), domain.Event{WorkflowID: "workflow-8", Sequence: 4, Type: domain.EventTaskCompleted}); err != nil {
		t.Fatal(err)
	}
	if attempts.Load() != 2 {
		t.Fatalf("webhook attempts = %d, want 2", attempts.Load())
	}
}

func TestWebhookDoesNotRetryClientErrors(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		http.Error(response, "invalid event", http.StatusBadRequest)
	}))
	defer server.Close()
	webhook, err := NewWebhook(WebhookConfig{Endpoint: server.URL, HTTPClient: server.Client(), MaxAttempts: 4})
	if err != nil {
		t.Fatal(err)
	}
	err = webhook.Publish(context.Background(), domain.Event{WorkflowID: "workflow-1", Sequence: 1})
	var webhookErr *WebhookError
	if !errors.As(err, &webhookErr) || webhookErr.StatusCode != http.StatusBadRequest {
		t.Fatalf("Publish() error = %v, want 400 WebhookError", err)
	}
	if attempts.Load() != 1 {
		t.Fatalf("webhook attempts = %d, want 1", attempts.Load())
	}
}
