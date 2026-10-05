package client

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/zephyr-workflow/zephyr/pkg/auth"
	"github.com/zephyr-workflow/zephyr/pkg/decider"
	"github.com/zephyr-workflow/zephyr/pkg/domain"
	"github.com/zephyr-workflow/zephyr/pkg/gateway"
	"github.com/zephyr-workflow/zephyr/pkg/lease"
	"github.com/zephyr-workflow/zephyr/pkg/queue"
	"github.com/zephyr-workflow/zephyr/pkg/store"
	"github.com/zephyr-workflow/zephyr/pkg/timer"
)

func TestConfigFromEnv(t *testing.T) {
	t.Setenv(EndpointEnv, " https://zephyr.example/ ")
	t.Setenv(TokenEnv, "token-value")
	t.Setenv(TimeoutEnv, "3s")

	config, err := ConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if config.Endpoint != "https://zephyr.example/" || config.Token != "token-value" || config.Timeout != 3*time.Second {
		t.Fatalf("unexpected config: %#v", config)
	}
}

func TestConfigFromEnvRequiresEndpoint(t *testing.T) {
	t.Setenv(EndpointEnv, " ")
	if _, err := ConfigFromEnv(); !errors.Is(err, ErrEndpointRequired) {
		t.Fatalf("ConfigFromEnv() error = %v, want endpoint-required error", err)
	}
}

func TestStartWorkflowUsesGatewayContract(t *testing.T) {
	type input struct {
		OrderID string `json:"order_id"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/v1/workflows/Checkout/instances" {
			t.Errorf("unexpected request: %s %s", request.Method, request.URL.Path)
		}
		if request.Header.Get("Authorization") != "Bearer token-value" {
			t.Errorf("unexpected authorization header: %q", request.Header.Get("Authorization"))
		}
		var body struct {
			Version int   `json:"version"`
			Context input `json:"context"`
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Errorf("decode request body: %v", err)
		}
		if body.Version != 1 || body.Context.OrderID != "order-42" {
			t.Errorf("unexpected request body: %#v", body)
		}
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"ID":"workflow-42"}`))
	}))
	defer server.Close()

	client, err := New(Config{Endpoint: server.URL, Token: "token-value", Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	run, err := client.StartWorkflow(context.Background(), "Checkout", 1, input{OrderID: "order-42"})
	if err != nil {
		t.Fatal(err)
	}
	if run.ID != "workflow-42" {
		t.Fatalf("workflow ID = %q, want workflow-42", run.ID)
	}
}

func TestStartWorkflowReturnsAPIError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		http.Error(response, `{"error":"workflow not registered"}`, http.StatusNotFound)
	}))
	defer server.Close()

	client, err := New(Config{Endpoint: server.URL, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.StartWorkflow(context.Background(), "Missing", 1, map[string]string{})
	var apiError *APIError
	if !errors.As(err, &apiError) || apiError.StatusCode != http.StatusNotFound || apiError.Message != "workflow not registered" {
		t.Fatalf("StartWorkflow() error = %#v, want 404 APIError", err)
	}
}

func TestIdempotentStartAndWaitForWorkflowResult(t *testing.T) {
	var reads int
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		switch request.Method {
		case http.MethodPost:
			if request.Header.Get("Idempotency-Key") != "checkout-42" {
				t.Errorf("Idempotency-Key = %q", request.Header.Get("Idempotency-Key"))
			}
			_, _ = response.Write([]byte(`{"id":"workflow-42"}`))
		case http.MethodGet:
			reads++
			if reads == 1 {
				_, _ = response.Write([]byte(`{"id":"workflow-42","status":"RUNNING"}`))
			} else {
				_, _ = response.Write([]byte(`{"id":"workflow-42","status":"COMPLETED","result":{"status":"paid"}}`))
			}
		default:
			t.Errorf("unexpected request method %s", request.Method)
		}
	}))
	defer server.Close()
	client, err := New(Config{Endpoint: server.URL, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	run, err := client.StartWorkflowWithIdempotencyKey(context.Background(), "Checkout", 1, map[string]any{"order_id": "order-42"}, "checkout-42")
	if err != nil || run.ID != "workflow-42" {
		t.Fatalf("idempotent start = %#v/%v", run, err)
	}
	completed, err := client.WaitWorkflow(context.Background(), run.ID, time.Millisecond)
	if err != nil || completed.Status != "COMPLETED" || completed.Result["status"] != "paid" {
		t.Fatalf("workflow wait result = %#v/%v", completed, err)
	}
}

func TestStartWorkflowAgainstGateway(t *testing.T) {
	workQueue := queue.NewMemoryQueue(2)
	timers := timer.NewService(1)
	leases, err := lease.NewManager(workQueue, timers, 1)
	if err != nil {
		t.Fatal(err)
	}
	executions := store.NewMemoryStore()
	engine := decider.New(executions)
	api, err := gateway.New(engine, executions, workQueue, leases)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		leases.Close()
		timers.Close()
		workQueue.Close()
	})
	definition := domain.WorkflowDef{
		Name:    "checkout",
		Version: 1,
		Nodes: map[string]domain.NodeDefinition{
			"charge": {ID: "charge", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "ChargePayment"}},
		},
	}
	if err := api.RegisterWorkflow(definition); err != nil {
		t.Fatal(err)
	}
	authenticator, err := auth.NewStaticTokenAuthenticator("local-development-token")
	if err != nil {
		t.Fatal(err)
	}
	protect, err := auth.Middleware(authenticator)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(protect(api.Handler()))
	defer server.Close()

	unauthorizedClient, err := New(Config{Endpoint: server.URL, Token: "wrong-token", Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := unauthorizedClient.StartWorkflow(context.Background(), "checkout", 1, nil); err == nil {
		t.Fatal("gateway accepted an invalid bearer token")
	}
	sdk, err := New(Config{Endpoint: server.URL, Token: "local-development-token", Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	run, err := sdk.StartWorkflow(context.Background(), "checkout", 1, map[string]any{"order_id": "order-42"})
	if err != nil {
		t.Fatal(err)
	}
	instance, err := executions.Get(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if instance.Context["order_id"] != "order-42" {
		t.Fatalf("persisted workflow context = %#v", instance.Context)
	}
}
