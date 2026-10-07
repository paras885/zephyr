package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"example.com/checkout/generated"
	zephyrclient "github.com/zephyr-workflow/zephyr/pkg/client"
	"github.com/zephyr-workflow/zephyr/pkg/gateway"
	"github.com/zephyr-workflow/zephyr/pkg/worker"
)

func TestApplicationUsesGeneratedClient(t *testing.T) {
	platform := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Error("missing platform authorization")
		}
		if r.Method == http.MethodPost {
			if r.URL.Path != "/v1/workflows/Checkout/instances" || r.Header.Get("Idempotency-Key") != "order-1" {
				t.Errorf("unexpected start request: %s", r.URL)
			}
			var input gateway.StartWorkflowRequest
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
				t.Error(err)
			}
			if input.Version != 1 || input.Context["order_id"] != "order-1" {
				t.Errorf("unexpected input: %+v", input)
			}
			fmtResponse(w, `{"id":"run-1","status":"RUNNING"}`)
		} else {
			fmtResponse(w, `{"id":"run-1","status":"COMPLETED","result":{"status":"receipt_sent"}}`)
		}
	}))
	defer platform.Close()
	config := zephyrclient.Config{Endpoint: platform.URL, Token: "test-token", Timeout: time.Second}
	client, err := generated.NewCheckoutClient(config)
	if err != nil {
		t.Fatal(err)
	}
	runs, err := zephyrclient.New(config)
	if err != nil {
		t.Fatal(err)
	}
	handler := newHandler(client, runs)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(`{"order_id":"order-1","customer_email":"test@example.test"}`)))
	if response.Code != http.StatusAccepted || !strings.Contains(response.Body.String(), "run-1") {
		t.Fatalf("start response: %d %s", response.Code, response.Body)
	}
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/orders/run-1", nil))
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "receipt_sent") {
		t.Fatalf("result response: %d %s", response.Code, response.Body)
	}
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(`{}`)))
	if response.Code != http.StatusBadRequest {
		t.Fatal("expected missing input rejection")
	}
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(`{"order_id":"order-1","customer_email":"test@example.test"} {}`)))
	if response.Code != http.StatusBadRequest {
		t.Fatal("expected trailing JSON rejection")
	}
}

func fmtResponse(w http.ResponseWriter, body string) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(body))
}

type recordingTransport struct {
	completion gateway.TaskCompletion
	failure    gateway.TaskFailure
}

func (*recordingTransport) Receive(context.Context, gateway.ReceiveWorkRequest) (gateway.WorkDelivery, error) {
	return gateway.WorkDelivery{}, nil
}
func (*recordingTransport) Heartbeat(context.Context, gateway.TaskHeartbeat) error { return nil }
func (r *recordingTransport) Complete(_ context.Context, completion gateway.TaskCompletion) error {
	r.completion = completion
	return nil
}
func (r *recordingTransport) Fail(_ context.Context, failure gateway.TaskFailure) error {
	r.failure = failure
	return nil
}

func TestWorkerRejectsUnsupportedAndMalformedTasks(t *testing.T) {
	for _, name := range []string{"UnknownTask", "AuthorizePayment"} {
		t.Run(name, func(t *testing.T) {
			transport := &recordingTransport{}
			consumer, err := worker.NewClient(transport, "test-worker", time.Second)
			if err != nil {
				t.Fatal(err)
			}
			delivery := gateway.WorkDelivery{LeaseID: "lease-1", LeaseToken: 1}
			delivery.Item.TaskName = name
			delivery.Item.WorkflowID = "run-1"
			delivery.Item.TaskID = "task-1"
			delivery.Item.NodeID = "node-1"
			delivery.Item.Payload = map[string]any{"order_id": "order-1", "customer_email": "test@example.test"}
			if name == "AuthorizePayment" {
				delivery.Item.Payload["order_id"] = 42
			}
			if err := execute(context.Background(), consumer, checkoutTasks{}, delivery); err != nil {
				t.Fatal(err)
			}
			if transport.failure.WorkflowID != "run-1" || transport.failure.LeaseID != "lease-1" || transport.failure.Error == "" {
				t.Fatalf("missing failure identity: %+v", transport.failure)
			}
		})
	}
}

func TestGeneratedTaskImplementations(t *testing.T) {
	input := generated.CheckoutInput{OrderId: "order-1", CustomerEmail: "test@example.test"}
	for _, status := range []string{"authorized", "receipt_sent"} {
		output, err := simulate(context.Background(), input, status)
		if err != nil || output.Status != status {
			t.Fatalf("output = %+v, error = %v", output, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := simulate(ctx, input, "authorized"); err == nil {
		t.Fatal("expected cancellation")
	}
}

func TestWorkerCompletesWithDeliveryIdentity(t *testing.T) {
	transport := &recordingTransport{}
	consumer, err := worker.NewClient(transport, "test-worker", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	delivery := gateway.WorkDelivery{LeaseID: "lease-1", LeaseToken: 7}
	delivery.Item.TaskName = "SendReceipt"
	delivery.Item.WorkflowID = "run-1"
	delivery.Item.TaskID = "task-1"
	delivery.Item.NodeID = "receipt"
	delivery.Item.Payload = map[string]any{"order_id": "order-1", "customer_email": "test@example.test"}
	if err := execute(context.Background(), consumer, checkoutTasks{}, delivery); err != nil {
		t.Fatal(err)
	}
	completion := transport.completion
	if completion.WorkflowID != "run-1" || completion.TaskID != "task-1" || completion.NodeID != "receipt" || completion.LeaseID != "lease-1" || completion.LeaseToken != 7 || completion.Result["status"] != "receipt_sent" {
		t.Fatalf("unexpected completion: %+v", completion)
	}
}
