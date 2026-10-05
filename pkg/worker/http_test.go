package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/zephyr-workflow/zephyr/pkg/gateway"
	"github.com/zephyr-workflow/zephyr/pkg/queue"
	"github.com/zephyr-workflow/zephyr/pkg/token"
)

type testPayload struct {
	OrderID string `json:"order_id"`
}

func TestHTTPTransportReceiveDecodeHeartbeatAndComplete(t *testing.T) {
	var heartbeat gateway.TaskHeartbeat
	var completion gateway.TaskCompletion
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case gateway.TaskReceivePath:
			var input gateway.ReceiveWorkRequest
			if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
				t.Errorf("decode receive request: %v", err)
			}
			if input.WorkerID != "worker-a" || input.LeaseDurationMS != 5000 {
				t.Errorf("receive request = %#v", input)
			}
			_ = json.NewEncoder(response).Encode(gateway.WorkDelivery{
				DeliveryID: "delivery-1",
				LeaseID:    "lease-1",
				LeaseToken: 7,
				Item:       queue.WorkItem{WorkflowID: "workflow-1", TaskID: "task-1", NodeID: "charge", Payload: map[string]any{"order_id": "order-1"}},
			})
		case gateway.TaskHeartbeatPath:
			if err := json.NewDecoder(request.Body).Decode(&heartbeat); err != nil {
				t.Errorf("decode heartbeat request: %v", err)
			}
			_ = json.NewEncoder(response).Encode(map[string]string{"status": "renewed"})
		case gateway.TaskCompletePath:
			if err := json.NewDecoder(request.Body).Decode(&completion); err != nil {
				t.Errorf("decode completion request: %v", err)
			}
			_ = json.NewEncoder(response).Encode(map[string]string{"status": "completed"})
		default:
			response.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	transport, err := NewHTTPTransport(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(transport, "worker-a", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := client.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	task, err := Decode[testPayload](delivery)
	if err != nil {
		t.Fatal(err)
	}
	if task.Payload.OrderID != "order-1" {
		t.Fatalf("decoded payload = %#v", task.Payload)
	}
	if err := client.Heartbeat(context.Background(), delivery, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	if heartbeat.LeaseID != "lease-1" || heartbeat.LeaseToken != 7 || heartbeat.LeaseDurationMS != 2000 {
		t.Fatalf("heartbeat request = %#v", heartbeat)
	}
	if err := client.Complete(context.Background(), Completion(task, map[string]any{"transaction_id": "tx-1"})); err != nil {
		t.Fatal(err)
	}
	if completion.WorkflowID != "workflow-1" || completion.LeaseID != "lease-1" || completion.Result["transaction_id"] != "tx-1" {
		t.Fatalf("completion request = %#v", completion)
	}
}

func TestHTTPTransportPreservesLeaseConflict(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		response.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(response).Encode(map[string]string{"error": "lease expired"})
	}))
	defer server.Close()
	transport, err := NewHTTPTransport(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	err = transport.Heartbeat(context.Background(), gateway.TaskHeartbeat{LeaseID: "lease-1", LeaseToken: 1, LeaseDurationMS: 1000})
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusConflict || httpErr.Message != "lease expired" {
		t.Fatalf("heartbeat error = %v, want HTTP 409 lease expired", err)
	}
}

func TestHTTPTransportUsesFreshAccessTokenSource(t *testing.T) {
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		requests++
		if request.Header.Get("Authorization") != fmt.Sprintf("Bearer worker-access-%d", requests) {
			t.Errorf("request %d authorization = %q", requests, request.Header.Get("Authorization"))
		}
		if request.URL.Path == gateway.TaskReceivePath {
			_ = json.NewEncoder(response).Encode(gateway.WorkDelivery{LeaseID: "lease-1", LeaseToken: 1})
			return
		}
		response.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	var issued int
	transport, err := NewHTTPTransportWithTokenSource(server.URL, server.Client(), token.SourceFunc(func(context.Context) (string, error) {
		issued++
		return fmt.Sprintf("worker-access-%d", issued), nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := transport.Receive(context.Background(), gateway.ReceiveWorkRequest{WorkerID: "worker-a", LeaseDurationMS: 1000}); err != nil {
		t.Fatal(err)
	}
	if err := transport.Heartbeat(context.Background(), gateway.TaskHeartbeat{LeaseID: "lease-1", LeaseToken: 1, LeaseDurationMS: 1000}); err != nil {
		t.Fatal(err)
	}
	if requests != 2 || issued != 2 {
		t.Fatalf("requests=%d issued tokens=%d, want 2 each", requests, issued)
	}
}
