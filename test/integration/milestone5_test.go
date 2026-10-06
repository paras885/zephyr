package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/zephyr-workflow/zephyr/pkg/decider"
	"github.com/zephyr-workflow/zephyr/pkg/domain"
	"github.com/zephyr-workflow/zephyr/pkg/gateway"
	"github.com/zephyr-workflow/zephyr/pkg/lease"
	"github.com/zephyr-workflow/zephyr/pkg/queue"
	"github.com/zephyr-workflow/zephyr/pkg/store"
	"github.com/zephyr-workflow/zephyr/pkg/timer"
	"github.com/zephyr-workflow/zephyr/pkg/worker"
)

type checkoutPayload struct {
	OrderID string `json:"order_id"`
}

func TestMilestone5HTTPGatewayRunsWorkflowThroughInMemoryComponents(t *testing.T) {
	workQueue := queue.NewMemoryQueue(4)
	timers := timer.NewService(2)
	manager, err := lease.NewManager(workQueue, timers, 2)
	if err != nil {
		t.Fatal(err)
	}
	executions := store.NewMemoryStore()
	engine := decider.New(executions)
	api, err := gateway.New(engine, executions, workQueue, manager)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		manager.Close()
		timers.Close()
		workQueue.Close()
	}()
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
	server := httptest.NewServer(api.Handler())
	defer server.Close()
	transport, err := worker.NewHTTPTransport(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	workerClient, err := worker.NewClient(transport, "worker-a", time.Second)
	if err != nil {
		t.Fatal(err)
	}

	startResponse := doJSON(t, server.Client(), http.MethodPost, server.URL+"/v1/workflows/checkout/instances", gateway.StartWorkflowRequest{Context: map[string]any{"order_id": "order-1"}})
	if startResponse.StatusCode != http.StatusOK {
		t.Fatalf("start status = %d", startResponse.StatusCode)
	}
	var instance domain.WorkflowInstance
	decodeResponse(t, startResponse, &instance)

	delivery, err := workerClient.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if delivery.Item.WorkflowID != instance.ID || delivery.Item.TaskID == "" || delivery.LeaseToken == 0 {
		t.Fatalf("delivery = %#v, want workflow, task, and lease token", delivery)
	}
	task, err := worker.Decode[checkoutPayload](delivery)
	if err != nil {
		t.Fatal(err)
	}
	if task.Payload.OrderID != "order-1" {
		t.Fatalf("decoded payload = %#v, want order-1", task.Payload)
	}

	staleHeartbeat := doJSON(t, server.Client(), http.MethodPost, server.URL+gateway.TaskHeartbeatPath, gateway.TaskHeartbeat{
		LeaseID: delivery.LeaseID, LeaseToken: delivery.LeaseToken + 1, LeaseDurationMS: 1000,
	})
	if staleHeartbeat.StatusCode != http.StatusConflict {
		staleHeartbeat.Body.Close()
		t.Fatalf("stale heartbeat status = %d, want %d", staleHeartbeat.StatusCode, http.StatusConflict)
	}
	staleHeartbeat.Body.Close()
	if err := workerClient.Heartbeat(context.Background(), delivery, time.Second); err != nil {
		t.Fatal(err)
	}
	heartbeats, err := workerClient.StartHeartbeat(context.Background(), delivery, 20*time.Millisecond, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(250 * time.Millisecond)
	if err := heartbeats.Stop(); err != nil {
		t.Fatal(err)
	}

	staleResponse := doJSON(t, server.Client(), http.MethodPost, server.URL+gateway.TaskCompletePath, gateway.TaskCompletion{
		WorkflowID: instance.ID,
		TaskID:     delivery.Item.TaskID,
		NodeID:     delivery.Item.NodeID,
		LeaseID:    delivery.LeaseID,
		LeaseToken: delivery.LeaseToken + 1,
	})
	if staleResponse.StatusCode != http.StatusConflict {
		staleResponse.Body.Close()
		t.Fatalf("stale completion status = %d, want %d", staleResponse.StatusCode, http.StatusConflict)
	}
	staleResponse.Body.Close()

	if err := workerClient.Complete(context.Background(), worker.Completion(task, map[string]any{"transaction_id": "tx-1"})); err != nil {
		t.Fatal(err)
	}
	completed, err := executions.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if completed.Status != domain.WorkflowCompleted || completed.Tasks[delivery.Item.NodeID].Status != domain.TaskCompleted {
		t.Fatalf("workflow/task status = %s/%s, want COMPLETED/COMPLETED", completed.Status, completed.Tasks[delivery.Item.NodeID].Status)
	}

	if _, err := api.StartWorkflow(context.Background(), "checkout", 1, map[string]any{"order_id": "order-2"}); err != nil {
		t.Fatal(err)
	}
	expiring, err := transport.Receive(context.Background(), gateway.ReceiveWorkRequest{WorkerID: "worker-b", LeaseDurationMS: 10})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	expiredHeartbeat := doJSON(t, server.Client(), http.MethodPost, server.URL+gateway.TaskHeartbeatPath, gateway.TaskHeartbeat{
		LeaseID: expiring.LeaseID, LeaseToken: expiring.LeaseToken, LeaseDurationMS: 1000,
	})
	if expiredHeartbeat.StatusCode != http.StatusConflict {
		expiredHeartbeat.Body.Close()
		t.Fatalf("expired heartbeat status = %d, want %d", expiredHeartbeat.StatusCode, http.StatusConflict)
	}
	expiredHeartbeat.Body.Close()
}

func doJSON(t *testing.T, client *http.Client, method, url string, value any) *http.Response {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(context.Background(), method, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func decodeResponse(t *testing.T, response *http.Response, value any) {
	t.Helper()
	defer response.Body.Close()
	if err := json.NewDecoder(response.Body).Decode(value); err != nil {
		t.Fatal(err)
	}
}
