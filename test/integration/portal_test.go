package integration_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/zephyr-workflow/zephyr/pkg/decider"
	"github.com/zephyr-workflow/zephyr/pkg/dsl/compiler"
	"github.com/zephyr-workflow/zephyr/pkg/gateway"
	"github.com/zephyr-workflow/zephyr/pkg/lease"
	"github.com/zephyr-workflow/zephyr/pkg/portal"
	"github.com/zephyr-workflow/zephyr/pkg/queue"
	"github.com/zephyr-workflow/zephyr/pkg/store"
	"github.com/zephyr-workflow/zephyr/pkg/timer"
	"github.com/zephyr-workflow/zephyr/pkg/worker"
)

func TestPortalWorkflowStartWorkerExecutionAndRunInspection(t *testing.T) {
	source, err := os.ReadFile("../../examples/quickstart/checkout.zephyr")
	if err != nil {
		t.Fatal(err)
	}
	definition, err := compiler.Compile(string(source))
	if err != nil {
		t.Fatal(err)
	}
	workQueue := queue.NewMemoryQueue(8)
	timers := timer.NewService(8)
	leases, err := lease.NewManager(workQueue, timers, 8)
	if err != nil {
		t.Fatal(err)
	}
	executions := store.NewMemoryStore()
	engine := decider.NewWithTimer(executions, timers)
	api, err := gateway.New(engine, executions, workQueue, leases)
	if err != nil {
		t.Fatal(err)
	}
	if err := api.RegisterWorkflow(definition); err != nil {
		t.Fatal(err)
	}
	portalHandler, err := portal.New(api.Handler())
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(portalHandler)
	defer server.Close()
	t.Cleanup(func() {
		leases.Close()
		engine.Close()
		timers.Close()
		_ = workQueue.Close()
	})

	pageResponse, err := server.Client().Get(server.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	if pageResponse.StatusCode != http.StatusOK {
		t.Fatalf("portal page status = %d", pageResponse.StatusCode)
	}
	pageResponse.Body.Close()
	workflowResponse, err := server.Client().Get(server.URL + "/v1/workflows")
	if err != nil {
		t.Fatal(err)
	}
	var workflows []gateway.WorkflowSummary
	if err := json.NewDecoder(workflowResponse.Body).Decode(&workflows); err != nil {
		t.Fatal(err)
	}
	workflowResponse.Body.Close()
	if len(workflows) != 1 || workflows[0].Name != "Checkout" {
		t.Fatalf("portal workflow catalog = %#v", workflows)
	}

	startBody := `{"version":1,"context":{"order_id":"order-portal-7","customer_email":"ops@example.test"}}`
	startResponse, err := server.Client().Post(server.URL+"/v1/workflows/Checkout/instances", "application/json", strings.NewReader(startBody))
	if err != nil {
		t.Fatal(err)
	}
	var started struct {
		ID string `json:"ID"`
	}
	if err := json.NewDecoder(startResponse.Body).Decode(&started); err != nil {
		t.Fatal(err)
	}
	startResponse.Body.Close()
	if startResponse.StatusCode != http.StatusOK || started.ID == "" {
		t.Fatalf("workflow start response = %d %#v", startResponse.StatusCode, started)
	}

	transport, err := worker.NewHTTPTransport(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	workerClient, err := worker.NewClient(transport, "portal-e2e-worker", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for index, result := range []map[string]any{{"authorization": "approved"}, {"status": "queued"}} {
		delivery, err := workerClient.Receive(context.Background())
		if err != nil {
			t.Fatalf("receive task %d: %v", index+1, err)
		}
		if err := workerClient.Heartbeat(context.Background(), delivery, 5*time.Second); err != nil {
			t.Fatalf("heartbeat task %d: %v", index+1, err)
		}
		if err := workerClient.Complete(context.Background(), gateway.TaskCompletion{
			WorkflowID: delivery.Item.WorkflowID,
			TaskID:     delivery.Item.TaskID,
			NodeID:     delivery.Item.NodeID,
			LeaseID:    delivery.LeaseID,
			LeaseToken: delivery.LeaseToken,
			Result:     result,
		}); err != nil {
			t.Fatalf("complete task %d: %v", index+1, err)
		}
	}

	runsResponse, err := server.Client().Get(server.URL + "/v1/instances?workflow_name=Checkout&limit=10")
	if err != nil {
		t.Fatal(err)
	}
	var runs gateway.WorkflowRunPage
	if err := json.NewDecoder(runsResponse.Body).Decode(&runs); err != nil {
		t.Fatal(err)
	}
	runsResponse.Body.Close()
	if runs.Total != 1 || len(runs.Items) != 1 || runs.Items[0].ID != started.ID || runs.Items[0].Status != "COMPLETED" {
		t.Fatalf("portal run list = %#v", runs)
	}
	detailResponse, err := server.Client().Get(server.URL + "/v1/instances/" + started.ID)
	if err != nil {
		t.Fatal(err)
	}
	var detail gateway.WorkflowRunDetail
	if err := json.NewDecoder(detailResponse.Body).Decode(&detail); err != nil {
		t.Fatal(err)
	}
	detailResponse.Body.Close()
	if detail.Status != "COMPLETED" || detail.Result["status"] != "queued" || len(detail.Tasks) != 2 || len(detail.Events) < 7 {
		t.Fatalf("portal run detail status/tasks/events = %s/%d/%d", detail.Status, len(detail.Tasks), len(detail.Events))
	}
	metricsResponse, err := server.Client().Get(server.URL + "/v1/metrics")
	if err != nil {
		t.Fatal(err)
	}
	var metrics gateway.WorkflowMetrics
	if err := json.NewDecoder(metricsResponse.Body).Decode(&metrics); err != nil {
		t.Fatal(err)
	}
	metricsResponse.Body.Close()
	if metrics.Total != 1 || metrics.Completed != 1 {
		t.Fatalf("portal summary metrics = %#v", metrics)
	}
}
