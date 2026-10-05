package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zephyr-workflow/zephyr/pkg/auth"
	"github.com/zephyr-workflow/zephyr/pkg/decider"
	"github.com/zephyr-workflow/zephyr/pkg/domain"
	"github.com/zephyr-workflow/zephyr/pkg/lease"
	"github.com/zephyr-workflow/zephyr/pkg/queue"
	"github.com/zephyr-workflow/zephyr/pkg/store"
	"github.com/zephyr-workflow/zephyr/pkg/timer"
)

type testCompletionQueue struct {
	deliveries chan queue.CompletionDelivery
	acked      chan string
	rejected   chan bool
}

type failOnceWorkQueue struct {
	queue.QueueDAO
	mu           sync.Mutex
	publishCalls int
	failOnCall   int
}

func (testQueue *failOnceWorkQueue) Publish(ctx context.Context, item queue.WorkItem) error {
	testQueue.mu.Lock()
	testQueue.publishCalls++
	shouldFail := testQueue.publishCalls == testQueue.failOnCall
	testQueue.mu.Unlock()
	if shouldFail {
		return errors.New("temporary publish failure")
	}
	return testQueue.QueueDAO.Publish(ctx, item)
}

func (testQueue *testCompletionQueue) PublishCompletion(context.Context, queue.CompletionMessage) error {
	return nil
}

func (testQueue *testCompletionQueue) ReceiveCompletion(ctx context.Context, _ string) (queue.CompletionDelivery, error) {
	select {
	case delivery := <-testQueue.deliveries:
		return delivery, nil
	case <-ctx.Done():
		return queue.CompletionDelivery{}, ctx.Err()
	}
}

func (testQueue *testCompletionQueue) AckCompletion(_ context.Context, deliveryID string) error {
	testQueue.acked <- deliveryID
	return nil
}

func (testQueue *testCompletionQueue) RejectCompletion(_ context.Context, _ string, requeue bool) error {
	testQueue.rejected <- requeue
	return nil
}

func (testQueue *testCompletionQueue) Close() error { return nil }

func newTestGateway(t *testing.T) (*Gateway, *store.MemoryStore, *queue.MemoryQueue, *lease.Manager, *timer.Service) {
	t.Helper()
	workQueue := queue.NewMemoryQueue(4)
	timers := timer.NewService(2)
	manager, err := lease.NewManager(workQueue, timers, 2)
	if err != nil {
		t.Fatal(err)
	}
	executions := store.NewMemoryStore()
	engine := decider.NewWithTimer(executions, timers)
	gateway, err := New(engine, executions, workQueue, manager)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		manager.Close()
		engine.Close()
		timers.Close()
		workQueue.Close()
	})
	return gateway, executions, workQueue, manager, timers
}

func testDefinition() domain.WorkflowDef {
	return domain.WorkflowDef{
		Name:    "checkout",
		Version: 1,
		Nodes: map[string]domain.NodeDefinition{
			"charge": {ID: "charge", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "ChargePayment"}},
		},
	}
}

func TestRegisterWorkflowSelectsLatestVersion(t *testing.T) {
	gateway, _, _, _, _ := newTestGateway(t)
	definition := testDefinition()
	if err := gateway.RegisterWorkflow(definition); err != nil {
		t.Fatal(err)
	}
	definition.Version = 2
	if err := gateway.RegisterWorkflow(definition); err != nil {
		t.Fatal(err)
	}
	instance, err := gateway.StartWorkflow(context.Background(), definition.Name, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if instance.Definition.Version != 2 {
		t.Fatalf("selected version = %d, want 2", instance.Definition.Version)
	}
}

func TestHandlerWithAuthProtectsGatewayRoutes(t *testing.T) {
	api, _, _, _, _ := newTestGateway(t)
	authenticator, err := auth.NewStaticTokenAuthenticator("gateway-token")
	if err != nil {
		t.Fatal(err)
	}
	handler, err := api.HandlerWithAuth(authenticator)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, TaskHeartbeatPath, strings.NewReader(`{"lease_id":"lease-1","lease_token":1,"lease_duration_ms":1000}`))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated gateway response = %d, want 401", response.Code)
	}
	request = httptest.NewRequest(http.MethodPost, TaskHeartbeatPath, strings.NewReader(`{"lease_id":"lease-1","lease_token":1,"lease_duration_ms":1000}`))
	request.Header.Set("Authorization", "Bearer gateway-token")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code == http.StatusUnauthorized {
		t.Fatal("valid bearer token was rejected by gateway")
	}
}

func TestGatewayReadAPIsListWorkflowsRunsAndRunDetails(t *testing.T) {
	api, _, _, _, _ := newTestGateway(t)
	definition := testDefinition()
	if err := api.RegisterWorkflow(definition); err != nil {
		t.Fatal(err)
	}
	definition.Version = 2
	if err := api.RegisterWorkflow(definition); err != nil {
		t.Fatal(err)
	}
	instance, err := api.StartWorkflow(context.Background(), "checkout", 2, map[string]any{"order_id": "order-9"})
	if err != nil {
		t.Fatal(err)
	}
	handler := api.Handler()
	doGET := func(path string, target any) {
		t.Helper()
		request := httptest.NewRequest(http.MethodGet, path, nil)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("GET %s returned %d: %s", path, response.Code, response.Body.String())
		}
		if err := json.Unmarshal(response.Body.Bytes(), target); err != nil {
			t.Fatalf("decode GET %s response: %v", path, err)
		}
	}
	var workflows []WorkflowSummary
	doGET("/v1/workflows", &workflows)
	if len(workflows) != 1 || workflows[0].Name != "checkout" || workflows[0].LatestVersion != 2 || len(workflows[0].Versions) != 2 {
		t.Fatalf("workflow summaries = %#v", workflows)
	}
	var registeredDefinition domain.WorkflowDef
	doGET("/v1/workflows/checkout?version=1", &registeredDefinition)
	if registeredDefinition.Name != "checkout" || registeredDefinition.Version != 1 {
		t.Fatalf("registered workflow definition = %#v", registeredDefinition)
	}
	var allRuns WorkflowRunPage
	doGET("/v1/instances?limit=10", &allRuns)
	if allRuns.Total != 1 || len(allRuns.Items) != 1 || allRuns.Items[0].ID != instance.ID {
		t.Fatalf("unfiltered workflow run page = %#v", allRuns)
	}
	var metrics WorkflowMetrics
	doGET("/v1/metrics", &metrics)
	if metrics.Total != 1 || metrics.Running != 1 {
		t.Fatalf("workflow metrics = %#v", metrics)
	}
	var page WorkflowRunPage
	doGET("/v1/workflows/checkout/instances?status=RUNNING&limit=10", &page)
	if page.Total != 1 || len(page.Items) != 1 || page.Items[0].ID != instance.ID || page.Items[0].Status != domain.WorkflowRunning || page.Items[0].Version != 2 {
		t.Fatalf("workflow run page = %#v", page)
	}
	var detail WorkflowRunDetail
	doGET("/v1/instances/"+instance.ID, &detail)
	if detail.ID != instance.ID || detail.WorkflowName != "checkout" || detail.Context["order_id"] != "order-9" || len(detail.Tasks) != 1 || len(detail.Events) < 2 {
		t.Fatalf("workflow run detail = %#v", detail)
	}
}

func TestIdempotentWorkflowStartReusesRunAndRejectsChangedInput(t *testing.T) {
	api, executions, _, _, _ := newTestGateway(t)
	if err := api.RegisterWorkflow(testDefinition()); err != nil {
		t.Fatal(err)
	}
	handler := api.Handler()
	start := func(contextBody string) *httptest.ResponseRecorder {
		t.Helper()
		request := httptest.NewRequest(http.MethodPost, "/v1/workflows/checkout/instances", strings.NewReader(contextBody))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Idempotency-Key", "checkout-request-1")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		return response
	}
	first := start(`{"version":1,"context":{"order_id":"order-1"}}`)
	if first.Code != http.StatusOK {
		t.Fatalf("first idempotent start = %d: %s", first.Code, first.Body.String())
	}
	var firstBody map[string]any
	if err := json.Unmarshal(first.Body.Bytes(), &firstBody); err != nil {
		t.Fatal(err)
	}
	if len(firstBody) != 1 || firstBody["id"] == nil {
		t.Fatalf("start response must contain only the workflow ID: %#v", firstBody)
	}
	second := start(`{"version":1,"context":{"order_id":"order-1"}}`)
	var secondBody map[string]any
	if err := json.Unmarshal(second.Body.Bytes(), &secondBody); err != nil {
		t.Fatal(err)
	}
	if second.Code != http.StatusOK || secondBody["id"] != firstBody["id"] {
		t.Fatalf("duplicate idempotent start = %d %#v, want original ID %v", second.Code, secondBody, firstBody["id"])
	}
	changed := start(`{"version":1,"context":{"order_id":"order-2"}}`)
	if changed.Code != http.StatusConflict {
		t.Fatalf("changed request with reused key = %d, want 409: %s", changed.Code, changed.Body.String())
	}
	page, total, err := executions.ListExecutions(context.Background(), store.ExecutionFilter{Limit: 10})
	if err != nil || total != 1 || len(page) != 1 {
		t.Fatalf("idempotent starts created %d runs: %v", total, err)
	}
}

func TestIdempotentWorkflowStartRetriesFailedPublish(t *testing.T) {
	workQueue := &failOnceWorkQueue{QueueDAO: queue.NewMemoryQueue(4), failOnCall: 2}
	timers := timer.NewService(2)
	manager, err := lease.NewManager(workQueue, timers, 2)
	if err != nil {
		t.Fatal(err)
	}
	executions := store.NewMemoryStore()
	engine := decider.NewWithTimer(executions, timers)
	api, err := New(engine, executions, workQueue, manager)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		manager.Close()
		engine.Close()
		timers.Close()
		workQueue.Close()
	})
	definition := domain.WorkflowDef{
		Name: "checkout", Version: 1,
		Nodes: map[string]domain.NodeDefinition{
			"charge":  {ID: "charge", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "ChargePayment"}},
			"receipt": {ID: "receipt", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "SendReceipt"}},
		},
	}
	if err := api.RegisterWorkflow(definition); err != nil {
		t.Fatal(err)
	}
	handler := api.Handler()
	start := func() *httptest.ResponseRecorder {
		t.Helper()
		request := httptest.NewRequest(http.MethodPost, "/v1/workflows/checkout/instances", strings.NewReader(`{"version":1,"context":{"order_id":"order-1"}}`))
		request.Header.Set("Idempotency-Key", "retry-publish")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		return response
	}

	first := start()
	if first.Code != http.StatusBadRequest {
		t.Fatalf("first idempotent start = %d, want 400: %s", first.Code, first.Body.String())
	}
	dispatchContext, cancelDispatcher := context.WithCancel(context.Background())
	defer cancelDispatcher()
	go func() { _ = api.RunTaskPublicationDispatcher(dispatchContext) }()
	second := start()
	if second.Code != http.StatusOK {
		t.Fatalf("retry idempotent start = %d, want 200: %s", second.Code, second.Body.String())
	}
	var responseBody map[string]any
	if err := json.Unmarshal(second.Body.Bytes(), &responseBody); err != nil {
		t.Fatal(err)
	}
	if len(responseBody) != 1 || responseBody["id"] == nil {
		t.Fatalf("retry response must contain only the workflow ID: %#v", responseBody)
	}

	instance, err := executions.Get(responseBody["id"].(string))
	if err != nil {
		t.Fatal(err)
	}
	if len(instance.Tasks) != 2 {
		t.Fatalf("retried workflow has %d tasks, want 2", len(instance.Tasks))
	}
	seen := make(map[string]bool)
	for range 2 {
		delivery, err := api.ReceiveWork(context.Background(), "worker-a", 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if delivery.Item.ID != delivery.Item.TaskID || seen[delivery.Item.NodeID] || delivery.Item.TaskID != instance.Tasks[delivery.Item.NodeID].ID {
			t.Fatalf("retried task delivery = %#v; expected one stable delivery per task", delivery.Item)
		}
		seen[delivery.Item.NodeID] = true
	}
	if !seen["charge"] || !seen["receipt"] {
		t.Fatalf("partial-publish retry deliveries = %#v", seen)
	}
	checkContext, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := workQueue.QueueDAO.Receive(checkContext, "worker-a"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unexpected queued duplicate after recovery: %v", err)
	}
	_, total, err := executions.ListExecutions(context.Background(), store.ExecutionFilter{Limit: 10})
	if err != nil || total != 1 {
		t.Fatalf("idempotent retry created %d runs: %v", total, err)
	}
}

func TestIdempotentWorkflowStartRejectsEmptyKey(t *testing.T) {
	api, _, _, _, _ := newTestGateway(t)
	if err := api.RegisterWorkflow(testDefinition()); err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/workflows/checkout/instances", strings.NewReader(`{"version":1,"context":{}}`))
	request.Header.Set("Idempotency-Key", "   ")
	response := httptest.NewRecorder()
	api.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("empty idempotency key = %d, want 400", response.Code)
	}
}

func TestConcurrentIdempotentWorkflowStartsCreateOneRun(t *testing.T) {
	api, executions, _, _, _ := newTestGateway(t)
	if err := api.RegisterWorkflow(testDefinition()); err != nil {
		t.Fatal(err)
	}
	const requests = 16
	type startResult struct {
		status int
		id     string
		body   string
	}
	results := make(chan startResult, requests)
	var wait sync.WaitGroup
	for range requests {
		wait.Add(1)
		go func() {
			defer wait.Done()
			request := httptest.NewRequest(http.MethodPost, "/v1/workflows/checkout/instances", strings.NewReader(`{"version":1,"context":{"order_id":"order-same"}}`))
			request.Header.Set("Idempotency-Key", "same-request")
			response := httptest.NewRecorder()
			api.Handler().ServeHTTP(response, request)
			var body struct {
				ID string `json:"id"`
			}
			_ = json.Unmarshal(response.Body.Bytes(), &body)
			results <- startResult{status: response.Code, id: body.ID, body: response.Body.String()}
		}()
	}
	wait.Wait()
	close(results)
	workflowID := ""
	for result := range results {
		if result.status != http.StatusOK || result.id == "" {
			t.Fatalf("concurrent idempotent start = %d %q: %s", result.status, result.id, result.body)
		}
		if workflowID == "" {
			workflowID = result.id
		} else if result.id != workflowID {
			t.Fatalf("concurrent starts returned different IDs %q and %q", workflowID, result.id)
		}
	}
	_, total, err := executions.ListExecutions(context.Background(), store.ExecutionFilter{Limit: 20})
	if err != nil || total != 1 {
		t.Fatalf("concurrent idempotent starts created %d runs: %v", total, err)
	}
}

func TestCompleteWorkRejectsStaleLeaseWithoutChangingWorkflow(t *testing.T) {
	gateway, executions, _, _, _ := newTestGateway(t)
	if err := gateway.RegisterWorkflow(testDefinition()); err != nil {
		t.Fatal(err)
	}
	instance, err := gateway.StartWorkflow(context.Background(), "checkout", 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := gateway.ReceiveWork(context.Background(), "worker-a", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	err = gateway.CompleteWork(context.Background(), TaskCompletion{
		WorkflowID: instance.ID,
		TaskID:     delivery.Item.TaskID,
		NodeID:     delivery.Item.NodeID,
		LeaseID:    delivery.LeaseID,
		LeaseToken: delivery.LeaseToken + 1,
	})
	if !errors.Is(err, lease.ErrStaleToken) {
		t.Fatalf("stale completion error = %v, want %v", err, lease.ErrStaleToken)
	}
	current, err := executions.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Tasks[delivery.Item.NodeID].Status != domain.TaskRunning {
		t.Fatalf("task status = %s, want RUNNING", current.Tasks[delivery.Item.NodeID].Status)
	}
}

func TestFailWorkTransitionsWorkflowToFailed(t *testing.T) {
	gateway, executions, _, _, _ := newTestGateway(t)
	if err := gateway.RegisterWorkflow(testDefinition()); err != nil {
		t.Fatal(err)
	}
	instance, err := gateway.StartWorkflow(context.Background(), "checkout", 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := gateway.ReceiveWork(context.Background(), "worker-a", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := gateway.FailWork(context.Background(), TaskFailure{
		WorkflowID: instance.ID,
		TaskID:     delivery.Item.TaskID,
		NodeID:     delivery.Item.NodeID,
		LeaseID:    delivery.LeaseID,
		LeaseToken: delivery.LeaseToken,
		Error:      "payment declined",
	}); err != nil {
		t.Fatal(err)
	}
	failed, err := executions.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if failed.Status != domain.WorkflowFailed || failed.Tasks[delivery.Item.NodeID].Status != domain.TaskFailed {
		t.Fatalf("workflow/task status = %s/%s, want FAILED/FAILED", failed.Status, failed.Tasks[delivery.Item.NodeID].Status)
	}
}

func TestGatewayRetriesFailedTaskThroughWorkerQueue(t *testing.T) {
	api, executions, _, _, _ := newTestGateway(t)
	definition := testDefinition()
	definition.Nodes["charge"].Task.Retries = 1
	definition.Nodes["charge"].Task.Backoff = 10 * time.Millisecond
	if err := api.RegisterWorkflow(definition); err != nil {
		t.Fatal(err)
	}
	instance, err := api.StartWorkflow(context.Background(), "checkout", 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	first, err := api.ReceiveWork(context.Background(), "worker-a", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := api.FailWork(context.Background(), TaskFailure{
		WorkflowID: instance.ID, TaskID: first.Item.TaskID, NodeID: first.Item.NodeID,
		LeaseID: first.LeaseID, LeaseToken: first.LeaseToken, Error: "temporary error",
	}); err != nil {
		t.Fatal(err)
	}
	retryContext, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	retry, err := api.ReceiveWork(retryContext, "worker-a", time.Second)
	if err != nil {
		t.Fatalf("receive retried work: %v", err)
	}
	if retry.Item.TaskID == first.Item.TaskID {
		t.Fatal("retry reused the previous task ID")
	}
	if err := api.CompleteWork(context.Background(), TaskCompletion{
		WorkflowID: instance.ID, TaskID: retry.Item.TaskID, NodeID: retry.Item.NodeID,
		LeaseID: retry.LeaseID, LeaseToken: retry.LeaseToken, Result: map[string]any{"transaction_id": "tx-retry"},
	}); err != nil {
		t.Fatal(err)
	}
	completed, err := executions.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if completed.Status != domain.WorkflowCompleted || completed.Tasks[retry.Item.NodeID].Attempt != 2 {
		t.Fatalf("retried workflow/task status = %s/%#v", completed.Status, completed.Tasks[retry.Item.NodeID])
	}
}

func TestGatewayLeaseExpiryUsesRetryBackoff(t *testing.T) {
	api, executions, _, manager, _ := newTestGateway(t)
	definition := testDefinition()
	definition.Nodes["charge"].Task.Retries = 1
	definition.Nodes["charge"].Task.Backoff = 250 * time.Millisecond
	if err := api.RegisterWorkflow(definition); err != nil {
		t.Fatal(err)
	}
	instance, err := api.StartWorkflow(context.Background(), "checkout", 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	first, err := api.ReceiveWork(context.Background(), "worker-a", 15*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case expiration := <-manager.Expirations():
		if expiration.Lease.ID != first.LeaseID {
			t.Fatalf("expired lease = %q, want %q", expiration.Lease.ID, first.LeaseID)
		}
	case <-time.After(time.Second):
		t.Fatal("lease did not expire")
	}
	updated, err := executions.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if task := updated.Tasks[first.Item.NodeID]; task.Status != domain.TaskRetrying || task.Attempt != 2 {
		t.Fatalf("expired task state = %#v, want retrying attempt 2", task)
	}
	if remaining := time.Until(updated.Tasks[first.Item.NodeID].RetryAt); remaining < 150*time.Millisecond {
		t.Fatalf("persisted retry deadline has only %s remaining, want at least 150ms", remaining)
	}
	shortContext, cancelShort := context.WithTimeout(context.Background(), 75*time.Millisecond)
	defer cancelShort()
	if repeated, err := api.ReceiveWork(shortContext, "worker-b", time.Second); err == nil {
		t.Fatalf("delivery was returned before retry deadline: first task %q, received task %q (%#v)", first.Item.TaskID, repeated.Item.TaskID, repeated)
	}
	retryContext, cancelRetry := context.WithTimeout(context.Background(), time.Second)
	defer cancelRetry()
	retry, err := api.ReceiveWork(retryContext, "worker-b", time.Second)
	if err != nil {
		t.Fatalf("receive retry after backoff: %v", err)
	}
	if retry.Item.TaskID == first.Item.TaskID || retry.LeaseToken <= first.LeaseToken {
		t.Fatalf("retry did not advance task/fencing identity: first=%#v retry=%#v", first, retry)
	}
}

func TestCompletionConsumerAppliesAndAcknowledgesCompletion(t *testing.T) {
	api, executions, _, _, _ := newTestGateway(t)
	if err := api.RegisterWorkflow(testDefinition()); err != nil {
		t.Fatal(err)
	}
	instance, err := api.StartWorkflow(context.Background(), "checkout", 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := api.ReceiveWork(context.Background(), "worker-a", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	completions := &testCompletionQueue{
		deliveries: make(chan queue.CompletionDelivery, 1),
		acked:      make(chan string, 1),
		rejected:   make(chan bool, 1),
	}
	completions.deliveries <- queue.CompletionDelivery{
		ID: "completion-delivery-1",
		Message: queue.CompletionMessage{
			MessageID:  "completion-1",
			Kind:       queue.CompletionSucceeded,
			WorkflowID: instance.ID,
			TaskID:     delivery.Item.TaskID,
			NodeID:     delivery.Item.NodeID,
			LeaseID:    delivery.LeaseID,
			LeaseToken: delivery.LeaseToken,
			Result:     map[string]any{"status": "paid"},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- api.RunCompletionConsumer(ctx, completions, "engine-a") }()
	select {
	case deliveryID := <-completions.acked:
		if deliveryID != "completion-delivery-1" {
			t.Fatalf("acked delivery ID = %q", deliveryID)
		}
	case <-time.After(time.Second):
		t.Fatal("completion was not acknowledged")
	}
	cancel()
	if err := <-result; err != nil {
		t.Fatalf("completion consumer returned error: %v", err)
	}
	completed, err := executions.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if completed.Status != domain.WorkflowCompleted || completed.Tasks[delivery.Item.NodeID].Result["status"] != "paid" {
		t.Fatalf("workflow/task result = %s/%#v", completed.Status, completed.Tasks[delivery.Item.NodeID])
	}
}

func TestCompletionConsumerAcknowledgesStaleLeaseMessage(t *testing.T) {
	api, executions, _, _, _ := newTestGateway(t)
	if err := api.RegisterWorkflow(testDefinition()); err != nil {
		t.Fatal(err)
	}
	instance, err := api.StartWorkflow(context.Background(), "checkout", 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := api.ReceiveWork(context.Background(), "worker-a", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	completions := &testCompletionQueue{
		deliveries: make(chan queue.CompletionDelivery, 1),
		acked:      make(chan string, 1),
		rejected:   make(chan bool, 1),
	}
	completions.deliveries <- queue.CompletionDelivery{
		ID: "stale-delivery",
		Message: queue.CompletionMessage{
			MessageID:  "stale-completion",
			Kind:       queue.CompletionSucceeded,
			WorkflowID: instance.ID,
			TaskID:     delivery.Item.TaskID,
			NodeID:     delivery.Item.NodeID,
			LeaseID:    delivery.LeaseID,
			LeaseToken: delivery.LeaseToken + 1,
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- api.RunCompletionConsumer(ctx, completions, "engine-a") }()
	select {
	case <-completions.acked:
	case <-time.After(time.Second):
		t.Fatal("stale completion was not acknowledged")
	}
	cancel()
	if err := <-result; err != nil {
		t.Fatalf("completion consumer returned error: %v", err)
	}
	current, err := executions.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Tasks[delivery.Item.NodeID].Status != domain.TaskRunning {
		t.Fatalf("stale completion changed task status to %s", current.Tasks[delivery.Item.NodeID].Status)
	}
	select {
	case requeue := <-completions.rejected:
		t.Fatalf("stale completion was rejected with requeue=%t", requeue)
	default:
	}
}

func TestCompletionConsumerRequeuesUnexpectedProcessingError(t *testing.T) {
	api, _, _, _, _ := newTestGateway(t)
	completions := &testCompletionQueue{
		deliveries: make(chan queue.CompletionDelivery, 1),
		acked:      make(chan string, 1),
		rejected:   make(chan bool, 1),
	}
	completions.deliveries <- queue.CompletionDelivery{
		ID: "unknown-workflow-delivery",
		Message: queue.CompletionMessage{
			MessageID:  "unknown-workflow-completion",
			Kind:       queue.CompletionSucceeded,
			WorkflowID: "workflow-missing",
			TaskID:     "task-missing",
			NodeID:     "charge",
			LeaseID:    "lease-missing",
			LeaseToken: 1,
		},
	}
	if err := api.RunCompletionConsumer(context.Background(), completions, "engine-a"); err == nil {
		t.Fatal("consumer did not surface the unexpected processing error")
	}
	select {
	case requeue := <-completions.rejected:
		if !requeue {
			t.Fatal("unexpected processing error was rejected without requeue")
		}
	default:
		t.Fatal("unexpected processing error was not requeued")
	}
}
