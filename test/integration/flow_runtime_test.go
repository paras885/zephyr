package integration_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/zephyr-workflow/zephyr/pkg/client"
	"github.com/zephyr-workflow/zephyr/pkg/decider"
	"github.com/zephyr-workflow/zephyr/pkg/domain"
	"github.com/zephyr-workflow/zephyr/pkg/dsl/compiler"
	"github.com/zephyr-workflow/zephyr/pkg/gateway"
	"github.com/zephyr-workflow/zephyr/pkg/lease"
	"github.com/zephyr-workflow/zephyr/pkg/queue"
	"github.com/zephyr-workflow/zephyr/pkg/store"
	"github.com/zephyr-workflow/zephyr/pkg/timer"
	"github.com/zephyr-workflow/zephyr/pkg/worker"
)

func TestConditionalWorkflowBranchesThroughHTTPWorker(t *testing.T) {
	definition, err := compiler.Compile(`type Input { amount: int }
type PaymentInput { amount: int }
type Output { status: string }
task Authorize(input: PaymentInput) -> Output { }
task Approve(input: PaymentInput) -> Output { }
task Reject(input: PaymentInput) -> Output { }
workflow Review(input: Input) -> Output {
    step authorize = Authorize({ amount: input.amount });
    if (authorize.result.approved == true && input.amount >= 100 || input.amount == 0) {
        step approve = Approve({ amount: input.amount });
		return Output { status: "approved" };
    } else {
        step reject = Reject({ amount: input.amount });
		return Output { status: "rejected" };
    }
}`)
	if err != nil {
		t.Fatal(err)
	}
	for _, testCase := range []struct {
		name         string
		amount       int
		approved     bool
		wantTaskName string
	}{
		{name: "approve branch", amount: 120, approved: true, wantTaskName: "Approve"},
		{name: "reject branch", amount: 40, approved: true, wantTaskName: "Reject"},
		{name: "logical or branch", amount: 0, approved: false, wantTaskName: "Approve"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			api, executions, server, workerClient := newHTTPWorkflowWorker(t, definition)
			_ = api
			workflowClient, err := client.New(client.Config{Endpoint: server.URL, HTTPClient: server.Client()})
			if err != nil {
				t.Fatal(err)
			}
			start, err := workflowClient.StartWorkflow(context.Background(), "Review", 1, map[string]any{"amount": testCase.amount})
			if err != nil {
				t.Fatal(err)
			}
			authorize, err := workerClient.Receive(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if authorize.Item.TaskName != "Authorize" || authorize.Item.Payload["amount"] != float64(testCase.amount) {
				t.Fatalf("authorize delivery = %#v", authorize.Item)
			}
			if err := workerClient.Complete(context.Background(), gateway.TaskCompletion{
				WorkflowID: start.ID, TaskID: authorize.Item.TaskID, NodeID: authorize.Item.NodeID,
				LeaseID: authorize.LeaseID, LeaseToken: authorize.LeaseToken,
				Result: map[string]any{"approved": testCase.approved},
			}); err != nil {
				t.Fatal(err)
			}
			branch, err := workerClient.Receive(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if branch.Item.TaskName != testCase.wantTaskName {
				t.Fatalf("selected task = %q, want %q", branch.Item.TaskName, testCase.wantTaskName)
			}
			if err := workerClient.Complete(context.Background(), gateway.TaskCompletion{
				WorkflowID: start.ID, TaskID: branch.Item.TaskID, NodeID: branch.Item.NodeID,
				LeaseID: branch.LeaseID, LeaseToken: branch.LeaseToken,
				Result: map[string]any{"status": "done"},
			}); err != nil {
				t.Fatal(err)
			}
			completed, err := executions.Get(start.ID)
			if err != nil {
				t.Fatal(err)
			}
			if completed.Status != domain.WorkflowCompleted {
				t.Fatalf("workflow status = %s", completed.Status)
			}
		})
	}
}

func TestBoundedFanOutThroughHTTPWorker(t *testing.T) {
	definition, err := compiler.Compile(`type Input { ids: list[int] }
type ItemInput { id: int }
type Output { status: string }
task Process(input: ItemInput) -> Output { }
workflow ProcessAll(input: Input) -> Output {
    fan_out item in input.ids concurrency 2 { step process = Process({ id: item }); }
	return Output { status: "processed" };
}`)
	if err != nil {
		t.Fatal(err)
	}
	_, executions, server, workerClient := newHTTPWorkflowWorker(t, definition)
	workflowClient, err := client.New(client.Config{Endpoint: server.URL, HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	start, err := workflowClient.StartWorkflow(context.Background(), "ProcessAll", 1, map[string]any{"ids": []any{1, 2, 3}})
	if err != nil {
		t.Fatal(err)
	}
	first, err := workerClient.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	second, err := workerClient.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(first.Item.Payload["id"]) != "1" || fmt.Sprint(second.Item.Payload["id"]) != "2" {
		t.Fatalf("first bounded fan-out deliveries = %#v/%#v", first.Item.Payload, second.Item.Payload)
	}
	if err := workerClient.Complete(context.Background(), gateway.TaskCompletion{
		WorkflowID: start.ID, TaskID: first.Item.TaskID, NodeID: first.Item.NodeID,
		LeaseID: first.LeaseID, LeaseToken: first.LeaseToken,
		Result: map[string]any{"done": true},
	}); err != nil {
		t.Fatal(err)
	}
	third, err := workerClient.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(third.Item.Payload["id"]) != "3" {
		t.Fatalf("third fan-out delivery = %#v", third.Item.Payload)
	}
	for _, delivery := range []gateway.WorkDelivery{second, third} {
		if err := workerClient.Complete(context.Background(), gateway.TaskCompletion{
			WorkflowID: start.ID, TaskID: delivery.Item.TaskID, NodeID: delivery.Item.NodeID,
			LeaseID: delivery.LeaseID, LeaseToken: delivery.LeaseToken,
			Result: map[string]any{"done": true},
		}); err != nil {
			t.Fatal(err)
		}
	}
	completed, err := executions.Get(start.ID)
	if err != nil {
		t.Fatal(err)
	}
	if completed.Status != domain.WorkflowCompleted || len(completed.Tasks) != 3 {
		t.Fatalf("fan-out final status/tasks = %s/%d", completed.Status, len(completed.Tasks))
	}
}

func TestSagaCompensationThroughHTTPWorker(t *testing.T) {
	definition, err := compiler.Compile(`type Input { order_id: string }
type PaymentInput { order_id: string }
type RefundInput { transaction_id: string }
type Output { status: string }
task Charge(input: PaymentInput) -> Output { }
task Review(input: PaymentInput) -> Output { }
task Refund(input: RefundInput) -> Output { }
workflow Payment(input: Input) -> Output {
    step charge = Charge({ order_id: input.order_id }) compensate with Refund({ transaction_id: charge.result.transaction_id });
    step review = Review({ order_id: input.order_id });
	return Output { status: review.result.status };
}`)
	if err != nil {
		t.Fatal(err)
	}
	api, executions, server, workerClient := newHTTPWorkflowWorker(t, definition)
	workflowClient, err := client.New(client.Config{Endpoint: server.URL, HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	start, err := workflowClient.StartWorkflow(context.Background(), "Payment", 1, map[string]any{"order_id": "order-saga"})
	if err != nil {
		t.Fatal(err)
	}
	charge, err := workerClient.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := workerClient.Complete(context.Background(), gateway.TaskCompletion{
		WorkflowID: start.ID, TaskID: charge.Item.TaskID, NodeID: charge.Item.NodeID,
		LeaseID: charge.LeaseID, LeaseToken: charge.LeaseToken,
		Result: map[string]any{"transaction_id": "tx-saga"},
	}); err != nil {
		t.Fatal(err)
	}
	review, err := workerClient.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := workerClient.Fail(context.Background(), gateway.TaskFailure{
		WorkflowID: start.ID, TaskID: review.Item.TaskID, NodeID: review.Item.NodeID,
		LeaseID: review.LeaseID, LeaseToken: review.LeaseToken, Error: "risk rejected",
	}); err != nil {
		t.Fatal(err)
	}
	refund, err := workerClient.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if refund.Item.TaskName != "Refund" || refund.Item.Payload["transaction_id"] != "tx-saga" {
		t.Fatalf("refund delivery = %#v", refund.Item)
	}
	if err := workerClient.Complete(context.Background(), gateway.TaskCompletion{
		WorkflowID: start.ID, TaskID: refund.Item.TaskID, NodeID: refund.Item.NodeID,
		LeaseID: refund.LeaseID, LeaseToken: refund.LeaseToken, Result: map[string]any{"refunded": true},
	}); err != nil {
		t.Fatal(err)
	}
	completed, err := executions.Get(start.ID)
	if err != nil {
		t.Fatal(err)
	}
	if completed.Status != domain.WorkflowCompensated {
		t.Fatalf("saga terminal status = %s, want COMPENSATED", completed.Status)
	}
	_ = api
}

func newHTTPWorkflowWorker(t *testing.T, definition domain.WorkflowDef) (*gateway.Gateway, *store.MemoryStore, *httptest.Server, *worker.Client) {
	t.Helper()
	workQueue := queue.NewMemoryQueue(16)
	timers := timer.NewService(16)
	leases, err := lease.NewManager(workQueue, timers, 16)
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
	server := httptest.NewServer(api.Handler())
	transport, err := worker.NewHTTPTransport(server.URL, server.Client())
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	workerClient, err := worker.NewClient(transport, "workflow-flow-worker", time.Minute)
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		server.Close()
		leases.Close()
		engine.Close()
		timers.Close()
		_ = workQueue.Close()
	})
	return api, executions, server, workerClient
}
