package worker

import (
	"context"
	"testing"

	"github.com/zephyr-workflow/zephyr/pkg/gateway"
	"github.com/zephyr-workflow/zephyr/pkg/queue"
)

type completionControlPlane struct{}

func (completionControlPlane) Receive(context.Context, gateway.ReceiveWorkRequest) (gateway.WorkDelivery, error) {
	return gateway.WorkDelivery{}, nil
}

func (completionControlPlane) Heartbeat(context.Context, gateway.TaskHeartbeat) error { return nil }

type completionPublisher struct {
	messages []queue.CompletionMessage
}

func (publisher *completionPublisher) PublishCompletion(_ context.Context, message queue.CompletionMessage) error {
	publisher.messages = append(publisher.messages, message)
	return nil
}

func TestRabbitMQTransportPublishesCompletionAndFailure(t *testing.T) {
	publisher := &completionPublisher{}
	transport, err := NewRabbitMQTransport(completionControlPlane{}, publisher)
	if err != nil {
		t.Fatal(err)
	}
	if err := transport.Complete(context.Background(), gateway.TaskCompletion{
		WorkflowID: "workflow-1",
		TaskID:     "task-1",
		NodeID:     "charge",
		LeaseID:    "lease-1",
		LeaseToken: 7,
		Result:     map[string]any{"transaction_id": "tx-1"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := transport.Fail(context.Background(), gateway.TaskFailure{
		WorkflowID: "workflow-2",
		TaskID:     "task-2",
		NodeID:     "refund",
		LeaseID:    "lease-2",
		LeaseToken: 8,
		Error:      "refund declined",
	}); err != nil {
		t.Fatal(err)
	}
	if len(publisher.messages) != 2 {
		t.Fatalf("published messages = %d, want 2", len(publisher.messages))
	}
	completed, failed := publisher.messages[0], publisher.messages[1]
	if completed.Kind != queue.CompletionSucceeded || completed.WorkflowID != "workflow-1" || completed.TaskID != "task-1" || completed.LeaseID != "lease-1" || completed.LeaseToken != 7 || completed.Result["transaction_id"] != "tx-1" {
		t.Fatalf("completion message = %#v", completed)
	}
	if failed.Kind != queue.CompletionFailed || failed.WorkflowID != "workflow-2" || failed.TaskID != "task-2" || failed.LeaseID != "lease-2" || failed.LeaseToken != 8 || failed.Error != "refund declined" {
		t.Fatalf("failure message = %#v", failed)
	}
	if completed.MessageID == "" || failed.MessageID == "" || completed.MessageID == failed.MessageID {
		t.Fatalf("message IDs must be unique and non-empty: %q, %q", completed.MessageID, failed.MessageID)
	}
}
