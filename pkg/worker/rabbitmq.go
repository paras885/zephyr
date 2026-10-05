package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/zephyr-workflow/zephyr/pkg/gateway"
	"github.com/zephyr-workflow/zephyr/pkg/queue"
)

type ControlPlane interface {
	Receive(context.Context, gateway.ReceiveWorkRequest) (gateway.WorkDelivery, error)
	Heartbeat(context.Context, gateway.TaskHeartbeat) error
}

type RabbitMQTransport struct {
	controlPlane ControlPlane
	completions  queue.CompletionPublisher
}

func NewRabbitMQTransport(controlPlane ControlPlane, completions queue.CompletionPublisher) (*RabbitMQTransport, error) {
	if controlPlane == nil {
		return nil, fmt.Errorf("worker control-plane transport is required")
	}
	if completions == nil {
		return nil, fmt.Errorf("RabbitMQ completion publisher is required")
	}
	return &RabbitMQTransport{controlPlane: controlPlane, completions: completions}, nil
}

func (transport *RabbitMQTransport) Receive(ctx context.Context, request gateway.ReceiveWorkRequest) (gateway.WorkDelivery, error) {
	return transport.controlPlane.Receive(ctx, request)
}

func (transport *RabbitMQTransport) Heartbeat(ctx context.Context, heartbeat gateway.TaskHeartbeat) error {
	return transport.controlPlane.Heartbeat(ctx, heartbeat)
}

func (transport *RabbitMQTransport) Complete(ctx context.Context, completion gateway.TaskCompletion) error {
	messageID := completionMessageID(queue.CompletionSucceeded, completion.WorkflowID, completion.TaskID, completion.NodeID, completion.LeaseID, completion.LeaseToken)
	return transport.completions.PublishCompletion(ctx, queue.CompletionMessage{
		MessageID:  messageID,
		Kind:       queue.CompletionSucceeded,
		WorkflowID: completion.WorkflowID,
		TaskID:     completion.TaskID,
		NodeID:     completion.NodeID,
		LeaseID:    completion.LeaseID,
		LeaseToken: completion.LeaseToken,
		Result:     completion.Result,
	})
}

func (transport *RabbitMQTransport) Fail(ctx context.Context, failure gateway.TaskFailure) error {
	messageID := completionMessageID(queue.CompletionFailed, failure.WorkflowID, failure.TaskID, failure.NodeID, failure.LeaseID, failure.LeaseToken)
	return transport.completions.PublishCompletion(ctx, queue.CompletionMessage{
		MessageID:  messageID,
		Kind:       queue.CompletionFailed,
		WorkflowID: failure.WorkflowID,
		TaskID:     failure.TaskID,
		NodeID:     failure.NodeID,
		LeaseID:    failure.LeaseID,
		LeaseToken: failure.LeaseToken,
		Error:      failure.Error,
	})
}

func completionMessageID(kind, workflowID, taskID, nodeID, leaseID string, leaseToken uint64) string {
	identity, _ := json.Marshal(struct {
		Kind       string `json:"kind"`
		WorkflowID string `json:"workflow_id"`
		TaskID     string `json:"task_id"`
		NodeID     string `json:"node_id"`
		LeaseID    string `json:"lease_id"`
		LeaseToken uint64 `json:"lease_token"`
	}{kind, workflowID, taskID, nodeID, leaseID, leaseToken})
	digest := sha256.Sum256(identity)
	return "completion-" + hex.EncodeToString(digest[:])
}
