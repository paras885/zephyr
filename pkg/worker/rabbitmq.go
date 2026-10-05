package worker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
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
	messageID, err := newCompletionMessageID()
	if err != nil {
		return err
	}
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
	messageID, err := newCompletionMessageID()
	if err != nil {
		return err
	}
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

func newCompletionMessageID() (string, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", fmt.Errorf("generate completion message ID: %w", err)
	}
	return "completion-" + hex.EncodeToString(random[:]), nil
}
