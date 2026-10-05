package queue

import (
	"context"
	"fmt"
)

const (
	CompletionSucceeded = "completed"
	CompletionFailed    = "failed"
)

type CompletionMessage struct {
	MessageID  string         `json:"message_id"`
	Kind       string         `json:"kind"`
	WorkflowID string         `json:"workflow_id"`
	TaskID     string         `json:"task_id"`
	NodeID     string         `json:"node_id"`
	LeaseID    string         `json:"lease_id"`
	LeaseToken uint64         `json:"lease_token"`
	Result     map[string]any `json:"result,omitempty"`
	Error      string         `json:"error,omitempty"`
}

func (message CompletionMessage) Validate() error {
	if message.MessageID == "" || message.WorkflowID == "" || message.TaskID == "" || message.NodeID == "" || message.LeaseID == "" || message.LeaseToken == 0 {
		return fmt.Errorf("message ID, workflow ID, task ID, node ID, lease ID, and lease token are required")
	}
	switch message.Kind {
	case CompletionSucceeded:
		if message.Error != "" {
			return fmt.Errorf("successful completion cannot include an error")
		}
	case CompletionFailed:
		if message.Error == "" {
			return fmt.Errorf("failed completion requires an error")
		}
	default:
		return fmt.Errorf("unsupported completion kind %q", message.Kind)
	}
	return nil
}

type CompletionDelivery struct {
	ID      string
	Message CompletionMessage
}

type CompletionPublisher interface {
	PublishCompletion(ctx context.Context, message CompletionMessage) error
}

type CompletionQueue interface {
	CompletionPublisher
	ReceiveCompletion(ctx context.Context, consumerID string) (CompletionDelivery, error)
	AckCompletion(ctx context.Context, deliveryID string) error
	RejectCompletion(ctx context.Context, deliveryID string, requeue bool) error
	Close() error
}
