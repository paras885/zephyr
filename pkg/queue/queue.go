package queue

import "context"

var (
	ErrClosed       = errorString("queue is closed")
	ErrNotFound     = errorString("delivery not found")
	ErrAlreadyAcked = errorString("delivery already acknowledged")
)

type errorString string

func (err errorString) Error() string { return string(err) }

type WorkItem struct {
	ID         string
	WorkflowID string
	TaskID     string
	NodeID     string
	TaskName   string
	Payload    map[string]any
	LeaseToken uint64
}

type Delivery struct {
	ID   string
	Item WorkItem
}

type QueueDAO interface {
	Publish(ctx context.Context, item WorkItem) error
	Receive(ctx context.Context, workerID string) (Delivery, error)
	Ack(ctx context.Context, deliveryID string) error
	Reject(ctx context.Context, deliveryID string, requeue bool) error
	Close() error
}

type QueueDepthProvider interface {
	QueueDepth(ctx context.Context) (int, error)
}
