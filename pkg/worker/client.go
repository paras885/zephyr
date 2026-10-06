package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/zephyr-workflow/zephyr/pkg/gateway"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type Transport interface {
	Receive(context.Context, gateway.ReceiveWorkRequest) (gateway.WorkDelivery, error)
	Heartbeat(context.Context, gateway.TaskHeartbeat) error
	Complete(context.Context, gateway.TaskCompletion) error
	Fail(context.Context, gateway.TaskFailure) error
}

type Client struct {
	transport     Transport
	workerID      string
	leaseDuration time.Duration
}

type HeartbeatLoop struct {
	cancel context.CancelFunc
	done   chan struct{}

	mu  sync.Mutex
	err error
}

type Task[T any] struct {
	Delivery gateway.WorkDelivery
	Payload  T
}

func NewClient(transport Transport, workerID string, leaseDuration time.Duration) (*Client, error) {
	if transport == nil {
		return nil, fmt.Errorf("worker transport is required")
	}
	if workerID == "" {
		return nil, fmt.Errorf("worker ID is required")
	}
	if leaseDuration <= 0 {
		return nil, fmt.Errorf("lease duration must be positive")
	}
	return &Client{transport: transport, workerID: workerID, leaseDuration: leaseDuration}, nil
}

func (client *Client) Receive(ctx context.Context) (gateway.WorkDelivery, error) {
	ctx, span := otel.Tracer("zephyr/worker").Start(ctx, "worker.receive", trace.WithAttributes(attribute.String("worker.id", client.workerID)))
	defer span.End()
	delivery, err := client.transport.Receive(ctx, gateway.ReceiveWorkRequest{
		WorkerID:        client.workerID,
		LeaseDurationMS: client.leaseDuration.Milliseconds(),
	})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "worker receive failed")
		return gateway.WorkDelivery{}, err
	}
	span.SetAttributes(
		attribute.String("workflow.id", delivery.Item.WorkflowID), attribute.String("task.id", delivery.Item.TaskID),
		attribute.String("lease.id", delivery.LeaseID), attribute.Int64("lease.fencing_token", int64(delivery.LeaseToken)),
	)
	return delivery, nil
}

func Decode[T any](delivery gateway.WorkDelivery) (Task[T], error) {
	var payload T
	encoded, err := json.Marshal(delivery.Item.Payload)
	if err != nil {
		return Task[T]{}, fmt.Errorf("marshal task payload: %w", err)
	}
	if err := json.Unmarshal(encoded, &payload); err != nil {
		return Task[T]{}, fmt.Errorf("decode task payload: %w", err)
	}
	return Task[T]{Delivery: delivery, Payload: payload}, nil
}

func (client *Client) Heartbeat(ctx context.Context, delivery gateway.WorkDelivery, duration time.Duration) error {
	ctx, span := otel.Tracer("zephyr/worker").Start(ctx, "worker.heartbeat", trace.WithAttributes(
		attribute.String("workflow.id", delivery.Item.WorkflowID), attribute.String("task.id", delivery.Item.TaskID),
		attribute.String("lease.id", delivery.LeaseID), attribute.Int64("lease.fencing_token", int64(delivery.LeaseToken)),
	))
	defer span.End()
	if duration <= 0 {
		return fmt.Errorf("lease duration must be positive")
	}
	err := client.transport.Heartbeat(ctx, gateway.TaskHeartbeat{
		LeaseID:         delivery.LeaseID,
		LeaseToken:      delivery.LeaseToken,
		LeaseDurationMS: duration.Milliseconds(),
	})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "worker heartbeat failed")
	}
	return err
}

func (client *Client) StartHeartbeat(ctx context.Context, delivery gateway.WorkDelivery, interval, duration time.Duration) (*HeartbeatLoop, error) {
	if ctx == nil {
		return nil, fmt.Errorf("heartbeat context is required")
	}
	if interval <= 0 {
		return nil, fmt.Errorf("heartbeat interval must be positive")
	}
	if duration <= 0 {
		return nil, fmt.Errorf("lease duration must be positive")
	}
	loopContext, cancel := context.WithCancel(ctx)
	loop := &HeartbeatLoop{cancel: cancel, done: make(chan struct{})}
	go loop.run(loopContext, client, delivery, interval, duration)
	return loop, nil
}

func (loop *HeartbeatLoop) run(ctx context.Context, client *Client, delivery gateway.WorkDelivery, interval, duration time.Duration) {
	defer close(loop.done)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := client.Heartbeat(ctx, delivery, duration); err != nil {
				if ctx.Err() != nil {
					return
				}
				loop.setError(err)
				loop.cancel()
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (loop *HeartbeatLoop) Stop() error {
	loop.cancel()
	<-loop.done
	return loop.Err()
}

func (loop *HeartbeatLoop) Wait() error {
	<-loop.done
	return loop.Err()
}

func (loop *HeartbeatLoop) Err() error {
	loop.mu.Lock()
	defer loop.mu.Unlock()
	return loop.err
}

func (loop *HeartbeatLoop) setError(err error) {
	if err == nil {
		return
	}
	loop.mu.Lock()
	defer loop.mu.Unlock()
	if loop.err == nil {
		loop.err = err
	}
}

func (client *Client) Complete(ctx context.Context, completion gateway.TaskCompletion) error {
	ctx, span := otel.Tracer("zephyr/worker").Start(ctx, "worker.complete", trace.WithAttributes(
		attribute.String("workflow.id", completion.WorkflowID), attribute.String("task.id", completion.TaskID),
		attribute.String("lease.id", completion.LeaseID), attribute.Int64("lease.fencing_token", int64(completion.LeaseToken)),
	))
	defer span.End()
	err := client.transport.Complete(ctx, completion)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "worker completion failed")
	}
	return err
}

func (client *Client) Fail(ctx context.Context, failure gateway.TaskFailure) error {
	ctx, span := otel.Tracer("zephyr/worker").Start(ctx, "worker.fail", trace.WithAttributes(
		attribute.String("workflow.id", failure.WorkflowID), attribute.String("task.id", failure.TaskID),
		attribute.String("lease.id", failure.LeaseID), attribute.Int64("lease.fencing_token", int64(failure.LeaseToken)),
	))
	defer span.End()
	err := client.transport.Fail(ctx, failure)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "worker failure report failed")
	}
	return err
}

func Completion[T any](task Task[T], result map[string]any) gateway.TaskCompletion {
	return gateway.TaskCompletion{
		WorkflowID: task.Delivery.Item.WorkflowID,
		TaskID:     task.Delivery.Item.TaskID,
		NodeID:     task.Delivery.Item.NodeID,
		LeaseID:    task.Delivery.LeaseID,
		LeaseToken: task.Delivery.LeaseToken,
		Result:     result,
	}
}

func Failure[T any](task Task[T], err error) gateway.TaskFailure {
	message := "task failed"
	if err != nil {
		message = err.Error()
	}
	return gateway.TaskFailure{
		WorkflowID: task.Delivery.Item.WorkflowID,
		TaskID:     task.Delivery.Item.TaskID,
		NodeID:     task.Delivery.Item.NodeID,
		LeaseID:    task.Delivery.LeaseID,
		LeaseToken: task.Delivery.LeaseToken,
		Error:      message,
	}
}
