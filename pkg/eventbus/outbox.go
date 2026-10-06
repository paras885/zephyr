package eventbus

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/zephyr-workflow/zephyr/pkg/store"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type OutboxDispatcherConfig struct {
	BatchSize     int
	ClaimDuration time.Duration
	RetryDelay    time.Duration
	PollInterval  time.Duration
	OnError       func(error)
}

type OutboxDispatcher struct {
	outbox    store.OutboxStore
	publisher Publisher
	config    OutboxDispatcherConfig
}

func NewOutboxDispatcher(outbox store.OutboxStore, publisher Publisher, config OutboxDispatcherConfig) (*OutboxDispatcher, error) {
	if outbox == nil {
		return nil, fmt.Errorf("durable event outbox is required")
	}
	if publisher == nil {
		return nil, fmt.Errorf("event publisher is required")
	}
	if config.BatchSize < 0 || config.ClaimDuration < 0 || config.RetryDelay < 0 || config.PollInterval < 0 {
		return nil, fmt.Errorf("outbox dispatcher settings cannot be negative")
	}
	if config.BatchSize == 0 {
		config.BatchSize = 64
	}
	if config.ClaimDuration == 0 {
		config.ClaimDuration = 30 * time.Second
	}
	if config.RetryDelay == 0 {
		config.RetryDelay = time.Second
	}
	if config.PollInterval == 0 {
		config.PollInterval = 250 * time.Millisecond
	}
	return &OutboxDispatcher{outbox: outbox, publisher: publisher, config: config}, nil
}

func (dispatcher *OutboxDispatcher) DispatchOnce(ctx context.Context) (int, error) {
	if ctx == nil {
		return 0, fmt.Errorf("dispatch context is required")
	}
	records, err := dispatcher.outbox.ClaimOutbox(ctx, dispatcher.config.BatchSize, dispatcher.config.ClaimDuration)
	if err != nil {
		return 0, fmt.Errorf("claim workflow event outbox: %w", err)
	}
	delivered := 0
	var failures []error
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return delivered, err
		}
		publishContext, span := otel.Tracer("zephyr/eventbus").Start(ctx, "event.publish", trace.WithAttributes(
			attribute.String("workflow.id", record.Event.WorkflowID),
			attribute.Int64("event.sequence", int64(record.Event.Sequence)),
			attribute.String("event.type", string(record.Event.Type)),
		))
		if err := dispatcher.publisher.Publish(publishContext, record.Event); err != nil {
			slog.WarnContext(publishContext, "workflow event outbox publish failed", "workflow_id", record.Event.WorkflowID, "event_sequence", record.Event.Sequence, "event_type", record.Event.Type, "error", err)
			span.RecordError(err)
			span.SetStatus(codes.Error, "event publish failed")
			if retryErr := dispatcher.outbox.RetryOutbox(ctx, record, dispatcher.config.RetryDelay); retryErr != nil {
				failures = append(failures, errors.Join(fmt.Errorf("publish workflow event %s:%d: %w", record.Event.WorkflowID, record.Event.Sequence, err), fmt.Errorf("release event for retry: %w", retryErr)))
				span.RecordError(retryErr)
				span.End()
				continue
			}
			failures = append(failures, fmt.Errorf("publish workflow event %s:%d: %w", record.Event.WorkflowID, record.Event.Sequence, err))
			span.End()
			continue
		}
		if err := dispatcher.outbox.MarkOutboxDelivered(ctx, record); err != nil {
			failures = append(failures, fmt.Errorf("mark workflow event %s:%d delivered: %w", record.Event.WorkflowID, record.Event.Sequence, err))
			span.RecordError(err)
			span.SetStatus(codes.Error, "mark event delivered failed")
			span.End()
			continue
		}
		span.End()
		delivered++
	}
	return delivered, errors.Join(failures...)
}

func (dispatcher *OutboxDispatcher) Run(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("dispatcher context is required")
	}
	ticker := time.NewTicker(dispatcher.config.PollInterval)
	defer ticker.Stop()
	for {
		delivered, err := dispatcher.DispatchOnce(ctx)
		if err != nil && ctx.Err() == nil && dispatcher.config.OnError != nil {
			dispatcher.config.OnError(err)
		}
		if delivered > 0 {
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
