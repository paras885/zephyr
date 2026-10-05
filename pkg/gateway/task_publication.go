package gateway

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/zephyr-workflow/zephyr/pkg/queue"
	"github.com/zephyr-workflow/zephyr/pkg/store"
)

type TaskPublicationDispatcherConfig struct {
	BatchSize     int
	ClaimDuration time.Duration
	RetryDelay    time.Duration
	PollInterval  time.Duration
	OnError       func(error)
}

type TaskPublicationDispatcher struct {
	outbox store.TaskPublicationStore
	queue  queue.QueueDAO
	config TaskPublicationDispatcherConfig
}

func NewTaskPublicationDispatcher(outbox store.TaskPublicationStore, workQueue queue.QueueDAO, config TaskPublicationDispatcherConfig) (*TaskPublicationDispatcher, error) {
	if outbox == nil {
		return nil, fmt.Errorf("task publication outbox is required")
	}
	if workQueue == nil {
		return nil, fmt.Errorf("work queue is required")
	}
	if config.BatchSize < 0 || config.ClaimDuration < 0 || config.RetryDelay < 0 || config.PollInterval < 0 {
		return nil, fmt.Errorf("task publication dispatcher settings cannot be negative")
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
	return &TaskPublicationDispatcher{outbox: outbox, queue: workQueue, config: config}, nil
}

func (dispatcher *TaskPublicationDispatcher) DispatchOnce(ctx context.Context) (int, error) {
	if ctx == nil {
		return 0, fmt.Errorf("dispatch context is required")
	}
	records, err := dispatcher.outbox.ClaimTaskPublications(ctx, dispatcher.config.BatchSize, dispatcher.config.ClaimDuration)
	if err != nil {
		return 0, fmt.Errorf("claim task publication outbox: %w", err)
	}
	published := 0
	var failures []error
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return published, err
		}
		if err := dispatcher.queue.Publish(ctx, record.Item); err != nil {
			if retryErr := dispatcher.outbox.RetryTaskPublication(ctx, record, dispatcher.config.RetryDelay); retryErr != nil {
				failures = append(failures, errors.Join(fmt.Errorf("publish task %s: %w", record.Item.TaskID, err), fmt.Errorf("release task publication for retry: %w", retryErr)))
				continue
			}
			failures = append(failures, fmt.Errorf("publish task %s: %w", record.Item.TaskID, err))
			continue
		}
		if err := dispatcher.outbox.MarkTaskPublished(ctx, record); err != nil {
			failures = append(failures, fmt.Errorf("mark task %s published: %w", record.Item.TaskID, err))
			continue
		}
		published++
	}
	return published, errors.Join(failures...)
}

func (dispatcher *TaskPublicationDispatcher) Run(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("dispatcher context is required")
	}
	ticker := time.NewTicker(dispatcher.config.PollInterval)
	defer ticker.Stop()
	for {
		published, err := dispatcher.DispatchOnce(ctx)
		if err != nil && ctx.Err() == nil && dispatcher.config.OnError != nil {
			dispatcher.config.OnError(err)
		}
		if published > 0 {
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
