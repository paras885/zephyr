package queue

import (
	"context"
	"sync"

	"github.com/zephyr-workflow/zephyr/pkg/domain"
)

type MemoryQueue struct {
	items     chan WorkItem
	done      chan struct{}
	mu        sync.Mutex
	inFlight  map[string]WorkItem
	activeIDs map[string]struct{}
	completed completedWorkIDs
	closed    bool
	close     sync.Once
}

func NewMemoryQueue(capacity int) *MemoryQueue {
	if capacity < 1 {
		capacity = 1
	}
	return &MemoryQueue{
		items:     make(chan WorkItem, capacity),
		done:      make(chan struct{}),
		inFlight:  make(map[string]WorkItem),
		activeIDs: make(map[string]struct{}),
		completed: newCompletedWorkIDs(completedWorkIDLimit),
	}
}

func (queue *MemoryQueue) Publish(ctx context.Context, item WorkItem) error {
	if item.ID == "" {
		item.ID = domain.NewID("work")
	}
	queue.mu.Lock()
	if queue.closed {
		queue.mu.Unlock()
		return ErrClosed
	}
	if _, exists := queue.activeIDs[item.ID]; exists {
		queue.mu.Unlock()
		return nil
	}
	if queue.completed.contains(item.ID) {
		queue.mu.Unlock()
		return nil
	}
	queue.activeIDs[item.ID] = struct{}{}
	queue.mu.Unlock()

	removeReservation := func() {
		queue.mu.Lock()
		delete(queue.activeIDs, item.ID)
		queue.mu.Unlock()
	}
	select {
	case queue.items <- cloneWorkItem(item):
		return nil
	case <-queue.done:
		removeReservation()
		return ErrClosed
	case <-ctx.Done():
		removeReservation()
		return ctx.Err()
	}
}

func (queue *MemoryQueue) Receive(ctx context.Context, workerID string) (Delivery, error) {
	if workerID == "" {
		return Delivery{}, errorString("worker ID is required")
	}
	select {
	case item, open := <-queue.items:
		if !open {
			return Delivery{}, ErrClosed
		}
		deliveryID := domain.NewID("delivery")
		queue.mu.Lock()
		if queue.closed {
			queue.mu.Unlock()
			return Delivery{}, ErrClosed
		}
		queue.inFlight[deliveryID] = item
		queue.mu.Unlock()
		return Delivery{ID: deliveryID, Item: cloneWorkItem(item)}, nil
	case <-queue.done:
		return Delivery{}, ErrClosed
	case <-ctx.Done():
		return Delivery{}, ctx.Err()
	}
}

func (queue *MemoryQueue) QueueDepth(ctx context.Context) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return len(queue.items), nil
}

func (queue *MemoryQueue) Ack(ctx context.Context, deliveryID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if _, ok := queue.inFlight[deliveryID]; !ok {
		return ErrNotFound
	}
	item := queue.inFlight[deliveryID]
	delete(queue.inFlight, deliveryID)
	delete(queue.activeIDs, item.ID)
	queue.completed.add(item.ID)
	return nil
}

func (queue *MemoryQueue) Reject(ctx context.Context, deliveryID string, requeue bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	queue.mu.Lock()
	item, ok := queue.inFlight[deliveryID]
	if !ok {
		queue.mu.Unlock()
		return ErrNotFound
	}
	delete(queue.inFlight, deliveryID)
	queue.mu.Unlock()
	if !requeue {
		queue.mu.Lock()
		delete(queue.activeIDs, item.ID)
		queue.completed.add(item.ID)
		queue.mu.Unlock()
		return nil
	}
	select {
	case queue.items <- cloneWorkItem(item):
		return nil
	case <-queue.done:
		queue.restoreInFlight(deliveryID, item)
		return ErrClosed
	case <-ctx.Done():
		queue.restoreInFlight(deliveryID, item)
		return ctx.Err()
	}
}

func (queue *MemoryQueue) restoreInFlight(deliveryID string, item WorkItem) {
	queue.mu.Lock()
	queue.inFlight[deliveryID] = item
	queue.activeIDs[item.ID] = struct{}{}
	queue.mu.Unlock()
}

func (queue *MemoryQueue) Close() error {
	queue.close.Do(func() {
		queue.mu.Lock()
		queue.closed = true
		close(queue.done)
		queue.mu.Unlock()
	})
	return nil
}

func cloneWorkItem(item WorkItem) WorkItem {
	clone := item
	if item.Payload != nil {
		clone.Payload = make(map[string]any, len(item.Payload))
		for key, value := range item.Payload {
			clone.Payload[key] = value
		}
	}
	return clone
}
