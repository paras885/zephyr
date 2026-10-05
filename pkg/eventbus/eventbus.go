package eventbus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/zephyr-workflow/zephyr/pkg/domain"
)

var ErrClosed = errors.New("event bus is closed")

type Publisher interface {
	Publish(ctx context.Context, event domain.Event) error
}

type Memory struct {
	mu             sync.RWMutex
	subscribers    map[uint64]*Subscription
	nextSubscriber uint64
	closed         bool
	done           chan struct{}
}

type Subscription struct {
	events chan domain.Event
	done   chan struct{}
	close  sync.Once
	remove func()
}

func NewMemory() *Memory {
	return &Memory{subscribers: make(map[uint64]*Subscription), done: make(chan struct{})}
}

func (bus *Memory) Subscribe(buffer int) (*Subscription, error) {
	if buffer < 0 {
		return nil, fmt.Errorf("subscription buffer cannot be negative")
	}
	bus.mu.Lock()
	defer bus.mu.Unlock()
	if bus.closed {
		return nil, ErrClosed
	}
	bus.nextSubscriber++
	id := bus.nextSubscriber
	subscription := &Subscription{events: make(chan domain.Event, buffer), done: make(chan struct{})}
	subscription.remove = func() {
		bus.mu.Lock()
		delete(bus.subscribers, id)
		bus.mu.Unlock()
	}
	bus.subscribers[id] = subscription
	return subscription, nil
}

func (bus *Memory) Publish(ctx context.Context, event domain.Event) error {
	if ctx == nil {
		return fmt.Errorf("publish context is required")
	}
	if event.WorkflowID == "" || event.Sequence == 0 {
		return fmt.Errorf("workflow ID and persisted event sequence are required")
	}
	bus.mu.RLock()
	if bus.closed {
		bus.mu.RUnlock()
		return ErrClosed
	}
	subscribers := make([]*Subscription, 0, len(bus.subscribers))
	for _, subscription := range bus.subscribers {
		subscribers = append(subscribers, subscription)
	}
	bus.mu.RUnlock()
	for _, subscription := range subscribers {
		copy, err := cloneEvent(event)
		if err != nil {
			return fmt.Errorf("copy workflow event: %w", err)
		}
		select {
		case subscription.events <- copy:
		case <-subscription.done:
		case <-bus.done:
			return ErrClosed
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (bus *Memory) Close() {
	bus.mu.Lock()
	if bus.closed {
		bus.mu.Unlock()
		return
	}
	bus.closed = true
	close(bus.done)
	subscribers := make([]*Subscription, 0, len(bus.subscribers))
	for _, subscription := range bus.subscribers {
		subscribers = append(subscribers, subscription)
	}
	bus.subscribers = make(map[uint64]*Subscription)
	bus.mu.Unlock()
	for _, subscription := range subscribers {
		subscription.stop()
	}
}

func (subscription *Subscription) Events() <-chan domain.Event { return subscription.events }

func (subscription *Subscription) Done() <-chan struct{} { return subscription.done }

func (subscription *Subscription) Close() {
	subscription.stop()
	if subscription.remove != nil {
		subscription.remove()
	}
}

func (subscription *Subscription) stop() {
	subscription.close.Do(func() { close(subscription.done) })
}

func cloneEvent(event domain.Event) (domain.Event, error) {
	encoded, err := json.Marshal(event)
	if err != nil {
		return domain.Event{}, err
	}
	var copy domain.Event
	if err := json.Unmarshal(encoded, &copy); err != nil {
		return domain.Event{}, err
	}
	return copy, nil
}

var _ Publisher = (*Memory)(nil)
