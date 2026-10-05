package queue

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	rabbitReconnectBase = 100 * time.Millisecond
	rabbitReconnectCap  = 5 * time.Second
)

// RabbitMQManager owns a supervised AMQP connection and reports readiness only
// when the connection and every registered adapter session are healthy.
type RabbitMQManager struct {
	url    string
	ctx    context.Context
	cancel context.CancelFunc

	mu         sync.Mutex
	connection *amqp.Connection
	changed    chan struct{}
	components map[*RabbitMQHealthComponent]struct{}
	closed     bool
	done       chan struct{}
}

// RabbitMQHealthComponent tracks the channel and consumer state of one adapter.
type RabbitMQHealthComponent struct {
	manager          *RabbitMQManager
	requiresConsumer bool
	mu               sync.RWMutex
	channelReady     bool
	consumerReady    bool
}

func NewRabbitMQManager(parent context.Context, url string) (*RabbitMQManager, error) {
	if parent == nil {
		return nil, fmt.Errorf("RabbitMQ manager context is required")
	}
	if url == "" {
		return nil, fmt.Errorf("RabbitMQ URL is required")
	}
	ctx, cancel := context.WithCancel(parent)
	manager := &RabbitMQManager{
		url:        url,
		ctx:        ctx,
		cancel:     cancel,
		changed:    make(chan struct{}),
		components: make(map[*RabbitMQHealthComponent]struct{}),
		done:       make(chan struct{}),
	}
	go manager.supervise()
	return manager, nil
}

func (manager *RabbitMQManager) supervise() {
	defer close(manager.done)
	backoff := rabbitReconnectBase
	for manager.ctx.Err() == nil {
		connection, err := amqp.Dial(manager.url)
		if err != nil {
			if !manager.waitBackoff(backoff) {
				return
			}
			backoff = nextRabbitBackoff(backoff)
			continue
		}
		backoff = rabbitReconnectBase
		closed := connection.NotifyClose(make(chan *amqp.Error, 1))
		manager.setConnection(connection)
		select {
		case <-manager.ctx.Done():
			_ = connection.Close()
			manager.setConnection(nil)
			return
		case <-closed:
			manager.setConnection(nil)
			_ = connection.Close()
		}
	}
}

func (manager *RabbitMQManager) setConnection(connection *amqp.Connection) {
	manager.mu.Lock()
	manager.connection = connection
	for component := range manager.components {
		component.SetChannelReady(false)
	}
	close(manager.changed)
	manager.changed = make(chan struct{})
	manager.mu.Unlock()
}

func (manager *RabbitMQManager) waitBackoff(backoff time.Duration) bool {
	jitter := time.Duration(rand.Int63n(int64(backoff/2) + 1))
	timer := time.NewTimer(backoff/2 + jitter)
	defer timer.Stop()
	select {
	case <-manager.ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func nextRabbitBackoff(backoff time.Duration) time.Duration {
	backoff *= 2
	if backoff > rabbitReconnectCap {
		return rabbitReconnectCap
	}
	return backoff
}

func (manager *RabbitMQManager) RegisterComponent(requiresConsumer bool) *RabbitMQHealthComponent {
	component := &RabbitMQHealthComponent{manager: manager, requiresConsumer: requiresConsumer}
	manager.mu.Lock()
	manager.components[component] = struct{}{}
	manager.mu.Unlock()
	return component
}

func (component *RabbitMQHealthComponent) SetChannelReady(ready bool) {
	component.mu.Lock()
	component.channelReady = ready
	component.mu.Unlock()
}

func (component *RabbitMQHealthComponent) SetConsumerReady(ready bool) {
	component.mu.Lock()
	component.consumerReady = ready
	component.mu.Unlock()
}

func (manager *RabbitMQManager) Ready() bool {
	manager.mu.Lock()
	connection := manager.connection
	closed := manager.closed
	components := make([]*RabbitMQHealthComponent, 0, len(manager.components))
	for component := range manager.components {
		components = append(components, component)
	}
	manager.mu.Unlock()
	if closed || connection == nil || connection.IsClosed() {
		return false
	}
	for _, component := range components {
		component.mu.RLock()
		ready := component.channelReady && (!component.requiresConsumer || component.consumerReady)
		component.mu.RUnlock()
		if !ready {
			return false
		}
	}
	return true
}

func (manager *RabbitMQManager) WaitConnection(ctx context.Context) (*amqp.Connection, error) {
	for {
		manager.mu.Lock()
		if manager.closed {
			manager.mu.Unlock()
			return nil, ErrClosed
		}
		connection := manager.connection
		changed := manager.changed
		manager.mu.Unlock()
		if connection != nil && !connection.IsClosed() {
			return connection, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-manager.ctx.Done():
			return nil, ErrClosed
		case <-changed:
		}
	}
}

func (manager *RabbitMQManager) ConnectionChanged() <-chan struct{} {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	return manager.changed
}

func (manager *RabbitMQManager) Context() context.Context { return manager.ctx }

func (manager *RabbitMQManager) Close() error {
	manager.mu.Lock()
	if manager.closed {
		manager.mu.Unlock()
		return nil
	}
	manager.closed = true
	connection := manager.connection
	manager.connection = nil
	close(manager.changed)
	manager.changed = make(chan struct{})
	manager.mu.Unlock()
	manager.cancel()
	if connection != nil {
		_ = connection.Close()
	}
	<-manager.done
	return nil
}
