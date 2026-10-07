package queue

import (
	"context"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const rabbitPrefetch = 16

type rabbitSession struct {
	manager         *RabbitMQManager
	component       *RabbitMQHealthComponent
	queueName       string
	requireConsumer bool
	ctx             context.Context
	cancel          context.CancelFunc
	done            chan struct{}

	mu               sync.Mutex
	publishMu        sync.Mutex
	channel          *amqp.Channel
	confirms         <-chan amqp.Confirmation
	consumers        map[string]<-chan amqp.Delivery
	desiredConsumers map[string]struct{}
	changed          chan struct{}
	generation       uint64
}

func newRabbitSession(manager *RabbitMQManager, queueName string, requireConsumer bool) *rabbitSession {
	ctx, cancel := context.WithCancel(manager.Context())
	session := &rabbitSession{
		manager:          manager,
		component:        manager.RegisterComponent(requireConsumer),
		queueName:        queueName,
		requireConsumer:  requireConsumer,
		ctx:              ctx,
		cancel:           cancel,
		done:             make(chan struct{}),
		consumers:        make(map[string]<-chan amqp.Delivery),
		desiredConsumers: make(map[string]struct{}),
		changed:          make(chan struct{}),
	}
	go session.supervise()
	return session
}

func (session *rabbitSession) supervise() {
	defer close(session.done)
	backoff := rabbitReconnectBase
	for session.ctx.Err() == nil {
		connection, err := session.manager.WaitConnection(session.ctx)
		if err != nil {
			return
		}
		channel, consumers, err := session.setupChannel(connection)
		if err != nil {
			if channel != nil {
				_ = channel.Close()
			}
			if !waitRabbitRetry(session.ctx, backoff) {
				return
			}
			backoff = nextRabbitBackoff(backoff)
			continue
		}
		backoff = rabbitReconnectBase
		session.runSession(channel, consumers)
	}
}

// setupChannel opens and configures a channel with topology, confirms, QoS, and
// consumers for every desired consumer ID. The caller must close the returned
// channel (even on error, where it may be partially configured) when done.
func (session *rabbitSession) setupChannel(connection *amqp.Connection) (*amqp.Channel, map[string]<-chan amqp.Delivery, error) {
	channel, err := connection.Channel()
	if err == nil {
		err = declareRabbitTopology(channel, session.queueName)
	}
	if err == nil {
		err = channel.Confirm(false)
	}
	if err == nil {
		err = channel.Qos(rabbitPrefetch, 0, false)
	}
	// Establish consumers while holding session.mu so a concurrent consumer()
	// call cannot write to session.consumers while install() replaces the map.
	// channel.Consume does not acquire session.mu, so this is safe.
	consumers := make(map[string]<-chan amqp.Delivery)
	if err == nil {
		session.mu.Lock()
		for consumerID := range session.desiredConsumers {
			consumer, consumeErr := channel.Consume(session.queueName, consumerID, false, false, false, false, nil)
			if consumeErr != nil {
				err = consumeErr
				break
			}
			consumers[consumerID] = consumer
		}
		session.mu.Unlock()
	}
	return channel, consumers, err
}

// runSession installs the live channel and blocks until it closes, the manager's
// connection changes, or the session context is cancelled, then tears it down.
func (session *rabbitSession) runSession(channel *amqp.Channel, consumers map[string]<-chan amqp.Delivery) {
	confirmations := channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	closed := channel.NotifyClose(make(chan *amqp.Error, 1))
	session.install(channel, confirmations, consumers)
	managerChanged := session.manager.ConnectionChanged()
	select {
	case <-session.ctx.Done():
		_ = channel.Close()
		return
	case <-closed:
	case <-managerChanged:
	}
	session.remove(channel)
	_ = channel.Close()
}

func declareRabbitTopology(channel *amqp.Channel, queueName string) error {
	dlxName := queueName + ".dlx"
	dlqName := queueName + ".dlq"
	routingKey := queueName + ".dead"
	if err := channel.ExchangeDeclare(dlxName, amqp.ExchangeDirect, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare RabbitMQ dead-letter exchange: %w", err)
	}
	dlqArguments := amqp.Table{"x-queue-type": "quorum"}
	if _, err := channel.QueueDeclare(dlqName, true, false, false, false, dlqArguments); err != nil {
		return fmt.Errorf("declare RabbitMQ dead-letter queue: %w", err)
	}
	if err := channel.QueueBind(dlqName, routingKey, dlxName, false, nil); err != nil {
		return fmt.Errorf("bind RabbitMQ dead-letter queue: %w", err)
	}
	arguments := amqp.Table{
		"x-queue-type":              "quorum",
		"x-dead-letter-exchange":    dlxName,
		"x-dead-letter-routing-key": routingKey,
		"x-delivery-limit":          int32(5),
	}
	if _, err := channel.QueueDeclare(queueName, true, false, false, false, arguments); err != nil {
		return fmt.Errorf("declare RabbitMQ quorum queue: %w", err)
	}
	return nil
}

func waitRabbitRetry(ctx context.Context, backoff time.Duration) bool {
	timer := time.NewTimer(backoff / 2)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (session *rabbitSession) install(channel *amqp.Channel, confirmations <-chan amqp.Confirmation, consumers map[string]<-chan amqp.Delivery) {
	session.mu.Lock()
	// Merge any consumers that a concurrent consumer() call established on the
	// new channel while supervise() was setting up; both operate under session.mu
	// on the same channel generation, so a union is correct and avoids losing one.
	for consumerID, consumer := range session.consumers {
		if _, present := consumers[consumerID]; !present {
			consumers[consumerID] = consumer
		}
	}
	session.channel = channel
	session.confirms = confirmations
	session.consumers = consumers
	session.generation++
	close(session.changed)
	session.changed = make(chan struct{})
	consumerCount := len(session.consumers)
	session.mu.Unlock()
	session.component.SetChannelReady(true)
	session.component.SetConsumerReady(!session.requireConsumer || consumerCount > 0)
}

func (session *rabbitSession) remove(channel *amqp.Channel) {
	session.mu.Lock()
	if session.channel == channel {
		session.channel = nil
		session.confirms = nil
		session.consumers = make(map[string]<-chan amqp.Delivery)
		session.generation++
		close(session.changed)
		session.changed = make(chan struct{})
	}
	session.mu.Unlock()
	session.component.SetChannelReady(false)
	session.component.SetConsumerReady(false)
}

func (session *rabbitSession) consumer(ctx context.Context, consumerID string) (<-chan amqp.Delivery, uint64, error) {
	for {
		session.mu.Lock()
		session.desiredConsumers[consumerID] = struct{}{}
		if consumer, ok := session.consumers[consumerID]; ok {
			generation := session.generation
			session.mu.Unlock()
			return consumer, generation, nil
		}
		channel, changed := session.channel, session.changed
		if channel != nil {
			consumer, err := channel.Consume(session.queueName, consumerID, false, false, false, false, nil)
			if err == nil {
				session.consumers[consumerID] = consumer
				generation := session.generation
				session.mu.Unlock()
				session.component.SetConsumerReady(true)
				return consumer, generation, nil
			}
		}
		session.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, 0, ctx.Err()
		case <-session.ctx.Done():
			return nil, 0, ErrClosed
		case <-changed:
		}
	}
}

func (session *rabbitSession) waitForGeneration(ctx context.Context, generation uint64) error {
	for {
		session.mu.Lock()
		if session.generation != generation {
			session.mu.Unlock()
			return nil
		}
		changed := session.changed
		session.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-session.ctx.Done():
			return ErrClosed
		case <-changed:
		}
	}
}

func (session *rabbitSession) state() (*amqp.Channel, <-chan amqp.Confirmation, <-chan struct{}, uint64) {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.channel, session.confirms, session.changed, session.generation
}

func (session *rabbitSession) queueDepth(ctx context.Context) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	channel, _, _, _ := session.state()
	if channel == nil || channel.IsClosed() {
		return 0, fmt.Errorf("RabbitMQ queue %q channel is unavailable", session.queueName)
	}
	info, err := channel.QueueInspect(session.queueName)
	if err != nil {
		return 0, fmt.Errorf("inspect RabbitMQ queue %q depth: %w", session.queueName, err)
	}
	return info.Messages, nil
}

func (session *rabbitSession) publish(ctx context.Context, exchange, routingKey string, publishing amqp.Publishing) error {
	session.publishMu.Lock()
	defer session.publishMu.Unlock()
	for {
		channel, confirmations, changed, _ := session.state()
		if channel == nil {
			if err := waitSessionChange(ctx, session.manager.Context(), changed); err != nil {
				return err
			}
			continue
		}
		if err := channel.PublishWithContext(ctx, exchange, routingKey, false, false, publishing); err != nil {
			if !channel.IsClosed() {
				return err
			}
			if err := waitSessionChange(ctx, session.manager.Context(), changed); err != nil {
				return err
			}
			continue
		}
		select {
		case confirmation, open := <-confirmations:
			if open {
				if confirmation.Ack {
					return nil
				}
				return fmt.Errorf("RabbitMQ negatively acknowledged publication %q", publishing.MessageId)
			}
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		case <-session.manager.Context().Done():
			return ErrClosed
		}
	}
}

func waitSessionChange(ctx, managerContext context.Context, changed <-chan struct{}) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-managerContext.Done():
		return ErrClosed
	case <-changed:
		return nil
	}
}

func (session *rabbitSession) close() {
	session.cancel()
	session.mu.Lock()
	channel := session.channel
	session.channel = nil
	close(session.changed)
	session.changed = make(chan struct{})
	session.mu.Unlock()
	if channel != nil {
		_ = channel.Close()
	}
	session.component.SetChannelReady(false)
	session.component.SetConsumerReady(false)
	<-session.done
}
