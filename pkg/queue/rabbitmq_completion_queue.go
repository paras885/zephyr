package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQCompletionQueue struct {
	channel       *amqp.Channel
	session       *rabbitSession
	name          string
	confirmations <-chan amqp.Confirmation
	publishMu     sync.Mutex
	mu            sync.Mutex
	consumers     map[string]<-chan amqp.Delivery
	inFlight      map[string]amqp.Delivery
	generation    uint64
}

func NewRabbitMQCompletionQueue(channel *amqp.Channel, name string) (*RabbitMQCompletionQueue, error) {
	if channel == nil {
		return nil, fmt.Errorf("RabbitMQ channel is required")
	}
	if name == "" {
		return nil, fmt.Errorf("completion queue name is required")
	}
	if err := declareRabbitTopology(channel, name); err != nil {
		return nil, fmt.Errorf("declare RabbitMQ completion topology: %w", err)
	}
	if err := channel.Qos(rabbitPrefetch, 0, false); err != nil {
		return nil, fmt.Errorf("set RabbitMQ completion prefetch: %w", err)
	}
	if err := channel.Confirm(false); err != nil {
		return nil, fmt.Errorf("enable RabbitMQ publisher confirms: %w", err)
	}
	return &RabbitMQCompletionQueue{
		channel:       channel,
		name:          name,
		confirmations: channel.NotifyPublish(make(chan amqp.Confirmation, 1)),
		consumers:     make(map[string]<-chan amqp.Delivery),
		inFlight:      make(map[string]amqp.Delivery),
	}, nil
}

func NewManagedRabbitMQCompletionQueue(manager *RabbitMQManager, name string) (*RabbitMQCompletionQueue, error) {
	if manager == nil {
		return nil, fmt.Errorf("RabbitMQ manager is required")
	}
	if name == "" {
		return nil, fmt.Errorf("completion queue name is required")
	}
	return &RabbitMQCompletionQueue{
		session:   newRabbitSession(manager, name, true),
		name:      name,
		consumers: make(map[string]<-chan amqp.Delivery),
		inFlight:  make(map[string]amqp.Delivery),
	}, nil
}

func (queue *RabbitMQCompletionQueue) PublishCompletion(ctx context.Context, message CompletionMessage) error {
	if err := message.Validate(); err != nil {
		return fmt.Errorf("validate completion message: %w", err)
	}
	queue.publishMu.Lock()
	defer queue.publishMu.Unlock()
	body, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("marshal completion message: %w", err)
	}
	publishing := amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		MessageId:    message.MessageID,
		Type:         message.Kind,
		Body:         body,
	}
	if queue.session != nil {
		return queue.session.publish(ctx, "", queue.name, publishing)
	}
	if err := queue.channel.PublishWithContext(ctx, "", queue.name, false, false, publishing); err != nil {
		return fmt.Errorf("publish completion message: %w", err)
	}
	select {
	case confirmation, open := <-queue.confirmations:
		if !open {
			return ErrClosed
		}
		if !confirmation.Ack {
			return fmt.Errorf("RabbitMQ negatively acknowledged completion message %q", message.MessageID)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (queue *RabbitMQCompletionQueue) ReceiveCompletion(ctx context.Context, consumerID string) (CompletionDelivery, error) {
	if consumerID == "" {
		return CompletionDelivery{}, fmt.Errorf("completion consumer ID is required")
	}
	if queue.session != nil {
		return queue.receiveManaged(ctx, consumerID)
	}
	consumer, err := queue.consumer(consumerID)
	if err != nil {
		return CompletionDelivery{}, err
	}
	select {
	case message, open := <-consumer:
		if !open {
			return CompletionDelivery{}, ErrClosed
		}
		var completion CompletionMessage
		if err := json.Unmarshal(message.Body, &completion); err != nil {
			_ = message.Reject(true)
			return CompletionDelivery{}, fmt.Errorf("unmarshal completion message: %w", err)
		}
		if err := completion.Validate(); err != nil {
			_ = message.Reject(true)
			return CompletionDelivery{}, fmt.Errorf("validate completion message: %w", err)
		}
		if message.MessageId != "" && message.MessageId != completion.MessageID {
			_ = message.Reject(true)
			return CompletionDelivery{}, fmt.Errorf("RabbitMQ message ID %q does not match completion message ID %q", message.MessageId, completion.MessageID)
		}
		deliveryID := rabbitDeliveryID(consumerID, message.DeliveryTag)
		queue.mu.Lock()
		queue.inFlight[deliveryID] = message
		queue.mu.Unlock()
		return CompletionDelivery{ID: deliveryID, Message: completion}, nil
	case <-ctx.Done():
		return CompletionDelivery{}, ctx.Err()
	}
}

func (queue *RabbitMQCompletionQueue) AckCompletion(ctx context.Context, deliveryID string) error {
	return queue.finish(ctx, deliveryID, false, false)
}

func (queue *RabbitMQCompletionQueue) RejectCompletion(ctx context.Context, deliveryID string, requeue bool) error {
	return queue.finish(ctx, deliveryID, true, requeue)
}

func (queue *RabbitMQCompletionQueue) finish(ctx context.Context, deliveryID string, reject, requeue bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	queue.mu.Lock()
	message, ok := queue.inFlight[deliveryID]
	queue.mu.Unlock()
	if !ok {
		return ErrNotFound
	}
	var err error
	if reject {
		err = message.Reject(requeue)
	} else {
		err = message.Ack(false)
	}
	if err != nil {
		if reject {
			return fmt.Errorf("reject RabbitMQ completion delivery: %w", err)
		}
		return fmt.Errorf("ack RabbitMQ completion delivery: %w", err)
	}
	queue.mu.Lock()
	delete(queue.inFlight, deliveryID)
	queue.mu.Unlock()
	return nil
}

func (queue *RabbitMQCompletionQueue) Close() error {
	if queue.session != nil {
		queue.session.close()
		return nil
	}
	return queue.channel.Close()
}

func (queue *RabbitMQCompletionQueue) receiveManaged(ctx context.Context, consumerID string) (CompletionDelivery, error) {
	for {
		consumer, generation, err := queue.session.consumer(ctx, consumerID)
		if err != nil {
			return CompletionDelivery{}, err
		}
		queue.syncGeneration(generation)
		_, _, changed, _ := queue.session.state()
		select {
		case message, open := <-consumer:
			if !open {
				if err := queue.session.waitForGeneration(ctx, generation); err != nil {
					return CompletionDelivery{}, err
				}
				continue
			}
			var completion CompletionMessage
			if err := json.Unmarshal(message.Body, &completion); err != nil {
				_ = message.Reject(true)
				return CompletionDelivery{}, fmt.Errorf("unmarshal completion message: %w", err)
			}
			if err := completion.Validate(); err != nil {
				_ = message.Reject(true)
				return CompletionDelivery{}, fmt.Errorf("validate completion message: %w", err)
			}
			if message.MessageId != "" && message.MessageId != completion.MessageID {
				_ = message.Reject(true)
				return CompletionDelivery{}, fmt.Errorf("RabbitMQ message ID %q does not match completion message ID %q", message.MessageId, completion.MessageID)
			}
			deliveryID := rabbitDeliveryID(consumerID, message.DeliveryTag)
			queue.mu.Lock()
			queue.inFlight[deliveryID] = message
			queue.mu.Unlock()
			return CompletionDelivery{ID: deliveryID, Message: completion}, nil
		case <-changed:
		case <-ctx.Done():
			return CompletionDelivery{}, ctx.Err()
		case <-queue.session.ctx.Done():
			return CompletionDelivery{}, ErrClosed
		}
	}
}

func (queue *RabbitMQCompletionQueue) syncGeneration(generation uint64) {
	queue.mu.Lock()
	if queue.generation != generation {
		queue.generation = generation
		queue.inFlight = make(map[string]amqp.Delivery)
	}
	queue.mu.Unlock()
}

func (queue *RabbitMQCompletionQueue) consumer(consumerID string) (<-chan amqp.Delivery, error) {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if consumer, ok := queue.consumers[consumerID]; ok {
		return consumer, nil
	}
	consumer, err := queue.channel.Consume(queue.name, consumerID, false, false, false, false, nil)
	if err != nil {
		return nil, fmt.Errorf("start RabbitMQ completion consumer: %w", err)
	}
	queue.consumers[consumerID] = consumer
	return consumer, nil
}
