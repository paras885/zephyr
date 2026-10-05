package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/zephyr-workflow/zephyr/pkg/domain"
)

type rabbitQueueDelivery struct {
	message amqp.Delivery
	itemID  string
}

type rabbitActiveItem struct {
	deliveryID     string
	requeuePending bool
}

type RabbitMQQueue struct {
	channel       *amqp.Channel
	name          string
	confirmations <-chan amqp.Confirmation
	publishMu     sync.Mutex
	mu            sync.Mutex
	consumers     map[string]<-chan amqp.Delivery
	inFlight      map[string]rabbitQueueDelivery
	activeIDs     map[string]rabbitActiveItem
	completed     completedWorkIDs
}

func NewRabbitMQQueue(channel *amqp.Channel, name string) (*RabbitMQQueue, error) {
	if channel == nil {
		return nil, fmt.Errorf("RabbitMQ channel is required")
	}
	if name == "" {
		return nil, fmt.Errorf("queue name is required")
	}
	if _, err := channel.QueueDeclare(name, true, false, false, false, nil); err != nil {
		return nil, fmt.Errorf("declare RabbitMQ queue: %w", err)
	}
	if err := channel.Confirm(false); err != nil {
		return nil, fmt.Errorf("enable RabbitMQ publisher confirms: %w", err)
	}
	return &RabbitMQQueue{
		channel:       channel,
		name:          name,
		confirmations: channel.NotifyPublish(make(chan amqp.Confirmation, 1)),
		consumers:     make(map[string]<-chan amqp.Delivery),
		inFlight:      make(map[string]rabbitQueueDelivery),
		activeIDs:     make(map[string]rabbitActiveItem),
		completed:     newCompletedWorkIDs(completedWorkIDLimit),
	}, nil
}

func (queue *RabbitMQQueue) Publish(ctx context.Context, item WorkItem) error {
	if item.ID == "" {
		item.ID = domain.NewID("work")
	}
	body, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("marshal work item: %w", err)
	}
	queue.publishMu.Lock()
	defer queue.publishMu.Unlock()
	queue.mu.Lock()
	if _, exists := queue.activeIDs[item.ID]; exists || queue.completed.contains(item.ID) {
		queue.mu.Unlock()
		return nil
	}
	queue.activeIDs[item.ID] = rabbitActiveItem{}
	queue.mu.Unlock()

	if err := queue.channel.PublishWithContext(ctx, "", queue.name, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		MessageId:    item.ID,
		Body:         body,
	}); err != nil {
		queue.releasePublishReservation(item.ID)
		return fmt.Errorf("publish work item: %w", err)
	}
	select {
	case confirmation, open := <-queue.confirmations:
		if !open {
			queue.releasePublishReservation(item.ID)
			return ErrClosed
		}
		if !confirmation.Ack {
			queue.releasePublishReservation(item.ID)
			return fmt.Errorf("RabbitMQ negatively acknowledged work item %q", item.ID)
		}
	case <-ctx.Done():
		queue.releasePublishReservation(item.ID)
		return ctx.Err()
	}
	return nil
}

func (queue *RabbitMQQueue) releasePublishReservation(itemID string) {
	queue.mu.Lock()
	if active, exists := queue.activeIDs[itemID]; exists && active.deliveryID == "" && !active.requeuePending {
		delete(queue.activeIDs, itemID)
	}
	queue.mu.Unlock()
}

func (queue *RabbitMQQueue) Receive(ctx context.Context, workerID string) (Delivery, error) {
	if workerID == "" {
		return Delivery{}, fmt.Errorf("worker ID is required")
	}
	consumer, err := queue.consumer(workerID)
	if err != nil {
		return Delivery{}, err
	}
	for {
		select {
		case message, open := <-consumer:
			if !open {
				return Delivery{}, ErrClosed
			}
			var item WorkItem
			if err := json.Unmarshal(message.Body, &item); err != nil {
				_ = message.Reject(false)
				return Delivery{}, fmt.Errorf("unmarshal work item: %w", err)
			}
			deliveryID := rabbitDeliveryID(workerID, message.DeliveryTag)
			queue.mu.Lock()
			if item.ID != "" {
				if queue.completed.contains(item.ID) {
					queue.mu.Unlock()
					if err := message.Ack(false); err != nil {
						return Delivery{}, fmt.Errorf("ack completed RabbitMQ duplicate: %w", err)
					}
					continue
				}
				active, exists := queue.activeIDs[item.ID]
				if exists && active.deliveryID != "" && !(active.requeuePending && message.Redelivered) {
					queue.mu.Unlock()
					if err := message.Ack(false); err != nil {
						return Delivery{}, fmt.Errorf("ack duplicate RabbitMQ delivery: %w", err)
					}
					continue
				}
				queue.activeIDs[item.ID] = rabbitActiveItem{deliveryID: deliveryID}
			}
			queue.inFlight[deliveryID] = rabbitQueueDelivery{message: message, itemID: item.ID}
			queue.mu.Unlock()
			return Delivery{ID: deliveryID, Item: item}, nil
		case <-ctx.Done():
			return Delivery{}, ctx.Err()
		}
	}
}

func (queue *RabbitMQQueue) Ack(ctx context.Context, deliveryID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return queue.finish(ctx, deliveryID, false, false)
}

func (queue *RabbitMQQueue) Reject(ctx context.Context, deliveryID string, requeue bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return queue.finish(ctx, deliveryID, true, requeue)
}

func (queue *RabbitMQQueue) finish(ctx context.Context, deliveryID string, reject, requeue bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	queue.mu.Lock()
	delivery, ok := queue.inFlight[deliveryID]
	if !ok {
		queue.mu.Unlock()
		return ErrNotFound
	}
	var err error
	if reject {
		err = delivery.message.Reject(requeue)
	} else {
		err = delivery.message.Ack(false)
	}
	if err != nil {
		queue.mu.Unlock()
		if reject {
			return fmt.Errorf("reject RabbitMQ delivery: %w", err)
		}
		return fmt.Errorf("ack RabbitMQ delivery: %w", err)
	}
	delete(queue.inFlight, deliveryID)
	if delivery.itemID != "" {
		active, exists := queue.activeIDs[delivery.itemID]
		if exists && active.deliveryID == deliveryID {
			if reject && requeue {
				queue.activeIDs[delivery.itemID] = rabbitActiveItem{
					deliveryID:     deliveryID,
					requeuePending: true,
				}
			} else {
				delete(queue.activeIDs, delivery.itemID)
				queue.completed.add(delivery.itemID)
			}
		}
	}
	queue.mu.Unlock()
	return nil
}

func (queue *RabbitMQQueue) Close() error {
	return queue.channel.Close()
}

func (queue *RabbitMQQueue) consumer(workerID string) (<-chan amqp.Delivery, error) {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if consumer, ok := queue.consumers[workerID]; ok {
		return consumer, nil
	}
	consumer, err := queue.channel.Consume(queue.name, workerID, false, false, false, false, nil)
	if err != nil {
		return nil, fmt.Errorf("start RabbitMQ consumer: %w", err)
	}
	queue.consumers[workerID] = consumer
	return consumer, nil
}

func rabbitDeliveryID(workerID string, tag uint64) string {
	return workerID + ":" + strconv.FormatUint(tag, 10)
}

func parseRabbitDeliveryID(deliveryID string) (string, uint64, error) {
	parts := strings.Split(deliveryID, ":")
	if len(parts) != 2 {
		return "", 0, fmt.Errorf("invalid RabbitMQ delivery ID %q", deliveryID)
	}
	tag, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return "", 0, fmt.Errorf("invalid RabbitMQ delivery tag: %w", err)
	}
	return parts[0], tag, nil
}
