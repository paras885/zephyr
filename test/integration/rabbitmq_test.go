package integration_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/zephyr-workflow/zephyr/pkg/decider"
	"github.com/zephyr-workflow/zephyr/pkg/domain"
	"github.com/zephyr-workflow/zephyr/pkg/gateway"
	"github.com/zephyr-workflow/zephyr/pkg/lease"
	"github.com/zephyr-workflow/zephyr/pkg/queue"
	"github.com/zephyr-workflow/zephyr/pkg/store"
	"github.com/zephyr-workflow/zephyr/pkg/timer"
	"github.com/zephyr-workflow/zephyr/pkg/worker"
)

func TestRabbitMQQueueEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "rabbitmq:3.13-management-alpine",
			ExposedPorts: []string{"5672/tcp"},
			WaitingFor:   wait.ForListeningPort("5672/tcp"),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start RabbitMQ Testcontainer: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if err := container.Terminate(cleanupCtx); err != nil {
			t.Errorf("terminate RabbitMQ Testcontainer: %v", err)
		}
	})

	endpoint, err := container.Endpoint(ctx, "amqp")
	if err != nil {
		t.Fatalf("get RabbitMQ endpoint: %v", err)
	}
	connection, err := amqp.Dial(endpoint)
	if err != nil {
		t.Fatalf("connect to RabbitMQ: %v", err)
	}
	defer connection.Close()
	duplicateChannel, err := connection.Channel()
	if err != nil {
		t.Fatalf("open duplicate publisher channel: %v", err)
	}
	defer duplicateChannel.Close()
	channel, err := connection.Channel()
	if err != nil {
		t.Fatalf("open RabbitMQ channel: %v", err)
	}
	defer channel.Close()

	queueName := "zephyr-integration-" + time.Now().UTC().Format("20060102150405.000000000")
	workQueue, err := queue.NewRabbitMQQueue(channel, queueName)
	if err != nil {
		t.Fatal(err)
	}
	defer workQueue.Close()

	item := queue.WorkItem{ID: "work-rabbit-1", WorkflowID: "workflow-1", TaskID: "task-1", NodeID: "charge", TaskName: "ChargePayment"}
	if err := workQueue.Publish(ctx, item); err != nil {
		t.Fatal(err)
	}
	first, err := workQueue.Receive(ctx, "worker-a")
	if err != nil {
		t.Fatal(err)
	}
	if first.Item.ID != item.ID || first.Item.WorkflowID != item.WorkflowID {
		t.Fatalf("first delivery = %#v, want %#v", first.Item, item)
	}
	duplicateBody, err := json.Marshal(item)
	if err != nil {
		t.Fatal(err)
	}
	if err := duplicateChannel.PublishWithContext(ctx, "", queueName, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        duplicateBody,
	}); err != nil {
		t.Fatalf("publish duplicate broker message: %v", err)
	}
	duplicateCtx, cancelDuplicate := context.WithTimeout(ctx, 50*time.Millisecond)
	_, duplicateErr := workQueue.Receive(duplicateCtx, "worker-a")
	cancelDuplicate()
	if duplicateErr != context.DeadlineExceeded {
		t.Fatalf("Receive() after duplicate broker message = %v, want deadline exceeded", duplicateErr)
	}
	if err := workQueue.Reject(ctx, first.ID, true); err != nil {
		t.Fatal(err)
	}

	second, err := workQueue.Receive(ctx, "worker-a")
	if err != nil {
		t.Fatal(err)
	}
	if second.Item.ID != item.ID {
		t.Fatalf("redelivered item ID = %q, want %q", second.Item.ID, item.ID)
	}
	if err := workQueue.Ack(ctx, second.ID); err != nil {
		t.Fatal(err)
	}
	if err := duplicateChannel.PublishWithContext(ctx, "", queueName, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        duplicateBody,
	}); err != nil {
		t.Fatalf("publish post-ack duplicate broker message: %v", err)
	}
	duplicateAfterAckCtx, cancelDuplicateAfterAck := context.WithTimeout(ctx, 50*time.Millisecond)
	_, duplicateAfterAckErr := workQueue.Receive(duplicateAfterAckCtx, "worker-a")
	cancelDuplicateAfterAck()
	if duplicateAfterAckErr != context.DeadlineExceeded {
		t.Fatalf("Receive() after publishing an acknowledged task ID = %v, want deadline exceeded", duplicateAfterAckErr)
	}

	engineCompletionChannel, err := connection.Channel()
	if err != nil {
		t.Fatalf("open engine completion channel: %v", err)
	}
	defer engineCompletionChannel.Close()
	completionName := queueName + "-completions"
	engineCompletions, err := queue.NewRabbitMQCompletionQueue(engineCompletionChannel, completionName)
	if err != nil {
		t.Fatal(err)
	}
	defer engineCompletions.Close()
	workerCompletionChannel, err := connection.Channel()
	if err != nil {
		t.Fatalf("open worker completion channel: %v", err)
	}
	defer workerCompletionChannel.Close()
	workerCompletions, err := queue.NewRabbitMQCompletionQueue(workerCompletionChannel, completionName)
	if err != nil {
		t.Fatal(err)
	}
	redelivery := queue.CompletionMessage{
		MessageID:  "completion-redelivery-test",
		Kind:       queue.CompletionSucceeded,
		WorkflowID: "workflow-redelivery",
		TaskID:     "task-redelivery",
		NodeID:     "charge",
		LeaseID:    "lease-redelivery",
		LeaseToken: 1,
	}
	if err := workerCompletions.PublishCompletion(ctx, redelivery); err != nil {
		t.Fatal(err)
	}
	firstCompletion, err := engineCompletions.ReceiveCompletion(ctx, "engine-rabbit")
	if err != nil {
		t.Fatal(err)
	}
	if err := engineCompletions.RejectCompletion(ctx, firstCompletion.ID, true); err != nil {
		t.Fatal(err)
	}
	secondCompletion, err := engineCompletions.ReceiveCompletion(ctx, "engine-rabbit")
	if err != nil {
		t.Fatal(err)
	}
	if secondCompletion.Message.MessageID != redelivery.MessageID {
		t.Fatalf("redelivered completion ID = %q, want %q", secondCompletion.Message.MessageID, redelivery.MessageID)
	}
	if err := engineCompletions.AckCompletion(ctx, secondCompletion.ID); err != nil {
		t.Fatal(err)
	}

	timers := timer.NewService(2)
	leases, err := lease.NewManager(workQueue, timers, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		leases.Close()
		timers.Close()
	}()
	executions := store.NewMemoryStore()
	api, err := gateway.New(decider.New(executions), executions, workQueue, leases)
	if err != nil {
		t.Fatal(err)
	}
	definition := domain.WorkflowDef{
		Name:    "rabbitmq-completion",
		Version: 1,
		Nodes: map[string]domain.NodeDefinition{
			"charge": {ID: "charge", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "ChargePayment"}},
		},
	}
	if err := api.RegisterWorkflow(definition); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(api.Handler())
	defer server.Close()
	controlPlane, err := worker.NewHTTPTransport(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	workerTransport, err := worker.NewRabbitMQTransport(controlPlane, workerCompletions)
	if err != nil {
		t.Fatal(err)
	}
	workerClient, err := worker.NewClient(workerTransport, "worker-a", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	instance, err := api.StartWorkflow(ctx, definition.Name, definition.Version, map[string]any{"order_id": "order-rabbit"})
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := workerClient.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	task, err := worker.Decode[map[string]any](delivery)
	if err != nil {
		t.Fatal(err)
	}
	if err := workerClient.Heartbeat(ctx, delivery, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	consumerCtx, cancelConsumer := context.WithCancel(ctx)
	consumerDone := make(chan error, 1)
	go func() { consumerDone <- api.RunCompletionConsumer(consumerCtx, engineCompletions, "engine-rabbit") }()
	if err := workerClient.Complete(ctx, worker.Completion(task, map[string]any{"transaction_id": "tx-rabbit"})); err != nil {
		t.Fatal(err)
	}
	completed := time.NewTicker(10 * time.Millisecond)
	defer completed.Stop()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case <-completed.C:
			current, getErr := executions.Get(instance.ID)
			if getErr != nil {
				t.Fatal(getErr)
			}
			if current.Status == domain.WorkflowCompleted {
				if current.Tasks[delivery.Item.NodeID].Result["transaction_id"] != "tx-rabbit" {
					t.Fatalf("completion result = %#v", current.Tasks[delivery.Item.NodeID].Result)
				}
				cancelConsumer()
				if err := <-consumerDone; err != nil {
					t.Fatalf("completion consumer failed: %v", err)
				}
				return
			}
		case <-deadline.C:
			cancelConsumer()
			t.Fatal("RabbitMQ completion was not applied before deadline")
		}
	}
}
