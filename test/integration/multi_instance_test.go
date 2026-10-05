package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/zephyr-workflow/zephyr/pkg/client"
	"github.com/zephyr-workflow/zephyr/pkg/decider"
	"github.com/zephyr-workflow/zephyr/pkg/domain"
	"github.com/zephyr-workflow/zephyr/pkg/dsl/compiler"
	"github.com/zephyr-workflow/zephyr/pkg/gateway"
	"github.com/zephyr-workflow/zephyr/pkg/lease"
	"github.com/zephyr-workflow/zephyr/pkg/queue"
	"github.com/zephyr-workflow/zephyr/pkg/store"
	"github.com/zephyr-workflow/zephyr/pkg/timer"
	"github.com/zephyr-workflow/zephyr/pkg/worker"
)

type completionAckSignal struct {
	queue.CompletionQueue
	acked chan struct{}
}

func (signal *completionAckSignal) AckCompletion(ctx context.Context, deliveryID string) error {
	if err := signal.CompletionQueue.AckCompletion(ctx, deliveryID); err != nil {
		return err
	}
	select {
	case signal.acked <- struct{}{}:
	default:
	}
	return nil
}

func TestTwoGatewayInstancesSharePostgresLeasesAndRabbitMQWork(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	postgresContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: "postgres:16-alpine", Env: map[string]string{
				"POSTGRES_USER": "zephyr", "POSTGRES_PASSWORD": "zephyr-test", "POSTGRES_DB": "zephyr",
			}, ExposedPorts: []string{"5432/tcp"}, WaitingFor: wait.ForListeningPort("5432/tcp"),
		}, Started: true,
	})
	if err != nil {
		t.Fatalf("start PostgreSQL Testcontainer: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if err := postgresContainer.Terminate(cleanupCtx); err != nil {
			t.Errorf("terminate PostgreSQL Testcontainer: %v", err)
		}
	})
	postgresHost, err := postgresContainer.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	postgresPort, err := postgresContainer.MappedPort(ctx, "5432/tcp")
	if err != nil {
		t.Fatal(err)
	}
	postgresURL := fmt.Sprintf("postgres://zephyr:zephyr-test@%s:%s/zephyr?sslmode=disable", postgresHost, postgresPort.Port())
	queryDB, err := sql.Open("pgx", postgresURL)
	if err != nil {
		t.Fatal(err)
	}
	defer queryDB.Close()
	storeA, err := store.OpenPostgresStore(ctx, postgresURL)
	if err != nil {
		t.Fatal(err)
	}
	defer storeA.Close()
	if err := storeA.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	storeB, err := store.OpenPostgresStore(ctx, postgresURL)
	if err != nil {
		t.Fatal(err)
	}
	defer storeB.Close()

	rabbitContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: "rabbitmq:3.13-management-alpine", ExposedPorts: []string{"5672/tcp"},
			WaitingFor: wait.ForListeningPort("5672/tcp"),
		}, Started: true,
	})
	if err != nil {
		t.Fatalf("start RabbitMQ Testcontainer: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if err := rabbitContainer.Terminate(cleanupCtx); err != nil {
			t.Errorf("terminate RabbitMQ Testcontainer: %v", err)
		}
	})
	amqpURL, err := rabbitContainer.Endpoint(ctx, "amqp")
	if err != nil {
		t.Fatal(err)
	}
	queueSuffix := domain.NewID("multi-instance")
	workQueueName := "zephyr-work-" + queueSuffix
	completionQueueName := "zephyr-completion-" + queueSuffix

	connectionA, err := amqp.Dial(amqpURL)
	if err != nil {
		t.Fatal(err)
	}
	defer connectionA.Close()
	connectionB, err := amqp.Dial(amqpURL)
	if err != nil {
		t.Fatal(err)
	}
	defer connectionB.Close()
	workerConnection, err := amqp.Dial(amqpURL)
	if err != nil {
		t.Fatal(err)
	}
	defer workerConnection.Close()

	workChannelA, err := connectionA.Channel()
	if err != nil {
		t.Fatal(err)
	}
	workQueueA, err := queue.NewRabbitMQQueue(workChannelA, workQueueName)
	if err != nil {
		_ = workChannelA.Close()
		t.Fatal(err)
	}
	defer workQueueA.Close()
	workChannelB, err := connectionB.Channel()
	if err != nil {
		t.Fatal(err)
	}
	workQueueB, err := queue.NewRabbitMQQueue(workChannelB, workQueueName)
	if err != nil {
		_ = workChannelB.Close()
		t.Fatal(err)
	}
	defer workQueueB.Close()
	completionChannelB, err := connectionB.Channel()
	if err != nil {
		t.Fatal(err)
	}
	completionQueueB, err := queue.NewRabbitMQCompletionQueue(completionChannelB, completionQueueName)
	if err != nil {
		_ = completionChannelB.Close()
		t.Fatal(err)
	}
	defer completionQueueB.Close()
	workerCompletionChannel, err := workerConnection.Channel()
	if err != nil {
		t.Fatal(err)
	}
	workerCompletions, err := queue.NewRabbitMQCompletionQueue(workerCompletionChannel, completionQueueName)
	if err != nil {
		_ = workerCompletionChannel.Close()
		t.Fatal(err)
	}
	defer workerCompletions.Close()

	timersA := timer.NewService(32)
	timersB := timer.NewService(32)
	defer timersA.Close()
	defer timersB.Close()
	engineA := decider.NewWithTimer(storeA, timersA)
	engineB := decider.NewWithTimer(storeB, timersB)
	defer engineA.Close()
	defer engineB.Close()
	leasesA, err := lease.NewManagerWithStateStore(workQueueA, timersA, 32, storeA)
	if err != nil {
		t.Fatal(err)
	}
	defer leasesA.Close()
	leasesB, err := lease.NewManagerWithStateStore(workQueueB, timersB, 32, storeB)
	if err != nil {
		t.Fatal(err)
	}
	defer leasesB.Close()
	apiA, err := gateway.New(engineA, storeA, workQueueA, leasesA)
	if err != nil {
		t.Fatal(err)
	}
	apiB, err := gateway.New(engineB, storeB, workQueueB, leasesB)
	if err != nil {
		t.Fatal(err)
	}
	definition, err := compiler.Compile(`type Input { value: string }
type TaskInput { value: string }
type TaskOutput { value: string }
type Output { value: string }
task Echo(input: TaskInput) -> TaskOutput { }
workflow EchoFlow(input: Input) -> Output {
    step echo = Echo({ value: input.value });
    return Output { value: echo.result.value };
}`)
	if err != nil {
		t.Fatal(err)
	}
	if err := apiA.RegisterWorkflow(definition); err != nil {
		t.Fatal(err)
	}
	if err := apiB.RegisterWorkflow(definition); err != nil {
		t.Fatal(err)
	}
	serverA := httptest.NewServer(apiA.Handler())
	defer serverA.Close()
	serverB := httptest.NewServer(apiB.Handler())
	defer serverB.Close()
	clientA, err := client.New(client.Config{Endpoint: serverA.URL, HTTPClient: serverA.Client()})
	if err != nil {
		t.Fatal(err)
	}
	run, err := clientA.StartWorkflow(ctx, "EchoFlow", 1, map[string]any{"value": "shared"})
	if err != nil {
		t.Fatal(err)
	}
	transportA, err := worker.NewHTTPTransport(serverA.URL, serverA.Client())
	if err != nil {
		t.Fatal(err)
	}
	transportB, err := worker.NewHTTPTransport(serverB.URL, serverB.Client())
	if err != nil {
		t.Fatal(err)
	}
	workerClient, err := worker.NewClient(transportA, "multi-instance-worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := workerClient.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if delivery.Item.Payload["value"] != "shared" {
		t.Fatalf("received work payload = %#v", delivery.Item.Payload)
	}
	if err := transportB.Heartbeat(ctx, gateway.TaskHeartbeat{
		LeaseID: delivery.LeaseID, LeaseToken: delivery.LeaseToken, LeaseDurationMS: int64((time.Minute).Milliseconds()),
	}); err != nil {
		t.Fatalf("heartbeat through second server instance: %v", err)
	}
	consumerContext, cancelConsumer := context.WithCancel(ctx)
	consumerDone := make(chan error, 1)
	completionAcked := &completionAckSignal{CompletionQueue: completionQueueB, acked: make(chan struct{}, 1)}
	go func() { consumerDone <- apiB.RunCompletionConsumer(consumerContext, completionAcked, "engine-b") }()
	defer func() {
		cancelConsumer()
		if err := <-consumerDone; err != nil {
			t.Errorf("completion consumer: %v", err)
		}
	}()
	if err := workerCompletions.PublishCompletion(ctx, queue.CompletionMessage{
		MessageID: domain.NewID("completion"), Kind: queue.CompletionSucceeded,
		WorkflowID: run.ID, TaskID: delivery.Item.TaskID, NodeID: delivery.Item.NodeID,
		LeaseID: delivery.LeaseID, LeaseToken: delivery.LeaseToken,
		Result: map[string]any{"value": "completed-by-other-replica"},
	}); err != nil {
		t.Fatalf("publish worker completion: %v", err)
	}
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		instance, err := storeB.Get(run.ID)
		if err != nil {
			t.Fatal(err)
		}
		if instance.Status == domain.WorkflowCompleted {
			if instance.Result["value"] != "completed-by-other-replica" {
				t.Fatalf("completed workflow result = %#v", instance.Result)
			}
			select {
			case <-completionAcked.acked:
			case <-deadline.C:
				t.Fatal("completion consumer did not acknowledge the RabbitMQ message")
			}
			break
		}
		select {
		case <-deadline.C:
			t.Fatalf("workflow did not complete through second instance; latest state=%s", instance.Status)
		case <-ticker.C:
		}
	}
}
