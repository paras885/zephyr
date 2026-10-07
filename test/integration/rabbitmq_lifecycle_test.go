package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"path"
	"testing"
	"time"

	dockercontainer "github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
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
)

const (
	rabbitTestUser     = "zephyr"
	rabbitTestPassword = "zephyr-test"
)

func TestRabbitMQPublishRecoversAcrossBrokerRestartDuringConfirm(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	container, amqpURL, _ := startRabbitLifecycleContainer(t)
	manager, workQueue := newManagedWorkQueue(t, ctx, amqpURL)
	waitFor(t, ctx, "initial queue session", manager.Ready)
	stopRabbitContainer(t, ctx, container)
	waitFor(t, ctx, "broker loss reflected in readiness", func() bool { return !manager.Ready() })

	item := queue.WorkItem{ID: "stable-task-restart", WorkflowID: "workflow-restart", TaskID: "stable-task-restart", NodeID: "charge", TaskName: "Charge"}
	published := make(chan error, 1)
	go func() { published <- workQueue.Publish(ctx, item) }()
	select {
	case err := <-published:
		t.Fatalf("publish completed while RabbitMQ was stopped: %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	startRabbitContainer(t, ctx, container, amqpURL)
	select {
	case err := <-published:
		if err != nil {
			t.Fatalf("publish after broker restart: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("publish did not recover after RabbitMQ restarted")
	}

	delivery, err := workQueue.Receive(ctx, "restart-worker")
	if err != nil {
		t.Fatalf("receive recovered task: %v", err)
	}
	if delivery.Item.ID != item.ID {
		t.Fatalf("received task ID = %q, want %q", delivery.Item.ID, item.ID)
	}
	if err := workQueue.Ack(ctx, delivery.ID); err != nil {
		t.Fatalf("ack recovered task: %v", err)
	}
	shortContext, stopShort := context.WithTimeout(ctx, 200*time.Millisecond)
	defer stopShort()
	if _, err := workQueue.Receive(shortContext, "restart-worker"); err != context.DeadlineExceeded {
		t.Fatalf("second receive = %v, want no duplicate delivery", err)
	}
}

func TestRabbitMQReadinessRecoversAfterBrokerRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	container, amqpURL, _ := startRabbitLifecycleContainer(t)
	manager, workQueue := newManagedWorkQueue(t, ctx, amqpURL)
	waitFor(t, ctx, "ready RabbitMQ session", manager.Ready)
	stopRabbitContainer(t, ctx, container)
	waitFor(t, ctx, "unready broker outage", func() bool { return !manager.Ready() })
	startRabbitContainer(t, ctx, container, amqpURL)
	waitFor(t, ctx, "readiness recovery", manager.Ready)
	if err := workQueue.Publish(ctx, queue.WorkItem{ID: "ready-after-restart", TaskName: "ReadyCheck"}); err != nil {
		t.Fatalf("publish after readiness recovery: %v", err)
	}
}

func TestRabbitMQCompletionPersistsBeforeAckAcrossBrokerRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	container, amqpURL, _ := startRabbitLifecycleContainer(t)
	manager, workQueue := newManagedWorkQueue(t, ctx, amqpURL)
	completionQueue, err := queue.NewManagedRabbitMQCompletionQueue(manager, "completion-ack-"+domain.NewID("test"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = completionQueue.Close() })
	api, executions, instance, delivery := newDurableWorkflow(t, ctx, workQueue)
	ackQueue := &gatedCompletionAckQueue{CompletionQueue: completionQueue, reached: make(chan struct{}), release: make(chan struct{})}
	consumerDone, cancelConsumer := startCompletionConsumer(ctx, api, ackQueue, "engine-ack-restart")
	completion := completionFor(instance.ID, delivery)
	if err := completionQueue.PublishCompletion(ctx, completion); err != nil {
		t.Fatalf("publish completion: %v", err)
	}
	select {
	case <-ackQueue.reached:
	case <-ctx.Done():
		t.Fatal("completion did not reach the ack boundary")
	}
	current, err := executions.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Status != domain.WorkflowCompleted {
		t.Fatalf("workflow state before ack = %s, want completed", current.Status)
	}
	stopRabbitContainer(t, ctx, container)
	waitFor(t, ctx, "completion broker outage", func() bool { return !manager.Ready() })
	close(ackQueue.release)
	select {
	case <-consumerDone:
	case <-ctx.Done():
		t.Fatal("consumer did not return after the broker dropped before ack")
	}
	startRabbitContainer(t, ctx, container, amqpURL)
	waitFor(t, ctx, "completion consumer re-armed", manager.Ready)
	consumerDone, cancelConsumer = startCompletionConsumer(ctx, api, completionQueue, "engine-ack-restart")
	waitFor(t, ctx, "redelivered completion acknowledged", func() bool {
		latest, getErr := executions.Get(instance.ID)
		return getErr == nil && latest.Status == domain.WorkflowCompleted
	})
	cancelConsumer()
	stopCompletionConsumer(t, consumerDone)
	assertSingleTaskCompletion(t, ctx, executions, instance.ID)
}

func TestRabbitMQDuplicateCompletionCausesOneWorkflowTransition(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	_, amqpURL, _ := startRabbitLifecycleContainer(t)
	manager, workQueue := newManagedWorkQueue(t, ctx, amqpURL)
	completionQueue, err := queue.NewManagedRabbitMQCompletionQueue(manager, "completion-duplicate-"+domain.NewID("test"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = completionQueue.Close() })
	api, executions, instance, delivery := newDurableWorkflow(t, ctx, workQueue)
	acked := &countingCompletionAckQueue{CompletionQueue: completionQueue, acked: make(chan struct{}, 2)}
	consumerDone, cancelConsumer := startCompletionConsumer(ctx, api, acked, "engine-duplicate")
	completion := completionFor(instance.ID, delivery)
	if err := completionQueue.PublishCompletion(ctx, completion); err != nil {
		t.Fatal(err)
	}
	if err := completionQueue.PublishCompletion(ctx, completion); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		select {
		case <-acked.acked:
		case <-ctx.Done():
			t.Fatal("both completion deliveries were not acknowledged")
		}
	}
	cancelConsumer()
	stopCompletionConsumer(t, consumerDone)
	assertSingleTaskCompletion(t, ctx, executions, instance.ID)
}

func TestRabbitMQPoisonCompletionMovesToDeadLetterQueue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	_, amqpURL, _ := startRabbitLifecycleContainer(t)
	manager, err := queue.NewRabbitMQManager(ctx, amqpURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	queueName := "completion-poison-" + domain.NewID("test")
	completionQueue, err := queue.NewManagedRabbitMQCompletionQueue(manager, queueName)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = completionQueue.Close() })
	poisonErrors := make(chan error, 8)
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		for ctx.Err() == nil {
			_, receiveErr := completionQueue.ReceiveCompletion(ctx, "poison-consumer")
			if receiveErr != nil && ctx.Err() == nil {
				select {
				case poisonErrors <- receiveErr:
				default:
				}
			}
		}
	}()
	waitFor(t, ctx, "poison consumer readiness", manager.Ready)
	connection, channel := openRabbitChannel(t, ctx, amqpURL)
	defer connection.Close()
	dlqDeliveries, err := channel.Consume(queueName+".dlq", "poison-dlq-check", true, false, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := channel.PublishWithContext(ctx, "", queueName, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		MessageId:    "malformed-poison",
		Body:         []byte("{not-json"),
	}); err != nil {
		t.Fatalf("publish poison completion: %v", err)
	}
	select {
	case dead := <-dlqDeliveries:
		if dead.MessageId != "malformed-poison" {
			t.Fatalf("dead-letter message ID = %q", dead.MessageId)
		}
		deathEntries, ok := dead.Headers["x-death"].([]interface{})
		if !ok || len(deathEntries) == 0 {
			t.Fatalf("dead-letter headers do not include x-death details: %#v", dead.Headers)
		}
		death, ok := deathEntries[0].(amqp.Table)
		if !ok || death["reason"] != "delivery_limit" {
			t.Fatalf("dead-letter reason = %#v, want delivery_limit", deathEntries[0])
		}
	case <-ctx.Done():
		t.Fatal("poison completion was not routed to the DLQ")
	}
	select {
	case <-poisonErrors:
	case <-ctx.Done():
		t.Fatal("poison delivery was not rejected by the completion consumer")
	}
	if err := channel.Close(); err != nil {
		t.Errorf("close DLQ channel: %v", err)
	}
	cancel()
	select {
	case <-consumerDone:
	case <-time.After(time.Second):
		t.Fatal("poison consumer did not stop")
	}
}

func TestRabbitMQConnectionFailureRecoversAllSessions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	_, amqpURL, managementURL := startRabbitLifecycleContainer(t)
	manager, workQueue := newManagedWorkQueue(t, ctx, amqpURL)
	completionQueue, err := queue.NewManagedRabbitMQCompletionQueue(manager, "completion-isolation-"+domain.NewID("test"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = completionQueue.Close() })
	completionConsumer := make(chan error, 1)
	go func() {
		_, receiveErr := completionQueue.ReceiveCompletion(ctx, "isolation-completion-consumer")
		completionConsumer <- receiveErr
	}()
	workConsumer := make(chan error, 1)
	go func() {
		_, receiveErr := workQueue.Receive(ctx, "isolation-work-consumer")
		workConsumer <- receiveErr
	}()
	waitFor(t, ctx, "both AMQP adapter sessions ready", manager.Ready)
	// The channel resource is read-only in RabbitMQ 3.13 management (allow:
	// HEAD/GET/OPTIONS), so force-close the shared connection instead. Both
	// adapter sessions share one managed connection; closing it must make
	// readiness false and then recover, re-arming both consumers.
	connectionName := waitForRabbitConnectionName(t, ctx, managementURL)
	closeRequest, err := http.NewRequestWithContext(ctx, http.MethodDelete, managementURL+"/api/connections/"+url.PathEscape(connectionName), nil)
	if err != nil {
		t.Fatal(err)
	}
	closeRequest.SetBasicAuth(rabbitTestUser, rabbitTestPassword)
	closeRequest.Header.Set("X-Reason", "test-induced connection failure")
	closeResponse, err := http.DefaultClient.Do(closeRequest)
	if err != nil {
		t.Fatalf("close connection through RabbitMQ management API: %v", err)
	}
	closeResponse.Body.Close()
	if closeResponse.StatusCode != http.StatusNoContent && closeResponse.StatusCode != http.StatusOK {
		t.Fatalf("close connection status = %d", closeResponse.StatusCode)
	}
	waitFor(t, ctx, "manager readiness drops after connection failure", func() bool {
		return !manager.Ready()
	})
	waitFor(t, ctx, "manager readiness recovers after connection failure", manager.Ready)
	if err := completionQueue.PublishCompletion(ctx, queue.CompletionMessage{
		MessageID: "sibling-session-still-active", Kind: queue.CompletionSucceeded,
		WorkflowID: "workflow-isolation", TaskID: "task-isolation", NodeID: "charge", LeaseID: "lease-isolation", LeaseToken: 1,
	}); err != nil {
		t.Fatalf("publish through unaffected completion session: %v", err)
	}
	select {
	case err := <-completionConsumer:
		if err != nil {
			t.Fatalf("unaffected completion consumer returned error: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("unaffected completion session stopped receiving")
	}
	if err := workQueue.Publish(ctx, queue.WorkItem{
		ID: "task-after-connection-recovery", WorkflowID: "workflow-isolation",
		TaskID: "task-isolation", NodeID: "charge", TaskName: "charge",
	}); err != nil {
		t.Fatalf("publish task after connection recovery: %v", err)
	}
	select {
	case err := <-workConsumer:
		if err != nil {
			t.Fatalf("work consumer returned error after reconnect: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("task consumer did not receive work after reconnect")
	}
}

// waitForRabbitConnectionName returns the management-API name of the first live
// AMQP connection, used to force-close it (channels are read-only in 3.13).
func waitForRabbitConnectionName(t *testing.T, ctx context.Context, managementURL string) string {
	t.Helper()
	var connectionName string
	waitFor(t, ctx, "management API connection", func() bool {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, managementURL+"/api/connections", nil)
		if err != nil {
			return false
		}
		request.SetBasicAuth(rabbitTestUser, rabbitTestPassword)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			return false
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			return false
		}
		var connections []struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(response.Body).Decode(&connections); err != nil {
			return false
		}
		for _, connection := range connections {
			if connection.Name != "" {
				connectionName = connection.Name
				return true
			}
		}
		return false
	})
	return connectionName
}

type gatedCompletionAckQueue struct {
	queue.CompletionQueue
	reached chan struct{}
	release chan struct{}
	once    chan struct{}
}

func (queue *gatedCompletionAckQueue) AckCompletion(ctx context.Context, deliveryID string) error {
	if queue.once == nil {
		queue.once = make(chan struct{}, 1)
	}
	select {
	case queue.once <- struct{}{}:
		close(queue.reached)
		<-queue.release
	default:
	}
	return queue.CompletionQueue.AckCompletion(ctx, deliveryID)
}

type countingCompletionAckQueue struct {
	queue.CompletionQueue
	acked chan struct{}
}

func (queue *countingCompletionAckQueue) AckCompletion(ctx context.Context, deliveryID string) error {
	if err := queue.CompletionQueue.AckCompletion(ctx, deliveryID); err != nil {
		return err
	}
	select {
	case queue.acked <- struct{}{}:
	default:
	}
	return nil
}

func startRabbitLifecycleContainer(t *testing.T) (testcontainers.Container, string, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	amqpHostPort := reserveHostPort(t)
	managementHostPort := reserveHostPort(t)
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "rabbitmq:3.13-management-alpine",
			Env:          map[string]string{"RABBITMQ_DEFAULT_USER": rabbitTestUser, "RABBITMQ_DEFAULT_PASS": rabbitTestPassword},
			ExposedPorts: []string{"5672/tcp", "15672/tcp"},
			WaitingFor: wait.ForAll(
				wait.ForListeningPort("5672/tcp"),
				wait.ForListeningPort("15672/tcp"),
			),
			HostConfigModifier: func(hostConfig *dockercontainer.HostConfig) {
				hostConfig.PortBindings = nat.PortMap{
					nat.Port("5672/tcp"):  {{HostIP: "127.0.0.1", HostPort: amqpHostPort}},
					nat.Port("15672/tcp"): {{HostIP: "127.0.0.1", HostPort: managementHostPort}},
				}
			},
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start RabbitMQ lifecycle Testcontainer: %v", err)
	}
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if err := container.Terminate(cleanupContext); err != nil {
			t.Errorf("terminate RabbitMQ lifecycle Testcontainer: %v", err)
		}
	})
	// Use PortEndpoint with the explicit AMQP port rather than Endpoint, which
	// picks the lowest-numbered port across *all* ports the rabbitmq image
	// declares via EXPOSE (e.g. 4369/tcp for EPMD) even when that port was
	// never published, causing a "port not found" error.
	amqpURL, err := container.PortEndpoint(ctx, "5672/tcp", "amqp")
	if err != nil {
		t.Fatalf("get RabbitMQ AMQP endpoint: %v", err)
	}
	parsedAMQPURL, err := url.Parse(amqpURL)
	if err != nil {
		t.Fatalf("parse RabbitMQ AMQP endpoint: %v", err)
	}
	parsedAMQPURL.User = url.UserPassword(rabbitTestUser, rabbitTestPassword)
	amqpURL = parsedAMQPURL.String()
	// container.Endpoint(ctx, "http") resolves the first exposed port (5672/AMQP),
	// not the management UI. Build the management URL from the mapped 15672 port.
	managementPort, err := container.MappedPort(ctx, "15672/tcp")
	if err != nil {
		t.Fatalf("get RabbitMQ management port: %v", err)
	}
	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("get RabbitMQ host: %v", err)
	}
	managementURL := fmt.Sprintf("http://%s:%s", host, managementPort.Port())
	return container, amqpURL, managementURL
}

func reserveHostPort(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve an ephemeral RabbitMQ host port: %v", err)
	}
	defer listener.Close()
	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("parse reserved RabbitMQ host port: %v", err)
	}
	return port
}

func newManagedWorkQueue(t *testing.T, ctx context.Context, amqpURL string) (*queue.RabbitMQManager, *queue.RabbitMQQueue) {
	t.Helper()
	manager, err := queue.NewRabbitMQManager(ctx, amqpURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	workQueue, err := queue.NewManagedRabbitMQQueue(manager, "task-lifecycle-"+domain.NewID("test"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = workQueue.Close() })
	return manager, workQueue
}

func newDurableWorkflow(t *testing.T, ctx context.Context, workQueue queue.QueueDAO) (*gateway.Gateway, *store.SQLiteStore, *domain.WorkflowInstance, gateway.WorkDelivery) {
	t.Helper()
	database, err := store.OpenSQLiteStore(ctx, path.Join(t.TempDir(), "executions.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	timers := timer.NewService(16)
	engine := decider.NewWithTimer(database, timers)
	leases, err := lease.NewManagerWithStateStore(workQueue, timers, 16, database)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		leases.Close()
		engine.Close()
		timers.Close()
	})
	api, err := gateway.New(engine, database, workQueue, leases)
	if err != nil {
		t.Fatal(err)
	}
	definition := domain.WorkflowDef{
		Name: "rabbitmq-lifecycle", Version: 1,
		Nodes: map[string]domain.NodeDefinition{
			"charge": {ID: "charge", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "Charge"}},
		},
	}
	if err := api.RegisterWorkflow(definition); err != nil {
		t.Fatal(err)
	}
	instance, err := api.StartWorkflow(ctx, definition.Name, definition.Version, map[string]any{"order_id": domain.NewID("order")})
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := api.ReceiveWork(ctx, "lifecycle-worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return api, database, instance, delivery
}

func completionFor(workflowID string, delivery gateway.WorkDelivery) queue.CompletionMessage {
	return queue.CompletionMessage{
		MessageID:  "completion-lifecycle-" + delivery.LeaseID,
		Kind:       queue.CompletionSucceeded,
		WorkflowID: workflowID, TaskID: delivery.Item.TaskID, NodeID: delivery.Item.NodeID,
		LeaseID: delivery.LeaseID, LeaseToken: delivery.LeaseToken,
		Result: map[string]any{"result": "persisted"},
	}
}

func startCompletionConsumer(ctx context.Context, api *gateway.Gateway, completions queue.CompletionQueue, consumerID string) (<-chan struct{}, context.CancelFunc) {
	consumerContext, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		_ = api.RunCompletionConsumer(consumerContext, completions, consumerID)
		close(done)
	}()
	return done, cancel
}

func stopCompletionConsumer(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("completion consumer did not stop")
	}
}

func assertSingleTaskCompletion(t *testing.T, ctx context.Context, executions store.ExecutionStore, workflowID string) {
	t.Helper()
	events, err := executions.(store.EventHistoryStore).ListEvents(ctx, workflowID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, event := range events {
		if event.Type == domain.EventTaskCompleted {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("task completion event count = %d, want exactly one", count)
	}
}

func openRabbitChannel(t *testing.T, ctx context.Context, amqpURL string) (*amqp.Connection, *amqp.Channel) {
	t.Helper()
	connection, err := amqp.Dial(amqpURL)
	if err != nil {
		t.Fatal(err)
	}
	channel, err := connection.Channel()
	if err != nil {
		_ = connection.Close()
		t.Fatal(err)
	}
	return connection, channel
}

func stopRabbitContainer(t *testing.T, ctx context.Context, container testcontainers.Container) {
	t.Helper()
	grace := 2 * time.Second
	if err := container.Stop(ctx, &grace); err != nil {
		t.Fatalf("stop RabbitMQ Testcontainer: %v", err)
	}
}

func startRabbitContainer(t *testing.T, ctx context.Context, container testcontainers.Container, amqpURL string) {
	t.Helper()
	if err := container.Start(ctx); err != nil {
		t.Fatalf("restart RabbitMQ Testcontainer: %v", err)
	}
	waitFor(t, ctx, "RabbitMQ listener after restart", func() bool {
		connection, err := amqp.Dial(amqpURL)
		if err != nil {
			return false
		}
		_ = connection.Close()
		return true
	})
}

func waitForRabbitConsumerChannel(t *testing.T, ctx context.Context, managementURL, queueName string) string {
	t.Helper()
	var channelName string
	waitFor(t, ctx, "management API consumer channel", func() bool {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, managementURL+"/api/consumers", nil)
		if err != nil {
			return false
		}
		request.SetBasicAuth(rabbitTestUser, rabbitTestPassword)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			return false
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			return false
		}
		var consumers []struct {
			ConsumerTag string `json:"consumer_tag"`
			Queue       struct {
				Name string `json:"name"`
			} `json:"queue"`
			ChannelDetails struct {
				Name string `json:"name"`
			} `json:"channel_details"`
		}
		if err := json.NewDecoder(response.Body).Decode(&consumers); err != nil {
			return false
		}
		for _, consumer := range consumers {
			if consumer.ConsumerTag == queueName && consumer.ChannelDetails.Name != "" {
				channelName = consumer.ChannelDetails.Name
				return true
			}
		}
		return false
	})
	return channelName
}

func waitFor(t *testing.T, ctx context.Context, description string, condition func() bool) {
	t.Helper()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s: %v", description, ctx.Err())
		case <-ticker.C:
		}
	}
}
