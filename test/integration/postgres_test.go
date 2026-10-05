package integration_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/zephyr-workflow/zephyr/pkg/decider"
	"github.com/zephyr-workflow/zephyr/pkg/domain"
	"github.com/zephyr-workflow/zephyr/pkg/lease"
	"github.com/zephyr-workflow/zephyr/pkg/queue"
	"github.com/zephyr-workflow/zephyr/pkg/store"
	"github.com/zephyr-workflow/zephyr/pkg/timer"
)

func TestPostgresExecutionStoreEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "postgres:16-alpine",
			Env:          map[string]string{"POSTGRES_USER": "zephyr", "POSTGRES_PASSWORD": "zephyr-test", "POSTGRES_DB": "zephyr"},
			ExposedPorts: []string{"5432/tcp"},
			WaitingFor:   wait.ForListeningPort("5432/tcp"),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start PostgreSQL Testcontainer: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if err := container.Terminate(cleanupCtx); err != nil {
			t.Errorf("terminate PostgreSQL Testcontainer: %v", err)
		}
	})
	host, err := container.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := container.MappedPort(ctx, "5432/tcp")
	if err != nil {
		t.Fatal(err)
	}
	dataSource := fmt.Sprintf("postgres://zephyr:zephyr-test@%s:%s/zephyr?sslmode=disable", host, port.Port())
	queryDB, err := sql.Open("pgx", dataSource)
	if err != nil {
		t.Fatal(err)
	}
	defer queryDB.Close()
	executions, err := store.OpenPostgresStore(ctx, dataSource)
	if err != nil {
		t.Fatalf("open PostgreSQL execution store: %v", err)
	}
	defer executions.Close()
	if err := executions.Migrate(ctx); err != nil {
		t.Fatalf("migrate PostgreSQL schema: %v", err)
	}
	if err := executions.Migrate(ctx); err != nil {
		t.Fatalf("rerun PostgreSQL migrations: %v", err)
	}

	definition := domain.WorkflowDef{
		Name:    "postgres-checkout",
		Version: 1,
		Nodes: map[string]domain.NodeDefinition{
			"charge": {ID: "charge", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "ChargePayment"}},
		},
	}
	instance, err := domain.NewWorkflowInstance(definition, map[string]any{"order_id": "order-pg"})
	if err != nil {
		t.Fatal(err)
	}
	if err := executions.Create(instance); err != nil {
		t.Fatalf("create workflow execution: %v", err)
	}
	if err := executions.Create(instance); !errors.Is(err, store.ErrAlreadyExists) {
		t.Fatalf("duplicate workflow creation error = %v, want ErrAlreadyExists", err)
	}
	changedDefinition := definition
	changedDefinition.Nodes = map[string]domain.NodeDefinition{
		"charge": {ID: "charge", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "DifferentTask"}},
	}
	conflictingInstance, err := domain.NewWorkflowInstance(changedDefinition, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := executions.Create(conflictingInstance); !errors.Is(err, store.ErrDefinitionConflict) {
		t.Fatalf("conflicting definition error = %v, want ErrDefinitionConflict", err)
	}
	if _, err := executions.Get("workflow-missing"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("missing workflow error = %v, want ErrNotFound", err)
	}
	appendEvent := func(event domain.Event) {
		t.Helper()
		if err := executions.Append(instance.ID, event); err != nil {
			t.Fatalf("append %s: %v", event.Type, err)
		}
	}
	appendEvent(domain.Event{WorkflowID: instance.ID, Type: domain.EventWorkflowStarted})
	appendEvent(domain.Event{WorkflowID: instance.ID, Type: domain.EventTaskScheduled, TaskID: "task-pg", NodeID: "charge", Payload: map[string]any{"task_name": "ChargePayment"}})
	appendEvent(domain.Event{WorkflowID: instance.ID, Type: domain.EventTaskStarted, TaskID: "task-pg", NodeID: "charge", Payload: map[string]any{"lease_token": uint64(9)}})
	appendEvent(domain.Event{WorkflowID: instance.ID, Type: domain.EventTaskCompleted, TaskID: "task-pg", NodeID: "charge", Payload: map[string]any{"result": map[string]any{"transaction_id": "tx-pg"}}})

	reopened, err := store.OpenPostgresStore(ctx, dataSource)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	loaded, err := reopened.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.NextSequence != 4 || loaded.Status != domain.WorkflowRunning || loaded.Context["order_id"] != "order-pg" {
		t.Fatalf("loaded workflow snapshot = %#v", loaded)
	}
	if task := loaded.Tasks["charge"]; task.Status != domain.TaskCompleted || task.LeaseToken != 9 || task.Result["transaction_id"] != "tx-pg" {
		t.Fatalf("loaded task state = %#v", task)
	}
	listed, total, err := reopened.ListExecutions(ctx, store.ExecutionFilter{WorkflowName: "postgres-checkout", Status: domain.WorkflowRunning, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 || len(listed) != 1 || listed[0].ID != instance.ID {
		t.Fatalf("listed PostgreSQL executions/total = %#v/%d", listed, total)
	}
	var eventCount int
	if err := queryDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM workflow_events WHERE workflow_id = $1`, instance.ID).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if eventCount != 4 {
		t.Fatalf("persisted event count = %d, want 4", eventCount)
	}
	eventPage, err := reopened.ListEvents(ctx, instance.ID, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(eventPage) != 2 || eventPage[0].Sequence != 2 || eventPage[1].Sequence != 3 {
		t.Fatalf("persisted event page = %#v, want sequences 2 and 3", eventPage)
	}
	var readyIndex bool
	if err := queryDB.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'task_executions_ready_idx')`).Scan(&readyIndex); err != nil {
		t.Fatal(err)
	}
	if !readyIndex {
		t.Fatal("ready-task partial index was not created")
	}
	firstBatch, err := executions.ClaimOutbox(ctx, 2, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(firstBatch) != 2 {
		t.Fatalf("first outbox claim size = %d, want 2", len(firstBatch))
	}
	secondBatch, err := reopened.ClaimOutbox(ctx, 8, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(secondBatch) != 2 {
		t.Fatalf("competing outbox claim size = %d, want remaining 2", len(secondBatch))
	}
	if err := executions.MarkOutboxDelivered(ctx, firstBatch[0]); err != nil {
		t.Fatal(err)
	}
	if err := executions.RetryOutbox(ctx, firstBatch[1], 0); err != nil {
		t.Fatal(err)
	}
	retriedBatch, err := reopened.ClaimOutbox(ctx, 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(retriedBatch) != 1 || retriedBatch[0].Event.Sequence != firstBatch[1].Event.Sequence || retriedBatch[0].ClaimToken == firstBatch[1].ClaimToken {
		t.Fatalf("retried outbox claim = %#v", retriedBatch)
	}
	if err := reopened.MarkOutboxDelivered(ctx, retriedBatch[0]); err != nil {
		t.Fatal(err)
	}
	if err := executions.MarkOutboxDelivered(ctx, firstBatch[1]); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("stale outbox claim mark error = %v, want ErrConflict", err)
	}

	const concurrentAppends = 8
	start := make(chan struct{})
	errorsFound := make(chan error, concurrentAppends)
	var group sync.WaitGroup
	for range concurrentAppends {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			for attempt := 0; attempt < concurrentAppends; attempt++ {
				err := executions.Append(instance.ID, domain.Event{WorkflowID: instance.ID, Type: domain.EventWorkflowStarted})
				if errors.Is(err, store.ErrConflict) {
					continue
				}
				errorsFound <- err
				return
			}
			errorsFound <- fmt.Errorf("append did not succeed after OCC retries")
		}()
	}
	close(start)
	group.Wait()
	close(errorsFound)
	for err := range errorsFound {
		if err != nil {
			t.Errorf("concurrent append: %v", err)
		}
	}
	loaded, err = reopened.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.NextSequence != 4+concurrentAppends {
		t.Fatalf("sequence after concurrent appends = %d, want %d", loaded.NextSequence, 4+concurrentAppends)
	}

	sharedQueue := queue.NewMemoryQueue(4)
	timersA := timer.NewService(4)
	timersB := timer.NewService(4)
	managerA, err := lease.NewManagerWithStateStore(sharedQueue, timersA, 4, executions)
	if err != nil {
		t.Fatal(err)
	}
	managerB, err := lease.NewManagerWithStateStore(sharedQueue, timersB, 4, reopened)
	if err != nil {
		managerA.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		managerA.Close()
		managerB.Close()
		timersA.Close()
		timersB.Close()
		_ = sharedQueue.Close()
	})
	if err := sharedQueue.Publish(ctx, queue.WorkItem{
		ID: "shared-lease-work", WorkflowID: instance.ID, TaskID: "shared-lease-task", NodeID: "charge",
	}); err != nil {
		t.Fatal(err)
	}
	delivery, err := sharedQueue.Receive(ctx, "lease-worker")
	if err != nil {
		t.Fatal(err)
	}
	startClaims := make(chan struct{})
	type claimResult struct {
		manager int
		lease   lease.Lease
		err     error
	}
	claims := make(chan claimResult, 2)
	for index, manager := range []*lease.Manager{managerA, managerB} {
		go func(index int, manager *lease.Manager) {
			<-startClaims
			claimed, claimErr := manager.Acquire(ctx, delivery, time.Minute)
			claims <- claimResult{manager: index, lease: claimed, err: claimErr}
		}(index, manager)
	}
	close(startClaims)
	results := []claimResult{<-claims, <-claims}
	twins := 0
	var winning claimResult
	for _, result := range results {
		if result.err == nil {
			winning = result
			twins++
		} else if !errors.Is(result.err, lease.ErrLeaseConflict) {
			t.Fatalf("competing lease claim error = %v, want conflict", result.err)
		}
	}
	if twins != 1 {
		t.Fatalf("successful simultaneous claims = %d, want 1", twins)
	}
	otherManager := managerA
	if winning.manager == 0 {
		otherManager = managerB
	}
	if err := otherManager.Heartbeat(ctx, winning.lease.ID, winning.lease.Token+1, time.Minute); !errors.Is(err, lease.ErrStaleToken) {
		t.Fatalf("cross-manager stale heartbeat error = %v, want ErrStaleToken", err)
	}
	if err := otherManager.CompleteWith(ctx, winning.lease.ID, winning.lease.Token+1, func(lease.Lease, lease.MutationStore) error {
		t.Fatal("stale completion invoked its operation")
		return nil
	}); !errors.Is(err, lease.ErrStaleToken) {
		t.Fatalf("cross-manager stale completion error = %v, want ErrStaleToken", err)
	}
	if err := otherManager.Heartbeat(ctx, winning.lease.ID, winning.lease.Token, time.Minute); err != nil {
		t.Fatalf("heartbeat through second manager: %v", err)
	}
	completedOperation := false
	if err := otherManager.CompleteWith(ctx, winning.lease.ID, winning.lease.Token, func(lease.Lease, lease.MutationStore) error {
		completedOperation = true
		return nil
	}); err != nil {
		t.Fatalf("completion through second manager: %v", err)
	}
	if !completedOperation {
		t.Fatal("completion operation was not called")
	}
	if err := managerA.Complete(ctx, winning.lease.ID, winning.lease.Token); err != nil {
		t.Fatalf("idempotent completion after cross-manager completion: %v", err)
	}

	expiring, err := executions.Claim(ctx, lease.Lease{
		ID: "lease-expiring", DeliveryID: "delivery-expiring",
		Item: queue.WorkItem{ID: "work-expiring", WorkflowID: instance.ID, TaskID: "task-expiring", NodeID: "charge"},
	}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := queryDB.ExecContext(ctx, `UPDATE task_leases SET expires_at = clock_timestamp() - INTERVAL '1 second' WHERE lease_id = $1`, expiring.ID); err != nil {
		t.Fatal(err)
	}
	expiryQueue := queue.NewMemoryQueue(4)
	expiryTimers := timer.NewService(4)
	expiryManager, err := lease.NewManagerWithStateStore(expiryQueue, expiryTimers, 4, reopened)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		expiryManager.Close()
		expiryTimers.Close()
		_ = expiryQueue.Close()
	}()
	expirationHandled := false
	expiryManager.SetExpirationHandler(func(context.Context, lease.Expiration, lease.MutationStore) error {
		expirationHandled = true
		return nil
	})
	processed, err := expiryManager.ScanExpired(ctx, 10)
	if err != nil || processed != 1 || !expirationHandled {
		t.Fatalf("recovered expired leases = %d, callback=%t, err=%v", processed, expirationHandled, err)
	}
	if _, err := reopened.CompleteWith(ctx, expiring.ID, expiring.Token, nil); !errors.Is(err, lease.ErrLeaseExpired) {
		t.Fatalf("completion of expired lease = %v, want ErrLeaseExpired", err)
	}
	if _, err := reopened.Claim(ctx, lease.Lease{
		ID: "lease-same-attempt", DeliveryID: "delivery-same-attempt",
		Item: queue.WorkItem{ID: "work-expiring", WorkflowID: instance.ID, TaskID: "task-expiring", NodeID: "charge"},
	}, time.Minute); !errors.Is(err, lease.ErrLeaseConflict) {
		t.Fatalf("same-attempt reclaim error = %v, want ErrLeaseConflict", err)
	}
	retryLease, err := reopened.Claim(ctx, lease.Lease{
		ID: "lease-retry-attempt", DeliveryID: "delivery-retry-attempt",
		Item: queue.WorkItem{ID: "work-retry", WorkflowID: instance.ID, TaskID: "task-expiring-retry", NodeID: "charge"},
	}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if retryLease.Token <= expiring.Token {
		t.Fatalf("retry fencing token = %d, want greater than expired token %d", retryLease.Token, expiring.Token)
	}

	batchDefinition := domain.WorkflowDef{
		Name: "postgres-batched-failure", Version: 1,
		Nodes: map[string]domain.NodeDefinition{
			"run": {ID: "run", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "Run", Retries: 1}},
		},
	}
	batchInstance, err := domain.NewWorkflowInstance(batchDefinition, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := executions.Create(batchInstance); err != nil {
		t.Fatal(err)
	}
	retryAt := time.Now().Add(time.Second)
	if err := executions.AppendMany(batchInstance.ID, []domain.Event{
		{WorkflowID: batchInstance.ID, Type: domain.EventWorkflowStarted},
		{WorkflowID: batchInstance.ID, TaskID: "batch-task", NodeID: "run", Type: domain.EventTaskScheduled, Payload: map[string]any{"task_name": "Run", "input": map[string]any{}, "retry_limit": 1}},
		{WorkflowID: batchInstance.ID, TaskID: "batch-task", NodeID: "run", Type: domain.EventTaskStarted, Payload: map[string]any{"lease_token": uint64(11)}},
		{WorkflowID: batchInstance.ID, TaskID: "batch-task", NodeID: "run", Type: domain.EventTaskFailed, Payload: map[string]any{"error": "temporary"}},
		{WorkflowID: batchInstance.ID, TaskID: "batch-task", NodeID: "run", Type: domain.EventTaskRetryScheduled, Payload: map[string]any{"error": "temporary", "retry_at": retryAt, "attempt": 2}},
	}); err != nil {
		t.Fatal(err)
	}
	batchLoaded, err := reopened.Get(batchInstance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if batchLoaded.NextSequence != 5 || batchLoaded.Tasks["run"].Status != domain.TaskRetrying || batchLoaded.Tasks["run"].Attempt != 2 || !batchLoaded.Tasks["run"].RetryAt.Equal(retryAt) {
		t.Fatalf("persisted atomic retry batch = sequence %d, task %#v", batchLoaded.NextSequence, batchLoaded.Tasks["run"])
	}

	recoveryDefinition := domain.WorkflowDef{
		Name: "postgres-concurrent-recovery", Version: 1,
		Nodes: map[string]domain.NodeDefinition{
			"run": {ID: "run", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "Run"}},
		},
	}
	recoveryInstance, err := domain.NewWorkflowInstance(recoveryDefinition, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := executions.Create(recoveryInstance); err != nil {
		t.Fatal(err)
	}
	if err := executions.Append(recoveryInstance.ID, domain.Event{WorkflowID: recoveryInstance.ID, Type: domain.EventWorkflowStarted}); err != nil {
		t.Fatal(err)
	}
	engineA := decider.NewWithTimer(executions, timersA)
	engineB := decider.NewWithTimer(reopened, timersB)
	defer engineA.Close()
	defer engineB.Close()
	var recoveryWait sync.WaitGroup
	recoveryErrors := make(chan error, 2)
	for _, engine := range []*decider.Decider{engineA, engineB} {
		recoveryWait.Add(1)
		go func(engine *decider.Decider) {
			defer recoveryWait.Done()
			recoveryErrors <- engine.Recover(ctx)
		}(engine)
	}
	recoveryWait.Wait()
	close(recoveryErrors)
	for err := range recoveryErrors {
		if err != nil {
			t.Fatalf("concurrent workflow recovery: %v", err)
		}
	}
	recovered, err := reopened.Get(recoveryInstance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(recovered.Tasks) != 1 || recovered.Tasks["run"].Status != domain.TaskReady {
		t.Fatalf("concurrently recovered task state = %#v", recovered.Tasks)
	}
	var scheduledCount, publicationCount int
	if err := queryDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM workflow_events WHERE workflow_id = $1 AND event_type = $2`, recoveryInstance.ID, string(domain.EventTaskScheduled)).Scan(&scheduledCount); err != nil {
		t.Fatal(err)
	}
	if err := queryDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM task_publication_outbox WHERE task_id = $1`, recovered.Tasks["run"].ID).Scan(&publicationCount); err != nil {
		t.Fatal(err)
	}
	if scheduledCount != 1 || publicationCount != 1 {
		t.Fatalf("concurrent recovery scheduled/outbox rows = %d/%d, want 1/1", scheduledCount, publicationCount)
	}
}
