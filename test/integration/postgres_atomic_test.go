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
	"github.com/zephyr-workflow/zephyr/pkg/gateway"
	"github.com/zephyr-workflow/zephyr/pkg/lease"
	"github.com/zephyr-workflow/zephyr/pkg/queue"
	"github.com/zephyr-workflow/zephyr/pkg/store"
	"github.com/zephyr-workflow/zephyr/pkg/timer"
)

func TestPostgresAtomicCompletionRollbackReplayAndPublication(t *testing.T) {
	ctx, executions, queryDB := newAtomicPostgres(t)
	workQueue, timers, engine, leases, api := newAtomicGateway(t, executions)
	definition := domain.WorkflowDef{
		Name: "atomic-completion", Version: 1, Start: []string{"first"},
		Nodes: map[string]domain.NodeDefinition{
			"first":  {ID: "first", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "First"}, Next: []string{"second"}},
			"second": {ID: "second", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "Second"}, DependsOn: []string{"first"}},
		},
	}
	instance, err := engine.Start(definition, nil)
	if err != nil {
		t.Fatal(err)
	}
	first := instance.Tasks["first"]
	active := claimAtomicLease(t, ctx, executions, instance.ID, first, time.Minute)
	if err := engine.StartTask(instance.ID, "first", active.Token); err != nil {
		t.Fatal(err)
	}
	injectedFailure := errors.New("injected before commit")
	result := map[string]any{"payment_id": "pay-1"}
	_, err = executions.CompleteWith(ctx, active.ID, active.Token, func(_ lease.Lease, mutation lease.MutationStore) error {
		if err := engine.CompleteTaskWithMutation(instance.ID, "first", result, mutation); err != nil {
			return err
		}
		return injectedFailure
	})
	if !errors.Is(err, injectedFailure) {
		t.Fatalf("injected completion error = %v, want injected error", err)
	}
	unchanged, err := executions.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if unchanged.Tasks["first"].Status != domain.TaskRunning || len(unchanged.Tasks) != 1 {
		t.Fatalf("completion rollback changed workflow: %#v", unchanged.Tasks)
	}
	if _, err := executions.Renew(ctx, active.ID, active.Token, time.Minute); err != nil {
		t.Fatalf("lease was consumed despite workflow rollback: %v", err)
	}
	if err := api.CompleteWork(ctx, gateway.TaskCompletion{
		WorkflowID: instance.ID, TaskID: first.ID, NodeID: "first", LeaseID: active.ID, LeaseToken: active.Token, Result: result,
	}); err != nil {
		t.Fatalf("retry completion after rollback: %v", err)
	}
	if err := api.CompleteWork(ctx, gateway.TaskCompletion{
		WorkflowID: instance.ID, TaskID: first.ID, NodeID: "first", LeaseID: active.ID, LeaseToken: active.Token, Result: result,
	}); err != nil {
		t.Fatalf("repeat completion after simulated response loss: %v", err)
	}
	updated, err := executions.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Tasks["first"].Status != domain.TaskCompleted || updated.Tasks["second"].Status != domain.TaskReady {
		t.Fatalf("completion workflow state = %#v", updated.Tasks)
	}
	second := updated.Tasks["second"]
	var completedEvents, downstreamEvents, publicationRows int
	if err := queryDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM workflow_events WHERE workflow_id = $1 AND event_type = $2 AND task_id = $3`, instance.ID, string(domain.EventTaskCompleted), first.ID).Scan(&completedEvents); err != nil {
		t.Fatal(err)
	}
	if err := queryDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM workflow_events WHERE workflow_id = $1 AND event_type = $2 AND task_id = $3`, instance.ID, string(domain.EventTaskScheduled), second.ID).Scan(&downstreamEvents); err != nil {
		t.Fatal(err)
	}
	if err := queryDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM task_publication_outbox WHERE task_id = $1`, second.ID).Scan(&publicationRows); err != nil {
		t.Fatal(err)
	}
	if completedEvents != 1 || downstreamEvents != 1 || publicationRows != 1 {
		t.Fatalf("completion/schedule/publication rows = %d/%d/%d, want 1/1/1", completedEvents, downstreamEvents, publicationRows)
	}
	_ = workQueue
	_ = timers
	_ = engine
	_ = leases
}

func TestPostgresAtomicFailureRetryTerminalAndExpiry(t *testing.T) {
	ctx, executions, queryDB := newAtomicPostgres(t)
	_, _, engine, leases, api := newAtomicGateway(t, executions)
	retryDefinition := domain.WorkflowDef{
		Name: "atomic-retry", Version: 1, Start: []string{"retry"},
		Nodes: map[string]domain.NodeDefinition{
			"retry": {ID: "retry", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "Retry", Retries: 1, Backoff: time.Hour}},
		},
	}
	retryRun, err := engine.Start(retryDefinition, nil)
	if err != nil {
		t.Fatal(err)
	}
	retryTask := retryRun.Tasks["retry"]
	retryLease := claimAtomicLease(t, ctx, executions, retryRun.ID, retryTask, time.Minute)
	if err := engine.StartTask(retryRun.ID, "retry", retryLease.Token); err != nil {
		t.Fatal(err)
	}
	injectedFailure := errors.New("injected failure before commit")
	_, err = executions.CompleteWith(ctx, retryLease.ID, retryLease.Token, func(_ lease.Lease, mutation lease.MutationStore) error {
		if err := engine.FailTaskWithMutation(retryRun.ID, "retry", "temporary", mutation); err != nil {
			return err
		}
		return injectedFailure
	})
	if !errors.Is(err, injectedFailure) {
		t.Fatalf("injected failure transition error = %v", err)
	}
	unchanged, err := executions.Get(retryRun.ID)
	if err != nil {
		t.Fatal(err)
	}
	if unchanged.Tasks["retry"].Status != domain.TaskRunning {
		t.Fatalf("failure rollback task status = %s, want RUNNING", unchanged.Tasks["retry"].Status)
	}
	if _, err := executions.Renew(ctx, retryLease.ID, retryLease.Token, time.Minute); err != nil {
		t.Fatalf("failure rollback consumed lease: %v", err)
	}
	failure := gateway.TaskFailure{
		WorkflowID: retryRun.ID, TaskID: retryTask.ID, NodeID: "retry", LeaseID: retryLease.ID, LeaseToken: retryLease.Token, Error: "temporary",
	}
	if err := api.FailWork(ctx, failure); err != nil {
		t.Fatalf("retryable task failure: %v", err)
	}
	if err := api.FailWork(ctx, failure); err != nil {
		t.Fatalf("replay failure after simulated response loss: %v", err)
	}
	retried, err := executions.Get(retryRun.ID)
	if err != nil {
		t.Fatal(err)
	}
	if task := retried.Tasks["retry"]; task.Status != domain.TaskRetrying || task.Attempt != 2 || task.Error != "temporary" || task.RetryAt.IsZero() {
		t.Fatalf("persisted retry transition = %#v", task)
	}
	var failedEvents, retryEvents int
	if err := queryDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM workflow_events WHERE workflow_id = $1 AND event_type = $2`, retryRun.ID, string(domain.EventTaskFailed)).Scan(&failedEvents); err != nil {
		t.Fatal(err)
	}
	if err := queryDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM workflow_events WHERE workflow_id = $1 AND event_type = $2`, retryRun.ID, string(domain.EventTaskRetryScheduled)).Scan(&retryEvents); err != nil {
		t.Fatal(err)
	}
	if failedEvents != 1 || retryEvents != 1 {
		t.Fatalf("failure/retry events = %d/%d, want 1/1", failedEvents, retryEvents)
	}

	expiryDefinition := retryDefinition
	expiryDefinition.Name = "atomic-expiry"
	expiryRun, err := engine.Start(expiryDefinition, nil)
	if err != nil {
		t.Fatal(err)
	}
	expiryTask := expiryRun.Tasks["retry"]
	expiryLease := claimAtomicLease(t, ctx, executions, expiryRun.ID, expiryTask, time.Minute)
	if err := engine.StartTask(expiryRun.ID, "retry", expiryLease.Token); err != nil {
		t.Fatal(err)
	}
	if _, err := queryDB.ExecContext(ctx, `UPDATE task_leases SET expires_at = clock_timestamp() - INTERVAL '1 second' WHERE lease_id = $1`, expiryLease.ID); err != nil {
		t.Fatal(err)
	}
	processed, err := leases.ScanExpired(ctx, 10)
	if err != nil || processed != 1 {
		t.Fatalf("expired lease scan = %d, error = %v", processed, err)
	}
	expiredRun, err := executions.Get(expiryRun.ID)
	if err != nil {
		t.Fatal(err)
	}
	if task := expiredRun.Tasks["retry"]; task.Status != domain.TaskRetrying || task.Attempt != 2 || task.Error != "task lease expired" {
		t.Fatalf("expired task/retry transition = %#v", task)
	}
	if processed, err := leases.ScanExpired(ctx, 10); err != nil || processed != 0 {
		t.Fatalf("repeat expired scan = %d, error = %v, want no replay", processed, err)
	}

	terminalDefinition := retryDefinition
	terminalDefinition.Name = "atomic-terminal"
	terminalDefinition.Start = []string{"terminal"}
	terminalDefinition.Nodes = map[string]domain.NodeDefinition{
		"terminal": {ID: "terminal", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "Terminal"}},
	}
	terminalRun, err := engine.Start(terminalDefinition, nil)
	if err != nil {
		t.Fatal(err)
	}
	terminalTask := terminalRun.Tasks["terminal"]
	terminalLease := claimAtomicLease(t, ctx, executions, terminalRun.ID, terminalTask, time.Minute)
	if err := engine.StartTask(terminalRun.ID, "terminal", terminalLease.Token); err != nil {
		t.Fatal(err)
	}
	terminalFailure := gateway.TaskFailure{
		WorkflowID: terminalRun.ID, TaskID: terminalTask.ID, NodeID: "terminal", LeaseID: terminalLease.ID, LeaseToken: terminalLease.Token, Error: "permanent",
	}
	if err := api.FailWork(ctx, terminalFailure); err != nil {
		t.Fatalf("terminal task failure: %v", err)
	}
	if err := api.FailWork(ctx, terminalFailure); err != nil {
		t.Fatalf("terminal failure replay: %v", err)
	}
	terminal, err := executions.Get(terminalRun.ID)
	if err != nil {
		t.Fatal(err)
	}
	if terminal.Status != domain.WorkflowFailed || terminal.Tasks["terminal"].Status != domain.TaskFailed {
		t.Fatalf("terminal failure state = workflow %s task %s", terminal.Status, terminal.Tasks["terminal"].Status)
	}

	compensationDefinition := domain.WorkflowDef{
		Name: "atomic-compensation-failure", Version: 1, Start: []string{"reserve"},
		Nodes: map[string]domain.NodeDefinition{
			"reserve": {ID: "reserve", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "Reserve"}, Compensation: &domain.TaskDefinition{Name: "Release"}},
			"charge":  {ID: "charge", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "Charge"}, DependsOn: []string{"reserve"}},
		},
	}
	compensationRun, err := engine.Start(compensationDefinition, nil)
	if err != nil {
		t.Fatal(err)
	}
	reserve := compensationRun.Tasks["reserve"]
	reserveLease := claimAtomicLease(t, ctx, executions, compensationRun.ID, reserve, time.Minute)
	if err := engine.StartTask(compensationRun.ID, "reserve", reserveLease.Token); err != nil {
		t.Fatal(err)
	}
	if err := api.CompleteWork(ctx, gateway.TaskCompletion{
		WorkflowID: compensationRun.ID, TaskID: reserve.ID, NodeID: "reserve", LeaseID: reserveLease.ID, LeaseToken: reserveLease.Token,
	}); err != nil {
		t.Fatalf("complete compensation source task: %v", err)
	}
	compensationRun, err = executions.Get(compensationRun.ID)
	if err != nil {
		t.Fatal(err)
	}
	charge := compensationRun.Tasks["charge"]
	chargeLease := claimAtomicLease(t, ctx, executions, compensationRun.ID, charge, time.Minute)
	if err := engine.StartTask(compensationRun.ID, "charge", chargeLease.Token); err != nil {
		t.Fatal(err)
	}
	if err := api.FailWork(ctx, gateway.TaskFailure{
		WorkflowID: compensationRun.ID, TaskID: charge.ID, NodeID: "charge", LeaseID: chargeLease.ID, LeaseToken: chargeLease.Token, Error: "charge failed",
	}); err != nil {
		t.Fatalf("fail task and begin compensation: %v", err)
	}
	compensationRun, err = executions.Get(compensationRun.ID)
	if err != nil {
		t.Fatal(err)
	}
	var compensation domain.TaskInstance
	for _, task := range compensationRun.Tasks {
		if task.IsCompensation {
			compensation = task
			break
		}
	}
	if compensation.ID == "" {
		t.Fatal("workflow did not schedule its compensation task")
	}
	compensationLease := claimAtomicLease(t, ctx, executions, compensationRun.ID, compensation, time.Minute)
	if err := engine.StartTask(compensationRun.ID, compensation.NodeID, compensationLease.Token); err != nil {
		t.Fatal(err)
	}
	if err := api.FailWork(ctx, gateway.TaskFailure{
		WorkflowID: compensationRun.ID, TaskID: compensation.ID, NodeID: compensation.NodeID,
		LeaseID: compensationLease.ID, LeaseToken: compensationLease.Token, Error: "release failed",
	}); err != nil {
		t.Fatalf("fail compensation task: %v", err)
	}
	failedCompensation, err := executions.Get(compensationRun.ID)
	if err != nil {
		t.Fatal(err)
	}
	if failedCompensation.Status != domain.WorkflowFailed || failedCompensation.Tasks[compensation.NodeID].Status != domain.TaskFailed {
		t.Fatalf("compensation failure state = workflow %s task %s", failedCompensation.Status, failedCompensation.Tasks[compensation.NodeID].Status)
	}
	var compensationFailedEvents, workflowFailedEvents int
	if err := queryDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM workflow_events WHERE workflow_id = $1 AND task_id = $2 AND event_type = $3`, compensationRun.ID, compensation.ID, string(domain.EventTaskFailed)).Scan(&compensationFailedEvents); err != nil {
		t.Fatal(err)
	}
	if err := queryDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM workflow_events WHERE workflow_id = $1 AND event_type = $2`, compensationRun.ID, string(domain.EventWorkflowFailed)).Scan(&workflowFailedEvents); err != nil {
		t.Fatal(err)
	}
	var compensationLeaseState string
	if err := queryDB.QueryRowContext(ctx, `SELECT state FROM task_leases WHERE lease_id = $1`, compensationLease.ID).Scan(&compensationLeaseState); err != nil {
		t.Fatal(err)
	}
	if compensationFailedEvents != 1 || workflowFailedEvents != 1 || compensationLeaseState != "COMPLETED" {
		t.Fatalf("compensation failure event/workflow/lease = %d/%d/%s", compensationFailedEvents, workflowFailedEvents, compensationLeaseState)
	}
}

func TestPostgresCompletionExpiryRaceHasSingleTransition(t *testing.T) {
	ctx, executions, queryDB := newAtomicPostgres(t)
	definition := domain.WorkflowDef{
		Name: "atomic-completion-expiry-race", Version: 1, Start: []string{"run"},
		Nodes: map[string]domain.NodeDefinition{
			"run": {ID: "run", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "Run", Retries: 1, Backoff: time.Hour}},
		},
	}
	instance, err := domain.NewWorkflowInstance(definition, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := executions.Create(instance); err != nil {
		t.Fatal(err)
	}
	if err := executions.AppendMany(instance.ID, []domain.Event{
		{WorkflowID: instance.ID, Type: domain.EventWorkflowStarted},
		{WorkflowID: instance.ID, TaskID: "racing-task", NodeID: "run", Type: domain.EventTaskScheduled, Payload: map[string]any{"task_name": "Run", "attempt": 1, "retry_limit": 1}},
		{WorkflowID: instance.ID, TaskID: "racing-task", NodeID: "run", Type: domain.EventTaskStarted, Payload: map[string]any{"lease_token": uint64(1)}},
	}); err != nil {
		t.Fatal(err)
	}
	active, err := executions.Claim(ctx, lease.Lease{
		ID: "racing-lease", DeliveryID: "racing-delivery",
		Item: queue.WorkItem{WorkflowID: instance.ID, TaskID: "racing-task", NodeID: "run", TaskName: "Run"},
	}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := queryDB.ExecContext(ctx, `UPDATE task_leases SET expires_at = clock_timestamp() - INTERVAL '1 millisecond' WHERE lease_id = $1`, active.ID); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	var waitGroup sync.WaitGroup
	var completionErr error
	var expireErr error
	var expired bool
	waitGroup.Add(2)
	go func() {
		defer waitGroup.Done()
		<-start
		_, completionErr = executions.CompleteWith(ctx, active.ID, active.Token, func(_ lease.Lease, mutation lease.MutationStore) error {
			return mutation.Append(instance.ID, domain.Event{WorkflowID: instance.ID, TaskID: "racing-task", NodeID: "run", Type: domain.EventTaskCompleted})
		})
	}()
	go func() {
		defer waitGroup.Done()
		<-start
		_, expired, expireErr = executions.ExpireWith(ctx, active.ID, active.Token, func(_ lease.Lease, mutation lease.MutationStore) error {
			return mutation.AppendMany(instance.ID, []domain.Event{
				{WorkflowID: instance.ID, TaskID: "racing-task", NodeID: "run", Type: domain.EventTaskFailed, Payload: map[string]any{"error": "task lease expired"}},
				{WorkflowID: instance.ID, TaskID: "racing-task", NodeID: "run", Type: domain.EventTaskRetryScheduled, Payload: map[string]any{"error": "task lease expired", "attempt": 2, "retry_at": time.Now().Add(time.Hour)}},
			})
		})
	}()
	close(start)
	waitGroup.Wait()
	if !errors.Is(completionErr, lease.ErrLeaseExpired) || expireErr != nil || !expired {
		t.Fatalf("completion/expiry race = completion %v, expired %t, expiry error %v", completionErr, expired, expireErr)
	}
	updated, err := executions.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if task := updated.Tasks["run"]; task.Status != domain.TaskRetrying || task.Attempt != 2 || len(updated.Events) != 5 {
		t.Fatalf("state after completion/expiry race = task %#v, events %d", task, len(updated.Events))
	}
}

func TestPostgresHeartbeatExpiryUsesDatabaseTimeAfterRowLock(t *testing.T) {
	ctx, executions, queryDB := newAtomicPostgres(t)
	instance := domainInstanceForLeaseTest(t, executions)
	active := claimAtomicLease(t, ctx, executions, instance.ID, domain.TaskInstance{ID: "clock-task", NodeID: "run", TaskName: "Run"}, time.Minute)
	lockTx, err := queryDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lockTx.Rollback()
	if _, err := lockTx.ExecContext(ctx, `SELECT lease_id FROM task_leases WHERE lease_id = $1 FOR UPDATE`, active.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := lockTx.ExecContext(ctx, `UPDATE task_leases SET expires_at = clock_timestamp() + INTERVAL '50 milliseconds' WHERE lease_id = $1`, active.ID); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{}, 2)
	type heartbeatResult struct{ err error }
	type expiryResult struct {
		expired bool
		err     error
	}
	heartbeat := make(chan heartbeatResult, 1)
	expiry := make(chan expiryResult, 1)
	go func() {
		started <- struct{}{}
		_, err := executions.Renew(ctx, active.ID, active.Token, time.Minute)
		heartbeat <- heartbeatResult{err: err}
	}()
	go func() {
		started <- struct{}{}
		_, expired, err := executions.ExpireWith(ctx, active.ID, active.Token, nil)
		expiry <- expiryResult{expired: expired, err: err}
	}()
	<-started
	<-started
	time.Sleep(100 * time.Millisecond)
	if err := lockTx.Commit(); err != nil {
		t.Fatal(err)
	}
	heartbeatOutcome := <-heartbeat
	expiryOutcome := <-expiry
	if !errors.Is(heartbeatOutcome.err, lease.ErrLeaseExpired) {
		t.Fatalf("heartbeat after database deadline = %v, want ErrLeaseExpired", heartbeatOutcome.err)
	}
	if expiryOutcome.err != nil || !expiryOutcome.expired {
		t.Fatalf("expiry after row-lock wait = %t, error %v", expiryOutcome.expired, expiryOutcome.err)
	}
}

func newAtomicPostgres(t *testing.T) (context.Context, *store.PostgresStore, *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "postgres:16-alpine",
			Env:          map[string]string{"POSTGRES_USER": "zephyr", "POSTGRES_PASSWORD": "zephyr-test", "POSTGRES_DB": "zephyr"},
			ExposedPorts: []string{"5432/tcp"},
			// ForListeningPort alone is insufficient: Postgres opens its TCP
			// listener for a temporary initdb server before restarting into
			// the real server, so "ready to accept connections" is logged
			// twice before it's actually ready for use.
			WaitingFor: wait.ForAll(
				wait.ForListeningPort("5432/tcp"),
				wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
			),
		},
		Started: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if err := container.Terminate(cleanupContext); err != nil {
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
	t.Cleanup(func() { _ = queryDB.Close() })
	executions, err := store.OpenPostgresStore(ctx, dataSource)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = executions.Close() })
	if err := executions.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	return ctx, executions, queryDB
}

func newAtomicGateway(t *testing.T, executions *store.PostgresStore) (*queue.MemoryQueue, *timer.Service, *decider.Decider, *lease.Manager, *gateway.Gateway) {
	t.Helper()
	workQueue := queue.NewMemoryQueue(16)
	timers := timer.NewService(16)
	engine := decider.NewWithTimer(executions, timers)
	leases, err := lease.NewManagerWithStateStore(workQueue, timers, 16, executions)
	if err != nil {
		t.Fatal(err)
	}
	api, err := gateway.New(engine, executions, workQueue, leases)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		leases.Close()
		engine.Close()
		timers.Close()
		_ = workQueue.Close()
	})
	return workQueue, timers, engine, leases, api
}

func claimAtomicLease(t *testing.T, ctx context.Context, executions *store.PostgresStore, workflowID string, task domain.TaskInstance, duration time.Duration) lease.Lease {
	t.Helper()
	active, err := executions.Claim(ctx, lease.Lease{
		ID: domain.NewID("lease"), DeliveryID: domain.NewID("delivery"),
		Item: queue.WorkItem{ID: domain.NewID("work"), WorkflowID: workflowID, TaskID: task.ID, NodeID: task.NodeID, TaskName: task.TaskName},
	}, duration)
	if err != nil {
		t.Fatal(err)
	}
	return active
}

func domainInstanceForLeaseTest(t *testing.T, executions *store.PostgresStore) *domain.WorkflowInstance {
	t.Helper()
	instance, err := domain.NewWorkflowInstance(domain.WorkflowDef{
		Name: "database-clock", Version: 1, Start: []string{"run"},
		Nodes: map[string]domain.NodeDefinition{"run": {ID: "run", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "Run"}}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := executions.Create(instance); err != nil {
		t.Fatal(err)
	}
	if err := executions.AppendMany(instance.ID, []domain.Event{
		{WorkflowID: instance.ID, Type: domain.EventWorkflowStarted},
		{WorkflowID: instance.ID, TaskID: "clock-task", NodeID: "run", Type: domain.EventTaskScheduled, Payload: map[string]any{"task_name": "Run", "attempt": 1}},
	}); err != nil {
		t.Fatal(err)
	}
	return instance
}
