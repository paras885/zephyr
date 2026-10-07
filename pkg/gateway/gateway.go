package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/zephyr-workflow/zephyr/pkg/auth"
	"github.com/zephyr-workflow/zephyr/pkg/decider"
	"github.com/zephyr-workflow/zephyr/pkg/domain"
	"github.com/zephyr-workflow/zephyr/pkg/lease"
	"github.com/zephyr-workflow/zephyr/pkg/queue"
	"github.com/zephyr-workflow/zephyr/pkg/store"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const (
	WorkflowInstancesPath = "/v1/workflows/"
	TaskReceivePath       = "/v1/tasks/receive"
	TaskPollPath          = "/v1/tasks/poll"
	TaskHeartbeatPath     = "/v1/tasks/heartbeat"
	TaskCompletePath      = "/v1/tasks/complete"
	TaskFailPath          = "/v1/tasks/fail"
)

var (
	ErrWorkflowNotRegistered = errors.New("workflow is not registered")
	ErrInvalidTaskLease      = errors.New("task lease does not match request")
)

type WorkflowSummary struct {
	Name          string `json:"name"`
	LatestVersion int    `json:"latest_version"`
	Versions      []int  `json:"versions"`
	NodeCount     int    `json:"node_count"`
	TaskCount     int    `json:"task_count"`
}

type WorkflowRunSummary struct {
	ID             string                `json:"id"`
	WorkflowName   string                `json:"workflow_name"`
	Version        int                   `json:"version"`
	Status         domain.WorkflowStatus `json:"status"`
	TaskCount      int                   `json:"task_count"`
	CompletedTasks int                   `json:"completed_tasks"`
	FailedTasks    int                   `json:"failed_tasks"`
	StartedAt      time.Time             `json:"started_at,omitempty"`
	UpdatedAt      time.Time             `json:"updated_at,omitempty"`
}

type WorkflowRunPage struct {
	Items  []WorkflowRunSummary `json:"items"`
	Total  int                  `json:"total"`
	Limit  int                  `json:"limit"`
	Offset int                  `json:"offset"`
}

type WorkflowMetrics struct {
	Total        int `json:"total"`
	Running      int `json:"running"`
	Completed    int `json:"completed"`
	Failed       int `json:"failed"`
	Compensating int `json:"compensating"`
	Compensated  int `json:"compensated"`
	Pending      int `json:"pending"`
}

type WorkflowRunDetail struct {
	ID            string                `json:"id"`
	WorkflowName  string                `json:"workflow_name"`
	Version       int                   `json:"version"`
	Status        domain.WorkflowStatus `json:"status"`
	FailureReason string                `json:"failure_reason,omitempty"`
	Context       map[string]any        `json:"context"`
	Result        map[string]any        `json:"result,omitempty"`
	Tasks         []WorkflowTaskSummary `json:"tasks"`
	Events        []domain.Event        `json:"events"`
	StartedAt     time.Time             `json:"started_at,omitempty"`
	UpdatedAt     time.Time             `json:"updated_at,omitempty"`
	NextSequence  uint64                `json:"next_sequence"`
}

type WorkflowTaskSummary struct {
	ID             string            `json:"id"`
	NodeID         string            `json:"node_id"`
	TaskName       string            `json:"task_name"`
	Status         domain.TaskStatus `json:"status"`
	Input          map[string]any    `json:"input,omitempty"`
	Result         map[string]any    `json:"result,omitempty"`
	Error          string            `json:"error,omitempty"`
	LeaseToken     uint64            `json:"lease_token,omitempty"`
	CompletedAt    time.Time         `json:"completed_at,omitempty"`
	IsCompensation bool              `json:"is_compensation,omitempty"`
	OriginalNodeID string            `json:"original_node_id,omitempty"`
}

// WorkflowService is the workflow-facing contract implemented by Gateway.
type WorkflowService interface {
	RegisterWorkflow(definition domain.WorkflowDef) error
	StartWorkflow(ctx context.Context, name string, version int, workflowContext map[string]any) (*domain.WorkflowInstance, error)
}

// TaskService is the worker-facing contract implemented by Gateway.
type TaskService interface {
	ReceiveWork(ctx context.Context, workerID string, leaseDuration time.Duration) (WorkDelivery, error)
	HeartbeatWork(ctx context.Context, heartbeat TaskHeartbeat) error
	CompleteWork(ctx context.Context, completion TaskCompletion) error
	FailWork(ctx context.Context, failure TaskFailure) error
}

type WorkDelivery struct {
	DeliveryID string         `json:"delivery_id"`
	LeaseID    string         `json:"lease_id"`
	LeaseToken uint64         `json:"lease_token"`
	Item       queue.WorkItem `json:"item"`
}

type TaskCompletion struct {
	WorkflowID string         `json:"workflow_id"`
	TaskID     string         `json:"task_id"`
	NodeID     string         `json:"node_id"`
	LeaseID    string         `json:"lease_id"`
	LeaseToken uint64         `json:"lease_token"`
	Result     map[string]any `json:"result"`
}

type TaskFailure struct {
	WorkflowID string `json:"workflow_id"`
	TaskID     string `json:"task_id"`
	NodeID     string `json:"node_id"`
	LeaseID    string `json:"lease_id"`
	LeaseToken uint64 `json:"lease_token"`
	Error      string `json:"error"`
}

type TaskHeartbeat struct {
	LeaseID         string `json:"lease_id"`
	LeaseToken      uint64 `json:"lease_token"`
	LeaseDurationMS int64  `json:"lease_duration_ms"`
}

type StartWorkflowRequest struct {
	Version int            `json:"version"`
	Context map[string]any `json:"context"`
}

type ReceiveWorkRequest struct {
	WorkerID        string `json:"worker_id"`
	LeaseDurationMS int64  `json:"lease_duration_ms"`
}

type Gateway struct {
	engine     *decider.Decider
	executions store.ExecutionStore
	workQueue  queue.QueueDAO
	leases     *lease.Manager

	mu           sync.Mutex
	publications *TaskPublicationDispatcher
}

func New(engine *decider.Decider, executions store.ExecutionStore, workQueue queue.QueueDAO, leases *lease.Manager) (*Gateway, error) {
	if engine == nil {
		return nil, fmt.Errorf("decider is required")
	}
	if executions == nil {
		return nil, fmt.Errorf("execution store is required")
	}
	if workQueue == nil {
		return nil, fmt.Errorf("work queue is required")
	}
	if leases == nil {
		return nil, fmt.Errorf("lease manager is required")
	}
	publicationStore, ok := executions.(store.TaskPublicationStore)
	if !ok {
		return nil, fmt.Errorf("execution store does not support task publication outbox")
	}
	publications, err := NewTaskPublicationDispatcher(publicationStore, workQueue, TaskPublicationDispatcherConfig{})
	if err != nil {
		return nil, err
	}
	gateway := &Gateway{
		engine:       engine,
		executions:   executions,
		workQueue:    workQueue,
		leases:       leases,
		publications: publications,
	}
	engine.SetRetryPublisher(func(workflowID string) error {
		return gateway.dispatchTaskPublications(context.Background())
	})
	leases.SetExpirationHandler(func(ctx context.Context, expiration lease.Expiration, mutation lease.MutationStore) error {
		activeLease := expiration.Lease
		var instanceStore interface {
			Get(string) (*domain.WorkflowInstance, error)
		} = executions
		if mutation != nil {
			instanceStore = mutation
		}
		instance, err := instanceStore.Get(activeLease.Item.WorkflowID)
		if err != nil {
			return err
		}
		task, exists := instance.Tasks[expiration.Lease.Item.NodeID]
		if !exists || task.ID != activeLease.Item.TaskID {
			return ErrInvalidTaskLease
		}
		switch task.Status {
		case domain.TaskCompleted, domain.TaskFailed, domain.TaskRetrying, domain.TaskSkipped:
			return nil
		default:
			return engine.FailTaskWithMutation(activeLease.Item.WorkflowID, activeLease.Item.NodeID, "task lease expired", mutation)
		}
	})
	return gateway, nil
}

func (gateway *Gateway) RegisterWorkflow(definition domain.WorkflowDef) error {
	if err := definition.Validate(); err != nil {
		return err
	}
	registry, ok := gateway.executions.(store.WorkflowRegistry)
	if !ok {
		return fmt.Errorf("execution store does not support workflow registration")
	}
	return registry.PutWorkflow(context.Background(), store.RegisteredWorkflow{Definition: definition})
}

func (gateway *Gateway) ListWorkflows(ctx context.Context) ([]WorkflowSummary, error) {
	registry, ok := gateway.executions.(store.WorkflowRegistry)
	if !ok {
		return nil, fmt.Errorf("execution store does not support workflow registration")
	}
	records, err := registry.ListDefinitions(ctx)
	if err != nil {
		return nil, err
	}
	definitions := make(map[string]map[int]domain.WorkflowDef)
	for _, record := range records {
		definition := record.Definition
		if definitions[definition.Name] == nil {
			definitions[definition.Name] = make(map[int]domain.WorkflowDef)
		}
		definitions[definition.Name][definition.Version] = definition
	}
	summaries := make([]WorkflowSummary, 0, len(definitions))
	for name, versions := range definitions {
		summary := WorkflowSummary{Name: name}
		for version, definition := range versions {
			summary.Versions = append(summary.Versions, version)
			if version > summary.LatestVersion {
				summary.LatestVersion = version
				summary.NodeCount = len(definition.Nodes)
				summary.TaskCount = 0
				for _, node := range definition.Nodes {
					if node.Type == domain.NodeTask {
						summary.TaskCount++
					}
				}
			}
		}
		sort.Ints(summary.Versions)
		summaries = append(summaries, summary)
	}
	sort.Slice(summaries, func(left, right int) bool { return summaries[left].Name < summaries[right].Name })
	return summaries, nil
}

func (gateway *Gateway) WorkflowDefinition(name string, version int) (domain.WorkflowDef, error) {
	return gateway.definition(context.Background(), name, version)
}

func (gateway *Gateway) ListWorkflowRuns(ctx context.Context, workflowName string, status domain.WorkflowStatus, limit, offset int) (WorkflowRunPage, error) {
	queryStore, ok := gateway.executions.(store.ExecutionQueryStore)
	if !ok {
		return WorkflowRunPage{}, fmt.Errorf("execution store does not support listing workflows")
	}
	if limit == 0 {
		limit = 25
	}
	if limit < 1 || limit > 100 || offset < 0 {
		return WorkflowRunPage{}, fmt.Errorf("limit must be between 1 and 100 and offset cannot be negative")
	}
	instances, total, err := queryStore.ListExecutions(ctx, store.ExecutionFilter{
		WorkflowName: workflowName,
		Status:       status,
		Limit:        limit,
		Offset:       offset,
	})
	if err != nil {
		return WorkflowRunPage{}, err
	}
	items := make([]WorkflowRunSummary, 0, len(instances))
	for _, instance := range instances {
		items = append(items, summarizeRun(instance))
	}
	return WorkflowRunPage{Items: items, Total: total, Limit: limit, Offset: offset}, nil
}

func (gateway *Gateway) WorkflowMetrics(ctx context.Context) (WorkflowMetrics, error) {
	queryStore, ok := gateway.executions.(store.ExecutionQueryStore)
	if !ok {
		return WorkflowMetrics{}, fmt.Errorf("execution store does not support listing workflows")
	}
	counts, err := queryStore.CountExecutions(ctx)
	if err != nil {
		return WorkflowMetrics{}, err
	}
	metrics := WorkflowMetrics{
		Running: counts[domain.WorkflowRunning], Completed: counts[domain.WorkflowCompleted],
		Failed: counts[domain.WorkflowFailed], Compensating: counts[domain.WorkflowCompensating],
		Compensated: counts[domain.WorkflowCompensated], Pending: counts[domain.WorkflowPending],
	}
	metrics.Total = metrics.Running + metrics.Completed + metrics.Failed + metrics.Compensating + metrics.Compensated + metrics.Pending
	return metrics, nil
}

func (gateway *Gateway) WorkflowRun(ctx context.Context, workflowID string) (WorkflowRunDetail, error) {
	if err := ctx.Err(); err != nil {
		return WorkflowRunDetail{}, err
	}
	instance, err := gateway.executions.Get(workflowID)
	if err != nil {
		return WorkflowRunDetail{}, err
	}
	detail := WorkflowRunDetail{
		ID:            instance.ID,
		WorkflowName:  instance.Definition.Name,
		Version:       instance.Definition.Version,
		Status:        instance.Status,
		FailureReason: instance.FailureReason,
		Context:       instance.Context,
		Result:        instance.Result,
		Events:        instance.Events,
		NextSequence:  instance.NextSequence,
	}
	for _, task := range instance.Tasks {
		detail.Tasks = append(detail.Tasks, WorkflowTaskSummary{
			ID: task.ID, NodeID: task.NodeID, TaskName: task.TaskName, Status: task.Status,
			Input: task.Input, Result: task.Result, Error: task.Error, LeaseToken: task.LeaseToken, CompletedAt: task.CompletedAt,
			IsCompensation: task.IsCompensation, OriginalNodeID: task.OriginalNodeID,
		})
	}
	sort.Slice(detail.Tasks, func(left, right int) bool { return detail.Tasks[left].NodeID < detail.Tasks[right].NodeID })
	if len(instance.Events) > 0 {
		detail.StartedAt = instance.Events[0].OccurredAt
		detail.UpdatedAt = instance.Events[len(instance.Events)-1].OccurredAt
	}
	return detail, nil
}

func summarizeRun(instance *domain.WorkflowInstance) WorkflowRunSummary {
	summary := WorkflowRunSummary{
		ID: instance.ID, WorkflowName: instance.Definition.Name, Version: instance.Definition.Version,
		Status: instance.Status, TaskCount: len(instance.Tasks),
	}
	for _, task := range instance.Tasks {
		switch task.Status {
		case domain.TaskCompleted:
			summary.CompletedTasks++
		case domain.TaskFailed:
			summary.FailedTasks++
		}
	}
	if len(instance.Events) > 0 {
		summary.StartedAt = instance.Events[0].OccurredAt
		summary.UpdatedAt = instance.Events[len(instance.Events)-1].OccurredAt
	}
	return summary
}

func (gateway *Gateway) StartWorkflow(ctx context.Context, name string, version int, workflowContext map[string]any) (*domain.WorkflowInstance, error) {
	ctx, span := otel.Tracer("zephyr/gateway").Start(ctx, "workflow.start", trace.WithAttributes(attribute.String("workflow.name", name), attribute.Int("workflow.version", version)))
	defer span.End()
	definition, err := gateway.definition(ctx, name, version)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "workflow definition lookup failed")
		return nil, err
	}
	instance, err := gateway.engine.Start(definition, workflowContext)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "workflow start failed")
		return nil, err
	}
	span.SetAttributes(attribute.String("workflow.id", instance.ID))
	if err := gateway.dispatchTaskPublications(ctx); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "task publication dispatch failed")
		return nil, err
	}
	return instance, nil
}

func (gateway *Gateway) StartWorkflowIdempotent(ctx context.Context, name string, version int, workflowContext map[string]any, key string) (*domain.WorkflowInstance, error) {
	ctx, span := otel.Tracer("zephyr/gateway").Start(ctx, "workflow.start_idempotent", trace.WithAttributes(attribute.String("workflow.name", name), attribute.Int("workflow.version", version)))
	defer span.End()
	key = strings.TrimSpace(key)
	if key == "" {
		return nil, fmt.Errorf("Idempotency-Key must not be empty")
	}
	definition, err := gateway.definition(ctx, name, version)
	if err != nil {
		return nil, err
	}
	if workflowContext == nil {
		workflowContext = map[string]any{}
	}
	requestBody, err := json.Marshal(struct {
		Version int            `json:"version"`
		Context map[string]any `json:"context"`
	}{Version: definition.Version, Context: workflowContext})
	if err != nil {
		return nil, fmt.Errorf("encode workflow start request: %w", err)
	}
	requestHash := fmt.Sprintf("%x", sha256.Sum256(requestBody))
	idempotencyStore, ok := gateway.executions.(store.IdempotencyStore)
	if !ok {
		return nil, fmt.Errorf("execution store does not support idempotent workflow starts")
	}
	instance, err := domain.NewWorkflowInstance(definition, workflowContext)
	if err != nil {
		return nil, err
	}
	created, existingID, err := idempotencyStore.CreateIdempotent(ctx, instance, key, requestHash)
	if err != nil {
		return nil, err
	}
	if created {
		instance, err = gateway.engine.StartExisting(instance)
	} else {
		instance, err = gateway.executions.Get(existingID)
		if err == nil && instance.Status == domain.WorkflowPending {
			instance, err = gateway.engine.StartExisting(instance)
		}
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "idempotent workflow start failed")
		return nil, err
	}
	span.SetAttributes(attribute.String("workflow.id", instance.ID))
	if err := gateway.dispatchTaskPublications(ctx); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "task publication dispatch failed")
		return nil, err
	}
	return instance, nil
}

func (gateway *Gateway) ReceiveWork(ctx context.Context, workerID string, leaseDuration time.Duration) (WorkDelivery, error) {
	ctx, span := otel.Tracer("zephyr/gateway").Start(ctx, "worker.receive", trace.WithAttributes(attribute.String("worker.id", workerID)))
	defer span.End()
	if workerID == "" {
		return WorkDelivery{}, fmt.Errorf("worker ID is required")
	}
	if leaseDuration <= 0 {
		return WorkDelivery{}, fmt.Errorf("lease duration must be positive")
	}
	for {
		delivery, err := gateway.workQueue.Receive(ctx, workerID)
		if err != nil {
			if ctx.Err() == nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, "receive work failed")
			}
			return WorkDelivery{}, err
		}
		activeLease, err := gateway.leases.Acquire(ctx, delivery, leaseDuration)
		if err != nil {
			if gateway.leases.UsesSharedStateStore() && errors.Is(err, lease.ErrLeaseConflict) {
				if ackErr := gateway.workQueue.Ack(ctx, delivery.ID); ackErr != nil {
					return WorkDelivery{}, fmt.Errorf("ack duplicate leased task: %w", ackErr)
				}
				continue
			}
			_ = gateway.workQueue.Reject(context.Background(), delivery.ID, true)
			return WorkDelivery{}, err
		}
		if gateway.leases.UsesSharedStateStore() {
			if err := gateway.workQueue.Ack(ctx, delivery.ID); err != nil {
				return WorkDelivery{}, fmt.Errorf("ack task after durable lease claim: %w", err)
			}
		}
		if err := gateway.engine.StartTask(delivery.Item.WorkflowID, delivery.Item.NodeID, activeLease.Token); err != nil {
			if !gateway.leases.UsesSharedStateStore() {
				_ = gateway.leases.Complete(context.Background(), activeLease.ID, activeLease.Token)
			}
			return WorkDelivery{}, err
		}
		slog.InfoContext(ctx, "task work leased",
			"worker_id", workerID, "workflow_id", activeLease.Item.WorkflowID,
			"task_id", activeLease.Item.TaskID, "node_id", activeLease.Item.NodeID,
			"lease_id", activeLease.ID, "fencing_token", activeLease.Token,
		)
		return WorkDelivery{
			DeliveryID: delivery.ID,
			LeaseID:    activeLease.ID,
			LeaseToken: activeLease.Token,
			Item:       activeLease.Item,
		}, nil
	}
}

func (gateway *Gateway) CompleteWork(ctx context.Context, completion TaskCompletion) error {
	ctx, span := otel.Tracer("zephyr/gateway").Start(ctx, "task.complete", trace.WithAttributes(
		attribute.String("workflow.id", completion.WorkflowID), attribute.String("task.id", completion.TaskID),
		attribute.String("lease.id", completion.LeaseID), attribute.Int64("lease.fencing_token", int64(completion.LeaseToken)),
	))
	defer span.End()
	return gateway.finishTask(ctx, completion.WorkflowID, completion.TaskID, completion.NodeID, completion.LeaseID, completion.LeaseToken, func(activeLease lease.Lease, mutation lease.MutationStore) error {
		if activeLease.Item.WorkflowID != completion.WorkflowID || activeLease.Item.TaskID != completion.TaskID || activeLease.Item.NodeID != completion.NodeID || activeLease.Token != completion.LeaseToken {
			return ErrInvalidTaskLease
		}
		var executionStore interface {
			Get(string) (*domain.WorkflowInstance, error)
		} = gateway.executions
		if mutation != nil {
			executionStore = mutation
		}
		instance, err := executionStore.Get(completion.WorkflowID)
		if err != nil {
			return err
		}
		task, exists := instance.Tasks[completion.NodeID]
		if !exists || task.ID != completion.TaskID {
			return ErrInvalidTaskLease
		}
		if task.Status == domain.TaskCompleted {
			if !reflect.DeepEqual(task.Result, completion.Result) {
				return ErrInvalidTaskLease
			}
			return nil
		}
		if task.Status != domain.TaskReady && task.Status != domain.TaskRunning {
			return ErrInvalidTaskLease
		}
		return gateway.engine.CompleteTaskWithMutation(completion.WorkflowID, completion.NodeID, completion.Result, mutation)
	})
}

func (gateway *Gateway) HeartbeatWork(ctx context.Context, heartbeat TaskHeartbeat) error {
	ctx, span := otel.Tracer("zephyr/gateway").Start(ctx, "task.heartbeat", trace.WithAttributes(
		attribute.String("lease.id", heartbeat.LeaseID), attribute.Int64("lease.fencing_token", int64(heartbeat.LeaseToken)),
	))
	defer span.End()
	if heartbeat.LeaseID == "" || heartbeat.LeaseToken == 0 {
		return fmt.Errorf("lease ID and lease token are required")
	}
	if heartbeat.LeaseDurationMS <= 0 {
		return fmt.Errorf("lease duration must be positive")
	}
	err := gateway.leases.Heartbeat(ctx, heartbeat.LeaseID, heartbeat.LeaseToken, time.Duration(heartbeat.LeaseDurationMS)*time.Millisecond)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "lease heartbeat failed")
	}
	return err
}

func (gateway *Gateway) FailWork(ctx context.Context, failure TaskFailure) error {
	ctx, span := otel.Tracer("zephyr/gateway").Start(ctx, "task.fail", trace.WithAttributes(
		attribute.String("workflow.id", failure.WorkflowID), attribute.String("task.id", failure.TaskID),
		attribute.String("lease.id", failure.LeaseID), attribute.Int64("lease.fencing_token", int64(failure.LeaseToken)),
	))
	defer span.End()
	return gateway.finishTask(ctx, failure.WorkflowID, failure.TaskID, failure.NodeID, failure.LeaseID, failure.LeaseToken, func(activeLease lease.Lease, mutation lease.MutationStore) error {
		if activeLease.Item.WorkflowID != failure.WorkflowID || activeLease.Item.TaskID != failure.TaskID || activeLease.Item.NodeID != failure.NodeID || activeLease.Token != failure.LeaseToken {
			return ErrInvalidTaskLease
		}
		var executionStore interface {
			Get(string) (*domain.WorkflowInstance, error)
		} = gateway.executions
		if mutation != nil {
			executionStore = mutation
		}
		instance, err := executionStore.Get(failure.WorkflowID)
		if err != nil {
			return err
		}
		task, exists := instance.Tasks[failure.NodeID]
		if !exists || task.ID != failure.TaskID {
			return ErrInvalidTaskLease
		}
		switch task.Status {
		case domain.TaskCompleted:
			return nil
		case domain.TaskFailed, domain.TaskRetrying:
			if task.Error != failure.Error {
				return ErrInvalidTaskLease
			}
			return nil
		case domain.TaskReady, domain.TaskRunning:
		default:
			return ErrInvalidTaskLease
		}
		return gateway.engine.FailTaskWithMutation(failure.WorkflowID, failure.NodeID, failure.Error, mutation)
	})
}

func (gateway *Gateway) RunCompletionConsumer(ctx context.Context, completions queue.CompletionQueue, consumerID string) error {
	if ctx == nil {
		return fmt.Errorf("completion consumer context is required")
	}
	if completions == nil {
		return fmt.Errorf("completion queue is required")
	}
	if consumerID == "" {
		return fmt.Errorf("completion consumer ID is required")
	}
	for {
		delivery, err := completions.ReceiveCompletion(ctx, consumerID)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("receive completion message: %w", err)
		}
		if err := delivery.Message.Validate(); err != nil {
			if rejectErr := completions.RejectCompletion(context.Background(), delivery.ID, false); rejectErr != nil {
				return fmt.Errorf("reject invalid completion message: %w", rejectErr)
			}
			continue
		}
		if err := gateway.applyCompletion(ctx, delivery.Message); err != nil {
			if isStaleCompletion(err) {
				if ackErr := completions.AckCompletion(context.Background(), delivery.ID); ackErr != nil {
					return fmt.Errorf("ack stale completion message: %w", ackErr)
				}
				continue
			}
			if rejectErr := completions.RejectCompletion(context.Background(), delivery.ID, true); rejectErr != nil {
				return fmt.Errorf("requeue completion after processing error %v: %w", err, rejectErr)
			}
			return fmt.Errorf("apply completion message: %w", err)
		}
		if err := completions.AckCompletion(context.Background(), delivery.ID); err != nil {
			return fmt.Errorf("ack completion message: %w", err)
		}
	}
}

func (gateway *Gateway) applyCompletion(ctx context.Context, message queue.CompletionMessage) error {
	ctx, span := otel.Tracer("zephyr/gateway").Start(ctx, "completion.apply", trace.WithAttributes(
		attribute.String("workflow.id", message.WorkflowID), attribute.String("task.id", message.TaskID),
		attribute.String("lease.id", message.LeaseID), attribute.String("completion.kind", string(message.Kind)),
	))
	defer span.End()
	switch message.Kind {
	case queue.CompletionSucceeded:
		return gateway.CompleteWork(ctx, TaskCompletion{
			WorkflowID: message.WorkflowID,
			TaskID:     message.TaskID,
			NodeID:     message.NodeID,
			LeaseID:    message.LeaseID,
			LeaseToken: message.LeaseToken,
			Result:     message.Result,
		})
	case queue.CompletionFailed:
		return gateway.FailWork(ctx, TaskFailure{
			WorkflowID: message.WorkflowID,
			TaskID:     message.TaskID,
			NodeID:     message.NodeID,
			LeaseID:    message.LeaseID,
			LeaseToken: message.LeaseToken,
			Error:      message.Error,
		})
	default:
		return fmt.Errorf("unsupported completion kind %q", message.Kind)
	}
}

func isStaleCompletion(err error) bool {
	return errors.Is(err, lease.ErrStaleToken) || errors.Is(err, lease.ErrLeaseExpired) || errors.Is(err, lease.ErrLeaseNotFound) || errors.Is(err, ErrInvalidTaskLease)
}

func (gateway *Gateway) finishTask(ctx context.Context, workflowID, taskID, nodeID, leaseID string, token uint64, operation func(lease.Lease, lease.MutationStore) error) error {
	if workflowID == "" || taskID == "" || nodeID == "" || leaseID == "" || token == 0 {
		return fmt.Errorf("workflow ID, task ID, node ID, lease ID, and lease token are required")
	}
	instance, err := gateway.executions.Get(workflowID)
	if err != nil {
		return err
	}
	task, ok := instance.Tasks[nodeID]
	if !ok || task.ID != taskID {
		return ErrInvalidTaskLease
	}
	if err := gateway.leases.CompleteWith(ctx, leaseID, token, operation); err != nil {
		return err
	}
	slog.InfoContext(ctx, "task lease transition committed",
		"workflow_id", workflowID, "task_id", taskID, "node_id", nodeID,
		"lease_id", leaseID, "fencing_token", token, "attempt", task.Attempt,
	)
	return gateway.dispatchTaskPublications(ctx)
}

func (gateway *Gateway) definition(ctx context.Context, name string, version int) (domain.WorkflowDef, error) {
	record, err := gateway.registeredWorkflow(ctx, name, version)
	return record.Definition, err
}

func (gateway *Gateway) dispatchTaskPublications(ctx context.Context) error {
	_, err := gateway.publications.DispatchOnce(ctx)
	return err
}

func (gateway *Gateway) RunTaskPublicationDispatcher(ctx context.Context) error {
	return gateway.publications.Run(ctx)
}

func (gateway *Gateway) Handler() http.Handler {
	return http.HandlerFunc(gateway.serveHTTP)
}

func (gateway *Gateway) HandlerWithAuth(authenticator auth.BearerAuthenticator) (http.Handler, error) {
	middleware, err := auth.Middleware(authenticator)
	if err != nil {
		return nil, err
	}
	return middleware(gateway.Handler()), nil
}

func (gateway *Gateway) serveHTTP(response http.ResponseWriter, request *http.Request) {
	if request.Method == http.MethodGet {
		gateway.serveGET(response, request)
		return
	}
	if request.Method != http.MethodPost {
		writeError(response, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	ctx := request.Context()
	switch {
	case request.URL.Path == "/v1/workflows/register":
		gateway.serveRegistration(response, request)
	case request.URL.Path == TaskReceivePath || request.URL.Path == TaskPollPath:
		var input ReceiveWorkRequest
		if !decodeJSON(response, request, &input) {
			return
		}
		leaseDuration := 30 * time.Second
		if input.LeaseDurationMS > 0 {
			leaseDuration = time.Duration(input.LeaseDurationMS) * time.Millisecond
		}
		output, err := gateway.ReceiveWork(ctx, input.WorkerID, leaseDuration)
		writeResult(response, output, err)
	case request.URL.Path == TaskCompletePath:
		var input TaskCompletion
		if !decodeJSON(response, request, &input) {
			return
		}
		writeResult(response, map[string]string{"status": "completed"}, gateway.CompleteWork(ctx, input))
	case request.URL.Path == TaskHeartbeatPath:
		var input TaskHeartbeat
		if !decodeJSON(response, request, &input) {
			return
		}
		writeResult(response, map[string]string{"status": "renewed"}, gateway.HeartbeatWork(ctx, input))
	case request.URL.Path == TaskFailPath:
		var input TaskFailure
		if !decodeJSON(response, request, &input) {
			return
		}
		writeResult(response, map[string]string{"status": "failed"}, gateway.FailWork(ctx, input))
	case strings.HasPrefix(request.URL.Path, WorkflowInstancesPath) && strings.HasSuffix(request.URL.Path, "/instances"):
		name := strings.TrimSuffix(strings.TrimPrefix(request.URL.Path, WorkflowInstancesPath), "/instances")
		var input StartWorkflowRequest
		if !decodeJSON(response, request, &input) {
			return
		}
		key := request.Header.Get("Idempotency-Key")
		var instance *domain.WorkflowInstance
		var err error
		if len(request.Header.Values("Idempotency-Key")) > 0 {
			if strings.TrimSpace(key) == "" {
				writeError(response, http.StatusBadRequest, "Idempotency-Key must not be empty")
				return
			}
			instance, err = gateway.StartWorkflowIdempotent(ctx, name, input.Version, input.Context, key)
		} else {
			instance, err = gateway.StartWorkflow(ctx, name, input.Version, input.Context)
		}
		var runID string
		if instance != nil {
			runID = instance.ID
		}
		writeResult(response, map[string]string{"id": runID}, err)
	default:
		writeError(response, http.StatusNotFound, "route not found")
	}
}

func (gateway *Gateway) serveGET(response http.ResponseWriter, request *http.Request) {
	path := request.URL.Path
	switch {
	case path == "/v1/metrics":
		metrics, err := gateway.WorkflowMetrics(request.Context())
		writeResult(response, metrics, err)
	case path == "/v1/workflows":
		workflows, err := gateway.ListWorkflows(request.Context())
		if err != nil {
			slog.Error("workflow catalog listing failed", "error", err)
			err = errWorkflowCatalogUnavailable
		}
		writeResult(response, workflows, err)
	case strings.HasPrefix(path, WorkflowInstancesPath) && strings.HasSuffix(path, "/contracts"):
		gateway.serveContracts(response, request)
	case path == "/v1/instances":
		limit, limitErr := parseQueryInt(request, "limit", 25)
		offset, offsetErr := parseQueryInt(request, "offset", 0)
		if limitErr != nil || offsetErr != nil {
			writeError(response, http.StatusBadRequest, "limit and offset must be integers")
			return
		}
		page, err := gateway.ListWorkflowRuns(request.Context(), request.URL.Query().Get("workflow_name"), domain.WorkflowStatus(request.URL.Query().Get("status")), limit, offset)
		writeResult(response, page, err)
	case strings.HasPrefix(path, WorkflowInstancesPath) && strings.HasSuffix(path, "/instances"):
		name := strings.TrimSuffix(strings.TrimPrefix(path, WorkflowInstancesPath), "/instances")
		limit, limitErr := parseQueryInt(request, "limit", 25)
		offset, offsetErr := parseQueryInt(request, "offset", 0)
		if limitErr != nil || offsetErr != nil {
			writeError(response, http.StatusBadRequest, "limit and offset must be integers")
			return
		}
		page, err := gateway.ListWorkflowRuns(request.Context(), name, domain.WorkflowStatus(request.URL.Query().Get("status")), limit, offset)
		writeResult(response, page, err)
	case strings.HasPrefix(path, WorkflowInstancesPath):
		name := strings.TrimPrefix(path, WorkflowInstancesPath)
		version, err := parseQueryInt(request, "version", 0)
		if err != nil || version < 0 {
			writeError(response, http.StatusBadRequest, "version must be a non-negative integer")
			return
		}
		definition, err := gateway.definition(request.Context(), name, version)
		writeResult(response, definition, err)
	case strings.HasPrefix(path, "/v1/instances/"):
		workflowID := strings.TrimPrefix(path, "/v1/instances/")
		if workflowID == "" || strings.Contains(workflowID, "/") {
			writeError(response, http.StatusNotFound, "route not found")
			return
		}
		detail, err := gateway.WorkflowRun(request.Context(), workflowID)
		writeResult(response, detail, err)
	default:
		writeError(response, http.StatusNotFound, "route not found")
	}
}

func parseQueryInt(request *http.Request, key string, defaultValue int) (int, error) {
	value := request.URL.Query().Get(key)
	if value == "" {
		return defaultValue, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, err
	}
	return parsed, nil
}

func decodeJSON(response http.ResponseWriter, request *http.Request, value any) bool {
	if err := json.NewDecoder(request.Body).Decode(value); err != nil {
		writeError(response, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return false
	}
	return true
}

func writeResult(response http.ResponseWriter, value any, err error) {
	if err != nil {
		status := http.StatusBadRequest
		switch {
		case errors.Is(err, ErrWorkflowNotRegistered):
			status = http.StatusNotFound
		case errors.Is(err, store.ErrDefinitionConflict), errors.Is(err, store.ErrIdempotencyConflict), errors.Is(err, lease.ErrStaleToken), errors.Is(err, lease.ErrLeaseExpired), errors.Is(err, lease.ErrLeaseNotFound), errors.Is(err, ErrInvalidTaskLease):
			status = http.StatusConflict
		case errors.Is(err, queue.ErrClosed), errors.Is(err, errWorkflowCatalogUnavailable):
			status = http.StatusServiceUnavailable
		}
		writeError(response, status, err.Error())
		return
	}
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(response).Encode(value)
}

func writeError(response http.ResponseWriter, status int, message string) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(map[string]string{"error": message})
}

func ParseVersion(value string) int {
	version, _ := strconv.Atoi(value)
	return version
}
