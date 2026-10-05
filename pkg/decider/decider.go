package decider

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/zephyr-workflow/zephyr/pkg/domain"
	"github.com/zephyr-workflow/zephyr/pkg/lease"
	"github.com/zephyr-workflow/zephyr/pkg/store"
	"github.com/zephyr-workflow/zephyr/pkg/timer"
)

const (
	recoveryPageSize   = 100
	maxConflictRetries = 8
)

type Decider struct {
	store          executionStore
	timers         *timer.Service
	timerEvents    <-chan timer.Deadline
	unsubscribe    func()
	retryPublisher func(string) error
	done           chan struct{}
	close          sync.Once
}

type executionStore interface {
	Get(workflowID string) (*domain.WorkflowInstance, error)
	Append(workflowID string, event domain.Event) error
	AppendMany(workflowID string, events []domain.Event) error
}

type taskRetryReference struct {
	WorkflowID string
	NodeID     string
	Attempt    int
}

func New(store store.ExecutionStore) *Decider {
	return &Decider{store: store}
}

func NewWithTimer(store store.ExecutionStore, timers *timer.Service) *Decider {
	decider := &Decider{store: store, timers: timers, done: make(chan struct{})}
	if timers != nil {
		decider.timerEvents, decider.unsubscribe = timers.Subscribe(256)
		go decider.consumeTimers()
	}
	return decider
}

func (decider *Decider) SetRetryPublisher(publish func(workflowID string) error) {
	decider.retryPublisher = publish
}

func (decider *Decider) Close() {
	decider.close.Do(func() {
		if decider.done != nil {
			close(decider.done)
		}
		if decider.unsubscribe != nil {
			decider.unsubscribe()
		}
	})
}

func (decider *Decider) Start(definition domain.WorkflowDef, context map[string]any) (*domain.WorkflowInstance, error) {
	instance, err := domain.NewWorkflowInstance(definition, context)
	if err != nil {
		return nil, err
	}
	creator, ok := decider.store.(interface {
		Create(*domain.WorkflowInstance) error
	})
	if !ok {
		return nil, fmt.Errorf("execution store does not support workflow creation")
	}
	if err := creator.Create(instance); err != nil {
		return nil, err
	}
	return decider.StartExisting(instance)
}

func (decider *Decider) StartExisting(instance *domain.WorkflowInstance) (*domain.WorkflowInstance, error) {
	if instance == nil {
		return nil, fmt.Errorf("workflow instance is required")
	}
	var updated *domain.WorkflowInstance
	err := decider.withWorkflowLock(instance.ID, func() error {
		var err error
		updated, err = decider.startExistingLocked(instance.ID)
		return err
	})
	return updated, err
}

func (decider *Decider) startExistingLocked(workflowID string) (*domain.WorkflowInstance, error) {
	for attempt := 0; attempt < maxConflictRetries; attempt++ {
		current, err := decider.store.Get(workflowID)
		if err != nil {
			return nil, err
		}
		if current.Status == domain.WorkflowPending {
			err = decider.store.Append(workflowID, domain.Event{
				WorkflowID: workflowID,
				Type:       domain.EventWorkflowStarted,
			})
			if errors.Is(err, store.ErrConflict) {
				continue
			}
			if err != nil {
				return nil, err
			}
		}
		if err := decider.advanceUnlocked(workflowID); err != nil {
			return nil, err
		}
		return decider.store.Get(workflowID)
	}
	return nil, store.ErrConflict
}

func (decider *Decider) Recover(ctx context.Context) error {
	queryStore, ok := decider.store.(store.ExecutionQueryStore)
	if !ok {
		return fmt.Errorf("execution store does not support recovery queries")
	}
	statuses := []domain.WorkflowStatus{domain.WorkflowPending, domain.WorkflowRunning, domain.WorkflowCompensating}
	executions := make([]*domain.WorkflowInstance, 0)
	for _, status := range statuses {
		for offset := 0; ; {
			if err := ctx.Err(); err != nil {
				return err
			}
			page, total, err := queryStore.ListExecutions(ctx, store.ExecutionFilter{
				Status: status, Limit: recoveryPageSize, Offset: offset,
			})
			if err != nil {
				return err
			}
			executions = append(executions, page...)
			offset += len(page)
			if offset >= total || len(page) == 0 {
				break
			}
		}
	}
	for _, instance := range executions {
		if err := ctx.Err(); err != nil {
			return err
		}
		if instance.Status == domain.WorkflowPending {
			if _, err := decider.StartExisting(instance); err != nil {
				return fmt.Errorf("resume workflow %q: %w", instance.ID, err)
			}
			continue
		}
		if err := decider.scheduleRetries(instance); err != nil {
			return fmt.Errorf("recover retries for workflow %q: %w", instance.ID, err)
		}
		if err := decider.schedulePendingDelays(instance); err != nil {
			return fmt.Errorf("recover delays for workflow %q: %w", instance.ID, err)
		}
		if instance.Status == domain.WorkflowRunning {
			if err := decider.advance(instance.ID); err != nil {
				return fmt.Errorf("advance recovered workflow %q: %w", instance.ID, err)
			}
		} else if instance.Status == domain.WorkflowCompensating && !hasActiveTaskWork(instance) {
			if err := decider.withWorkflowLock(instance.ID, func() error {
				current, err := decider.store.Get(instance.ID)
				if err != nil {
					return err
				}
				if current.Status == domain.WorkflowCompensating && !hasActiveTaskWork(current) {
					return decider.scheduleNextCompensation(instance.ID)
				}
				return nil
			}); err != nil {
				return fmt.Errorf("resume compensation for workflow %q: %w", instance.ID, err)
			}
		}
	}
	return nil
}

func (decider *Decider) RunRecovery(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		return fmt.Errorf("recovery interval must be positive")
	}
	if err := decider.Recover(ctx); err != nil {
		return err
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := decider.Recover(ctx); err != nil {
				return err
			}
		}
	}
}

func (decider *Decider) StartTask(workflowID, nodeID string, leaseToken uint64) error {
	return decider.withWorkflowLock(workflowID, func() error {
		return decider.startTaskLocked(workflowID, nodeID, leaseToken)
	})
}

func (decider *Decider) startTaskLocked(workflowID, nodeID string, leaseToken uint64) error {
	instance, err := decider.store.Get(workflowID)
	if err != nil {
		return err
	}
	task, ok := instance.Tasks[nodeID]
	if !ok {
		return fmt.Errorf("task node %q is not scheduled", nodeID)
	}
	if task.Status != domain.TaskReady {
		return fmt.Errorf("task node %q has status %s", nodeID, task.Status)
	}
	return decider.store.Append(workflowID, domain.Event{
		WorkflowID: workflowID,
		TaskID:     task.ID,
		NodeID:     nodeID,
		Type:       domain.EventTaskStarted,
		Payload:    map[string]any{"lease_token": leaseToken},
	})
}

func (decider *Decider) CompleteTask(workflowID, nodeID string, result map[string]any) error {
	return decider.withWorkflowLock(workflowID, func() error {
		return decider.completeTaskLocked(workflowID, nodeID, result)
	})
}

func (decider *Decider) CompleteTaskWithMutation(workflowID, nodeID string, result map[string]any, mutation lease.MutationStore) error {
	if mutation == nil {
		return decider.CompleteTask(workflowID, nodeID, result)
	}
	return decider.withWorkflowLock(workflowID, func() error {
		return decider.withStore(mutation).completeTaskLocked(workflowID, nodeID, result)
	})
}

func (decider *Decider) completeTaskLocked(workflowID, nodeID string, result map[string]any) error {
	instance, err := decider.store.Get(workflowID)
	if err != nil {
		return err
	}
	task, ok := instance.Tasks[nodeID]
	if !ok {
		return fmt.Errorf("task node %q is not scheduled", nodeID)
	}
	if task.Status != domain.TaskReady && task.Status != domain.TaskRunning {
		return fmt.Errorf("task node %q has status %s", nodeID, task.Status)
	}
	isCompensation := task.IsCompensation
	if err := decider.store.Append(workflowID, domain.Event{
		WorkflowID: workflowID,
		TaskID:     task.ID,
		NodeID:     nodeID,
		Type:       domain.EventTaskCompleted,
		Payload:    map[string]any{"result": result},
	}); err != nil {
		return err
	}
	if isCompensation {
		return decider.scheduleNextCompensation(workflowID)
	}
	return decider.advanceUnlocked(workflowID)
}

func (decider *Decider) FailTask(workflowID, nodeID, taskError string) error {
	return decider.withWorkflowLock(workflowID, func() error {
		return decider.failTaskLocked(workflowID, nodeID, taskError)
	})
}

func (decider *Decider) FailTaskWithMutation(workflowID, nodeID, taskError string, mutation lease.MutationStore) error {
	if mutation == nil {
		return decider.FailTask(workflowID, nodeID, taskError)
	}
	return decider.withWorkflowLock(workflowID, func() error {
		return decider.withStore(mutation).failTaskLocked(workflowID, nodeID, taskError)
	})
}

func (decider *Decider) failTaskLocked(workflowID, nodeID, taskError string) error {
	instance, err := decider.store.Get(workflowID)
	if err != nil {
		return err
	}
	task, ok := instance.Tasks[nodeID]
	if !ok {
		return fmt.Errorf("task node %q is not scheduled", nodeID)
	}
	if task.Status != domain.TaskReady && task.Status != domain.TaskRunning {
		return fmt.Errorf("task node %q has status %s", nodeID, task.Status)
	}
	if task.Attempt <= task.RetryLimit {
		retryAt := time.Now().Add(task.RetryBackoff)
		if err := decider.store.AppendMany(workflowID, []domain.Event{
			{WorkflowID: workflowID, TaskID: task.ID, NodeID: nodeID, Type: domain.EventTaskFailed, Payload: map[string]any{"error": taskError}},
			{WorkflowID: workflowID, TaskID: task.ID, NodeID: nodeID, Type: domain.EventTaskRetryScheduled, Payload: map[string]any{"error": taskError, "retry_at": retryAt, "attempt": task.Attempt + 1}},
		}); err != nil {
			return err
		}
		if task.IsCompensation {
			updated, err := decider.store.Get(workflowID)
			if err != nil {
				return err
			}
			return decider.scheduleRetries(updated)
		}
		return decider.advanceUnlocked(workflowID)
	}
	if task.IsCompensation {
		return decider.store.AppendMany(workflowID, []domain.Event{
			{WorkflowID: workflowID, TaskID: task.ID, NodeID: nodeID, Type: domain.EventTaskFailed, Payload: map[string]any{"error": taskError}},
			{WorkflowID: workflowID, Type: domain.EventWorkflowFailed, Payload: map[string]any{"error": "compensation failed: " + taskError}},
		})
	}
	if task.FanOutID != "" {
		if err := decider.store.AppendMany(workflowID, []domain.Event{
			{WorkflowID: workflowID, TaskID: task.ID, NodeID: nodeID, Type: domain.EventTaskFailed, Payload: map[string]any{"error": taskError}},
			{WorkflowID: workflowID, NodeID: nodeID, Type: domain.EventFanOutItemFailed, Payload: map[string]any{
				"fan_out_id": task.FanOutID, "item_index": task.FanOutIndex,
				"fan_out_ancestors": task.FanOutAncestors, "error": taskError,
			}},
		}); err != nil {
			return err
		}
		return decider.advanceUnlocked(workflowID)
	}
	if err := decider.store.AppendMany(workflowID, []domain.Event{
		{WorkflowID: workflowID, TaskID: task.ID, NodeID: nodeID, Type: domain.EventTaskFailed, Payload: map[string]any{"error": taskError}},
		{WorkflowID: workflowID, Type: domain.EventWorkflowFailureRequested, Payload: map[string]any{"error": taskError}},
	}); err != nil {
		return err
	}
	return decider.advanceUnlocked(workflowID)
}

func (decider *Decider) scheduleNextCompensation(workflowID string) error {
	instance, err := decider.store.Get(workflowID)
	if err != nil {
		return err
	}
	compensated := make(map[string]bool)
	for _, task := range instance.Tasks {
		if task.IsCompensation && task.Status == domain.TaskCompleted {
			compensated[task.OriginalNodeID] = true
		}
	}
	candidates := compensationNodes(instance)
	for _, originalNodeID := range candidates {
		if compensated[originalNodeID] {
			continue
		}
		originalNode, ok := lookupNode(instance, originalNodeID)
		if !ok {
			return decider.store.Append(workflowID, domain.Event{WorkflowID: workflowID, Type: domain.EventWorkflowFailed, Payload: map[string]any{"error": fmt.Sprintf("compensation source node %q not found", originalNodeID)}})
		}
		input := map[string]any{}
		if originalNode.CompensationInput != nil {
			value, err := evaluateExpression(originalNode.CompensationInput, instance, originalNode.RuntimeScope)
			if err != nil {
				return decider.store.Append(workflowID, domain.Event{
					WorkflowID: workflowID,
					Type:       domain.EventWorkflowFailed,
					Payload:    map[string]any{"error": fmt.Sprintf("evaluate compensation input for %q: %v", originalNodeID, err)},
				})
			}
			var ok bool
			input, ok = value.(map[string]any)
			if !ok {
				return decider.store.Append(workflowID, domain.Event{
					WorkflowID: workflowID,
					Type:       domain.EventWorkflowFailed,
					Payload:    map[string]any{"error": fmt.Sprintf("compensation input for %q returned %T, want object", originalNodeID, value)},
				})
			}
		}
		return decider.store.Append(workflowID, domain.Event{
			WorkflowID: workflowID,
			TaskID:     domain.NewID("compensation-task"),
			NodeID:     domain.NewID("compensation-node"),
			Type:       domain.EventCompensationScheduled,
			Payload: map[string]any{
				"task_name":        originalNode.Compensation.Name,
				"input":            input,
				"original_node_id": originalNodeID,
				"attempt":          1,
				"retry_limit":      originalNode.Compensation.Retries,
				"retry_backoff_ms": int(originalNode.Compensation.Backoff.Milliseconds()),
			},
		})
	}
	if len(candidates) == 0 {
		return decider.store.Append(workflowID, domain.Event{
			WorkflowID: workflowID,
			Type:       domain.EventWorkflowFailed,
			Payload:    map[string]any{"error": instance.FailureReason},
		})
	}
	return decider.store.Append(workflowID, domain.Event{WorkflowID: workflowID, Type: domain.EventWorkflowCompensated})
}

func (decider *Decider) withWorkflowLock(workflowID string, operation func() error) error {
	locker, ok := decider.store.(store.WorkflowLocker)
	if !ok {
		return operation()
	}
	return locker.WithWorkflowLock(context.Background(), workflowID, operation)
}

// withStore returns a shallow copy of the decider that routes store mutations
// through the given transactional handle. The sync.Once-based lifecycle fields
// are intentionally not copied; the copy is never closed and starts no goroutines.
func (decider *Decider) withStore(mutationStore executionStore) *Decider {
	return &Decider{
		store:          mutationStore,
		timers:         decider.timers,
		retryPublisher: decider.retryPublisher,
		done:           decider.done,
	}
}

func (decider *Decider) advance(workflowID string) error {
	return decider.withWorkflowLock(workflowID, func() error {
		return decider.advanceUnlocked(workflowID)
	})
}

func (decider *Decider) advanceUnlocked(workflowID string) error {
	var err error
	for attempt := 0; attempt < maxConflictRetries; attempt++ {
		err = decider.advanceOnce(workflowID)
		if !errors.Is(err, store.ErrConflict) {
			return err
		}
	}
	return err
}

func (decider *Decider) advanceOnce(workflowID string) error {
	for {
		instance, err := decider.store.Get(workflowID)
		if err != nil {
			return err
		}
		if instance.Status != domain.WorkflowRunning {
			return nil
		}
		if err := decider.scheduleRetries(instance); err != nil {
			return err
		}
		if err := decider.schedulePendingDelays(instance); err != nil {
			return err
		}
		if workflowFailureRequested(instance) {
			if hasActiveTaskWork(instance) {
				return nil
			}
			return decider.scheduleNextCompensation(workflowID)
		}
		if ready, failedFanOutID := fanOutFailureReady(instance); ready {
			if instance.FailureReason == "" {
				if err := decider.store.Append(workflowID, domain.Event{WorkflowID: workflowID, Type: domain.EventWorkflowFailureRequested, Payload: map[string]any{"error": fmt.Sprintf("fan-out %q item failed", failedFanOutID)}}); err != nil {
					return err
				}
			}
			return decider.scheduleNextCompensation(workflowID)
		}
		if readyFanOut := readyFanOutNodes(instance); len(readyFanOut) > 0 {
			if err := decider.expandFanOut(instance, readyFanOut[0]); err != nil {
				return err
			}
			continue
		}
		if failNodeID := readyFailNode(instance); failNodeID != "" {
			if err := decider.executeFailNode(instance, failNodeID); err != nil {
				return err
			}
			failNode, _ := lookupNode(instance, failNodeID)
			if failNode.FanOutID != "" {
				continue
			}
			continue
		}
		if switchID := readySwitch(instance); switchID != "" {
			if err := decider.evaluateSwitch(instance, switchID); err != nil {
				return err
			}
			continue
		}
		if returnNodeID := readyReturnNode(instance); returnNodeID != "" {
			node, _ := lookupNode(instance, returnNodeID)
			value, err := evaluateExpression(node.ReturnValue, instance, node.RuntimeScope)
			if err != nil {
				return decider.failWorkflow(workflowID, fmt.Errorf("evaluate workflow return: %w", err))
			}
			result, ok := value.(map[string]any)
			if !ok {
				return decider.failWorkflow(workflowID, fmt.Errorf("workflow return expression returned %T, want object", value))
			}
			return decider.store.Append(workflowID, domain.Event{
				WorkflowID: workflowID, NodeID: returnNodeID, Type: domain.EventWorkflowCompleted,
				Payload: map[string]any{"result": result},
			})
		}
		if err := decider.scheduleDelays(instance); err != nil {
			return err
		}
		ready := readyNodes(instance)
		if len(ready) == 0 {
			if instance.Status == domain.WorkflowRunning && allNodesComplete(instance) {
				return decider.store.Append(workflowID, domain.Event{WorkflowID: workflowID, Type: domain.EventWorkflowCompleted})
			}
			return nil
		}
		for _, nodeID := range ready {
			node, ok := lookupNode(instance, nodeID)
			if !ok {
				return fmt.Errorf("ready node %q not found", nodeID)
			}
			input := instance.Context
			if node.RuntimeScope != nil {
				input = nil
			}
			if node.Input != nil {
				value, err := evaluateExpression(node.Input, instance, node.RuntimeScope)
				if err != nil {
					return decider.failNode(instance, nodeID, fmt.Errorf("evaluate input for task %q: %w", nodeID, err))
				}
				var ok bool
				input, ok = value.(map[string]any)
				if !ok {
					return decider.failNode(instance, nodeID, fmt.Errorf("task %q input expression returned %T, want object", nodeID, value))
				}
			}
			if err := decider.store.Append(workflowID, domain.Event{
				WorkflowID: workflowID,
				TaskID:     domain.NewID("task"),
				NodeID:     nodeID,
				Type:       domain.EventTaskScheduled,
				Payload: map[string]any{
					"task_name": node.Task.Name, "input": input,
					"attempt": 1, "retry_limit": node.Task.Retries,
					"retry_backoff_ms": node.Task.Backoff.Milliseconds(),
					"fan_out_id":       node.FanOutID, "fan_out_index": node.FanOutIndex,
					"fan_out_ancestors": node.FanOutAncestors,
				},
			}); err != nil {
				return err
			}
		}
	}
}

func (decider *Decider) scheduleRetries(instance *domain.WorkflowInstance) error {
	if decider.timers == nil {
		for _, task := range instance.Tasks {
			if task.Status == domain.TaskRetrying {
				return fmt.Errorf("task retry requires a timer service")
			}
		}
		return nil
	}
	for nodeID, task := range instance.Tasks {
		if task.Status != domain.TaskRetrying {
			continue
		}
		id := fmt.Sprintf("%s:%s:retry:%d", instance.ID, nodeID, task.Attempt)
		if err := decider.timers.Schedule(timer.Deadline{
			ID: id, At: task.RetryAt,
			Payload: taskRetryReference{WorkflowID: instance.ID, NodeID: nodeID, Attempt: task.Attempt},
		}); err != nil {
			return fmt.Errorf("schedule retry timer for task %q: %w", nodeID, err)
		}
	}
	return nil
}

func (decider *Decider) evaluateSwitch(instance *domain.WorkflowInstance, nodeID string) error {
	node, ok := lookupNode(instance, nodeID)
	if !ok {
		return fmt.Errorf("switch node %q not found", nodeID)
	}
	value, err := evaluateExpression(node.Condition, instance, node.RuntimeScope)
	if err != nil {
		return decider.failNode(instance, nodeID, fmt.Errorf("evaluate condition for switch %q: %w", nodeID, err))
	}
	selected, ok := value.(bool)
	if !ok {
		return decider.failNode(instance, nodeID, fmt.Errorf("condition for switch %q returned %T, want boolean", nodeID, value))
	}
	selectedBranch := "else"
	unselectedNodes := node.ThenNodes
	if selected {
		selectedBranch = "then"
		unselectedNodes = node.ElseNodes
	}
	if err := decider.store.Append(instance.ID, domain.Event{
		WorkflowID: instance.ID,
		NodeID:     nodeID,
		Type:       domain.EventSwitchEvaluated,
		Payload:    map[string]any{"branch": selectedBranch, "condition": selected},
	}); err != nil {
		return err
	}
	for _, skippedNodeID := range unselectedNodes {
		if err := decider.store.Append(instance.ID, domain.Event{
			WorkflowID: instance.ID,
			NodeID:     skippedNodeID,
			Type:       domain.EventNodeSkipped,
			Payload:    map[string]any{"switch_node_id": nodeID, "selected_branch": selectedBranch},
		}); err != nil {
			return err
		}
	}
	return nil
}

func (decider *Decider) failWorkflow(workflowID string, cause error) error {
	if err := decider.store.Append(workflowID, domain.Event{
		WorkflowID: workflowID,
		Type:       domain.EventWorkflowFailureRequested,
		Payload:    map[string]any{"error": cause.Error()},
	}); err != nil {
		return err
	}
	return decider.advanceUnlocked(workflowID)
}

func (decider *Decider) failNode(instance *domain.WorkflowInstance, nodeID string, cause error) error {
	node, ok := lookupNode(instance, nodeID)
	if !ok || node.FanOutID == "" {
		return decider.failWorkflow(instance.ID, cause)
	}
	return decider.store.Append(instance.ID, domain.Event{
		WorkflowID: instance.ID,
		NodeID:     nodeID,
		Type:       domain.EventFanOutItemFailed,
		Payload: map[string]any{
			"fan_out_id": node.FanOutID, "item_index": node.FanOutIndex,
			"failed_node_id": nodeID, "fan_out_ancestors": node.FanOutAncestors,
			"error": cause.Error(),
		},
	})
}

func (decider *Decider) executeFailNode(instance *domain.WorkflowInstance, nodeID string) error {
	node, ok := lookupNode(instance, nodeID)
	if !ok {
		return fmt.Errorf("fail node %q not found", nodeID)
	}
	value, err := evaluateExpression(node.FailureMessage, instance, node.RuntimeScope)
	message := "workflow explicitly failed"
	if err != nil {
		message = fmt.Sprintf("evaluate fail message for %q: %v", nodeID, err)
	} else if value != nil {
		message = fmt.Sprint(value)
	}
	if node.FanOutID != "" {
		return decider.store.Append(instance.ID, domain.Event{
			WorkflowID: instance.ID, NodeID: nodeID, Type: domain.EventFanOutItemFailed,
			Payload: map[string]any{"fan_out_id": node.FanOutID, "item_index": node.FanOutIndex, "failed_node_id": nodeID, "fan_out_ancestors": node.FanOutAncestors, "error": message},
		})
	}
	return decider.store.Append(instance.ID, domain.Event{
		WorkflowID: instance.ID, NodeID: nodeID, Type: domain.EventWorkflowFailureRequested,
		Payload: map[string]any{"error": message},
	})
}

type delayReference struct {
	WorkflowID string
	NodeID     string
}

func (decider *Decider) scheduleDelays(instance *domain.WorkflowInstance) error {
	ready := readyDelayNodes(instance)
	if len(ready) == 0 {
		return nil
	}
	if decider.timers == nil {
		return fmt.Errorf("workflow contains delay nodes but no timer service is configured")
	}
	for _, nodeID := range ready {
		node, ok := lookupNode(instance, nodeID)
		if !ok {
			return fmt.Errorf("delay node %q not found", nodeID)
		}
		deadlineID := instance.ID + ":" + nodeID
		scheduledAt := time.Now().UTC()
		if err := decider.store.Append(instance.ID, domain.Event{
			WorkflowID: instance.ID,
			NodeID:     nodeID,
			Type:       domain.EventDelayScheduled,
			OccurredAt: scheduledAt,
			Payload: map[string]any{
				"fan_out_id": node.FanOutID, "fan_out_index": node.FanOutIndex,
				"fan_out_ancestors": node.FanOutAncestors,
			},
		}); err != nil {
			return err
		}
		if err := decider.timers.Schedule(timer.Deadline{
			ID: deadlineID, At: scheduledAt.Add(node.Delay),
			Payload: delayReference{WorkflowID: instance.ID, NodeID: nodeID},
		}); err != nil {
			return err
		}
	}
	return nil
}

func (decider *Decider) schedulePendingDelays(instance *domain.WorkflowInstance) error {
	pending := make(map[string]domain.Event)
	for _, event := range instance.Events {
		if event.Type == domain.EventDelayScheduled {
			pending[event.NodeID] = event
		} else if event.Type == domain.EventDelayCompleted {
			delete(pending, event.NodeID)
		}
	}
	if len(pending) == 0 {
		return nil
	}
	if decider.timers == nil {
		return fmt.Errorf("workflow contains pending delays but no timer service is configured")
	}
	for nodeID, event := range pending {
		node, ok := lookupNode(instance, nodeID)
		if !ok {
			return fmt.Errorf("delay node %q not found", nodeID)
		}
		if event.OccurredAt.IsZero() {
			return fmt.Errorf("delay event for node %q has no durable timestamp", nodeID)
		}
		if err := decider.timers.Schedule(timer.Deadline{
			ID:      instance.ID + ":" + nodeID,
			At:      event.OccurredAt.Add(node.Delay),
			Payload: delayReference{WorkflowID: instance.ID, NodeID: nodeID},
		}); err != nil {
			return fmt.Errorf("schedule delay timer for node %q: %w", nodeID, err)
		}
	}
	return nil
}

func (decider *Decider) consumeTimers() {
	for {
		select {
		case deadline, open := <-decider.timerEvents:
			if !open {
				return
			}
			switch reference := deadline.Payload.(type) {
			case delayReference:
				_ = decider.completeDelay(reference)
			case taskRetryReference:
				_ = decider.completeRetry(reference)
			}
		case <-decider.done:
			return
		}
	}
}

func (decider *Decider) completeRetry(reference taskRetryReference) error {
	return decider.withWorkflowLock(reference.WorkflowID, func() error {
		var err error
		for attempt := 0; attempt < maxConflictRetries; attempt++ {
			err = decider.completeRetryOnce(reference)
			if !errors.Is(err, store.ErrConflict) {
				return err
			}
		}
		return err
	})
}

func (decider *Decider) completeRetryOnce(reference taskRetryReference) error {
	instance, err := decider.store.Get(reference.WorkflowID)
	if err != nil {
		return err
	}
	task, ok := instance.Tasks[reference.NodeID]
	if !ok || task.Status != domain.TaskRetrying || task.Attempt != reference.Attempt {
		return nil
	}
	eventType := domain.EventTaskScheduled
	payload := map[string]any{
		"task_name": task.TaskName, "input": task.Input, "attempt": task.Attempt,
		"retry_limit": task.RetryLimit, "retry_backoff_ms": int(task.RetryBackoff.Milliseconds()),
		"fan_out_id": task.FanOutID, "fan_out_index": task.FanOutIndex,
		"fan_out_ancestors": task.FanOutAncestors,
	}
	if task.IsCompensation {
		eventType = domain.EventCompensationScheduled
		payload["original_node_id"] = task.OriginalNodeID
	}
	if err := decider.store.Append(reference.WorkflowID, domain.Event{
		WorkflowID: reference.WorkflowID,
		TaskID:     domain.NewID("task"),
		NodeID:     reference.NodeID,
		Type:       eventType,
		Payload:    payload,
	}); err != nil {
		return err
	}
	if decider.retryPublisher != nil {
		updated, err := decider.store.Get(reference.WorkflowID)
		if err != nil {
			return err
		}
		if err := decider.retryPublisher(reference.WorkflowID); err != nil {
			return fmt.Errorf("publish retried task: %w", err)
		}
		_ = updated
	}
	return decider.advanceUnlocked(reference.WorkflowID)
}

func (decider *Decider) completeDelay(reference delayReference) error {
	return decider.withWorkflowLock(reference.WorkflowID, func() error {
		var err error
		for attempt := 0; attempt < maxConflictRetries; attempt++ {
			err = decider.completeDelayOnce(reference)
			if !errors.Is(err, store.ErrConflict) {
				return err
			}
		}
		return err
	})
}

func (decider *Decider) completeDelayOnce(reference delayReference) error {
	instance, err := decider.store.Get(reference.WorkflowID)
	if err != nil {
		return err
	}
	if !delayScheduled(instance, reference.NodeID) || delayCompleted(instance, reference.NodeID) {
		return nil
	}
	if err := decider.store.Append(reference.WorkflowID, domain.Event{
		WorkflowID: reference.WorkflowID,
		NodeID:     reference.NodeID,
		Type:       domain.EventDelayCompleted,
	}); err != nil {
		return err
	}
	return decider.advanceUnlocked(reference.WorkflowID)
}
