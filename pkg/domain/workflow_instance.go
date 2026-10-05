package domain

import (
	"encoding/json"
	"fmt"
	"time"
)

type TaskInstance struct {
	ID              string
	NodeID          string
	TaskName        string
	Status          TaskStatus
	Input           map[string]any
	Result          map[string]any
	Error           string
	LeaseToken      uint64
	CompletedAt     time.Time
	IsCompensation  bool
	OriginalNodeID  string
	FanOutID        string
	FanOutIndex     int
	FanOutAncestors []FanOutItemRef
	Attempt         int
	RetryLimit      int
	RetryBackoff    time.Duration
	RetryAt         time.Time
}

type WorkflowInstance struct {
	ID            string
	Definition    WorkflowDef
	Status        WorkflowStatus
	FailureReason string
	Context       map[string]any
	Result        map[string]any
	Tasks         map[string]TaskInstance
	Events        []Event
	NextSequence  uint64
	RuntimeNodes  map[string]NodeDefinition
	FanOuts       map[string]FanOutExecution
}

type FanOutExecution struct {
	Expanded bool
	Stopped  bool
	Items    []FanOutItemExecution
}

type FanOutItemExecution struct {
	Index   int
	NodeIDs []string
	Leaves  []string
	Started bool
	Failed  bool
}

func NewWorkflowInstance(definition WorkflowDef, context map[string]any) (*WorkflowInstance, error) {
	if err := definition.Validate(); err != nil {
		return nil, err
	}
	if context == nil {
		context = make(map[string]any)
	}
	return &WorkflowInstance{
		ID:           NewID("workflow"),
		Definition:   definition,
		Status:       WorkflowPending,
		Context:      context,
		Tasks:        make(map[string]TaskInstance),
		RuntimeNodes: make(map[string]NodeDefinition),
		FanOuts:      make(map[string]FanOutExecution),
	}, nil
}

func (instance *WorkflowInstance) Append(event Event) error {
	if event.WorkflowID != instance.ID {
		return fmt.Errorf("event belongs to workflow %q, want %q", event.WorkflowID, instance.ID)
	}
	instance.NextSequence++
	event.Sequence = instance.NextSequence
	if event.OccurredAt.IsZero() {
		event.OccurredAt = time.Now().UTC()
	}
	instance.Events = append(instance.Events, event)
	return instance.apply(event)
}

func (instance *WorkflowInstance) apply(event Event) error {
	switch event.Type {
	case EventWorkflowStarted:
		instance.Status = WorkflowRunning
	case EventTaskScheduled:
		taskName, _ := event.Payload["task_name"].(string)
		input, _ := event.Payload["input"].(map[string]any)
		fanOutID, _ := event.Payload["fan_out_id"].(string)
		fanOutIndex := int(number(event.Payload["fan_out_index"]))
		var fanOutAncestors []FanOutItemRef
		if encoded, err := json.Marshal(event.Payload["fan_out_ancestors"]); err == nil {
			_ = json.Unmarshal(encoded, &fanOutAncestors)
		}
		attempt := int(number(event.Payload["attempt"]))
		if attempt < 1 {
			attempt = 1
		}
		instance.Tasks[event.NodeID] = TaskInstance{
			ID: event.TaskID, NodeID: event.NodeID, TaskName: taskName, Input: input, Status: TaskReady,
			FanOutID: fanOutID, FanOutIndex: fanOutIndex, FanOutAncestors: fanOutAncestors,
			Attempt: attempt, RetryLimit: int(number(event.Payload["retry_limit"])),
			RetryBackoff: time.Duration(number(event.Payload["retry_backoff_ms"])) * time.Millisecond,
		}
		if fanOutID != "" {
			markFanOutItemStarted(instance, fanOutID, fanOutIndex)
		}
		for _, ancestor := range fanOutAncestors {
			markFanOutItemStarted(instance, ancestor.FanOutID, ancestor.Index)
		}
	case EventTaskStarted:
		task := instance.Tasks[event.NodeID]
		task.Status = TaskRunning
		task.LeaseToken = number(event.Payload["lease_token"])
		instance.Tasks[event.NodeID] = task
	case EventTaskCompleted:
		task := instance.Tasks[event.NodeID]
		task.Status = TaskCompleted
		task.Result, _ = event.Payload["result"].(map[string]any)
		task.CompletedAt = event.OccurredAt
		instance.Tasks[event.NodeID] = task
	case EventFanOutExpanded:
		if instance.RuntimeNodes == nil {
			instance.RuntimeNodes = make(map[string]NodeDefinition)
		}
		if instance.FanOuts == nil {
			instance.FanOuts = make(map[string]FanOutExecution)
		}
		var runtimeNodes map[string]NodeDefinition
		var items []FanOutItemExecution
		if encoded, err := json.Marshal(event.Payload["runtime_nodes"]); err == nil {
			_ = json.Unmarshal(encoded, &runtimeNodes)
		}
		if encoded, err := json.Marshal(event.Payload["items"]); err == nil {
			_ = json.Unmarshal(encoded, &items)
		}
		for nodeID, node := range runtimeNodes {
			instance.RuntimeNodes[nodeID] = node
		}
		instance.FanOuts[event.NodeID] = FanOutExecution{Expanded: true, Items: items}
		if parentFanOutID, ok := event.Payload["parent_fan_out_id"].(string); ok && parentFanOutID != "" {
			markFanOutItemStarted(instance, parentFanOutID, int(number(event.Payload["parent_fan_out_index"])))
		}
		var ancestors []FanOutItemRef
		if encoded, err := json.Marshal(event.Payload["parent_fan_out_ancestors"]); err == nil {
			_ = json.Unmarshal(encoded, &ancestors)
		}
		for _, ancestor := range ancestors {
			markFanOutItemStarted(instance, ancestor.FanOutID, ancestor.Index)
		}
	case EventFanOutItemFailed:
		failureReason, _ := event.Payload["error"].(string)
		if instance.FailureReason == "" {
			instance.FailureReason = failureReason
		}
		fanOutID, _ := event.Payload["fan_out_id"].(string)
		itemIndex := int(number(event.Payload["item_index"]))
		state := instance.FanOuts[fanOutID]
		state.Stopped = true
		for index := range state.Items {
			if state.Items[index].Index == itemIndex {
				state.Items[index].Failed = true
				break
			}
		}
		instance.FanOuts[fanOutID] = state
		var ancestors []FanOutItemRef
		if encoded, err := json.Marshal(event.Payload["fan_out_ancestors"]); err == nil {
			_ = json.Unmarshal(encoded, &ancestors)
		}
		for _, ancestor := range ancestors {
			ancestorState := instance.FanOuts[ancestor.FanOutID]
			ancestorState.Stopped = true
			for index := range ancestorState.Items {
				if ancestorState.Items[index].Index == ancestor.Index {
					ancestorState.Items[index].Failed = true
					break
				}
			}
			instance.FanOuts[ancestor.FanOutID] = ancestorState
		}
	case EventDelayScheduled:
		fanOutID, _ := event.Payload["fan_out_id"].(string)
		if fanOutID != "" {
			markFanOutItemStarted(instance, fanOutID, int(number(event.Payload["fan_out_index"])))
		}
		var ancestors []FanOutItemRef
		if encoded, err := json.Marshal(event.Payload["fan_out_ancestors"]); err == nil {
			_ = json.Unmarshal(encoded, &ancestors)
		}
		for _, ancestor := range ancestors {
			markFanOutItemStarted(instance, ancestor.FanOutID, ancestor.Index)
		}
	case EventTaskFailed:
		task := instance.Tasks[event.NodeID]
		task.Status = TaskFailed
		task.Error, _ = event.Payload["error"].(string)
		instance.Tasks[event.NodeID] = task
	case EventTaskRetryScheduled:
		task := instance.Tasks[event.NodeID]
		task.Status = TaskRetrying
		task.RetryAt, _ = event.Payload["retry_at"].(time.Time)
		task.Attempt = int(number(event.Payload["attempt"]))
		instance.Tasks[event.NodeID] = task
	case EventTaskSkipped:
		task := instance.Tasks[event.NodeID]
		task.Status = TaskSkipped
		instance.Tasks[event.NodeID] = task
	case EventCompensationScheduled:
		instance.Status = WorkflowCompensating
		taskName, _ := event.Payload["task_name"].(string)
		input, _ := event.Payload["input"].(map[string]any)
		originalNodeID, _ := event.Payload["original_node_id"].(string)
		attempt := int(number(event.Payload["attempt"]))
		if attempt < 1 {
			attempt = 1
		}
		instance.Tasks[event.NodeID] = TaskInstance{
			ID: event.TaskID, NodeID: event.NodeID, TaskName: taskName, Status: TaskReady,
			Input: input, IsCompensation: true, OriginalNodeID: originalNodeID,
			Attempt: attempt, RetryLimit: int(number(event.Payload["retry_limit"])),
			RetryBackoff: time.Duration(number(event.Payload["retry_backoff_ms"])) * time.Millisecond,
		}
	case EventWorkflowCompensated:
		instance.Status = WorkflowCompensated
	case EventWorkflowFailureRequested:
		instance.FailureReason, _ = event.Payload["error"].(string)
	case EventWorkflowCompleted:
		instance.Status = WorkflowCompleted
		instance.Result, _ = event.Payload["result"].(map[string]any)
	case EventWorkflowFailed:
		instance.Status = WorkflowFailed
		if failureReason, ok := event.Payload["error"].(string); ok {
			instance.FailureReason = failureReason
		}
	}
	return nil
}

func number(value any) uint64 {
	switch typed := value.(type) {
	case uint64:
		return typed
	case int:
		return uint64(typed)
	case int64:
		if typed >= 0 {
			return uint64(typed)
		}
		return 0
	case float64:
		if typed >= 0 {
			return uint64(typed)
		}
		return 0
	default:
		return 0
	}
}

func markFanOutItemStarted(instance *WorkflowInstance, fanOutID string, itemIndex int) {
	if fanOutID == "" {
		return
	}
	state, exists := instance.FanOuts[fanOutID]
	if !exists {
		return
	}
	for index := range state.Items {
		if state.Items[index].Index == itemIndex {
			state.Items[index].Started = true
			break
		}
	}
	instance.FanOuts[fanOutID] = state
}
