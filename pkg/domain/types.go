package domain

import "time"

type NodeType string

const (
	NodeTask   NodeType = "TASK"
	NodeFork   NodeType = "FORK"
	NodeJoin   NodeType = "JOIN"
	NodeSwitch NodeType = "SWITCH"
	NodeFanOut NodeType = "FAN_OUT"
	NodeDelay  NodeType = "DELAY"
	NodeFail   NodeType = "FAIL"
	NodeReturn NodeType = "RETURN"
)

type TaskStatus string

const (
	TaskPending     TaskStatus = "PENDING"
	TaskReady       TaskStatus = "READY"
	TaskRunning     TaskStatus = "RUNNING"
	TaskCompleted   TaskStatus = "COMPLETED"
	TaskFailed      TaskStatus = "FAILED"
	TaskSkipped     TaskStatus = "SKIPPED"
	TaskCompensate  TaskStatus = "COMPENSATING"
	TaskCompensated TaskStatus = "COMPENSATED"
	TaskRetrying    TaskStatus = "RETRYING"
)

type WorkflowStatus string

const (
	WorkflowPending      WorkflowStatus = "PENDING"
	WorkflowRunning      WorkflowStatus = "RUNNING"
	WorkflowCompleted    WorkflowStatus = "COMPLETED"
	WorkflowFailed       WorkflowStatus = "FAILED"
	WorkflowCompensating WorkflowStatus = "COMPENSATING"
	WorkflowCompensated  WorkflowStatus = "COMPENSATED"
)

type EventType string

const (
	EventWorkflowStarted          EventType = "WORKFLOW_STARTED"
	EventTaskScheduled            EventType = "TASK_SCHEDULED"
	EventTaskStarted              EventType = "TASK_STARTED"
	EventTaskCompleted            EventType = "TASK_COMPLETED"
	EventTaskFailed               EventType = "TASK_FAILED"
	EventTaskSkipped              EventType = "TASK_SKIPPED"
	EventTaskRetryScheduled       EventType = "TASK_RETRY_SCHEDULED"
	EventCompensationScheduled    EventType = "COMPENSATION_SCHEDULED"
	EventWorkflowCompensated      EventType = "WORKFLOW_COMPENSATED"
	EventWorkflowCompleted        EventType = "WORKFLOW_COMPLETED"
	EventWorkflowFailed           EventType = "WORKFLOW_FAILED"
	EventWorkflowFailureRequested EventType = "WORKFLOW_FAILURE_REQUESTED"
	EventDelayScheduled           EventType = "DELAY_SCHEDULED"
	EventDelayCompleted           EventType = "DELAY_COMPLETED"
	EventSwitchEvaluated          EventType = "SWITCH_EVALUATED"
	EventNodeSkipped              EventType = "NODE_SKIPPED"
	EventFanOutExpanded           EventType = "FAN_OUT_EXPANDED"
	EventFanOutItemFailed         EventType = "FAN_OUT_ITEM_FAILED"
)

type Event struct {
	Sequence   uint64
	Type       EventType
	WorkflowID string
	TaskID     string
	NodeID     string
	Payload    map[string]any
	OccurredAt time.Time
}

type ExpressionKind string

const (
	ExpressionIdentifier ExpressionKind = "IDENTIFIER"
	ExpressionString     ExpressionKind = "STRING"
	ExpressionNumber     ExpressionKind = "NUMBER"
	ExpressionBoolean    ExpressionKind = "BOOLEAN"
	ExpressionMember     ExpressionKind = "MEMBER"
	ExpressionObject     ExpressionKind = "OBJECT"
	ExpressionBinary     ExpressionKind = "BINARY"
	ExpressionUnary      ExpressionKind = "UNARY"
)

type Expression struct {
	Kind     ExpressionKind    `json:"kind"`
	Name     string            `json:"name,omitempty"`
	Value    any               `json:"value,omitempty"`
	Operator string            `json:"operator,omitempty"`
	Object   *Expression       `json:"object,omitempty"`
	Left     *Expression       `json:"left,omitempty"`
	Right    *Expression       `json:"right,omitempty"`
	Fields   []ExpressionField `json:"fields,omitempty"`
}

type ExpressionField struct {
	Name  string     `json:"name"`
	Value Expression `json:"value"`
}
