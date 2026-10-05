package domain

import (
	"testing"
	"time"
)

func TestWorkflowDefinitionValidation(t *testing.T) {
	tests := []struct {
		name       string
		definition WorkflowDef
		wantError  bool
	}{
		{name: "missing name", definition: WorkflowDef{Version: 1, Nodes: map[string]NodeDefinition{"task": {Type: NodeTask, Task: &TaskDefinition{Name: "run"}}}}, wantError: true},
		{name: "invalid version", definition: WorkflowDef{Name: "demo", Version: 0, Nodes: map[string]NodeDefinition{"task": {Type: NodeTask, Task: &TaskDefinition{Name: "run"}}}}, wantError: true},
		{name: "empty nodes", definition: WorkflowDef{Name: "demo", Version: 1}, wantError: true},
		{name: "missing task definition", definition: WorkflowDef{Name: "demo", Version: 1, Nodes: map[string]NodeDefinition{"task": {Type: NodeTask}}}, wantError: true},
		{name: "unknown dependency", definition: WorkflowDef{Name: "demo", Version: 1, Nodes: map[string]NodeDefinition{"task": {Type: NodeTask, Task: &TaskDefinition{Name: "run"}, DependsOn: []string{"missing"}}}}, wantError: true},
		{name: "valid definition", definition: WorkflowDef{Name: "demo", Version: 1, Nodes: map[string]NodeDefinition{"task": {ID: "task", Type: NodeTask, Task: &TaskDefinition{Name: "run"}}}}, wantError: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.definition.Validate()
			if (err != nil) != test.wantError {
				t.Fatalf("Validate() error = %v, want error = %t", err, test.wantError)
			}
		})
	}
}

func TestWorkflowInstanceAppendsOrderedEvents(t *testing.T) {
	definition := WorkflowDef{
		Name:    "demo",
		Version: 1,
		Nodes: map[string]NodeDefinition{
			"task": {ID: "task", Type: NodeTask, Task: &TaskDefinition{Name: "run"}},
		},
	}
	instance, err := NewWorkflowInstance(definition, nil)
	if err != nil {
		t.Fatal(err)
	}
	occurredAt := time.Date(2026, time.September, 29, 12, 0, 0, 0, time.UTC)
	if err := instance.Append(Event{WorkflowID: instance.ID, Type: EventWorkflowStarted, OccurredAt: occurredAt}); err != nil {
		t.Fatal(err)
	}
	if err := instance.Append(Event{WorkflowID: instance.ID, Type: EventTaskScheduled, TaskID: "task-1", NodeID: "task", Payload: map[string]any{"task_name": "run"}, OccurredAt: occurredAt}); err != nil {
		t.Fatal(err)
	}
	if instance.NextSequence != 2 || instance.Events[0].Sequence != 1 || instance.Events[1].Sequence != 2 {
		t.Fatalf("sequences = %d, %d, next = %d; want 1, 2, 2", instance.Events[0].Sequence, instance.Events[1].Sequence, instance.NextSequence)
	}
	if instance.Status != WorkflowRunning || instance.Tasks["task"].Status != TaskReady {
		t.Fatalf("state = workflow %s, task %s; want RUNNING, READY", instance.Status, instance.Tasks["task"].Status)
	}
	if err := instance.Append(Event{WorkflowID: "other", Type: EventWorkflowCompleted}); err == nil {
		t.Fatal("Append accepted an event for another workflow")
	}
}
