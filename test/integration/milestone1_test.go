package integration_test

import (
	"testing"

	"github.com/zephyr-workflow/zephyr/pkg/decider"
	"github.com/zephyr-workflow/zephyr/pkg/domain"
	"github.com/zephyr-workflow/zephyr/pkg/store"
)

func TestMilestone1WorkflowEndToEnd(t *testing.T) {
	definition := domain.WorkflowDef{
		Name:    "order-checkout",
		Version: 1,
		Nodes: map[string]domain.NodeDefinition{
			"charge": {
				ID:   "charge",
				Type: domain.NodeTask,
				Task: &domain.TaskDefinition{Name: "ChargePayment"},
			},
			"notify": {
				ID:        "notify",
				Type:      domain.NodeTask,
				Task:      &domain.TaskDefinition{Name: "SendConfirmationEmail"},
				DependsOn: []string{"charge"},
			},
		},
	}
	memory := store.NewMemoryStore()
	engine := decider.New(memory)
	instance, err := engine.Start(definition, map[string]any{"order_id": "order-1"})
	if err != nil {
		t.Fatal(err)
	}
	if instance.Tasks["charge"].Status != domain.TaskReady {
		t.Fatalf("charge status = %s, want %s", instance.Tasks["charge"].Status, domain.TaskReady)
	}
	if err := engine.StartTask(instance.ID, "charge", 1); err != nil {
		t.Fatal(err)
	}
	if err := engine.CompleteTask(instance.ID, "charge", map[string]any{"transaction_id": "tx-1"}); err != nil {
		t.Fatal(err)
	}
	if err := engine.StartTask(instance.ID, "notify", 2); err != nil {
		t.Fatal(err)
	}
	if err := engine.CompleteTask(instance.ID, "notify", map[string]any{"delivered": true}); err != nil {
		t.Fatal(err)
	}

	result, err := memory.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != domain.WorkflowCompleted {
		t.Fatalf("workflow status = %s, want %s", result.Status, domain.WorkflowCompleted)
	}
	if len(result.Events) != 8 {
		t.Fatalf("event count = %d, want 8", len(result.Events))
	}
	for index, event := range result.Events {
		if event.Sequence != uint64(index+1) {
			t.Fatalf("event %d sequence = %d, want %d", index, event.Sequence, index+1)
		}
		if event.WorkflowID != result.ID {
			t.Fatalf("event %d workflow ID = %q, want %q", index, event.WorkflowID, result.ID)
		}
	}
	if result.Tasks["charge"].Result["transaction_id"] != "tx-1" {
		t.Fatal("charge result was not retained")
	}
	if result.Tasks["notify"].Result["delivered"] != true {
		t.Fatal("notify result was not retained")
	}
}
