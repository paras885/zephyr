package integration_test

import (
	"testing"
	"time"

	"github.com/zephyr-workflow/zephyr/pkg/decider"
	"github.com/zephyr-workflow/zephyr/pkg/domain"
	"github.com/zephyr-workflow/zephyr/pkg/dsl/compiler"
	"github.com/zephyr-workflow/zephyr/pkg/store"
	"github.com/zephyr-workflow/zephyr/pkg/timer"
)

func TestMilestone3ZephyrSourceToRunnableWorkflowEndToEnd(t *testing.T) {
	source := `namespace ecommerce.orders;
type Input { value: string }
type Output { status: string }
task Notify(input: Input) -> Output { timeout 10s }
workflow DelayedNotify(input: Input) -> Output {
    delay wait = 10ms;
    step notify = Notify({});
    return Output { status: "COMPLETED" };
}`
	definition, err := compiler.Compile(source)
	if err != nil {
		t.Fatal(err)
	}
	if definition.Nodes["wait"].Type != domain.NodeDelay || definition.Nodes["notify"].Type != domain.NodeTask {
		t.Fatalf("compiled nodes = %#v", definition.Nodes)
	}

	memory := store.NewMemoryStore()
	timers := timer.NewService(2)
	engine := decider.NewWithTimer(memory, timers)
	defer func() {
		engine.Close()
		timers.Close()
	}()
	instance, err := engine.Start(definition, nil)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.After(250 * time.Millisecond)
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			current, getErr := memory.Get(instance.ID)
			if getErr != nil {
				t.Fatal(getErr)
			}
			if current.Tasks["notify"].Status == domain.TaskReady {
				if err := engine.CompleteTask(instance.ID, "notify", nil); err != nil {
					t.Fatal(err)
				}
				completed, getErr := memory.Get(instance.ID)
				if getErr != nil {
					t.Fatal(getErr)
				}
				if completed.Status != domain.WorkflowCompleted {
					t.Fatalf("workflow status = %s, want %s", completed.Status, domain.WorkflowCompleted)
				}
				return
			}
		case <-deadline:
			t.Fatal("compiled delay workflow did not release task")
		}
	}
}
