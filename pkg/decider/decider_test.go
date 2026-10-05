package decider

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/zephyr-workflow/zephyr/pkg/domain"
	"github.com/zephyr-workflow/zephyr/pkg/dsl/compiler"
	"github.com/zephyr-workflow/zephyr/pkg/store"
	"github.com/zephyr-workflow/zephyr/pkg/timer"
)

type conflictOnceStore struct {
	*store.MemoryStore
	mu            sync.Mutex
	conflictType  domain.EventType
	conflictsLeft int
}

func (memory *conflictOnceStore) Append(workflowID string, event domain.Event) error {
	memory.mu.Lock()
	if event.Type == memory.conflictType && memory.conflictsLeft > 0 {
		memory.conflictsLeft--
		memory.mu.Unlock()
		return store.ErrConflict
	}
	memory.mu.Unlock()
	return memory.MemoryStore.Append(workflowID, event)
}

func TestDeciderSchedulesDependenciesAndCompletesWorkflow(t *testing.T) {
	definition := domain.WorkflowDef{
		Name:    "checkout",
		Version: 1,
		Start:   []string{"charge"},
		Nodes: map[string]domain.NodeDefinition{
			"charge": {ID: "charge", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "ChargePayment"}, Next: []string{"notify"}},
			"notify": {ID: "notify", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "SendEmail"}, DependsOn: []string{"charge"}},
		},
	}
	memory := store.NewMemoryStore()
	decider := New(memory)
	instance, err := decider.Start(definition, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := instance.Tasks["charge"].Status; got != domain.TaskReady {
		t.Fatalf("charge status = %s, want %s", got, domain.TaskReady)
	}
	if _, scheduled := instance.Tasks["notify"]; scheduled {
		t.Fatal("notify was scheduled before charge completed")
	}

	if err := decider.CompleteTask(instance.ID, "charge", map[string]any{"transaction_id": "tx-1"}); err != nil {
		t.Fatal(err)
	}
	instance, err = memory.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := instance.Tasks["notify"].Status; got != domain.TaskReady {
		t.Fatalf("notify status = %s, want %s", got, domain.TaskReady)
	}
	if err := decider.CompleteTask(instance.ID, "notify", nil); err != nil {
		t.Fatal(err)
	}
	instance, err = memory.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if instance.Status != domain.WorkflowCompleted {
		t.Fatalf("workflow status = %s, want %s", instance.Status, domain.WorkflowCompleted)
	}
}

func TestDeciderSchedulesConfiguredCompensationInReverseOrder(t *testing.T) {
	definition := domain.WorkflowDef{
		Name:    "payment",
		Version: 1,
		Start:   []string{"charge"},
		Nodes: map[string]domain.NodeDefinition{
			"charge": {
				ID:           "charge",
				Type:         domain.NodeTask,
				Task:         &domain.TaskDefinition{Name: "ChargePayment"},
				Compensation: &domain.TaskDefinition{Name: "RefundPayment"},
			},
			"review": {
				ID:        "review",
				Type:      domain.NodeTask,
				Task:      &domain.TaskDefinition{Name: "Review"},
				DependsOn: []string{"charge"},
			},
		},
	}
	memory := store.NewMemoryStore()
	decider := New(memory)
	instance, err := decider.Start(definition, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := decider.CompleteTask(instance.ID, "charge", map[string]any{"transaction_id": "tx-1"}); err != nil {
		t.Fatal(err)
	}
	if err := decider.FailTask(instance.ID, "review", "rejected"); err != nil {
		t.Fatal(err)
	}

	instance, err = memory.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if instance.Status != domain.WorkflowCompensating {
		t.Fatalf("workflow status = %s, want %s", instance.Status, domain.WorkflowCompensating)
	}
	if len(instance.Events) != 7 {
		t.Fatalf("event count = %d, want 7 before compensation execution", len(instance.Events))
	}
	if instance.Events[5].Type != domain.EventWorkflowFailureRequested || instance.Events[6].Type != domain.EventCompensationScheduled {
		t.Fatalf("failure/compensation events = %s/%s", instance.Events[5].Type, instance.Events[6].Type)
	}
	var compensationTask domain.TaskInstance
	for _, task := range instance.Tasks {
		if task.IsCompensation {
			compensationTask = task
		}
	}
	if compensationTask.Status != domain.TaskReady || compensationTask.TaskName != "RefundPayment" || compensationTask.OriginalNodeID != "charge" {
		t.Fatalf("scheduled compensation task = %#v", compensationTask)
	}
	if err := decider.CompleteTask(instance.ID, compensationTask.NodeID, map[string]any{"refunded": true}); err != nil {
		t.Fatal(err)
	}
	instance, err = memory.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if instance.Status != domain.WorkflowCompensated {
		t.Fatalf("workflow status after compensation = %s, want %s", instance.Status, domain.WorkflowCompensated)
	}
}

func TestDeciderRunsSagaCompensationsInReverseOrder(t *testing.T) {
	definition := domain.WorkflowDef{
		Name:    "saga",
		Version: 1,
		Nodes: map[string]domain.NodeDefinition{
			"reserve": {ID: "reserve", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "Reserve"}, Compensation: &domain.TaskDefinition{Name: "Release"}},
			"charge":  {ID: "charge", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "Charge"}, DependsOn: []string{"reserve"}, Compensation: &domain.TaskDefinition{Name: "Refund"}},
			"ship":    {ID: "ship", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "Ship"}, DependsOn: []string{"charge"}},
		},
	}
	memory := store.NewMemoryStore()
	engine := New(memory)
	instance, err := engine.Start(definition, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, nodeID := range []string{"reserve", "charge"} {
		if err := engine.CompleteTask(instance.ID, nodeID, map[string]any{"ok": true}); err != nil {
			t.Fatal(err)
		}
	}
	if err := engine.FailTask(instance.ID, "ship", "carrier unavailable"); err != nil {
		t.Fatal(err)
	}
	instance, err = memory.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	compensation := readyCompensation(t, instance)
	if compensation.TaskName != "Refund" || compensation.OriginalNodeID != "charge" {
		t.Fatalf("first compensation = %#v, want refund charge", compensation)
	}
	if err := engine.CompleteTask(instance.ID, compensation.NodeID, nil); err != nil {
		t.Fatal(err)
	}
	instance, err = memory.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	compensation = readyCompensation(t, instance)
	if compensation.TaskName != "Release" || compensation.OriginalNodeID != "reserve" {
		t.Fatalf("second compensation = %#v, want release reserve", compensation)
	}
	if err := engine.CompleteTask(instance.ID, compensation.NodeID, nil); err != nil {
		t.Fatal(err)
	}
	instance, err = memory.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if instance.Status != domain.WorkflowCompensated {
		t.Fatalf("workflow status = %s, want COMPENSATED", instance.Status)
	}
}

func TestDeciderDrainsAdmittedTasksBeforeCompensation(t *testing.T) {
	definition := domain.WorkflowDef{
		Name:    "concurrent-failure-drain",
		Version: 1,
		Nodes: map[string]domain.NodeDefinition{
			"reserve": {ID: "reserve", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "Reserve"}, Compensation: &domain.TaskDefinition{Name: "ReleaseReserve"}},
			"charge":  {ID: "charge", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "Charge"}, Compensation: &domain.TaskDefinition{Name: "RefundCharge"}},
			"audit":   {ID: "audit", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "Audit"}, Compensation: &domain.TaskDefinition{Name: "UndoAudit"}},
			"notify":  {ID: "notify", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "Notify"}},
			"finish":  {ID: "finish", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "Finish"}, DependsOn: []string{"audit"}},
		},
	}
	memory := store.NewMemoryStore()
	engine := New(memory)
	instance, err := engine.Start(definition, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.CompleteTask(instance.ID, "reserve", nil); err != nil {
		t.Fatal(err)
	}
	if err := engine.StartTask(instance.ID, "audit", 19); err != nil {
		t.Fatal(err)
	}
	if err := engine.FailTask(instance.ID, "charge", "payment declined"); err != nil {
		t.Fatal(err)
	}

	instance, err = memory.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if instance.Status != domain.WorkflowRunning {
		t.Fatalf("workflow status during drain = %s, want RUNNING", instance.Status)
	}
	if _, scheduled := instance.Tasks["finish"]; scheduled {
		t.Fatal("failure advanced to a new node while admitted tasks were draining")
	}
	for _, task := range instance.Tasks {
		if task.IsCompensation {
			t.Fatal("compensation started before admitted tasks drained")
		}
	}
	if instance.Tasks["notify"].Status != domain.TaskReady || instance.Tasks["audit"].Status != domain.TaskRunning {
		t.Fatalf("admitted tasks not preserved for drain: notify=%s audit=%s", instance.Tasks["notify"].Status, instance.Tasks["audit"].Status)
	}

	if err := engine.CompleteTask(instance.ID, "audit", nil); err != nil {
		t.Fatal(err)
	}
	instance, err = memory.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, scheduled := instance.Tasks["finish"]; scheduled {
		t.Fatal("failure advanced to a newly unlocked node during drain")
	}
	if instance.Tasks["notify"].Status != domain.TaskReady {
		t.Fatal("READY admitted task was not retained while the workflow drained")
	}
	if err := engine.CompleteTask(instance.ID, "notify", nil); err != nil {
		t.Fatal(err)
	}
	instance, err = memory.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if instance.Status != domain.WorkflowCompensating {
		t.Fatalf("workflow status after drain = %s, want COMPENSATING", instance.Status)
	}
	compensation := readyCompensation(t, instance)
	if compensation.OriginalNodeID != "audit" {
		t.Fatalf("first compensation original node = %q, want audit", compensation.OriginalNodeID)
	}
	if err := engine.CompleteTask(instance.ID, compensation.NodeID, nil); err != nil {
		t.Fatal(err)
	}
	instance, err = memory.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	compensation = readyCompensation(t, instance)
	if compensation.OriginalNodeID != "reserve" {
		t.Fatalf("second compensation original node = %q, want reserve", compensation.OriginalNodeID)
	}
	if err := engine.CompleteTask(instance.ID, compensation.NodeID, nil); err != nil {
		t.Fatal(err)
	}
	instance, err = memory.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if instance.Status != domain.WorkflowCompensated {
		t.Fatalf("workflow status after compensation = %s, want COMPENSATED", instance.Status)
	}
}

func TestCompensationUsesRetryPolicyAndFailsWhenExhausted(t *testing.T) {
	definition := domain.WorkflowDef{
		Name:    "compensation-retry",
		Version: 1,
		Nodes: map[string]domain.NodeDefinition{
			"reserve": {ID: "reserve", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "Reserve"}, Compensation: &domain.TaskDefinition{Name: "Release", Retries: 1, Backoff: 10 * time.Millisecond}},
			"ship":    {ID: "ship", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "Ship"}, DependsOn: []string{"reserve"}},
		},
	}
	memory := store.NewMemoryStore()
	timers := timer.NewService(8)
	engine := NewWithTimer(memory, timers)
	defer func() { engine.Close(); timers.Close() }()
	instance, err := engine.Start(definition, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.CompleteTask(instance.ID, "reserve", nil); err != nil {
		t.Fatal(err)
	}
	if err := engine.FailTask(instance.ID, "ship", "carrier unavailable"); err != nil {
		t.Fatal(err)
	}
	instance, err = memory.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	firstAttempt := readyCompensation(t, instance)
	if firstAttempt.RetryLimit != 1 || firstAttempt.RetryBackoff != 10*time.Millisecond || firstAttempt.Attempt != 1 {
		t.Fatalf("compensation retry policy = limit %d, backoff %s, attempt %d", firstAttempt.RetryLimit, firstAttempt.RetryBackoff, firstAttempt.Attempt)
	}
	if err := engine.FailTask(instance.ID, firstAttempt.NodeID, "release temporarily unavailable"); err != nil {
		t.Fatal(err)
	}

	deadline := time.After(time.Second)
	var retry domain.TaskInstance
	for {
		current, getErr := memory.Get(instance.ID)
		if getErr != nil {
			t.Fatal(getErr)
		}
		candidate := current.Tasks[firstAttempt.NodeID]
		if candidate.Status == domain.TaskReady && candidate.Attempt == 2 {
			retry = candidate
			break
		}
		select {
		case <-time.After(time.Millisecond):
		case <-deadline:
			t.Fatal("compensation retry did not become ready after backoff")
		}
	}
	if !retry.IsCompensation || retry.OriginalNodeID != "reserve" || retry.TaskName != "Release" || retry.ID == firstAttempt.ID {
		t.Fatalf("compensation retry lost identity or metadata: %#v", retry)
	}
	if err := engine.FailTask(instance.ID, retry.NodeID, "release permanently unavailable"); err != nil {
		t.Fatal(err)
	}
	failed, err := memory.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if failed.Status != domain.WorkflowFailed || failed.FailureReason != "compensation failed: release permanently unavailable" {
		t.Fatalf("exhausted compensation status/reason = %s/%q", failed.Status, failed.FailureReason)
	}
}

func TestDeciderStopsSagaWhenCompensationFails(t *testing.T) {
	definition := domain.WorkflowDef{
		Name:    "saga-failure",
		Version: 1,
		Nodes: map[string]domain.NodeDefinition{
			"reserve": {ID: "reserve", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "Reserve"}, Compensation: &domain.TaskDefinition{Name: "Release"}},
			"ship":    {ID: "ship", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "Ship"}, DependsOn: []string{"reserve"}},
		},
	}
	memory := store.NewMemoryStore()
	engine := New(memory)
	instance, err := engine.Start(definition, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.CompleteTask(instance.ID, "reserve", nil); err != nil {
		t.Fatal(err)
	}
	if err := engine.FailTask(instance.ID, "ship", "ship failed"); err != nil {
		t.Fatal(err)
	}
	instance, err = memory.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	compensation := readyCompensation(t, instance)
	if err := engine.FailTask(instance.ID, compensation.NodeID, "refund service unavailable"); err != nil {
		t.Fatal(err)
	}
	instance, err = memory.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if instance.Status != domain.WorkflowFailed {
		t.Fatalf("workflow status after compensation failure = %s, want FAILED", instance.Status)
	}
	for _, task := range instance.Tasks {
		if task.IsCompensation && task.Status == domain.TaskReady && task.TaskName == "Release" {
			t.Fatal("saga continued after the first compensation failure")
		}
	}
}

func TestSagaCompensationReceivesExplicitMappedInput(t *testing.T) {
	definition, err := compiler.Compile(`type Input { order_id: string }
type ChargeInput { order_id: string }
type RefundInput { transaction_id: string }
type Output { status: string }
task Charge(input: ChargeInput) -> Output { }
task Ship(input: ChargeInput) -> Output { }
task Refund(input: RefundInput) -> Output { }
workflow Checkout(input: Input) -> Output {
    step charge = Charge({ order_id: input.order_id }) compensate with Refund({ transaction_id: charge.result.transaction_id });
    step ship = Ship({ order_id: input.order_id });
	return Output { status: "shipped" };
}`)
	if err != nil {
		t.Fatal(err)
	}
	memory := store.NewMemoryStore()
	engine := New(memory)
	instance, err := engine.Start(definition, map[string]any{"order_id": "order-17"})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.CompleteTask(instance.ID, "charge", map[string]any{"transaction_id": "txn-17"}); err != nil {
		t.Fatal(err)
	}
	if err := engine.FailTask(instance.ID, "ship", "shipment rejected"); err != nil {
		t.Fatal(err)
	}
	instance, err = memory.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	compensation := readyCompensation(t, instance)
	if compensation.TaskName != "Refund" || compensation.Input["transaction_id"] != "txn-17" {
		t.Fatalf("compensation task input = %#v", compensation)
	}
}

func TestDSLFailRunsConfiguredSagaCompensation(t *testing.T) {
	definition, err := compiler.Compile(`type Input { order_id: string }
type PaymentInput { order_id: string }
type RefundInput { transaction_id: string }
type Output { status: string }
task Charge(input: PaymentInput) -> Output { }
task Refund(input: RefundInput) -> Output { }
workflow Payment(input: Input) -> Output {
    step charge = Charge({ order_id: input.order_id }) compensate with Refund({ transaction_id: charge.result.transaction_id });
    fail("risk review rejected payment");
}`)
	if err != nil {
		t.Fatal(err)
	}
	memory := store.NewMemoryStore()
	engine := New(memory)
	instance, err := engine.Start(definition, map[string]any{"order_id": "order-fail"})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.CompleteTask(instance.ID, "charge", map[string]any{"transaction_id": "txn-fail"}); err != nil {
		t.Fatal(err)
	}
	instance, err = memory.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if instance.Status != domain.WorkflowCompensating {
		t.Fatalf("workflow status after explicit fail = %s, want COMPENSATING", instance.Status)
	}
	compensation := readyCompensation(t, instance)
	if compensation.TaskName != "Refund" || compensation.Input["transaction_id"] != "txn-fail" || instance.FailureReason != "risk review rejected payment" {
		t.Fatalf("explicit-fail compensation/reason = %#v/%q", compensation, instance.FailureReason)
	}
	if err := engine.CompleteTask(instance.ID, compensation.NodeID, nil); err != nil {
		t.Fatal(err)
	}
	completed, err := memory.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if completed.Status != domain.WorkflowCompensated {
		t.Fatalf("workflow status after compensation = %s, want COMPENSATED", completed.Status)
	}
}

func TestDSLFailWithoutCompensationEndsFailed(t *testing.T) {
	definition, err := compiler.Compile(`type Input { valid: bool }
type Output { status: string }
workflow Validate(input: Input) -> Output {
	if (!input.valid) { fail("input rejected"); } else { return Output { status: "valid" }; }
}`)
	if err != nil {
		t.Fatal(err)
	}
	instance, err := New(store.NewMemoryStore()).Start(definition, map[string]any{"valid": false})
	if err != nil {
		t.Fatal(err)
	}
	if instance.Status != domain.WorkflowFailed || instance.FailureReason != "input rejected" {
		t.Fatalf("explicit failure status/reason = %s/%q", instance.Status, instance.FailureReason)
	}
}

func TestFanOutFailureCompensatesSuccessfulItemsInReverseCompletionOrder(t *testing.T) {
	definition, err := compiler.Compile(`type Input { ids: list[int] }
type ItemInput { id: int }
type RefundInput { id: int }
type Output { status: string }
task Charge(input: ItemInput) -> Output { }
task Refund(input: RefundInput) -> Output { }
workflow BatchPayment(input: Input) -> Output {
    fan_out item in input.ids concurrency 2 {
        step charge = Charge({ id: item }) compensate with Refund({ id: item });
    }
	return Output { status: "charged" };
}`)
	if err != nil {
		t.Fatal(err)
	}
	memory := store.NewMemoryStore()
	engine := New(memory)
	instance, err := engine.Start(definition, map[string]any{"ids": []any{11, 22, 33}})
	if err != nil {
		t.Fatal(err)
	}
	var first, second domain.TaskInstance
	for _, task := range instance.Tasks {
		if task.FanOutIndex == 0 {
			first = task
		} else if task.FanOutIndex == 1 {
			second = task
		}
	}
	if first.ID == "" || second.ID == "" {
		t.Fatalf("initial fan-out tasks = %#v", instance.Tasks)
	}
	if err := engine.CompleteTask(instance.ID, first.NodeID, map[string]any{"charged": true}); err != nil {
		t.Fatal(err)
	}
	if err := engine.FailTask(instance.ID, second.NodeID, "card declined"); err != nil {
		t.Fatal(err)
	}
	instance, err = memory.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if instance.Status != domain.WorkflowRunning {
		t.Fatalf("workflow status while an already-started item drains = %s, want RUNNING", instance.Status)
	}
	var drainingTask domain.TaskInstance
	for _, task := range instance.Tasks {
		if task.FanOutIndex == 2 && task.Status == domain.TaskReady {
			drainingTask = task
		}
		if task.FanOutIndex == 3 {
			t.Fatal("fan-out admitted item 3 after item 1 failed")
		}
	}
	if drainingTask.ID == "" {
		t.Fatalf("already-started item 2 was not left available to drain: %#v", instance.Tasks)
	}
	if err := engine.CompleteTask(instance.ID, drainingTask.NodeID, map[string]any{"charged": true}); err != nil {
		t.Fatal(err)
	}
	instance, err = memory.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if instance.Status != domain.WorkflowCompensating {
		t.Fatalf("workflow status after fan-out drain = %s, want COMPENSATING", instance.Status)
	}
	compensation := readyCompensation(t, instance)
	if compensation.OriginalNodeID != drainingTask.NodeID || fmt.Sprint(compensation.Input["id"]) != "33" {
		t.Fatalf("first fan-out compensation = %#v, want original node %q and input id 33", compensation, drainingTask.NodeID)
	}
	if err := engine.CompleteTask(instance.ID, compensation.NodeID, nil); err != nil {
		t.Fatal(err)
	}
	instance, err = memory.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	compensation = readyCompensation(t, instance)
	if compensation.OriginalNodeID != first.NodeID || fmt.Sprint(compensation.Input["id"]) != "11" {
		t.Fatalf("second fan-out compensation = %#v, want first completed item mapping", compensation)
	}
	if err := engine.CompleteTask(instance.ID, compensation.NodeID, nil); err != nil {
		t.Fatal(err)
	}
	completed, err := memory.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if completed.Status != domain.WorkflowCompensated {
		t.Fatalf("workflow status = %s, want COMPENSATED", completed.Status)
	}
	for _, task := range completed.Tasks {
		if task.FanOutIndex == 3 {
			t.Fatal("fan-out scheduled an unstarted item after an earlier item failed")
		}
	}
}

func readyCompensation(t *testing.T, instance *domain.WorkflowInstance) domain.TaskInstance {
	t.Helper()
	for _, task := range instance.Tasks {
		if task.IsCompensation && task.Status == domain.TaskReady {
			return task
		}
	}
	t.Fatal("no READY compensation task found")
	return domain.TaskInstance{}
}

func TestDeciderResolvesForkJoinBarriers(t *testing.T) {
	definition := domain.WorkflowDef{
		Name:    "parallel",
		Version: 1,
		Nodes: map[string]domain.NodeDefinition{
			"fork":    {ID: "fork", Type: domain.NodeFork},
			"email":   {ID: "email", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "SendEmail"}, DependsOn: []string{"fork"}},
			"reserve": {ID: "reserve", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "ReserveItem"}, DependsOn: []string{"fork"}},
			"join":    {ID: "join", Type: domain.NodeJoin, DependsOn: []string{"email", "reserve"}},
			"finish":  {ID: "finish", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "Finish"}, DependsOn: []string{"join"}},
		},
	}
	memory := store.NewMemoryStore()
	decider := New(memory)
	instance, err := decider.Start(definition, nil)
	if err != nil {
		t.Fatal(err)
	}
	if instance.Tasks["email"].Status != domain.TaskReady || instance.Tasks["reserve"].Status != domain.TaskReady {
		t.Fatal("fork did not release both branches")
	}
	if _, scheduled := instance.Tasks["finish"]; scheduled {
		t.Fatal("join released finish before both branches completed")
	}
	if err := decider.CompleteTask(instance.ID, "email", nil); err != nil {
		t.Fatal(err)
	}
	if err := decider.CompleteTask(instance.ID, "reserve", nil); err != nil {
		t.Fatal(err)
	}
	instance, err = memory.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if instance.Tasks["finish"].Status != domain.TaskReady {
		t.Fatal("join did not release finish after both branches completed")
	}
	if err := decider.CompleteTask(instance.ID, "finish", nil); err != nil {
		t.Fatal(err)
	}
	instance, err = memory.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if instance.Status != domain.WorkflowCompleted {
		t.Fatalf("workflow status = %s, want %s", instance.Status, domain.WorkflowCompleted)
	}
}

func TestDeciderRejectsInvalidTaskTransitions(t *testing.T) {
	definition := domain.WorkflowDef{
		Name:    "transitions",
		Version: 1,
		Nodes: map[string]domain.NodeDefinition{
			"task": {ID: "task", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "Run"}},
		},
	}
	decider := New(store.NewMemoryStore())
	instance, err := decider.Start(definition, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := decider.StartTask(instance.ID, "missing", 1); err == nil {
		t.Fatal("StartTask accepted an unknown task")
	}
	if err := decider.StartTask(instance.ID, "task", 7); err != nil {
		t.Fatal(err)
	}
	if err := decider.StartTask(instance.ID, "task", 8); err == nil {
		t.Fatal("StartTask accepted a running task")
	}
	if err := decider.CompleteTask(instance.ID, "task", nil); err != nil {
		t.Fatal(err)
	}
	if err := decider.CompleteTask(instance.ID, "task", nil); err == nil {
		t.Fatal("CompleteTask accepted an already completed task")
	}
}

func TestDeciderReleasesDownstreamTaskAfterDelayNode(t *testing.T) {
	definition := domain.WorkflowDef{
		Name:    "delayed",
		Version: 1,
		Nodes: map[string]domain.NodeDefinition{
			"wait":   {ID: "wait", Type: domain.NodeDelay, Delay: 15 * time.Millisecond},
			"notify": {ID: "notify", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "Notify"}, DependsOn: []string{"wait"}},
		},
	}
	memory := store.NewMemoryStore()
	timers := timer.NewService(2)
	decider := NewWithTimer(memory, timers)
	defer func() {
		decider.Close()
		timers.Close()
	}()
	instance, err := decider.Start(definition, nil)
	if err != nil {
		t.Fatal(err)
	}
	if instance.Status != domain.WorkflowRunning {
		t.Fatalf("workflow status = %s, want %s while delay is pending", instance.Status, domain.WorkflowRunning)
	}
	if _, scheduled := instance.Tasks["notify"]; scheduled {
		t.Fatal("downstream task scheduled before delay completed")
	}

	deadline := time.After(250 * time.Millisecond)
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			instance, err = memory.Get(instance.ID)
			if err != nil {
				t.Fatal(err)
			}
			if instance.Tasks["notify"].Status == domain.TaskReady {
				if err := decider.CompleteTask(instance.ID, "notify", nil); err != nil {
					t.Fatal(err)
				}
				completed, err := memory.Get(instance.ID)
				if err != nil {
					t.Fatal(err)
				}
				if completed.Status != domain.WorkflowCompleted {
					t.Fatalf("workflow status = %s, want COMPLETED", completed.Status)
				}
				return
			}
		case <-deadline:
			t.Fatal("delay did not release downstream task")
		}
	}
}

func TestDeciderRequiresTimerForDelayNode(t *testing.T) {
	definition := domain.WorkflowDef{
		Name:    "missing-timer",
		Version: 1,
		Nodes: map[string]domain.NodeDefinition{
			"wait": {ID: "wait", Type: domain.NodeDelay, Delay: time.Second},
		},
	}
	if _, err := New(store.NewMemoryStore()).Start(definition, nil); err == nil {
		t.Fatal("Start accepted a delay workflow without a timer service")
	}
}

func TestRecoverResumesPendingWorkflowStart(t *testing.T) {
	definition := domain.WorkflowDef{
		Name: "pending-start", Version: 1,
		Nodes: map[string]domain.NodeDefinition{
			"task": {ID: "task", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "Run"}},
		},
	}
	instance, err := domain.NewWorkflowInstance(definition, nil)
	if err != nil {
		t.Fatal(err)
	}
	memory := store.NewMemoryStore()
	if err := memory.Create(instance); err != nil {
		t.Fatal(err)
	}
	if err := New(memory).Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	recovered, err := memory.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Status != domain.WorkflowRunning || recovered.Tasks["task"].Status != domain.TaskReady {
		t.Fatalf("recovered pending workflow = status %s, task %#v", recovered.Status, recovered.Tasks["task"])
	}
	started := 0
	for _, event := range recovered.Events {
		if event.Type == domain.EventWorkflowStarted {
			started++
		}
	}
	if started != 1 {
		t.Fatalf("workflow-started events = %d, want 1", started)
	}
}

func TestRecoverContinuesAfterPersistedCompensationCompletion(t *testing.T) {
	definition := domain.WorkflowDef{
		Name: "compensation-recovery", Version: 1,
		Nodes: map[string]domain.NodeDefinition{
			"reserve": {ID: "reserve", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "Reserve"}, Compensation: &domain.TaskDefinition{Name: "Release"}},
			"ship":    {ID: "ship", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "Ship"}, DependsOn: []string{"reserve"}},
		},
	}
	memory := store.NewMemoryStore()
	first := New(memory)
	instance, err := first.Start(definition, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.CompleteTask(instance.ID, "reserve", nil); err != nil {
		t.Fatal(err)
	}
	if err := first.FailTask(instance.ID, "ship", "shipment failed"); err != nil {
		t.Fatal(err)
	}
	current, err := memory.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	compensation := readyCompensation(t, current)
	if err := memory.Append(instance.ID, domain.Event{
		WorkflowID: instance.ID, TaskID: compensation.ID, NodeID: compensation.NodeID,
		Type: domain.EventTaskCompleted, Payload: map[string]any{"result": map[string]any{}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := New(memory).Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	recovered, err := memory.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Status != domain.WorkflowCompensated {
		t.Fatalf("recovered compensation status = %s, want COMPENSATED", recovered.Status)
	}
}

func TestRecoverReschedulesPersistedRetryDeadline(t *testing.T) {
	definition := domain.WorkflowDef{
		Name: "retry-recovery", Version: 1,
		Nodes: map[string]domain.NodeDefinition{
			"task": {ID: "task", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "Run", Retries: 1, Backoff: 120 * time.Millisecond}},
		},
	}
	memory := store.NewMemoryStore()
	firstTimers := timer.NewService(4)
	first := NewWithTimer(memory, firstTimers)
	instance, err := first.Start(definition, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.FailTask(instance.ID, "task", "temporary"); err != nil {
		t.Fatal(err)
	}
	first.Close()
	firstTimers.Close()

	restartedTimers := timer.NewService(4)
	restarted := NewWithTimer(memory, restartedTimers)
	defer func() { restarted.Close(); restartedTimers.Close() }()
	if err := restarted.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(time.Second)
	for {
		recovered, getErr := memory.Get(instance.ID)
		if getErr != nil {
			t.Fatal(getErr)
		}
		if task := recovered.Tasks["task"]; task.Status == domain.TaskReady && task.Attempt == 2 {
			if task.ID == instance.Tasks["task"].ID {
				t.Fatal("recovered retry reused the previous task ID")
			}
			return
		}
		select {
		case <-time.After(5 * time.Millisecond):
		case <-deadline:
			t.Fatal("recovered retry deadline did not schedule the next attempt")
		}
	}
}

func TestRecoverRestoresDelayWithoutExtendingDeadline(t *testing.T) {
	definition := domain.WorkflowDef{
		Name: "delay-recovery", Version: 1,
		Nodes: map[string]domain.NodeDefinition{
			"wait":   {ID: "wait", Type: domain.NodeDelay, Delay: 800 * time.Millisecond},
			"notify": {ID: "notify", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "Notify"}, DependsOn: []string{"wait"}},
		},
	}
	memory := store.NewMemoryStore()
	firstTimers := timer.NewService(4)
	first := NewWithTimer(memory, firstTimers)
	instance, err := first.Start(definition, nil)
	if err != nil {
		t.Fatal(err)
	}
	first.Close()
	firstTimers.Close()
	time.Sleep(600 * time.Millisecond)

	restartedTimers := timer.NewService(4)
	restarted := NewWithTimer(memory, restartedTimers)
	defer func() { restarted.Close(); restartedTimers.Close() }()
	recoveryStarted := time.Now()
	if err := restarted.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(450 * time.Millisecond)
	for {
		recovered, getErr := memory.Get(instance.ID)
		if getErr != nil {
			t.Fatal(getErr)
		}
		if recovered.Tasks["notify"].Status == domain.TaskReady {
			if elapsed := time.Since(recoveryStarted); elapsed >= 450*time.Millisecond {
				t.Fatalf("delay took %s after recovery; original deadline should have been retained", elapsed)
			}
			return
		}
		select {
		case <-time.After(5 * time.Millisecond):
		case <-deadline:
			t.Fatal("recovered delay deadline did not release its dependent task")
		}
	}
}

func TestAdvanceReloadsAfterOCCConflict(t *testing.T) {
	definition := domain.WorkflowDef{
		Name: "advance-conflict", Version: 1,
		Nodes: map[string]domain.NodeDefinition{
			"task": {ID: "task", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "Run"}},
		},
	}
	memory := &conflictOnceStore{
		MemoryStore:   store.NewMemoryStore(),
		conflictType:  domain.EventTaskScheduled,
		conflictsLeft: 1,
	}
	instance, err := New(memory).Start(definition, nil)
	if err != nil {
		t.Fatal(err)
	}
	if instance.Tasks["task"].Status != domain.TaskReady {
		t.Fatalf("task status after injected conflict = %s, want READY", instance.Tasks["task"].Status)
	}
	scheduled := 0
	for _, event := range instance.Events {
		if event.Type == domain.EventTaskScheduled && event.NodeID == "task" {
			scheduled++
		}
	}
	if scheduled != 1 {
		t.Fatalf("task-scheduled events = %d, want 1", scheduled)
	}
}

func TestDeciderRetriesFailedTaskAfterFixedBackoffAndExhausts(t *testing.T) {
	definition := domain.WorkflowDef{
		Name:    "retry",
		Version: 1,
		Nodes: map[string]domain.NodeDefinition{
			"task": {ID: "task", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "Run", Retries: 1, Backoff: 10 * time.Millisecond}},
		},
	}
	memory := store.NewMemoryStore()
	timers := timer.NewService(8)
	engine := NewWithTimer(memory, timers)
	defer func() { engine.Close(); timers.Close() }()
	instance, err := engine.Start(definition, nil)
	if err != nil {
		t.Fatal(err)
	}
	firstAttempt := instance.Tasks["task"]
	if firstAttempt.Attempt != 1 || firstAttempt.RetryLimit != 1 {
		t.Fatalf("initial task retry metadata = %#v", firstAttempt)
	}
	if err := engine.FailTask(instance.ID, "task", "temporary failure"); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(time.Second)
	for {
		current, getErr := memory.Get(instance.ID)
		if getErr != nil {
			t.Fatal(getErr)
		}
		attempt := current.Tasks["task"]
		if attempt.Status == domain.TaskReady && attempt.Attempt == 2 {
			if attempt.ID == firstAttempt.ID {
				t.Fatal("retry reused the original task ID")
			}
			if err := engine.FailTask(instance.ID, "task", "retry exhausted"); err != nil {
				t.Fatal(err)
			}
			failed, getErr := memory.Get(instance.ID)
			if getErr != nil {
				t.Fatal(getErr)
			}
			if failed.Status != domain.WorkflowFailed || failed.FailureReason != "retry exhausted" {
				t.Fatalf("exhausted retry status/reason = %s/%q", failed.Status, failed.FailureReason)
			}
			return
		}
		select {
		case <-time.After(time.Millisecond):
		case <-deadline:
			t.Fatal("retry task did not become ready after backoff")
		}
	}
}

func TestFanOutRetryRetainsItemOwnershipUntilRetryCompletes(t *testing.T) {
	definition, err := compiler.Compile(`type Input { ids: list[int] }
type ItemInput { id: int }
type Output { status: string }
task Process(input: ItemInput) -> Output { retries 1 with backoff 5ms }
workflow RetryItems(input: Input) -> Output {
    fan_out item in input.ids concurrency 1 { step process = Process({ id: item }); }
	return Output { status: "processed" };
}`)
	if err != nil {
		t.Fatal(err)
	}
	memory := store.NewMemoryStore()
	timers := timer.NewService(8)
	engine := NewWithTimer(memory, timers)
	defer func() { engine.Close(); timers.Close() }()
	instance, err := engine.Start(definition, map[string]any{"ids": []any{1, 2}})
	if err != nil {
		t.Fatal(err)
	}
	var first domain.TaskInstance
	for _, task := range instance.Tasks {
		first = task
	}
	if first.FanOutIndex != 0 {
		t.Fatalf("first scheduled item index = %d, want 0", first.FanOutIndex)
	}
	if err := engine.FailTask(instance.ID, first.NodeID, "retry me"); err != nil {
		t.Fatal(err)
	}
	current, err := memory.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Tasks[first.NodeID].Status != domain.TaskRetrying || len(current.Tasks) != 1 {
		t.Fatalf("retrying task retained state = %#v", current.Tasks)
	}
	deadline := time.After(time.Second)
	for {
		current, err = memory.Get(instance.ID)
		if err != nil {
			t.Fatal(err)
		}
		for nodeID, task := range current.Tasks {
			if task.Attempt == 2 && task.Status == domain.TaskReady {
				if task.FanOutIndex != 0 {
					t.Fatalf("retry changed item ownership: %#v", task)
				}
				if err := engine.CompleteTask(instance.ID, nodeID, nil); err != nil {
					t.Fatal(err)
				}
				updated, getErr := memory.Get(instance.ID)
				if getErr != nil {
					t.Fatal(getErr)
				}
				if len(updated.Tasks) != 2 {
					t.Fatalf("next fan-out item was not admitted after retry completed: %#v", updated.Tasks)
				}
				return
			}
		}
		select {
		case <-time.After(time.Millisecond):
		case <-deadline:
			t.Fatal("fan-out retry did not reach attempt 2")
		}
	}
}

func TestDeciderExecutesOnlySelectedConditionalBranch(t *testing.T) {
	source := `type Input { amount: int }
type PaymentInput { amount: int }
type ReceiptInput { approved: bool }
type Output { status: string }
task Authorize(input: PaymentInput) -> Output { }
task SendReceipt(input: ReceiptInput) -> Output { }
task RejectPayment(input: PaymentInput) -> Output { }
workflow Checkout(input: Input) -> Output {
    step authorize = Authorize({ amount: input.amount });
    if (authorize.result.approved == true && input.amount >= 10 || input.amount == 0) {
        step receipt = SendReceipt({ approved: authorize.result.approved });
		return Output { status: "approved" };
    } else {
        step reject = RejectPayment({ amount: input.amount });
		return Output { status: "rejected" };
    }
}`
	definition, err := compiler.Compile(source)
	if err != nil {
		t.Fatal(err)
	}
	for _, testCase := range []struct {
		name          string
		amount        int
		approved      bool
		selectedNode  string
		skippedNode   string
		selectedInput map[string]any
	}{
		{name: "then branch", amount: 20, approved: true, selectedNode: "receipt", skippedNode: "reject", selectedInput: map[string]any{"approved": true}},
		{name: "else branch", amount: 5, approved: true, selectedNode: "reject", skippedNode: "receipt", selectedInput: map[string]any{"amount": 5}},
		{name: "logical OR branch", amount: 0, approved: false, selectedNode: "receipt", skippedNode: "reject", selectedInput: map[string]any{"approved": false}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			memory := store.NewMemoryStore()
			engine := New(memory)
			instance, err := engine.Start(definition, map[string]any{"amount": testCase.amount})
			if err != nil {
				t.Fatal(err)
			}
			authorize := instance.Tasks["authorize"]
			if authorize.Input["amount"] != testCase.amount {
				t.Fatalf("authorize input = %#v", authorize.Input)
			}
			if _, scheduled := instance.Tasks[testCase.selectedNode]; scheduled {
				t.Fatal("conditional branch scheduled before prerequisite task completed")
			}
			if err := engine.CompleteTask(instance.ID, "authorize", map[string]any{"approved": testCase.approved}); err != nil {
				t.Fatal(err)
			}
			current, err := memory.Get(instance.ID)
			if err != nil {
				t.Fatal(err)
			}
			selected, ok := current.Tasks[testCase.selectedNode]
			if !ok || selected.Status != domain.TaskReady {
				t.Fatalf("selected task %q = %#v", testCase.selectedNode, selected)
			}
			if !reflect.DeepEqual(selected.Input, testCase.selectedInput) {
				t.Fatalf("selected task input = %#v, want %#v", selected.Input, testCase.selectedInput)
			}
			if _, scheduled := current.Tasks[testCase.skippedNode]; scheduled {
				t.Fatalf("unselected task %q was scheduled", testCase.skippedNode)
			}
			skipped := false
			for _, event := range current.Events {
				if event.Type == domain.EventNodeSkipped && event.NodeID == testCase.skippedNode {
					skipped = true
				}
			}
			if !skipped {
				t.Fatalf("unselected branch node %q lacks skip event", testCase.skippedNode)
			}
			if err := engine.CompleteTask(instance.ID, testCase.selectedNode, nil); err != nil {
				t.Fatal(err)
			}
			completed, err := memory.Get(instance.ID)
			if err != nil {
				t.Fatal(err)
			}
			if completed.Status != domain.WorkflowCompleted {
				t.Fatalf("workflow status = %s, want COMPLETED", completed.Status)
			}
		})
	}
}

func TestDeciderFailsWorkflowWhenConditionDataIsMissing(t *testing.T) {
	definition, err := compiler.Compile(`type Input { amount: int }
type Output { status: string }
task Run(input: Input) -> Output { }
workflow BrokenCondition(input: Input) -> Output {
	if (input.missing == true) { step yes = Run({}); return Output { status: "yes" }; } else { step no = Run({}); return Output { status: "no" }; }
}`)
	if err != nil {
		t.Fatal(err)
	}
	memory := store.NewMemoryStore()
	instance, err := New(memory).Start(definition, map[string]any{"amount": 10})
	if err != nil {
		t.Fatal(err)
	}
	if instance.Status != domain.WorkflowFailed || len(instance.Events) != 3 || instance.Events[1].Type != domain.EventWorkflowFailureRequested || instance.Events[2].Type != domain.EventWorkflowFailed {
		t.Fatalf("invalid condition produced status/events %s/%#v", instance.Status, instance.Events)
	}
	if _, scheduled := instance.Tasks["yes"]; scheduled {
		t.Fatal("task from invalid conditional branch was scheduled")
	}
}

func TestDeciderExpandsFanOutWithinPerBlockConcurrency(t *testing.T) {
	definition, err := compiler.Compile(`type Input { ids: list[int] }
type ItemInput { id: int }
type Output { status: string }
task Process(input: ItemInput) -> Output { }
workflow ProcessAll(input: Input) -> Output {
    fan_out item in input.ids concurrency 2 { step process = Process({ id: item }); }
	return Output { status: "processed" };
}`)
	if err != nil {
		t.Fatal(err)
	}
	memory := store.NewMemoryStore()
	engine := New(memory)
	instance, err := engine.Start(definition, map[string]any{"ids": []any{10, 20, 30}})
	if err != nil {
		t.Fatal(err)
	}
	if len(instance.Tasks) != 2 {
		t.Fatalf("initial fan-out tasks = %d, want concurrency limit 2", len(instance.Tasks))
	}
	seenInputs := map[string]bool{}
	var firstTask domain.TaskInstance
	for _, task := range instance.Tasks {
		seenInputs[fmt.Sprint(task.Input["id"])] = true
		if task.FanOutIndex == 0 {
			firstTask = task
		}
	}
	if !seenInputs["10"] || !seenInputs["20"] || seenInputs["30"] {
		t.Fatalf("first fan-out batch item inputs = %#v", seenInputs)
	}
	if err := engine.CompleteTask(instance.ID, firstTask.NodeID, nil); err != nil {
		t.Fatal(err)
	}
	instance, err = memory.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(instance.Tasks) != 3 {
		t.Fatalf("tasks after one item completes = %d, want third item admitted", len(instance.Tasks))
	}
	active := 0
	for _, task := range instance.Tasks {
		if task.Status == domain.TaskReady || task.Status == domain.TaskRunning {
			active++
		}
	}
	if active != 2 {
		t.Fatalf("active fan-out tasks after refill = %d, want 2", active)
	}
	for _, task := range instance.Tasks {
		if task.Status == domain.TaskReady {
			if err := engine.CompleteTask(instance.ID, task.NodeID, nil); err != nil {
				t.Fatal(err)
			}
		}
	}
	instance, err = memory.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if instance.Status != domain.WorkflowCompleted {
		t.Fatalf("fan-out workflow status = %s, want COMPLETED", instance.Status)
	}
}

func TestDeciderEmptyFanOutSkipsBodyAndContinues(t *testing.T) {
	definition, err := compiler.Compile(`type Input { ids: list[int] }
type ItemInput { id: int }
type Output { status: string }
task Process(input: ItemInput) -> Output { }
task Finish(input: ItemInput) -> Output { }
workflow EmptySafe(input: Input) -> Output {
    fan_out item in input.ids concurrency 2 { step process = Process({ id: item }); }
    step finish = Finish({ id: 1 });
	return Output { status: "finished" };
}`)
	if err != nil {
		t.Fatal(err)
	}
	memory := store.NewMemoryStore()
	engine := New(memory)
	instance, err := engine.Start(definition, map[string]any{"ids": []any{}})
	if err != nil {
		t.Fatal(err)
	}
	if len(instance.Tasks) != 1 || instance.Tasks["finish"].Status != domain.TaskReady {
		t.Fatalf("empty fan-out tasks = %#v, want only finish ready", instance.Tasks)
	}
	for _, task := range instance.Tasks {
		if task.TaskName == "Process" {
			t.Fatal("empty fan-out scheduled a body task")
		}
	}
}

func TestDeciderNestedFanOutUsesIndependentLimits(t *testing.T) {
	definition, err := compiler.Compile(`type Item { ids: list[int] }
type Input { groups: list[Item] }
type ChildInput { id: int }
type Output { status: string }
task Process(input: ChildInput) -> Output { }
workflow Nested(input: Input) -> Output {
    fan_out group in input.groups concurrency 3 {
        fan_out child in group.ids concurrency 2 { step process = Process({ id: child }); }
    }
	return Output { status: "processed" };
}`)
	if err != nil {
		t.Fatal(err)
	}
	memory := store.NewMemoryStore()
	engine := New(memory)
	instance, err := engine.Start(definition, map[string]any{"groups": []any{
		map[string]any{"ids": []any{1, 2, 3}},
		map[string]any{"ids": []any{4}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	readyByInnerFanOut := make(map[string]int)
	for _, task := range instance.Tasks {
		if task.Status == domain.TaskReady {
			readyByInnerFanOut[task.FanOutID]++
		}
	}
	if len(readyByInnerFanOut) != 2 {
		t.Fatalf("nested fan-out active blocks = %#v", readyByInnerFanOut)
	}
	for innerID, count := range readyByInnerFanOut {
		if count > 2 {
			t.Fatalf("nested fan-out %q scheduled %d items beyond its limit 2", innerID, count)
		}
	}
	if total := len(instance.Tasks); total != 3 {
		t.Fatalf("nested fan-out initially scheduled %d item tasks, want 3 under independent limits", total)
	}
}

func TestDeciderFanOutFailureStopsAdmissionAndDrainsStartedItems(t *testing.T) {
	definition, err := compiler.Compile(`type Input { ids: list[int] }
type ItemInput { id: int }
type Output { status: string }
task Process(input: ItemInput) -> Output { }
workflow BestEffort(input: Input) -> Output {
    fan_out item in input.ids concurrency 2 { step process = Process({ id: item }); }
	return Output { status: "processed" };
}`)
	if err != nil {
		t.Fatal(err)
	}
	memory := store.NewMemoryStore()
	engine := New(memory)
	instance, err := engine.Start(definition, map[string]any{"ids": []any{1, 2, 3, 4}})
	if err != nil {
		t.Fatal(err)
	}
	var failedTask domain.TaskInstance
	for _, task := range instance.Tasks {
		if task.FanOutIndex == 0 {
			failedTask = task
		}
	}
	if failedTask.ID == "" {
		t.Fatalf("could not find first fan-out item task: %#v", instance.Tasks)
	}
	if err := engine.FailTask(instance.ID, failedTask.NodeID, "item rejected"); err != nil {
		t.Fatal(err)
	}
	instance, err = memory.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(instance.Tasks) != 2 {
		t.Fatalf("failure admitted new items: task count = %d, want only already-started items", len(instance.Tasks))
	}
	for _, task := range instance.Tasks {
		if task.Status == domain.TaskReady {
			if err := engine.CompleteTask(instance.ID, task.NodeID, nil); err != nil {
				t.Fatal(err)
			}
		}
	}
	instance, err = memory.Get(instance.ID)
	if err != nil {
		t.Fatal(err)
	}
	if instance.Status != domain.WorkflowFailed {
		t.Fatalf("workflow status after fan-out drain = %s, want FAILED", instance.Status)
	}
	for _, task := range instance.Tasks {
		if task.FanOutIndex >= 2 {
			t.Fatalf("unstarted item %d was admitted after failure", task.FanOutIndex)
		}
	}
}
