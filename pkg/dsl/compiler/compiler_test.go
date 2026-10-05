package compiler

import (
	"testing"
	"time"

	"github.com/zephyr-workflow/zephyr/pkg/domain"
)

func TestCompileBuildsDedicatedWorkflowDAG(t *testing.T) {
	source := `namespace ecommerce.orders;
type Input { order_id: string }
type Output { status: string }
task ChargePayment(input: Input) -> Output { timeout 30s }
task RefundPayment(input: Input) -> Output { }
task Notify(input: Input) -> Output { }
workflow Checkout(input: Input) -> Output {
    delay wait = 10s;
    step charge = ChargePayment({});
    fork {
        branch { step email = Notify({}); }
        branch { step audit = Notify({}) compensate with RefundPayment({}); }
    }
    return Output { status: "COMPLETED" };
}`
	definition, err := Compile(source)
	if err != nil {
		t.Fatal(err)
	}
	if definition.Name != "Checkout" || definition.Version != 1 {
		t.Fatalf("definition identity = %s:%d", definition.Name, definition.Version)
	}
	if len(definition.Start) != 1 || definition.Start[0] != "wait" {
		t.Fatalf("workflow start nodes = %#v, want [wait]", definition.Start)
	}
	if definition.Nodes["wait"].Type != domain.NodeDelay || definition.Nodes["wait"].Delay != 10*time.Second {
		t.Fatalf("wait node = %#v", definition.Nodes["wait"])
	}
	if definition.Nodes["charge"].Type != domain.NodeTask || definition.Nodes["charge"].Task.Name != "ChargePayment" {
		t.Fatalf("charge node = %#v", definition.Nodes["charge"])
	}
	foundFork, foundJoin := false, false
	for _, node := range definition.Nodes {
		foundFork = foundFork || node.Type == domain.NodeFork
		foundJoin = foundJoin || node.Type == domain.NodeJoin
	}
	if !foundFork || !foundJoin {
		t.Fatal("compiled graph does not contain fork and join nodes")
	}
	if err := definition.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestCompileRejectsUnknownTask(t *testing.T) {
	source := `type Input { value: string }
type Output { status: string }
workflow Broken(input: Input) -> Output { step run = Missing({}); }`
	if _, err := Compile(source); err == nil {
		t.Fatal("compiler accepted an unknown task")
	}
}

func TestCompileRequiresReturnOnEverySuccessfulPath(t *testing.T) {
	source := `type Input { value: string }
type Output { status: string }
workflow MissingReturn(input: Input) -> Output { if (input.value == "ok") { fail("invalid"); } }`
	if _, err := Compile(source); err == nil {
		t.Fatal("compiler accepted a successful path without a return")
	}
}

func TestCompileRejectsMalformedWorkflowReturn(t *testing.T) {
	for _, source := range []string{
		`type Input { value: string }
type Output { status: string }
workflow WrongType(input: Input) -> Output { return Input { value: input.value }; }`,
		`type Input { value: string }
type Output { status: string }
workflow UnknownField(input: Input) -> Output { return Output { missing: input.value }; }`,
		`type Input { value: string }
type Output { status: string }
workflow MissingField(input: Input) -> Output { return Output { }; }`,
		`type Input { value: string }
type Output { status: string }
workflow WrongFieldType(input: Input) -> Output { return Output { status: 42 }; }`,
	} {
		if _, err := Compile(source); err == nil {
			t.Errorf("compiler accepted malformed workflow return: %s", source)
		}
	}
}

func TestCompileRejectsReturnReferencesOutsideItsBranch(t *testing.T) {
	source := `type Input { ok: bool }
type TaskOutput { status: string }
type Output { status: string }
task Run(input: Input) -> TaskOutput { }
workflow BranchScope(input: Input) -> Output {
    if (input.ok) {
        step accepted = Run({});
        return Output { status: accepted.result.status };
    } else {
        return Output { status: accepted.result.status };
    }
}`
	if _, err := Compile(source); err == nil {
		t.Fatal("compiler accepted a step result from an unselected branch")
	}
}

func TestCompileKeepsContinuingBranchResultsVisible(t *testing.T) {
	source := `type Input { ok: bool }
type TaskOutput { status: string }
type Output { status: string }
task Run(input: Input) -> TaskOutput { }
workflow ContinuingBranch(input: Input) -> Output {
	if (input.ok) {
		return Output { status: "done" };
	} else {
		step continued = Run({});
	}
	return Output { status: continued.result.status };
}`
	if _, err := Compile(source); err != nil {
		t.Fatalf("compiler rejected result on the only continuing branch: %v", err)
	}
}

func TestCompileRejectsReturnsInsideConcurrentBlocks(t *testing.T) {
	for _, body := range []string{
		`fork { branch { return Output { status: "early" }; } branch { step run = Run({}); } } return Output { status: "done" };`,
		`fan_out item in input.items { return Output { status: "early" }; } return Output { status: "done" };`,
	} {
		source := `type Input { items: list[int] }
type TaskOutput { status: string }
type Output { status: string }
task Run(input: Input) -> TaskOutput { }
workflow ConcurrentReturn(input: Input) -> Output { ` + body + ` }`
		if _, err := Compile(source); err == nil {
			t.Errorf("compiler accepted return inside concurrent block: %s", body)
		}
	}
}

func TestCompileRejectsUnresolvedReturnExpressions(t *testing.T) {
	for _, expression := range []string{
		`mystery.value`,
		`input.missing`,
		`run.result.missing`,
		`run.status`,
		`input.value && true`,
		`!input.value`,
		`input.value < true`,
	} {
		source := `type Input { value: string }
type TaskOutput { status: string }
type Output { value: string }
task Run(input: Input) -> TaskOutput { }
workflow Broken(input: Input) -> Output {
    step run = Run({ value: input.value });
    return Output { value: ` + expression + ` };
}`
		if _, err := Compile(source); err == nil {
			t.Errorf("compiler accepted invalid return expression %q", expression)
		}
	}
}

func TestCompileSupportsTypedReturnExpressionsAndNestedMappings(t *testing.T) {
	source := `type Input { value: string valid: bool }
type TaskOutput { status: string details: Details }
type Details { code: int }
type Output { value: string accepted: bool details: Details }
task Run(input: Input) -> TaskOutput { }
workflow Valid(input: Input) -> Output {
    step run = Run({ value: input.value, valid: input.valid });
    return Output {
        value: run.result.status,
		accepted: !input.valid == false && input.value == "ok" || input.value != "no",
        details: run.result.details
    };
}`
	if _, err := Compile(source); err != nil {
		t.Fatal(err)
	}
}

func TestCompileSupportsCollectionTypes(t *testing.T) {
	source := `type Input { items: list[string] }
type Output { statuses: list[string] }
task Reserve(input: Input) -> Output { }
workflow ReserveAll(input: Input) -> Output {
    step reserve = Reserve({ items: input.items });
    return Output { statuses: input.items };
}`
	if _, err := Compile(source); err != nil {
		t.Fatal(err)
	}
}

func TestCompilePreservesTaskRetryPolicy(t *testing.T) {
	definition, err := Compile(`type Input { value: string }
type Output { status: string }
task Charge(input: Input) -> Output { retries 3 with backoff 250ms }
task Refund(input: Input) -> Output { retries 2 with backoff 1s }
workflow Payment(input: Input) -> Output {
    step charge = Charge({ value: input.value }) compensate with Refund({ value: input.value });
	return Output { status: "charged" };
}`)
	if err != nil {
		t.Fatal(err)
	}
	charge := definition.Nodes["charge"]
	if charge.Task.Retries != 3 || charge.Task.Backoff != 250*time.Millisecond {
		t.Fatalf("charge retry policy = %d/%s", charge.Task.Retries, charge.Task.Backoff)
	}
	if charge.Compensation.Retries != 2 || charge.Compensation.Backoff != time.Second {
		t.Fatalf("refund retry policy = %d/%s", charge.Compensation.Retries, charge.Compensation.Backoff)
	}
}

func TestCompileRejectsUnknownTaskInputField(t *testing.T) {
	source := `type Input { value: string }
type Output { status: string }
task Run(input: Input) -> Output { }
workflow Broken(input: Input) -> Output {
    step run = Run({ missing: input.value });
}`
	if _, err := Compile(source); err == nil {
		t.Fatal("compiler accepted an unknown task input field")
	}
}

func TestCompilePreservesConditionalBranchesAndTaskInputs(t *testing.T) {
	source := `type Input { amount: int region: string }
type Output { status: string }
task Charge(input: Input) -> Output { }
workflow Checkout(input: Input) -> Output {
    if (input.amount > 5 && input.region == "us") {
        step domestic = Charge({ amount: input.amount, region: input.region });
		return Output { status: "domestic" };
    } else {
        step international = Charge({ amount: input.amount, region: "intl" });
		return Output { status: "international" };
    }
}`
	definition, err := Compile(source)
	if err != nil {
		t.Fatal(err)
	}
	var switchNode *domain.NodeDefinition
	for nodeID, node := range definition.Nodes {
		if node.Type == domain.NodeSwitch {
			copy := node
			switchNode = &copy
			_ = nodeID
			break
		}
	}
	if switchNode == nil || switchNode.Condition == nil || switchNode.Condition.Kind != domain.ExpressionBinary || switchNode.Condition.Operator != "&&" {
		t.Fatalf("compiled switch condition = %#v", switchNode)
	}
	if len(switchNode.ThenNodes) != 2 || switchNode.ThenNodes[0] != "domestic" || definition.Nodes[switchNode.ThenNodes[1]].Type != domain.NodeReturn || len(switchNode.ElseNodes) != 2 || switchNode.ElseNodes[0] != "international" || definition.Nodes[switchNode.ElseNodes[1]].Type != domain.NodeReturn {
		t.Fatalf("compiled switch branches = %#v / %#v", switchNode.ThenNodes, switchNode.ElseNodes)
	}
	if definition.Nodes["domestic"].Input == nil || definition.Nodes["domestic"].Input.Kind != domain.ExpressionObject || len(definition.Nodes["domestic"].Input.Fields) != 2 {
		t.Fatalf("compiled task input = %#v", definition.Nodes["domestic"].Input)
	}
}
