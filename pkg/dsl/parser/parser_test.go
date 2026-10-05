package parser

import (
	"strings"
	"testing"
	"time"

	"github.com/zephyr-workflow/zephyr/pkg/dsl/ast"
)

func TestParseWorkflowWithDedicatedNodes(t *testing.T) {
	source := `namespace ecommerce.orders;

type Input { order_id: string }
type Output { status: string }
task ChargePayment(input: Input) -> Output {
    timeout 30s
    retries 3 with backoff 200ms
}
workflow Checkout(input: Input) -> Output {
    delay wait = 10s;
    step charge = ChargePayment({ order_id: input.order_id }) compensate with RefundPayment({ order_id: input.order_id });
    if (input.order_id != "") {
        fork {
            branch { step notify = SendEmail({}); }
            branch { fan_out item in input.items { step reserve = ReserveItem({ item_id: item }); } }
        }
    }
    return Output { status: "COMPLETED" };
}`
	file, err := Parse(source)
	if err != nil {
		t.Fatal(err)
	}
	if file.Namespace != "ecommerce.orders" || len(file.Types) != 2 || len(file.Tasks) != 1 || len(file.Workflows) != 1 {
		t.Fatalf("file declarations = %#v", file)
	}
	workflow := file.Workflows[0]
	if len(workflow.Body) != 4 {
		t.Fatalf("workflow body length = %d, want 4", len(workflow.Body))
	}
	if delay, ok := workflow.Body[0].(ast.DelayNode); !ok || delay.Duration != 10*time.Second {
		t.Fatalf("first node = %#v, want 10-second delay", workflow.Body[0])
	}
	step, ok := workflow.Body[1].(ast.StepNode)
	if !ok || step.Compensation == nil || step.Call.Name != "ChargePayment" {
		t.Fatalf("second node = %#v, want compensated ChargePayment step", workflow.Body[1])
	}
	ifNode, ok := workflow.Body[2].(ast.IfNode)
	if !ok || len(ifNode.Then) != 1 {
		t.Fatalf("third node = %#v, want if with one body node", workflow.Body[2])
	}
	if _, ok := ifNode.Then[0].(ast.ForkNode); !ok {
		t.Fatalf("if body node = %#v, want fork", ifNode.Then[0])
	}
	if _, ok := workflow.Body[3].(ast.ReturnNode); !ok {
		t.Fatalf("fourth node = %#v, want return", workflow.Body[3])
	}
}

func TestParseRejectsMalformedWorkflow(t *testing.T) {
	if _, err := Parse("workflow Broken(input: Input) -> Output {"); err == nil {
		t.Fatal("parser accepted an unterminated workflow")
	}
}

func TestParseConditionalLogicalOperatorPrecedence(t *testing.T) {
	source := `type Input { amount: int vip: bool }
type Output { status: string }
task Run(input: Input) -> Output { }
workflow Conditional(input: Input) -> Output {
    if (input.amount >= 100 && input.vip == true || !false) {
        step run = Run({});
    }
}`
	file, err := Parse(source)
	if err != nil {
		t.Fatal(err)
	}
	condition := file.Workflows[0].Body[0].(ast.IfNode).Condition
	orExpr, ok := condition.(ast.BinaryExpr)
	if !ok || orExpr.Operator != "||" {
		t.Fatalf("condition root = %#v, want OR", condition)
	}
	andExpr, ok := orExpr.Left.(ast.BinaryExpr)
	if !ok || andExpr.Operator != "&&" {
		t.Fatalf("OR left = %#v, want AND", orExpr.Left)
	}
	ifExpr, ok := orExpr.Right.(ast.UnaryExpr)
	if !ok || ifExpr.Operator != "!" {
		t.Fatalf("OR right = %#v, want NOT", orExpr.Right)
	}
}

func TestParseFanOutConcurrencyDefaultAndOverride(t *testing.T) {
	source := `type Input { items: list[int] }
type ItemInput { item: int }
type Output { status: string }
task Run(input: ItemInput) -> Output { }
workflow Process(input: Input) -> Output {
    fan_out item in input.items { step run = Run({ item: item }); }
    fan_out other in input.items concurrency 3 { step run_other = Run({ item: other }); }
}`
	file, err := Parse(source)
	if err != nil {
		t.Fatal(err)
	}
	defaultFanOut := file.Workflows[0].Body[0].(ast.FanOutNode)
	explicitFanOut := file.Workflows[0].Body[1].(ast.FanOutNode)
	if defaultFanOut.Concurrency != 8 || explicitFanOut.Concurrency != 3 {
		t.Fatalf("fan-out limits = %d/%d, want default 8 and explicit 3", defaultFanOut.Concurrency, explicitFanOut.Concurrency)
	}
	if _, err := Parse(strings.Replace(source, "concurrency 3", "concurrency 0", 1)); err == nil {
		t.Fatal("parser accepted non-positive fan-out concurrency")
	}
}
