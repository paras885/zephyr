package generator

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zephyr-workflow/zephyr/pkg/dsl/ast"
)

func TestGenerateMatchesGoldenFiles(t *testing.T) {
	fixturePath := filepath.Join("testdata", "golden.zephyr")
	source, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatal(err)
	}
	files, err := Generate(string(source))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"models.go", "workers.go", "client.go", "models.py", "workers.py", "models.ts"} {
		expected, err := os.ReadFile(filepath.Join("testdata", "golden", name+".golden"))
		if err != nil {
			t.Fatal(err)
		}
		if string(files[name]) != string(expected) {
			t.Errorf("generated %s differs from golden file:\n--- got ---\n%s\n--- want ---\n%s", name, files[name], expected)
		}
	}
	if got := string(files[".env.example"]); got != "ZEPHYR_ENDPOINT=http://localhost:8080\nZEPHYR_TOKEN=\nZEPHYR_TIMEOUT=10s\n" {
		t.Errorf("generated .env.example differs from expected:\n%s", got)
	}
}

func TestGenerateProducesCompilableGoFiles(t *testing.T) {
	source := `type Input { order_id: string items: list[string] }
type Output { status: string }
task ChargePayment(input: Input) -> Output { timeout 30s }
workflow Checkout(input: Input) -> Output { step charge = ChargePayment({}); return Output { status: "charged" }; }`
	files, err := Generate(source)
	if err != nil {
		t.Fatal(err)
	}
	for name, content := range files {
		if len(content) == 0 {
			t.Fatalf("generated %s is empty", name)
		}
		if strings.HasSuffix(name, ".go") {
			if _, err := parser.ParseFile(token.NewFileSet(), name, content, parser.AllErrors); err != nil {
				t.Fatalf("generated %s does not parse: %v\n%s", name, err, content)
			}
		}
	}
	for _, name := range []string{"models.go", "workers.go", "client.go", ".env.example", "models.py", "workers.py", "models.ts"} {
		if len(files[name]) == 0 {
			t.Fatalf("generator did not produce %s", name)
		}
	}
	if string(files["models.go"]) == "" || string(files["workers.go"]) == "" || string(files["client.go"]) == "" {
		t.Fatal("generator did not produce all expected files")
	}
	clientSource := string(files["client.go"])
	for _, expected := range []string{"type CheckoutClient struct", "NewCheckoutClientFromEnv", "StartCheckout", "StartCheckoutWithIdempotencyKey", "GetCheckoutResult", "WaitForCheckout", "\"Checkout\", 1, input"} {
		if !strings.Contains(clientSource, expected) {
			t.Errorf("generated client is missing %q:\n%s", expected, clientSource)
		}
	}
	if got := string(files[".env.example"]); got != "ZEPHYR_ENDPOINT=http://localhost:8080\nZEPHYR_TOKEN=\nZEPHYR_TIMEOUT=10s\n" {
		t.Errorf("unexpected generated config example:\n%s", got)
	}
}

func TestGenerateRejectsGeneratedGoDeclarationCollisions(t *testing.T) {
	tests := []struct {
		name        string
		file        *ast.File
		wantName    string
		wantSources []string
	}{
		{
			name: "type and worker interface",
			file: &ast.File{
				Types: []ast.TypeDecl{{Name: "charge_worker"}},
				Tasks: []ast.TaskDecl{{Name: "Charge", Input: "string", Output: "string"}},
			},
			wantName:    "ChargeWorker",
			wantSources: []string{`type "charge_worker"`, `task "Charge"`},
		},
		{
			name: "workflow clients",
			file: &ast.File{
				Workflows: []ast.WorkflowDecl{{Name: "check_out"}, {Name: "CheckOut"}},
			},
			wantName:    "CheckOutClient",
			wantSources: []string{`workflow "check_out"`, `workflow "CheckOut"`},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := GenerateFile(test.file)
			if err == nil {
				t.Fatal("GenerateFile() succeeded, want collision error")
			}
			for _, expected := range append([]string{test.wantName}, test.wantSources...) {
				if !strings.Contains(err.Error(), expected) {
					t.Errorf("error %q does not contain %q", err, expected)
				}
			}
		})
	}
}

func TestGenerateAllowsNonCollidingGoDeclarations(t *testing.T) {
	file := &ast.File{
		Types:     []ast.TypeDecl{{Name: "charge_input"}, {Name: "charge_output"}},
		Tasks:     []ast.TaskDecl{{Name: "Charge", Input: "charge_input", Output: "charge_output"}},
		Workflows: []ast.WorkflowDecl{{Name: "Checkout", Input: "charge_input", Output: "charge_output"}},
	}
	if _, err := GenerateFile(file); err != nil {
		t.Fatalf("GenerateFile() returned unexpected error: %v", err)
	}
}

func TestGenerateRejectsGeneratedGoFieldCollisions(t *testing.T) {
	file := &ast.File{Types: []ast.TypeDecl{{
		Name: "Input",
		Fields: []ast.FieldDecl{
			{Name: "order_id", Type: "string"},
			{Name: "OrderId", Type: "string"},
		},
	}}}
	_, err := GenerateFile(file)
	if err == nil || !strings.Contains(err.Error(), `generated Go field "OrderId" collides within type "Input"`) {
		t.Fatalf("field collision error = %v", err)
	}
}
