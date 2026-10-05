package integration_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/zephyr-workflow/zephyr/pkg/generator"
)

func TestMilestone4GeneratedGoWorkerEndToEnd(t *testing.T) {
	source := `type Input { value: string }
type Output { status: string }
task Run(input: Input) -> Output { }
workflow Demo(input: Input) -> Output { step run = Run({ value: input.value }); return Output { status: run.result.status }; }`
	files, err := generator.Generate(source)
	if err != nil {
		t.Fatal(err)
	}
	workspace := t.TempDir()
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("could not locate integration test source")
	}
	moduleRoot, err := filepath.Abs(filepath.Join(filepath.Dir(currentFile), "../.."))
	if err != nil {
		t.Fatal(err)
	}
	module := strings.Join([]string{
		"module generatedtest",
		"",
		"go 1.27.1",
		"",
		"require github.com/zephyr-workflow/zephyr v0.0.0",
		"replace github.com/zephyr-workflow/zephyr => " + filepath.ToSlash(moduleRoot),
		"",
	}, "\n")
	if err := os.WriteFile(filepath.Join(workspace, "go.mod"), []byte(module), 0o644); err != nil {
		t.Fatal(err)
	}
	for name, content := range files {
		if filepath.Ext(name) != ".go" {
			continue
		}
		if err := os.WriteFile(filepath.Join(workspace, name), content, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	worker := `package generated

import "context"

type runWorker struct{}

func (runWorker) Run(context.Context, Input) (Output, error) {
    return Output{Status: "COMPLETED"}, nil
}

var _ RunWorker = runWorker{}
`
	if err := os.WriteFile(filepath.Join(workspace, "worker_test.go"), []byte(worker), 0o644); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "test", "./...")
	command.Dir = workspace
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("generated worker failed: %v\n%s", err, output)
	}
}
