package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zephyr-workflow/zephyr/pkg/decider"
	"github.com/zephyr-workflow/zephyr/pkg/gateway"
	"github.com/zephyr-workflow/zephyr/pkg/lease"
	"github.com/zephyr-workflow/zephyr/pkg/queue"
	"github.com/zephyr-workflow/zephyr/pkg/store"
	"github.com/zephyr-workflow/zephyr/pkg/timer"
)

func TestConsumerWorkflowCommandsEndToEnd(t *testing.T) {
	t.Chdir(t.TempDir())
	t.Setenv("ZEPHYR_CONFIG_DIR", filepath.Join(t.TempDir(), "private"))
	t.Setenv("ZEPHYR_TOKEN", "development-test")
	executions := store.NewMemoryStore()
	work := queue.NewMemoryQueue(16)
	timers := timer.NewService(16)
	engine := decider.NewWithTimer(executions, timers)
	leases, err := lease.NewManager(work, timers, 16)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { leases.Close(); engine.Close(); timers.Close(); work.Close() })
	api, err := gateway.New(engine, executions, work, leases)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer development-test" {
			t.Error("missing API identity")
		}
		api.Handler().ServeHTTP(w, r)
	}))
	defer server.Close()
	t.Setenv("ZEPHYR_ENDPOINT", server.URL)
	run := func(args ...string) map[string]any {
		t.Helper()
		var out, diagnostic bytes.Buffer
		if err := Run(context.Background(), args, &out, &diagnostic); err != nil {
			t.Fatalf("%v: %v, %s", args, err, diagnostic.String())
		}
		var result map[string]any
		if err := json.Unmarshal(out.Bytes(), &result); err != nil {
			t.Fatalf("not JSON: %s", out.String())
		}
		return result
	}
	run("workflow", "generate")
	result := run("workflow", "validate", "--version", "2")
	if result["valid"] != true {
		t.Fatal(result)
	}
	run("workflow", "artifacts", "--version", "2")
	body, err := os.ReadFile("client.go")
	if err != nil || !strings.Contains(string(body), `"Hello", 2, input`) {
		t.Fatal("artifacts do not pin version")
	}
	if err := Run(context.Background(), []string{"workflow", "artifacts"}, &bytes.Buffer{}, &bytes.Buffer{}); err == nil {
		t.Fatal("overwrote existing artifacts")
	}
	result = run("workflow", "publish", "--version", "2", "--force")
	if result["published"] != true {
		t.Fatal(result)
	}
	config, err := os.ReadFile(".env.example")
	if err != nil || !strings.Contains(string(config), server.URL) || strings.Contains(string(config), "development-test") {
		t.Fatal("invalid/non-secret client config")
	}
	result = run("workflow", "start", "Hello", "--version", "2", "--input", `{"value":"done"}`, "--idempotency-key", "same")
	id, ok := result["id"].(string)
	if !ok {
		t.Fatal(result)
	}
	repeat := run("workflow", "start", "--version", "2", "Hello", "--input", `{"value":"done"}`, "--idempotency-key", "same")
	if repeat["id"] != id {
		t.Fatal("idempotency key did not preserve run")
	}
	detail := run("workflow", "status", id)
	if detail["status"] != "COMPLETED" || detail["version"] != float64(2) {
		t.Fatal(detail)
	}
	page := run("workflow", "runs", "Hello", "--limit", "1", "--status", "COMPLETED")
	if page["total"] != float64(1) || len(page["items"].([]any)) != 1 {
		t.Fatal(page)
	}
	if err := os.WriteFile("workflow.zephyr", []byte(sample+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := Run(context.Background(), []string{"workflow", "publish", "--version", "2", "--force"}, &bytes.Buffer{}, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "409") {
		t.Fatalf("conflict = %v", err)
	}
}

func TestInvalidCommandsAndFiles(t *testing.T) {
	t.Chdir(t.TempDir())
	for _, args := range [][]string{
		{"workflow", "unknown"}, {"workflow", "start"}, {"workflow", "status", "one", "two"},
		{"workflow", "generate", "--unknown"}, {"workflow", "start", "Hello", "--input", "[]"},
		{"workflow", "runs", "Hello", "--limit", "101"}, {"workflow", "runs", "Hello", "--offset", "-1"},
		{"workflow", "generate", "--file"}, {"auth", "unknown"},
	} {
		if err := Run(context.Background(), args, &bytes.Buffer{}, &bytes.Buffer{}); err == nil {
			t.Errorf("%v did not fail", args)
		}
	}
	if err := saveFiles(".", map[string][]byte{"../outside": []byte("bad")}, true); err == nil {
		t.Fatal("path traversal allowed")
	}
	target := filepath.Join(t.TempDir(), "target")
	if err := os.WriteFile(target, []byte("original"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, "client.go"); err != nil {
		t.Fatal(err)
	}
	if err := saveFiles(".", map[string][]byte{"client.go": []byte("new")}, true); err == nil {
		t.Fatal("overwrote symlink")
	}
	if err := os.WriteFile("workflow.zephyr", []byte("invalid DSL"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := Run(context.Background(), []string{"workflow", "validate"}, &bytes.Buffer{}, &bytes.Buffer{}); err == nil {
		t.Fatal("invalid DSL accepted")
	}
}
