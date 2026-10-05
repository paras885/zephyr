package integration_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zephyr-workflow/zephyr/pkg/decider"
	"github.com/zephyr-workflow/zephyr/pkg/dsl/compiler"
	"github.com/zephyr-workflow/zephyr/pkg/gateway"
	"github.com/zephyr-workflow/zephyr/pkg/lease"
	"github.com/zephyr-workflow/zephyr/pkg/queue"
	"github.com/zephyr-workflow/zephyr/pkg/store"
	"github.com/zephyr-workflow/zephyr/pkg/timer"
)

func TestIdempotentWorkflowStartPersistsAcrossSQLiteGatewayRestart(t *testing.T) {
	definition, err := compiler.Compile(`type Input { value: string }
type Output { value: string }
workflow Echo(input: Input) -> Output { return Output { value: input.value }; }`)
	if err != nil {
		t.Fatal(err)
	}
	databasePath := filepath.Join(t.TempDir(), "gateway-idempotency.db")
	openGateway := func() (*httptest.Server, *store.SQLiteStore, func()) {
		t.Helper()
		executions, err := store.OpenSQLiteStore(context.Background(), databasePath)
		if err != nil {
			t.Fatal(err)
		}
		workQueue := queue.NewMemoryQueue(8)
		timers := timer.NewService(8)
		leases, err := lease.NewManager(workQueue, timers, 8)
		if err != nil {
			_ = executions.Close()
			t.Fatal(err)
		}
		engine := decider.NewWithTimer(executions, timers)
		api, err := gateway.New(engine, executions, workQueue, leases)
		if err != nil {
			leases.Close()
			engine.Close()
			timers.Close()
			_ = workQueue.Close()
			_ = executions.Close()
			t.Fatal(err)
		}
		if err := api.RegisterWorkflow(definition); err != nil {
			t.Fatal(err)
		}
		server := httptest.NewServer(api.Handler())
		closeAll := func() {
			server.Close()
			leases.Close()
			engine.Close()
			timers.Close()
			_ = workQueue.Close()
			_ = executions.Close()
		}
		return server, executions, closeAll
	}
	start := func(server *httptest.Server, value string) *httptest.ResponseRecorder {
		t.Helper()
		body := `{"version":1,"context":{"value":"` + value + `"}}`
		request := httptest.NewRequest(http.MethodPost, "/v1/workflows/Echo/instances", strings.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Idempotency-Key", "echo-request-1")
		response := httptest.NewRecorder()
		server.Config.Handler.ServeHTTP(response, request)
		return response
	}

	firstServer, firstStore, closeFirst := openGateway()
	firstResponse := start(firstServer, "persisted")
	if firstResponse.Code != http.StatusOK {
		closeFirst()
		t.Fatalf("first workflow start = %d: %s", firstResponse.Code, firstResponse.Body.String())
	}
	var first struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(firstResponse.Body.Bytes(), &first); err != nil {
		closeFirst()
		t.Fatal(err)
	}
	if first.ID == "" {
		closeFirst()
		t.Fatal("first workflow response omitted ID")
	}
	firstInstance, err := firstStore.Get(first.ID)
	if err != nil || firstInstance.Result["value"] != "persisted" {
		closeFirst()
		t.Fatalf("persisted workflow result = %#v, error = %v", firstInstance, err)
	}
	closeFirst()

	secondServer, secondStore, closeSecond := openGateway()
	defer closeSecond()
	secondResponse := start(secondServer, "persisted")
	var second struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(secondResponse.Body.Bytes(), &second); err != nil {
		t.Fatal(err)
	}
	if secondResponse.Code != http.StatusOK || second.ID != first.ID {
		t.Fatalf("restarted idempotent start = %d/%q, want original ID %q: %s", secondResponse.Code, second.ID, first.ID, secondResponse.Body.String())
	}
	changedResponse := start(secondServer, "changed")
	if changedResponse.Code != http.StatusConflict {
		t.Fatalf("changed request status = %d, want 409: %s", changedResponse.Code, changedResponse.Body.String())
	}
	_, total, err := secondStore.ListExecutions(context.Background(), store.ExecutionFilter{Limit: 10})
	if err != nil || total != 1 {
		t.Fatalf("persisted execution count = %d, error = %v", total, err)
	}
}
