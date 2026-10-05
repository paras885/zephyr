package eventstream

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zephyr-workflow/zephyr/pkg/domain"
	"github.com/zephyr-workflow/zephyr/pkg/store"
)

type cancelAfterHistory struct {
	store.EventHistoryStore
	cancel context.CancelFunc
}

func (history cancelAfterHistory) ListEvents(ctx context.Context, workflowID string, afterSequence uint64, limit int) ([]domain.Event, error) {
	if afterSequence >= 2 {
		history.cancel()
		return nil, context.Canceled
	}
	return history.EventHistoryStore.ListEvents(ctx, workflowID, afterSequence, limit)
}

func TestHandlerReplaysAndResumesFromLastEventID(t *testing.T) {
	executions, err := store.OpenSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "stream.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer executions.Close()
	instance, err := domain.NewWorkflowInstance(domain.WorkflowDef{
		Name:    "stream-workflow",
		Version: 1,
		Nodes: map[string]domain.NodeDefinition{
			"task": {ID: "task", Type: domain.NodeTask, Task: &domain.TaskDefinition{Name: "Run"}},
		},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := executions.Create(instance); err != nil {
		t.Fatal(err)
	}
	for _, event := range []domain.Event{
		{WorkflowID: instance.ID, Type: domain.EventWorkflowStarted},
		{
			WorkflowID: instance.ID, TaskID: "task-1", NodeID: "task", Type: domain.EventTaskScheduled,
			Payload: map[string]any{"task_name": "Run", "input": map[string]any{}},
		},
	} {
		if err := executions.Append(instance.ID, event); err != nil {
			t.Fatal(err)
		}
	}
	fullContext, cancelFull := context.WithCancel(context.Background())
	defer cancelFull()
	fullHistory := httptest.NewRequest(http.MethodGet, WorkflowEventsPath+instance.ID+"/events", nil).WithContext(fullContext)
	handler, err := NewHandler(cancelAfterHistory{EventHistoryStore: executions, cancel: cancelFull}, Config{PollInterval: time.Millisecond, BatchSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	fullResponse := httptest.NewRecorder()
	handler.ServeHTTP(fullResponse, fullHistory)
	if fullResponse.Code != http.StatusOK || fullResponse.Header().Get("Content-Type") != "text/event-stream" {
		t.Fatalf("stream response = %d %q", fullResponse.Code, fullResponse.Header().Get("Content-Type"))
	}
	if body := fullResponse.Body.String(); !strings.Contains(body, "id: 1\nevent: WORKFLOW_STARTED") || !strings.Contains(body, "id: 2\nevent: TASK_SCHEDULED") {
		t.Fatalf("full stream did not replay both events:\n%s", body)
	}
	resumeRequest := httptest.NewRequest(http.MethodGet, WorkflowEventsPath+instance.ID+"/events", nil)
	resumeRequest.Header.Set("Last-Event-ID", "1")
	resumeContext, cancelResume := context.WithCancel(context.Background())
	resumeRequest = resumeRequest.WithContext(resumeContext)
	resumeHandler, err := NewHandler(cancelAfterHistory{EventHistoryStore: executions, cancel: cancelResume}, Config{PollInterval: time.Millisecond, BatchSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	resumeResponse := httptest.NewRecorder()
	resumeHandler.ServeHTTP(resumeResponse, resumeRequest)
	cancelResume()
	if resumeResponse.Code != http.StatusOK || !strings.Contains(resumeResponse.Body.String(), "id: 2\nevent: TASK_SCHEDULED") || strings.Contains(resumeResponse.Body.String(), "id: 1\nevent:") {
		t.Fatalf("resumed stream response = %d:\n%s", resumeResponse.Code, resumeResponse.Body.String())
	}
}

func TestHandlerRejectsInvalidEventCursors(t *testing.T) {
	executions, err := store.OpenSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "invalid-cursor.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer executions.Close()
	handler, err := NewHandler(executions, Config{})
	if err != nil {
		t.Fatal(err)
	}
	for _, cursor := range []string{"not-a-number", "9223372036854775808"} {
		request := httptest.NewRequest(http.MethodGet, WorkflowEventsPath+"workflow-1/events?after="+cursor, nil)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusBadRequest {
			t.Errorf("cursor %q returned status %d, want 400", cursor, response.Code)
		}
	}
}
