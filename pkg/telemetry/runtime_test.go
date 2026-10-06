package telemetry

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/zephyr-workflow/zephyr/pkg/store"
	"go.opentelemetry.io/otel/trace"
)

func TestRuntimeExposesBoundedRequestAndOperationalMetrics(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	runtime, err := New(context.Background(), "zephyr-test", 0.1)
	if err != nil {
		t.Fatal(err)
	}
	requestHandler := runtime.Handler(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusNoContent)
	}))
	request := httptest.NewRequest(http.MethodGet, "/v1/instances/private-workflow-id", nil)
	requestRecorder := httptest.NewRecorder()
	requestHandler.ServeHTTP(requestRecorder, request)
	if requestRecorder.Code != http.StatusNoContent {
		t.Fatalf("request status = %d", requestRecorder.Code)
	}
	runtime.SetDependency("postgres", true)
	runtime.SetDependency("rabbitmq", false)
	runtime.RecordOperationalMetrics(store.OperationalMetrics{
		WorkflowStates: map[string]int64{"RUNNING": 2}, TaskStates: map[string]int64{"RETRYING": 3}, LeaseStates: map[string]int64{"EXPIRED": 4},
		WorkflowEventOutboxPending: 3, WorkflowEventOutboxOldestAge: 2 * time.Minute, WorkflowEventOutboxRetries: 6,
		TaskPublicationPending: 4, TaskPublicationOldestAge: 30 * time.Second, TaskPublicationRetries: 8,
		ExpiredLeaseBacklog: 5,
	})
	runtime.SetQueueDepth("tasks", 7)
	runtime.SetQueueDepth("completions", 2)
	runtime.RecordOperation("workflow_decision_conflict", "conflict", 3*time.Millisecond)
	runtime.RecordOperation("untrusted-workflow-name", "untrusted-result", time.Second)
	metrics := httptest.NewRecorder()
	runtime.MetricsHandler().ServeHTTP(metrics, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if metrics.Code != http.StatusOK {
		t.Fatalf("metrics status = %d", metrics.Code)
	}
	for _, expected := range []string{
		`zephyr_http_requests_total{method="GET",route="instance_detail",status="204"} 1`,
		`zephyr_dependency_ready{dependency="postgres"} 1`,
		`zephyr_dependency_ready{dependency="rabbitmq"} 0`,
		`zephyr_execution_records{kind="workflow",state="RUNNING"} 2`,
		`zephyr_execution_records{kind="task",state="RETRYING"} 3`,
		`zephyr_execution_records{kind="lease",state="EXPIRED"} 4`,
		`zephyr_queue_ready_messages{queue="tasks"} 7`,
		`zephyr_queue_ready_messages{queue="completions"} 2`,
		`zephyr_outbox_pending_records{kind="workflow_events"} 3`,
		`zephyr_outbox_pending_records{kind="task_publications"} 4`,
		`zephyr_outbox_attempts{kind="workflow_events"} 6`,
		`zephyr_outbox_attempts{kind="task_publications"} 8`,
		`zephyr_expired_lease_backlog 5`,
		`zephyr_operation_events_total{operation="workflow_decision_conflict",result="conflict"} 1`,
		`zephyr_operation_events_total{operation="other",result="other"} 1`,
		`zephyr_operation_duration_seconds_count{operation="workflow_decision_conflict"} 1`,
	} {
		if !strings.Contains(metrics.Body.String(), expected) {
			t.Errorf("metrics output missing %q", expected)
		}
	}
	if strings.Contains(metrics.Body.String(), "private-workflow-id") {
		t.Fatal("metrics expose a workflow identifier label")
	}
}

func TestRuntimePropagatesTraceContextWithoutExporter(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	runtime, err := New(context.Background(), "zephyr-test", 0.1)
	if err != nil {
		t.Fatal(err)
	}
	var propagatedTraceID string
	handler := runtime.Handler(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		propagatedTraceID = trace.SpanContextFromContext(request.Context()).TraceID().String()
		response.WriteHeader(http.StatusNoContent)
	}))
	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	request.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent || propagatedTraceID != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Fatalf("trace propagation status=%d traceID=%q", response.Code, propagatedTraceID)
	}
}

func TestNewRejectsInvalidTraceSamplingRatio(t *testing.T) {
	if _, err := New(context.Background(), "zephyr-test", 1.1); err == nil {
		t.Fatal("telemetry accepted trace sample ratio greater than one")
	}
	if _, err := New(context.Background(), "zephyr-test", -0.1); err == nil {
		t.Fatal("telemetry accepted negative trace sample ratio")
	}
}

func TestTraceSampleRatioFromEnv(t *testing.T) {
	t.Setenv("ZEPHYR_TRACE_SAMPLE_RATIO", "")
	if ratio, err := TraceSampleRatioFromEnv(); err != nil || ratio != 0.1 {
		t.Fatalf("default trace sample ratio = %v, err=%v", ratio, err)
	}
	t.Setenv("ZEPHYR_TRACE_SAMPLE_RATIO", "0.25")
	if ratio, err := TraceSampleRatioFromEnv(); err != nil || ratio != 0.25 {
		t.Fatalf("configured trace sample ratio = %v, err=%v", ratio, err)
	}
	t.Setenv("ZEPHYR_TRACE_SAMPLE_RATIO", "invalid")
	if _, err := TraceSampleRatioFromEnv(); err == nil {
		t.Fatal("invalid trace sample ratio was accepted")
	}
}
