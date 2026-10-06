package telemetry

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/zephyr-workflow/zephyr/pkg/queue"
	"github.com/zephyr-workflow/zephyr/pkg/store"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

type Runtime struct {
	serviceName         string
	registry            *prometheus.Registry
	requests            *prometheus.CounterVec
	requestDuration     *prometheus.HistogramVec
	dependencyReadiness *prometheus.GaugeVec
	stateCounts         *prometheus.GaugeVec
	queueDepth          *prometheus.GaugeVec
	operationDuration   *prometheus.HistogramVec
	operationEvents     *prometheus.CounterVec
	outboxBacklog       *prometheus.GaugeVec
	outboxOldestAge     *prometheus.GaugeVec
	outboxRetryAttempts *prometheus.GaugeVec
	leaseBacklog        prometheus.Gauge
	tracerProvider      *sdktrace.TracerProvider
}

func New(ctx context.Context, serviceName string, sampleRatio float64) (*Runtime, error) {
	if ctx == nil {
		return nil, fmt.Errorf("telemetry context is required")
	}
	if strings.TrimSpace(serviceName) == "" {
		return nil, fmt.Errorf("telemetry service name is required")
	}
	if sampleRatio < 0 || sampleRatio > 1 {
		return nil, fmt.Errorf("trace sampling ratio must be between 0 and 1")
	}
	registry := prometheus.NewRegistry()
	if err := registry.Register(collectors.NewGoCollector()); err != nil {
		return nil, fmt.Errorf("register Go runtime metrics: %w", err)
	}
	if err := registry.Register(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{})); err != nil {
		return nil, fmt.Errorf("register process metrics: %w", err)
	}
	runtime := &Runtime{
		serviceName: serviceName,
		registry:    registry,
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "zephyr_http_requests_total", Help: "HTTP requests handled by the Zephyr server.",
		}, []string{"method", "route", "status"}),
		requestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "zephyr_http_request_duration_seconds", Help: "HTTP request duration in seconds.",
			Buckets: prometheus.DefBuckets,
		}, []string{"method", "route"}),
		dependencyReadiness: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "zephyr_dependency_ready", Help: "Whether a Zephyr dependency is ready (1) or unavailable (0).",
		}, []string{"dependency"}),
		stateCounts: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "zephyr_execution_records", Help: "Workflow, task, and lease records by state.",
		}, []string{"kind", "state"}),
		queueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "zephyr_queue_ready_messages", Help: "Ready messages currently waiting in a RabbitMQ queue.",
		}, []string{"queue"}),
		operationDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "zephyr_operation_duration_seconds", Help: "Duration of key workflow and worker operations.", Buckets: prometheus.DefBuckets,
		}, []string{"operation"}),
		operationEvents: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "zephyr_operation_events_total", Help: "Key operation outcomes such as retries and decision conflicts.",
		}, []string{"operation", "result"}),
		outboxBacklog: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "zephyr_outbox_pending_records", Help: "Pending durable outbox records.",
		}, []string{"kind"}),
		outboxOldestAge: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "zephyr_outbox_oldest_pending_age_seconds", Help: "Age of the oldest pending outbox record.",
		}, []string{"kind"}),
		outboxRetryAttempts: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "zephyr_outbox_attempts", Help: "Publish attempts summed over pending outbox records.",
		}, []string{"kind"}),
		leaseBacklog: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "zephyr_expired_lease_backlog", Help: "Expired active leases awaiting recovery.",
		}),
	}
	for _, collector := range []prometheus.Collector{
		runtime.requests, runtime.requestDuration, runtime.dependencyReadiness, runtime.stateCounts, runtime.queueDepth,
		runtime.operationDuration, runtime.operationEvents,
		runtime.outboxBacklog, runtime.outboxOldestAge, runtime.outboxRetryAttempts, runtime.leaseBacklog,
	} {
		if err := registry.Register(collector); err != nil {
			return nil, fmt.Errorf("register Zephyr metrics: %w", err)
		}
	}
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	if endpoint := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")); endpoint != "" || strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")) != "" {
		exporter, err := otlptracehttp.New(ctx)
		if err != nil {
			return nil, fmt.Errorf("create OTLP trace exporter: %w", err)
		}
		res, err := resource.New(ctx, resource.WithAttributes(attribute.String("service.name", serviceName)))
		if err != nil {
			return nil, fmt.Errorf("create telemetry resource: %w", err)
		}
		runtime.tracerProvider = sdktrace.NewTracerProvider(
			sdktrace.WithBatcher(exporter),
			sdktrace.WithResource(res),
			sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(sampleRatio))),
		)
		otel.SetTracerProvider(runtime.tracerProvider)
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo, ReplaceAttr: replaceLogAttribute,
	})))
	return runtime, nil
}

func (runtime *Runtime) MetricsHandler() http.Handler {
	return promhttp.HandlerFor(runtime.registry, promhttp.HandlerOpts{EnableOpenMetrics: true})
}

func (runtime *Runtime) Handler(next http.Handler) http.Handler {
	return otelhttp.NewHandler(tracingHandler{runtime: runtime, next: next}.wrap(), runtime.serviceName,
		otelhttp.WithSpanNameFormatter(func(_ string, request *http.Request) string {
			return request.Method + " " + routeName(request.Method, request.URL.Path)
		}),
	)
}

func (runtime *Runtime) SetDependency(name string, ready bool) {
	value := 0.0
	if ready {
		value = 1
	}
	runtime.dependencyReadiness.WithLabelValues(name).Set(value)
}

func (runtime *Runtime) RecordOperationalMetrics(metrics store.OperationalMetrics) {
	runtime.recordStateCounts("workflow", []string{"PENDING", "RUNNING", "COMPLETED", "FAILED", "COMPENSATING", "COMPENSATED"}, metrics.WorkflowStates)
	runtime.recordStateCounts("task", []string{"READY", "RUNNING", "COMPLETED", "FAILED", "RETRYING", "SKIPPED"}, metrics.TaskStates)
	runtime.recordStateCounts("lease", []string{"ACTIVE", "EXPIRED", "COMPLETED"}, metrics.LeaseStates)
	runtime.outboxBacklog.WithLabelValues("workflow_events").Set(float64(metrics.WorkflowEventOutboxPending))
	runtime.outboxBacklog.WithLabelValues("task_publications").Set(float64(metrics.TaskPublicationPending))
	runtime.outboxOldestAge.WithLabelValues("workflow_events").Set(metrics.WorkflowEventOutboxOldestAge.Seconds())
	runtime.outboxOldestAge.WithLabelValues("task_publications").Set(metrics.TaskPublicationOldestAge.Seconds())
	runtime.outboxRetryAttempts.WithLabelValues("workflow_events").Set(float64(metrics.WorkflowEventOutboxRetries))
	runtime.outboxRetryAttempts.WithLabelValues("task_publications").Set(float64(metrics.TaskPublicationRetries))
	runtime.leaseBacklog.Set(float64(metrics.ExpiredLeaseBacklog))
}

func (runtime *Runtime) recordStateCounts(kind string, states []string, counts map[string]int64) {
	for _, state := range states {
		runtime.stateCounts.WithLabelValues(kind, state).Set(float64(counts[state]))
	}
}

func (runtime *Runtime) SetQueueDepth(name string, depth int) {
	if name == "tasks" || name == "completions" {
		runtime.queueDepth.WithLabelValues(name).Set(float64(depth))
	}
}

func (runtime *Runtime) RecordOperation(operation, result string, duration time.Duration) {
	operation = boundedOperation(operation)
	result = boundedResult(result)
	if duration >= 0 {
		runtime.operationDuration.WithLabelValues(operation).Observe(duration.Seconds())
	}
	runtime.operationEvents.WithLabelValues(operation, result).Inc()
}

func boundedOperation(operation string) string {
	switch operation {
	case "workflow_decision_lock", "workflow_decision_conflict", "task_retry_schedule", "task_transition", "lease_heartbeat", "worker_receive", "compensation_schedule":
		return operation
	default:
		return "other"
	}
}

func boundedResult(result string) string {
	switch result {
	case "success", "error", "conflict", "scheduled", "failed":
		return result
	default:
		return "other"
	}
}

func (runtime *Runtime) RunOperationalMetrics(ctx context.Context, source store.OperationalMetricsStore, interval time.Duration, taskQueue, completionQueue queue.QueueDepthProvider) error {
	if ctx == nil || source == nil || interval <= 0 {
		return fmt.Errorf("metrics context, operational store, and positive poll interval are required")
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		metrics, err := source.OperationalMetrics(ctx)
		if err != nil && ctx.Err() == nil {
			slog.WarnContext(ctx, "read operational store metrics", "error", err)
		} else if err == nil {
			runtime.RecordOperationalMetrics(metrics)
		}
		for _, observedQueue := range []struct {
			name     string
			provider queue.QueueDepthProvider
		}{{name: "tasks", provider: taskQueue}, {name: "completions", provider: completionQueue}} {
			if observedQueue.provider == nil {
				continue
			}
			depth, depthErr := observedQueue.provider.QueueDepth(ctx)
			if depthErr != nil {
				if ctx.Err() == nil {
					slog.WarnContext(ctx, "read queue depth", "queue", observedQueue.name, "error", depthErr)
				}
				continue
			}
			runtime.SetQueueDepth(observedQueue.name, depth)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func TraceSampleRatioFromEnv() (float64, error) {
	value := strings.TrimSpace(os.Getenv("ZEPHYR_TRACE_SAMPLE_RATIO"))
	if value == "" {
		return 0.1, nil
	}
	ratio, err := strconv.ParseFloat(value, 64)
	if err != nil || ratio < 0 || ratio > 1 {
		return 0, fmt.Errorf("ZEPHYR_TRACE_SAMPLE_RATIO must be a number between 0 and 1")
	}
	return ratio, nil
}

func (runtime *Runtime) Shutdown(ctx context.Context) error {
	if runtime.tracerProvider == nil {
		return nil
	}
	return runtime.tracerProvider.Shutdown(ctx)
}

type tracingHandler struct {
	runtime *Runtime
	next    http.Handler
}

func (handler tracingHandler) wrap() http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		route := routeName(request.Method, request.URL.Path)
		start := time.Now()
		recorder := &statusRecorder{ResponseWriter: response, status: http.StatusOK}
		handler.next.ServeHTTP(recorder, request)
		elapsed := time.Since(start)
		method := normalizedMethod(request.Method)
		handler.runtime.requests.WithLabelValues(method, route, strconv.Itoa(recorder.status)).Inc()
		handler.runtime.requestDuration.WithLabelValues(method, route).Observe(elapsed.Seconds())
		spanContext := trace.SpanContextFromContext(request.Context())
		attrs := []any{"method", method, "route", route, "status", recorder.status, "duration_ms", elapsed.Milliseconds()}
		if spanContext.IsValid() {
			attrs = append(attrs, "trace_id", spanContext.TraceID().String(), "span_id", spanContext.SpanID().String())
		}
		if recorder.status >= http.StatusInternalServerError {
			slog.ErrorContext(request.Context(), "HTTP request failed", attrs...)
		} else {
			slog.InfoContext(request.Context(), "HTTP request", attrs...)
		}
		if route == "readiness" {
			handler.runtime.SetDependency("all", recorder.status == http.StatusOK)
		}
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (recorder *statusRecorder) WriteHeader(status int) {
	if recorder.status != http.StatusOK {
		return
	}
	recorder.status = status
	recorder.ResponseWriter.WriteHeader(status)
}

func (recorder *statusRecorder) Unwrap() http.ResponseWriter {
	return recorder.ResponseWriter
}

func routeName(method, path string) string {
	switch {
	case path == "/healthz":
		return "liveness"
	case path == "/readyz":
		return "readiness"
	case path == "/metrics":
		return "metrics"
	case path == "/v1/workflows":
		return "workflow_list"
	case path == "/v1/metrics":
		return "workflow_metrics"
	case path == "/v1/instances":
		return "instance_list"
	case strings.HasPrefix(path, "/v1/tasks/"):
		return "worker_task"
	case strings.HasPrefix(path, "/v1/workflows/") && method == http.MethodPost:
		return "workflow_start"
	case strings.HasPrefix(path, "/v1/workflows/"):
		return "workflow_definition_or_runs"
	case strings.HasPrefix(path, "/v1/instances/"):
		return "instance_detail"
	case strings.HasPrefix(path, "/auth/"):
		return "authentication"
	default:
		return "other"
	}
}

func normalizedMethod(method string) string {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete, http.MethodOptions:
		return method
	default:
		return "OTHER"
	}
}
