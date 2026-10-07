package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/zephyr-workflow/zephyr/pkg/auth"
	"github.com/zephyr-workflow/zephyr/pkg/decider"
	"github.com/zephyr-workflow/zephyr/pkg/domain"
	"github.com/zephyr-workflow/zephyr/pkg/dsl/compiler"
	"github.com/zephyr-workflow/zephyr/pkg/gateway"
	"github.com/zephyr-workflow/zephyr/pkg/lease"
	"github.com/zephyr-workflow/zephyr/pkg/portal"
	"github.com/zephyr-workflow/zephyr/pkg/queue"
	"github.com/zephyr-workflow/zephyr/pkg/store"
	"github.com/zephyr-workflow/zephyr/pkg/telemetry"
	"github.com/zephyr-workflow/zephyr/pkg/timer"
)

// serverFlags holds the parsed command-line flags (and their env-var
// fallbacks) used to select and configure the server's run mode.
type serverFlags struct {
	address           string
	databasePath      string
	postgresURL       string
	amqpURL           string
	taskQueue         string
	completionQueue   string
	workflowDirectory string
	token             string
	migrateOnly       bool
	cleanupOnly       bool
	cleanupRetention  time.Duration
	cleanupBatchSize  int
}

func parseServerFlags() serverFlags {
	address := flag.String("addr", envOr("ZEPHYR_HTTP_ADDR", "127.0.0.1:8080"), "HTTP listen address")
	databasePath := flag.String("db", envOr("ZEPHYR_SQLITE_PATH", "zephyr.db"), "SQLite database file for local mode")
	postgresURL := flag.String("postgres-url", os.Getenv("DATABASE_URL"), "PostgreSQL DSN for distributed mode or one-shot migration")
	amqpURL := flag.String("amqp-url", os.Getenv("AMQP_URL"), "RabbitMQ connection URL for distributed mode")
	taskQueue := flag.String("task-queue", envOr("ZEPHYR_TASK_QUEUE", "zephyr-tasks"), "RabbitMQ task queue name")
	completionQueue := flag.String("completion-queue", envOr("ZEPHYR_COMPLETION_QUEUE", "zephyr-completions"), "RabbitMQ completion queue name")
	workflowDirectory := flag.String("workflows", envOr("ZEPHYR_WORKFLOWS_DIR", "workflows"), "directory containing .zephyr definitions")
	token := flag.String("token", os.Getenv("ZEPHYR_SERVER_TOKEN"), "local or development-only static bearer token (or ZEPHYR_SERVER_TOKEN)")
	migrateOnly := flag.Bool("migrate-only", false, "apply PostgreSQL schema migrations and exit")
	cleanupOnly := flag.Bool("cleanup-only", false, "delete delivered outbox and completed lease records older than the configured retention period, then exit")
	cleanupRetention := flag.Duration("cleanup-retention", 90*24*time.Hour, "age threshold for -cleanup-only (default 90 days)")
	cleanupBatchSize := flag.Int("cleanup-batch-size", 1000, "maximum records deleted per table by -cleanup-only")
	flag.Parse()
	return serverFlags{
		address: *address, databasePath: *databasePath, postgresURL: *postgresURL, amqpURL: *amqpURL,
		taskQueue: *taskQueue, completionQueue: *completionQueue, workflowDirectory: *workflowDirectory,
		token: *token, migrateOnly: *migrateOnly, cleanupOnly: *cleanupOnly,
		cleanupRetention: *cleanupRetention, cleanupBatchSize: *cleanupBatchSize,
	}
}

// distributedConfigFromFlags builds the distributed-mode config from parsed
// flags and the environment variables that configure authentication.
func distributedConfigFromFlags(flags serverFlags) distributedConfig {
	return distributedConfig{
		Address: flags.address, PostgresURL: flags.postgresURL, AMQPURL: flags.amqpURL,
		TaskQueue: flags.taskQueue, CompletionQueue: flags.completionQueue,
		WorkflowDirectory: flags.workflowDirectory, Token: flags.token,
		Environment:           os.Getenv("ZEPHYR_ENV"),
		DevelopmentStaticAuth: strings.EqualFold(os.Getenv("ZEPHYR_DEV_STATIC_AUTH"), "true"),
		OIDCIssuerURL:         os.Getenv("OIDC_ISSUER_URL"),
		OIDCClientID:          os.Getenv("OIDC_CLIENT_ID"),
		OIDCCLIClientID:       os.Getenv("OIDC_CLI_CLIENT_ID"),
		OIDCClientSecret:      os.Getenv("OIDC_CLIENT_SECRET"),
		OIDCAudience:          os.Getenv("OIDC_API_AUDIENCE"),
		OIDCRedirectURL:       os.Getenv("OIDC_REDIRECT_URL"),
		OIDCCookieHashKey:     os.Getenv("OIDC_COOKIE_HASH_KEY"),
		OIDCCookieBlockKey:    os.Getenv("OIDC_COOKIE_BLOCK_KEY"),
		OIDCScopes:            oidcScopesFromEnv(os.Getenv("OIDC_SCOPES")),
	}
}

// runServerMode dispatches to the run mode selected by the parsed flags:
// one-shot migration or cleanup, distributed mode, or local mode.
func runServerMode(flags serverFlags) error {
	switch {
	case flags.migrateOnly && flags.cleanupOnly:
		return fmt.Errorf("-migrate-only and -cleanup-only cannot be combined")
	case flags.cleanupOnly:
		return cleanupPostgres(flags.postgresURL, flags.cleanupRetention, flags.cleanupBatchSize)
	case flags.migrateOnly:
		return migratePostgres(flags.postgresURL)
	case flags.postgresURL != "" || flags.amqpURL != "":
		if flags.postgresURL == "" || flags.amqpURL == "" {
			return fmt.Errorf("distributed mode requires both DATABASE_URL and AMQP_URL")
		}
		return runDistributed(distributedConfigFromFlags(flags))
	default:
		return run(flags.address, flags.databasePath, flags.workflowDirectory, flags.token)
	}
}

func main() {
	flags := parseServerFlags()
	if err := runServerMode(flags); err != nil {
		slog.Error("Zephyr server stopped", "error", err)
		os.Exit(1)
	}
}

func cleanupPostgres(dataSource string, retention time.Duration, batchSize int) error {
	if dataSource == "" {
		return fmt.Errorf("DATABASE_URL is required for PostgreSQL retention cleanup")
	}
	if retention <= 0 || batchSize < 1 {
		return fmt.Errorf("cleanup retention and batch size must be positive")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	executions, err := store.OpenPostgresStore(ctx, dataSource)
	if err != nil {
		return err
	}
	defer executions.Close()
	result, err := executions.CleanupRetention(ctx, time.Now().Add(-retention), batchSize)
	if err != nil {
		return err
	}
	slog.Info("retention cleanup completed",
		"cutoff", time.Now().Add(-retention).UTC(), "batch_size", batchSize,
		"workflow_event_outbox_deleted", result.WorkflowEventOutboxDeleted,
		"task_publication_outbox_deleted", result.TaskPublicationDeleted,
		"completed_leases_deleted", result.CompletedLeasesDeleted,
	)
	return nil
}

func envOr(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func run(address, databasePath, workflowDirectory, token string) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return serve(ctx, address, databasePath, workflowDirectory, token)
}

// localRuntime bundles the wired-up backend resources needed to serve the
// local (SQLite + in-memory queue) API, along with a closeAll function that
// tears them all down in the correct order.
type localRuntime struct {
	observability *telemetry.Runtime
	executions    *store.SQLiteStore
	workQueue     *queue.MemoryQueue
	timers        *timer.Service
	engine        *decider.Decider
	leases        *lease.Manager
	api           *gateway.Gateway
	definitions   []domain.WorkflowDef
	closeAll      func()
}

// localEngine bundles the in-process decision/lease machinery that sits on
// top of the local store and in-memory queue.
type localEngine struct {
	workQueue *queue.MemoryQueue
	timers    *timer.Service
	engine    *decider.Decider
	leases    *lease.Manager
}

func (built *localEngine) Close() {
	built.leases.Close()
	built.engine.Close()
	built.timers.Close()
	_ = built.workQueue.Close()
}

func buildLocalAPI(built *localEngine, executions *store.SQLiteStore, workflowDirectory string) (*gateway.Gateway, []domain.WorkflowDef, error) {
	api, err := gateway.New(built.engine, executions, built.workQueue, built.leases)
	if err != nil {
		return nil, nil, err
	}
	definitions, err := loadDefinitions(workflowDirectory)
	if err != nil {
		return nil, nil, err
	}
	for _, definition := range definitions {
		if err := api.RegisterWorkflow(definition); err != nil {
			return nil, nil, err
		}
	}
	return api, definitions, nil
}

func buildLocalEngine(executions *store.SQLiteStore, observability *telemetry.Runtime) (*localEngine, error) {
	workQueue := queue.NewMemoryQueue(256)
	timers := timer.NewService(256)
	engine := decider.NewWithTimer(executions, timers)
	engine.SetMetricsRecorder(observability)
	leases, err := lease.NewManagerWithStateStore(workQueue, timers, 256, executions)
	if err != nil {
		_ = workQueue.Close()
		timers.Close()
		return nil, err
	}
	return &localEngine{workQueue: workQueue, timers: timers, engine: engine, leases: leases}, nil
}

// setupLocalRuntime constructs the telemetry, store, queue, engine, and API
// for local mode, tracking partially-built resources so they are cleaned up
// correctly if a later step fails.
func setupLocalRuntime(ctx context.Context, databasePath, workflowDirectory string) (*localRuntime, error) {
	sampleRatio, err := telemetry.TraceSampleRatioFromEnv()
	if err != nil {
		return nil, err
	}
	observability, err := telemetry.New(ctx, "zephyr-server", sampleRatio)
	if err != nil {
		return nil, err
	}
	var closers []func()
	cleanup := func() {
		for i := len(closers) - 1; i >= 0; i-- {
			closers[i]()
		}
	}
	closers = append(closers, func() { shutdownTelemetry(observability) })
	executions, err := store.OpenSQLiteStore(ctx, databasePath)
	if err != nil {
		cleanup()
		return nil, err
	}
	closers = append(closers, func() { executions.Close() })

	built, err := buildLocalEngine(executions, observability)
	if err != nil {
		cleanup()
		return nil, err
	}
	closers = append(closers, built.Close)

	api, definitions, err := buildLocalAPI(built, executions, workflowDirectory)
	if err != nil {
		cleanup()
		return nil, err
	}
	return &localRuntime{
		observability: observability, executions: executions, workQueue: built.workQueue,
		timers: built.timers, engine: built.engine, leases: built.leases, api: api,
		definitions: definitions, closeAll: cleanup,
	}, nil
}

// buildLocalPortalHandler wraps the API handler with optional static bearer
// token authentication and the portal UI.
func buildLocalPortalHandler(api *gateway.Gateway, token string) (http.Handler, error) {
	var apiHandler http.Handler = api.Handler()
	if token != "" {
		authenticator, err := auth.NewStaticTokenAuthenticator(token)
		if err != nil {
			return nil, err
		}
		apiHandler, err = api.HandlerWithAuth(authenticator)
		if err != nil {
			return nil, err
		}
	}
	return portal.New(apiHandler)
}

func buildLocalServer(address string, observability *telemetry.Runtime, portalHandler http.Handler) *http.Server {
	handler := observability.Handler(portalHandler)
	root := http.NewServeMux()
	root.Handle("/metrics", observability.MetricsHandler())
	root.Handle("/", handler)
	return &http.Server{Addr: address, Handler: root, ReadHeaderTimeout: 5 * time.Second}
}

// startLocalBackgroundTasks launches the task publication dispatcher and
// operational metrics collector, returning a function that stops them and
// logs any error encountered while doing so.
func startLocalBackgroundTasks(ctx context.Context, runtime *localRuntime) func() {
	dispatchContext, cancelDispatcher := context.WithCancel(ctx)
	dispatcherDone := make(chan error, 1)
	go func() { dispatcherDone <- runtime.api.RunTaskPublicationDispatcher(dispatchContext) }()

	metricsContext, cancelMetrics := context.WithCancel(ctx)
	metricsDone := make(chan error, 1)
	go func() {
		metricsDone <- runtime.observability.RunOperationalMetrics(metricsContext, runtime.executions, 15*time.Second, runtime.workQueue, nil)
	}()

	return func() {
		cancelDispatcher()
		if err := <-dispatcherDone; err != nil {
			slog.Error("task publication dispatcher stopped", "error", err)
		}
		cancelMetrics()
		if err := <-metricsDone; err != nil {
			slog.Error("operational metrics collector stopped", "error", err)
		}
	}
}

// runLocalServer starts the HTTP server and blocks until the context is
// cancelled or the server stops on its own, returning any resulting error.
func runLocalServer(ctx context.Context, server *http.Server, address, token string, definitionCount int) error {
	serverErrors := make(chan error, 1)
	go func() { serverErrors <- server.ListenAndServe() }()
	slog.Info("Zephyr portal listening", "address", address, "workflow_count", definitionCount)
	if token != "" {
		slog.Info("local API bearer authentication is enabled")
	}
	select {
	case <-ctx.Done():
		shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return server.Shutdown(shutdownContext)
	case err := <-serverErrors:
		if err == http.ErrServerClosed {
			return nil
		}
		return err
	}
}

func serve(ctx context.Context, address, databasePath, workflowDirectory, token string) error {
	runtime, err := setupLocalRuntime(ctx, databasePath, workflowDirectory)
	if err != nil {
		return err
	}
	defer runtime.closeAll()

	portalHandler, err := buildLocalPortalHandler(runtime.api, token)
	if err != nil {
		return err
	}
	server := buildLocalServer(address, runtime.observability, portalHandler)

	stopBackgroundTasks := startLocalBackgroundTasks(ctx, runtime)
	defer stopBackgroundTasks()

	return runLocalServer(ctx, server, address, token, len(runtime.definitions))
}

func loadDefinitions(directory string) ([]domain.WorkflowDef, error) {
	var definitions []domain.WorkflowDef
	err := filepath.WalkDir(directory, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.EqualFold(filepath.Ext(path), ".zephyr") {
			return nil
		}
		source, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		definition, err := compiler.Compile(string(source))
		if err != nil {
			return fmt.Errorf("compile %s: %w", path, err)
		}
		definitions = append(definitions, definition)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("load workflow definitions from %s: %w", directory, err)
	}
	return definitions, nil
}
