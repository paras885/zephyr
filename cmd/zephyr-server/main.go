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

func main() {
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

	var err error
	switch {
	case *migrateOnly && *cleanupOnly:
		err = fmt.Errorf("-migrate-only and -cleanup-only cannot be combined")
	case *cleanupOnly:
		err = cleanupPostgres(*postgresURL, *cleanupRetention, *cleanupBatchSize)
	case *migrateOnly:
		err = migratePostgres(*postgresURL)
	case *postgresURL != "" || *amqpURL != "":
		if *postgresURL == "" || *amqpURL == "" {
			err = fmt.Errorf("distributed mode requires both DATABASE_URL and AMQP_URL")
		} else {
			err = runDistributed(distributedConfig{
				Address: *address, PostgresURL: *postgresURL, AMQPURL: *amqpURL,
				TaskQueue: *taskQueue, CompletionQueue: *completionQueue,
				WorkflowDirectory: *workflowDirectory, Token: *token,
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
			})
		}
	default:
		err = run(*address, *databasePath, *workflowDirectory, *token)
	}
	if err != nil {
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

func serve(ctx context.Context, address, databasePath, workflowDirectory, token string) error {
	sampleRatio, err := telemetry.TraceSampleRatioFromEnv()
	if err != nil {
		return err
	}
	observability, err := telemetry.New(ctx, "zephyr-server", sampleRatio)
	if err != nil {
		return err
	}
	defer shutdownTelemetry(observability)
	executions, err := store.OpenSQLiteStore(ctx, databasePath)
	if err != nil {
		return err
	}
	defer executions.Close()

	workQueue := queue.NewMemoryQueue(256)
	timers := timer.NewService(256)
	engine := decider.NewWithTimer(executions, timers)
	engine.SetMetricsRecorder(observability)
	leases, err := lease.NewManagerWithStateStore(workQueue, timers, 256, executions)
	if err != nil {
		_ = workQueue.Close()
		timers.Close()
		return err
	}
	defer func() {
		leases.Close()
		engine.Close()
		timers.Close()
		_ = workQueue.Close()
	}()

	api, err := gateway.New(engine, executions, workQueue, leases)
	if err != nil {
		return err
	}
	definitions, err := loadDefinitions(workflowDirectory)
	if err != nil {
		return err
	}
	for _, definition := range definitions {
		if err := api.RegisterWorkflow(definition); err != nil {
			return err
		}
	}

	var apiHandler http.Handler = api.Handler()
	if token != "" {
		authenticator, err := auth.NewStaticTokenAuthenticator(token)
		if err != nil {
			return err
		}
		apiHandler, err = api.HandlerWithAuth(authenticator)
		if err != nil {
			return err
		}
	}
	portalHandler, err := portal.New(apiHandler)
	if err != nil {
		return err
	}
	handler := observability.Handler(portalHandler)
	root := http.NewServeMux()
	root.Handle("/metrics", observability.MetricsHandler())
	root.Handle("/", handler)
	server := &http.Server{
		Addr:              address,
		Handler:           root,
		ReadHeaderTimeout: 5 * time.Second,
	}
	dispatchContext, cancelDispatcher := context.WithCancel(ctx)
	dispatcherDone := make(chan error, 1)
	go func() { dispatcherDone <- api.RunTaskPublicationDispatcher(dispatchContext) }()
	defer func() {
		cancelDispatcher()
		if err := <-dispatcherDone; err != nil {
			slog.Error("task publication dispatcher stopped", "error", err)
		}
	}()
	metricsContext, cancelMetrics := context.WithCancel(ctx)
	metricsDone := make(chan error, 1)
	go func() {
		metricsDone <- observability.RunOperationalMetrics(metricsContext, executions, 15*time.Second, workQueue, nil)
	}()
	defer func() {
		cancelMetrics()
		if err := <-metricsDone; err != nil {
			slog.Error("operational metrics collector stopped", "error", err)
		}
	}()
	serverErrors := make(chan error, 1)
	go func() { serverErrors <- server.ListenAndServe() }()
	slog.Info("Zephyr portal listening", "address", address, "workflow_count", len(definitions))
	if token != "" {
		slog.Info("local API bearer authentication is enabled")
	}
	select {
	case <-ctx.Done():
		shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownContext); err != nil {
			return err
		}
		return nil
	case err := <-serverErrors:
		if err == http.ErrServerClosed {
			return nil
		}
		return err
	}
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
