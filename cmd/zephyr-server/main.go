package main

import (
	"context"
	"flag"
	"fmt"
	"log"
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
	token := flag.String("token", os.Getenv("ZEPHYR_SERVER_TOKEN"), "optional static bearer token (or ZEPHYR_SERVER_TOKEN)")
	migrateOnly := flag.Bool("migrate-only", false, "apply PostgreSQL schema migrations and exit")
	flag.Parse()

	var err error
	switch {
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
			})
		}
	default:
		err = run(*address, *databasePath, *workflowDirectory, *token)
	}
	if err != nil {
		log.Fatal(err)
	}
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
	executions, err := store.OpenSQLiteStore(ctx, databasePath)
	if err != nil {
		return err
	}
	defer executions.Close()

	workQueue := queue.NewMemoryQueue(256)
	timers := timer.NewService(256)
	engine := decider.NewWithTimer(executions, timers)
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
	handler, err := portal.New(apiHandler)
	if err != nil {
		return err
	}
	server := &http.Server{
		Addr:              address,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}
	dispatchContext, cancelDispatcher := context.WithCancel(ctx)
	dispatcherDone := make(chan error, 1)
	go func() { dispatcherDone <- api.RunTaskPublicationDispatcher(dispatchContext) }()
	defer func() {
		cancelDispatcher()
		if err := <-dispatcherDone; err != nil {
			log.Printf("task publication dispatcher stopped: %v", err)
		}
	}()
	serverErrors := make(chan error, 1)
	go func() { serverErrors <- server.ListenAndServe() }()
	log.Printf("Zephyr portal listening at http://%s (%d workflows)", address, len(definitions))
	if token != "" {
		log.Print("API bearer authentication is enabled")
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
