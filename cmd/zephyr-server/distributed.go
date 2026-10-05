package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/zephyr-workflow/zephyr/pkg/auth"
	"github.com/zephyr-workflow/zephyr/pkg/decider"
	"github.com/zephyr-workflow/zephyr/pkg/domain"
	"github.com/zephyr-workflow/zephyr/pkg/gateway"
	"github.com/zephyr-workflow/zephyr/pkg/lease"
	"github.com/zephyr-workflow/zephyr/pkg/portal"
	"github.com/zephyr-workflow/zephyr/pkg/queue"
	"github.com/zephyr-workflow/zephyr/pkg/store"
	"github.com/zephyr-workflow/zephyr/pkg/timer"
)

type distributedConfig struct {
	Address           string
	PostgresURL       string
	AMQPURL           string
	TaskQueue         string
	CompletionQueue   string
	WorkflowDirectory string
	Token             string
}

func runDistributed(config distributedConfig) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return serveDistributed(ctx, config)
}

func migratePostgres(dataSource string) error {
	if dataSource == "" {
		return fmt.Errorf("DATABASE_URL is required for PostgreSQL migrations")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	executions, err := store.OpenPostgresStore(ctx, dataSource)
	if err != nil {
		return err
	}
	defer executions.Close()
	return executions.Migrate(ctx)
}

func serveDistributed(ctx context.Context, config distributedConfig) error {
	if config.PostgresURL == "" || config.AMQPURL == "" {
		return fmt.Errorf("distributed mode requires PostgreSQL and RabbitMQ URLs")
	}
	if config.Address == "" || config.TaskQueue == "" || config.CompletionQueue == "" {
		return fmt.Errorf("distributed mode requires an address and queue names")
	}
	definitions, err := loadDefinitions(config.WorkflowDirectory)
	if err != nil {
		return err
	}
	executions, err := store.OpenPostgresStore(ctx, config.PostgresURL)
	if err != nil {
		return err
	}
	defer executions.Close()
	if err := executions.Ping(ctx); err != nil {
		return fmt.Errorf("verify PostgreSQL readiness: %w", err)
	}
	connection, err := amqp.Dial(config.AMQPURL)
	if err != nil {
		return fmt.Errorf("connect to RabbitMQ: %w", err)
	}
	defer connection.Close()
	workChannel, err := connection.Channel()
	if err != nil {
		return fmt.Errorf("open RabbitMQ work channel: %w", err)
	}
	workQueue, err := queue.NewRabbitMQQueue(workChannel, config.TaskQueue)
	if err != nil {
		_ = workChannel.Close()
		return err
	}
	defer workQueue.Close()
	completionChannel, err := connection.Channel()
	if err != nil {
		return fmt.Errorf("open RabbitMQ completion channel: %w", err)
	}
	completionQueue, err := queue.NewRabbitMQCompletionQueue(completionChannel, config.CompletionQueue)
	if err != nil {
		_ = completionChannel.Close()
		return err
	}
	defer completionQueue.Close()

	timers := timer.NewService(256)
	engine := decider.NewWithTimer(executions, timers)
	leases, err := lease.NewManagerWithStateStore(workQueue, timers, 256, executions)
	if err != nil {
		timers.Close()
		return err
	}
	defer func() {
		leases.Close()
		engine.Close()
		timers.Close()
	}()
	api, err := gateway.New(engine, executions, workQueue, leases)
	if err != nil {
		return err
	}
	for _, definition := range definitions {
		if err := api.RegisterWorkflow(definition); err != nil {
			return err
		}
	}

	var apiHandler http.Handler = api.Handler()
	if config.Token != "" {
		authenticator, err := auth.NewStaticTokenAuthenticator(config.Token)
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
	var ready atomic.Bool
	dependencyReady := func(requestContext context.Context) error {
		if !ready.Load() {
			return fmt.Errorf("server is starting or shutting down")
		}
		if err := executions.Ping(requestContext); err != nil {
			return fmt.Errorf("PostgreSQL unavailable: %w", err)
		}
		if connection.IsClosed() {
			return fmt.Errorf("RabbitMQ connection is closed")
		}
		return nil
	}
	handler := withHealthRoutes(portalHandler, dependencyReady)
	server := &http.Server{Addr: config.Address, Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	workerContext, cancelWorkers := context.WithCancel(ctx)
	var workers sync.WaitGroup
	workerErrors := make(chan error, 4)
	startWorker := func(name string, run func(context.Context) error) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			err := run(workerContext)
			if workerContext.Err() != nil {
				return
			}
			if err == nil {
				err = fmt.Errorf("%s stopped unexpectedly", name)
			}
			ready.Store(false)
			workerErrors <- fmt.Errorf("%s: %w", name, err)
		}()
	}
	startWorker("task publication dispatcher", api.RunTaskPublicationDispatcher)
	consumerID := domain.NewID("engine")
	startWorker("completion consumer", func(workerContext context.Context) error {
		return api.RunCompletionConsumer(workerContext, completionQueue, consumerID)
	})
	startWorker("workflow recovery", func(workerContext context.Context) error {
		return engine.RunRecovery(workerContext, 500*time.Millisecond)
	})
	startWorker("lease expiry scanner", func(workerContext context.Context) error {
		return leases.RunExpiryScanner(workerContext, 128, 250*time.Millisecond, func(err error) {
			log.Printf("lease expiry scan: %v", err)
		})
	})
	ready.Store(true)
	defer func() {
		ready.Store(false)
		cancelWorkers()
		workers.Wait()
	}()

	serverErrors := make(chan error, 1)
	go func() { serverErrors <- server.ListenAndServe() }()
	log.Printf("distributed Zephyr server listening at http://%s (%d workflows)", config.Address, len(definitions))
	select {
	case <-ctx.Done():
		shutdownContext, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		return server.Shutdown(shutdownContext)
	case err := <-serverErrors:
		if err == http.ErrServerClosed {
			return nil
		}
		return err
	case err := <-workerErrors:
		shutdownContext, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownContext)
		return err
	}
}

func withHealthRoutes(next http.Handler, readiness func(context.Context) error) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusOK)
		_, _ = response.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(response http.ResponseWriter, request *http.Request) {
		ctx, cancel := context.WithTimeout(request.Context(), time.Second)
		defer cancel()
		if err := readiness(ctx); err != nil {
			http.Error(response, err.Error(), http.StatusServiceUnavailable)
			return
		}
		response.WriteHeader(http.StatusOK)
		_, _ = response.Write([]byte("ready\n"))
	})
	mux.Handle("/", next)
	return mux
}
