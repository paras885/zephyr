package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/zephyr-workflow/zephyr/pkg/auth"
	"github.com/zephyr-workflow/zephyr/pkg/decider"
	"github.com/zephyr-workflow/zephyr/pkg/domain"
	"github.com/zephyr-workflow/zephyr/pkg/gateway"
	"github.com/zephyr-workflow/zephyr/pkg/identity"
	"github.com/zephyr-workflow/zephyr/pkg/lease"
	"github.com/zephyr-workflow/zephyr/pkg/portal"
	"github.com/zephyr-workflow/zephyr/pkg/queue"
	"github.com/zephyr-workflow/zephyr/pkg/store"
	"github.com/zephyr-workflow/zephyr/pkg/telemetry"
	"github.com/zephyr-workflow/zephyr/pkg/timer"
)

type distributedConfig struct {
	Address               string
	PostgresURL           string
	AMQPURL               string
	TaskQueue             string
	CompletionQueue       string
	WorkflowDirectory     string
	Token                 string
	Environment           string
	DevelopmentStaticAuth bool
	OIDCIssuerURL         string
	OIDCClientID          string
	OIDCCLIClientID       string
	OIDCClientSecret      string
	OIDCAudience          string
	OIDCRedirectURL       string
	OIDCCookieHashKey     string
	OIDCCookieBlockKey    string
	OIDCScopes            []string
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

// distributedQueues bundles the RabbitMQ-backed queue resources used by the
// distributed server so they can be constructed and torn down together.
type distributedQueues struct {
	manager         *queue.RabbitMQManager
	workQueue       *queue.RabbitMQQueue
	completionQueue *queue.RabbitMQCompletionQueue
}

// distributedEngine bundles the in-process decision/lease machinery that sits
// on top of the durable store and queues.
type distributedEngine struct {
	timers *timer.Service
	engine *decider.Decider
	leases *lease.Manager
}

func validateDistributedConfig(config distributedConfig) error {
	if config.PostgresURL == "" || config.AMQPURL == "" {
		return fmt.Errorf("distributed mode requires PostgreSQL and RabbitMQ URLs")
	}
	if config.Address == "" || config.TaskQueue == "" || config.CompletionQueue == "" {
		return fmt.Errorf("distributed mode requires an address and queue names")
	}
	return validateDistributedAuthConfig(config)
}

func setupDistributedTelemetry(ctx context.Context) (*telemetry.Runtime, error) {
	sampleRatio, err := telemetry.TraceSampleRatioFromEnv()
	if err != nil {
		return nil, err
	}
	return telemetry.New(ctx, "zephyr-server", sampleRatio)
}

func setupDistributedStore(ctx context.Context, postgresURL string) (*store.PostgresStore, error) {
	executions, err := store.OpenPostgresStore(ctx, postgresURL)
	if err != nil {
		return nil, err
	}
	if err := executions.Ping(ctx); err != nil {
		executions.Close()
		return nil, fmt.Errorf("verify PostgreSQL readiness: %w", err)
	}
	return executions, nil
}

func setupDistributedQueues(ctx context.Context, config distributedConfig) (*distributedQueues, error) {
	manager, err := queue.NewRabbitMQManager(ctx, config.AMQPURL)
	if err != nil {
		return nil, fmt.Errorf("start RabbitMQ manager: %w", err)
	}
	workQueue, err := queue.NewManagedRabbitMQQueue(manager, config.TaskQueue)
	if err != nil {
		manager.Close()
		return nil, err
	}
	completionQueue, err := queue.NewManagedRabbitMQCompletionQueue(manager, config.CompletionQueue)
	if err != nil {
		workQueue.Close()
		manager.Close()
		return nil, err
	}
	return &distributedQueues{manager: manager, workQueue: workQueue, completionQueue: completionQueue}, nil
}

func (queues *distributedQueues) Close() {
	queues.completionQueue.Close()
	queues.workQueue.Close()
	queues.manager.Close()
}

func buildDistributedEngine(executions *store.PostgresStore, workQueue *queue.RabbitMQQueue, observability *telemetry.Runtime) (*distributedEngine, error) {
	timers := timer.NewService(256)
	engine := decider.NewWithTimer(executions, timers)
	engine.SetMetricsRecorder(observability)
	leases, err := lease.NewManagerWithStateStore(workQueue, timers, 256, executions)
	if err != nil {
		timers.Close()
		return nil, err
	}
	return &distributedEngine{timers: timers, engine: engine, leases: leases}, nil
}

func (built *distributedEngine) Close() {
	built.leases.Close()
	built.engine.Close()
	built.timers.Close()
}

func buildDistributedAPI(built *distributedEngine, executions *store.PostgresStore, workQueue *queue.RabbitMQQueue, definitions []domain.WorkflowDef) (*gateway.Gateway, error) {
	api, err := gateway.New(built.engine, executions, workQueue, built.leases)
	if err != nil {
		return nil, err
	}
	for _, definition := range definitions {
		if err := api.RegisterWorkflow(definition); err != nil {
			return nil, err
		}
	}
	return api, nil
}

// buildDistributedStaticPortal wires the portal behind the development-only
// static bearer token authenticator.
func buildDistributedStaticPortal(config distributedConfig, api *gateway.Gateway) (http.Handler, error) {
	if config.Environment != "development" || config.Token == "" {
		return nil, fmt.Errorf("static bearer authentication is allowed only with ZEPHYR_ENV=development and a non-empty ZEPHYR_SERVER_TOKEN")
	}
	authenticator, err := auth.NewStaticTokenAuthenticator(config.Token)
	if err != nil {
		return nil, err
	}
	apiHandler, err := api.HandlerWithAuth(authenticator)
	if err != nil {
		return nil, err
	}
	return portal.New(apiHandler)
}

// buildDistributedOIDCPortal wires the portal behind OIDC-backed session
// authentication.
func buildDistributedOIDCPortal(ctx context.Context, config distributedConfig, api *gateway.Gateway, executions *store.PostgresStore) (http.Handler, error) {
	hashKey, err := decodeOIDCCookieKey(config.OIDCCookieHashKey, "OIDC_COOKIE_HASH_KEY")
	if err != nil {
		return nil, err
	}
	blockKey, err := decodeOIDCCookieKey(config.OIDCCookieBlockKey, "OIDC_COOKIE_BLOCK_KEY")
	if err != nil {
		return nil, err
	}
	oidcClient, err := identity.NewOIDCClient(ctx, identity.OIDCConfig{
		IssuerURL: config.OIDCIssuerURL, ClientID: config.OIDCClientID,
		ClientSecret: config.OIDCClientSecret,
		Audience:     config.OIDCAudience, RedirectURL: config.OIDCRedirectURL,
		Scopes: config.OIDCScopes,
	})
	if err != nil {
		return nil, err
	}
	return portal.NewOIDC(api.Handler(), portal.OIDCOptions{
		Client: oidcClient, CookieHashKey: hashKey, CookieBlockKey: blockKey, SecureCookies: true, Sessions: executions,
		CLIIssuerURL: config.OIDCIssuerURL, CLIClientID: config.OIDCCLIClientID,
	})
}

func buildDistributedPortalHandler(ctx context.Context, config distributedConfig, api *gateway.Gateway, executions *store.PostgresStore) (http.Handler, error) {
	if config.DevelopmentStaticAuth {
		return buildDistributedStaticPortal(config, api)
	}
	return buildDistributedOIDCPortal(ctx, config, api, executions)
}

// distributedReadinessCheck reports readiness based on server start state plus
// live PostgreSQL and RabbitMQ health, recording dependency status for metrics.
func distributedReadinessCheck(ready *atomic.Bool, observability *telemetry.Runtime, executions *store.PostgresStore, manager *queue.RabbitMQManager) func(context.Context) error {
	return func(requestContext context.Context) error {
		if !ready.Load() {
			return fmt.Errorf("server is starting or shutting down")
		}
		if err := executions.Ping(requestContext); err != nil {
			observability.SetDependency("postgres", false)
			return fmt.Errorf("PostgreSQL unavailable: %w", err)
		}
		observability.SetDependency("postgres", true)
		if !manager.Ready() {
			observability.SetDependency("rabbitmq", false)
			return fmt.Errorf("RabbitMQ connection, channels, or consumers are unavailable")
		}
		observability.SetDependency("rabbitmq", true)
		return nil
	}
}

func buildDistributedServer(config distributedConfig, observability *telemetry.Runtime, portalHandler http.Handler, dependencyReady func(context.Context) error) *http.Server {
	handler := observability.Handler(withHealthRoutes(portalHandler, dependencyReady))
	root := http.NewServeMux()
	root.Handle("/metrics", observability.MetricsHandler())
	root.Handle("/", handler)
	return &http.Server{Addr: config.Address, Handler: root, ReadHeaderTimeout: 5 * time.Second}
}

// runRestartingWorker runs a background worker, restarting it with
// exponential backoff whenever it returns while the context is still active.
func runRestartingWorker(ctx context.Context, workers *sync.WaitGroup, name string, run func(context.Context) error) {
	workers.Add(1)
	go func() {
		defer workers.Done()
		backoff := 100 * time.Millisecond
		for ctx.Err() == nil {
			err := run(ctx)
			if ctx.Err() != nil {
				return
			}
			if err == nil {
				err = fmt.Errorf("stopped unexpectedly")
			}
			slog.Error("background worker stopped; restarting", "worker", name, "error", err)
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			if backoff < 5*time.Second {
				backoff *= 2
				if backoff > 5*time.Second {
					backoff = 5 * time.Second
				}
			}
		}
	}()
}

// startDistributedWorkers launches all background workers for the
// distributed server and returns a function that stops them and waits for
// them to finish.
func startDistributedWorkers(ctx context.Context, api *gateway.Gateway, built *distributedEngine, queues *distributedQueues, observability *telemetry.Runtime, executions *store.PostgresStore) func() {
	workerContext, cancelWorkers := context.WithCancel(ctx)
	var workers sync.WaitGroup
	runRestartingWorker(workerContext, &workers, "task publication dispatcher", api.RunTaskPublicationDispatcher)
	consumerID := domain.NewID("engine")
	runRestartingWorker(workerContext, &workers, "completion consumer", func(workerContext context.Context) error {
		return api.RunCompletionConsumer(workerContext, queues.completionQueue, consumerID)
	})
	runRestartingWorker(workerContext, &workers, "workflow recovery", func(workerContext context.Context) error {
		return built.engine.RunRecovery(workerContext, 500*time.Millisecond)
	})
	runRestartingWorker(workerContext, &workers, "lease expiry scanner", func(workerContext context.Context) error {
		return built.leases.RunExpiryScanner(workerContext, 128, 250*time.Millisecond, func(err error) {
			slog.Error("lease expiry scan failed", "error", err)
		})
	})
	runRestartingWorker(workerContext, &workers, "operational metrics collector", func(workerContext context.Context) error {
		return observability.RunOperationalMetrics(workerContext, executions, 15*time.Second, queues.workQueue, queues.completionQueue)
	})
	return func() {
		cancelWorkers()
		workers.Wait()
	}
}

// runDistributedServer starts the HTTP server and blocks until the context
// is cancelled or the server stops on its own, returning any resulting error.
func runDistributedServer(ctx context.Context, server *http.Server, config distributedConfig, definitionCount int) error {
	serverErrors := make(chan error, 1)
	go func() { serverErrors <- server.ListenAndServe() }()
	slog.Info("distributed Zephyr server listening", "address", config.Address, "workflow_count", definitionCount)
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
	}
}

// distributedRuntime bundles the wired-up backend resources needed to serve
// the distributed API, along with a closeAll function that tears them all
// down in the correct order.
type distributedRuntime struct {
	observability *telemetry.Runtime
	executions    *store.PostgresStore
	queues        *distributedQueues
	built         *distributedEngine
	api           *gateway.Gateway
	definitions   []domain.WorkflowDef
	closeAll      func()
}

// setupDistributedRuntime validates the config and constructs the store,
// queues, engine, and API, tracking partially-built resources so they are
// cleaned up correctly if a later step fails.
func setupDistributedRuntime(ctx context.Context, config distributedConfig) (*distributedRuntime, error) {
	if err := validateDistributedConfig(config); err != nil {
		return nil, err
	}
	observability, err := setupDistributedTelemetry(ctx)
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
	definitions, err := loadDefinitions(config.WorkflowDirectory)
	if err != nil {
		cleanup()
		return nil, err
	}
	executions, err := setupDistributedStore(ctx, config.PostgresURL)
	if err != nil {
		cleanup()
		return nil, err
	}
	closers = append(closers, func() { executions.Close() })
	queues, err := setupDistributedQueues(ctx, config)
	if err != nil {
		cleanup()
		return nil, err
	}
	closers = append(closers, queues.Close)
	built, err := buildDistributedEngine(executions, queues.workQueue, observability)
	if err != nil {
		cleanup()
		return nil, err
	}
	closers = append(closers, built.Close)
	api, err := buildDistributedAPI(built, executions, queues.workQueue, definitions)
	if err != nil {
		cleanup()
		return nil, err
	}
	return &distributedRuntime{
		observability: observability, executions: executions, queues: queues,
		built: built, api: api, definitions: definitions, closeAll: cleanup,
	}, nil
}

func serveDistributed(ctx context.Context, config distributedConfig) error {
	runtime, err := setupDistributedRuntime(ctx, config)
	if err != nil {
		return err
	}
	defer runtime.closeAll()

	portalHandler, err := buildDistributedPortalHandler(ctx, config, runtime.api, runtime.executions)
	if err != nil {
		return err
	}
	var ready atomic.Bool
	runtime.observability.SetDependency("postgres", false)
	runtime.observability.SetDependency("rabbitmq", false)
	dependencyReady := distributedReadinessCheck(&ready, runtime.observability, runtime.executions, runtime.queues.manager)
	server := buildDistributedServer(config, runtime.observability, portalHandler, dependencyReady)

	stopWorkers := startDistributedWorkers(ctx, runtime.api, runtime.built, runtime.queues, runtime.observability, runtime.executions)
	ready.Store(true)
	defer func() {
		ready.Store(false)
		stopWorkers()
	}()

	return runDistributedServer(ctx, server, config, len(runtime.definitions))
}

func shutdownTelemetry(observability *telemetry.Runtime) {
	shutdownContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := observability.Shutdown(shutdownContext); err != nil {
		slog.Error("shutdown telemetry provider", "error", err)
	}
}

func decodeOIDCCookieKey(encoded, name string) ([]byte, error) {
	if encoded == "" {
		return nil, fmt.Errorf("%s is required for OIDC sessions", name)
	}
	key, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil || len(key) != 32 {
		return nil, fmt.Errorf("%s must be base64-encoded 32-byte key material", name)
	}
	return key, nil
}

func validateDistributedAuthConfig(config distributedConfig) error {
	if config.DevelopmentStaticAuth {
		if config.Environment != "development" || config.Token == "" {
			return fmt.Errorf("static bearer authentication is allowed only with ZEPHYR_ENV=development and a non-empty ZEPHYR_SERVER_TOKEN")
		}
		_, err := auth.NewStaticTokenAuthenticator(config.Token)
		return err
	}
	if config.Token != "" {
		return fmt.Errorf("ZEPHYR_SERVER_TOKEN is only accepted with ZEPHYR_DEV_STATIC_AUTH=true and ZEPHYR_ENV=development")
	}
	for _, required := range []struct{ name, value string }{
		{"OIDC_ISSUER_URL", config.OIDCIssuerURL},
		{"OIDC_CLIENT_ID", config.OIDCClientID},
		{"OIDC_API_AUDIENCE", config.OIDCAudience},
		{"OIDC_REDIRECT_URL", config.OIDCRedirectURL},
	} {
		if strings.TrimSpace(required.value) == "" {
			return fmt.Errorf("%s is required for distributed OIDC authentication", required.name)
		}
	}
	if _, err := decodeOIDCCookieKey(config.OIDCCookieHashKey, "OIDC_COOKIE_HASH_KEY"); err != nil {
		return err
	}
	if _, err := decodeOIDCCookieKey(config.OIDCCookieBlockKey, "OIDC_COOKIE_BLOCK_KEY"); err != nil {
		return err
	}
	return nil
}

func oidcScopesFromEnv(value string) []string {
	if strings.TrimSpace(value) == "" {
		return []string{"zephyr:workflow:read", "zephyr:workflow:start"}
	}
	return strings.Fields(value)
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
