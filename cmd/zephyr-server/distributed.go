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

func serveDistributed(ctx context.Context, config distributedConfig) error {
	if config.PostgresURL == "" || config.AMQPURL == "" {
		return fmt.Errorf("distributed mode requires PostgreSQL and RabbitMQ URLs")
	}
	if config.Address == "" || config.TaskQueue == "" || config.CompletionQueue == "" {
		return fmt.Errorf("distributed mode requires an address and queue names")
	}
	if err := validateDistributedAuthConfig(config); err != nil {
		return err
	}
	sampleRatio, err := telemetry.TraceSampleRatioFromEnv()
	if err != nil {
		return err
	}
	observability, err := telemetry.New(ctx, "zephyr-server", sampleRatio)
	if err != nil {
		return err
	}
	defer shutdownTelemetry(observability)
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
	manager, err := queue.NewRabbitMQManager(ctx, config.AMQPURL)
	if err != nil {
		return fmt.Errorf("start RabbitMQ manager: %w", err)
	}
	defer manager.Close()
	workQueue, err := queue.NewManagedRabbitMQQueue(manager, config.TaskQueue)
	if err != nil {
		return err
	}
	defer workQueue.Close()
	completionQueue, err := queue.NewManagedRabbitMQCompletionQueue(manager, config.CompletionQueue)
	if err != nil {
		return err
	}
	defer completionQueue.Close()

	timers := timer.NewService(256)
	engine := decider.NewWithTimer(executions, timers)
	engine.SetMetricsRecorder(observability)
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

	var portalHandler http.Handler
	var portalErr error
	if config.DevelopmentStaticAuth {
		if config.Environment != "development" || config.Token == "" {
			return fmt.Errorf("static bearer authentication is allowed only with ZEPHYR_ENV=development and a non-empty ZEPHYR_SERVER_TOKEN")
		}
		authenticator, err := auth.NewStaticTokenAuthenticator(config.Token)
		if err != nil {
			return err
		}
		apiHandler, err := api.HandlerWithAuth(authenticator)
		if err != nil {
			return err
		}
		portalHandler, portalErr = portal.New(apiHandler)
	} else {
		hashKey, err := decodeOIDCCookieKey(config.OIDCCookieHashKey, "OIDC_COOKIE_HASH_KEY")
		if err != nil {
			return err
		}
		blockKey, err := decodeOIDCCookieKey(config.OIDCCookieBlockKey, "OIDC_COOKIE_BLOCK_KEY")
		if err != nil {
			return err
		}
		oidcClient, err := identity.NewOIDCClient(ctx, identity.OIDCConfig{
			IssuerURL: config.OIDCIssuerURL, ClientID: config.OIDCClientID,
			ClientSecret: config.OIDCClientSecret,
			Audience:     config.OIDCAudience, RedirectURL: config.OIDCRedirectURL,
			Scopes: config.OIDCScopes,
		})
		if err != nil {
			return err
		}
		portalHandler, portalErr = portal.NewOIDC(api.Handler(), portal.OIDCOptions{
			Client: oidcClient, CookieHashKey: hashKey, CookieBlockKey: blockKey, SecureCookies: true,
		})
	}
	if portalErr != nil {
		return portalErr
	}
	var ready atomic.Bool
	observability.SetDependency("postgres", false)
	observability.SetDependency("rabbitmq", false)
	dependencyReady := func(requestContext context.Context) error {
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
	handler := observability.Handler(withHealthRoutes(portalHandler, dependencyReady))
	root := http.NewServeMux()
	root.Handle("/metrics", observability.MetricsHandler())
	root.Handle("/", handler)
	server := &http.Server{Addr: config.Address, Handler: root, ReadHeaderTimeout: 5 * time.Second}
	workerContext, cancelWorkers := context.WithCancel(ctx)
	var workers sync.WaitGroup
	startWorker := func(name string, run func(context.Context) error) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			backoff := 100 * time.Millisecond
			for workerContext.Err() == nil {
				err := run(workerContext)
				if workerContext.Err() != nil {
					return
				}
				if err == nil {
					err = fmt.Errorf("stopped unexpectedly")
				}
				slog.Error("background worker stopped; restarting", "worker", name, "error", err)
				timer := time.NewTimer(backoff)
				select {
				case <-workerContext.Done():
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
			slog.Error("lease expiry scan failed", "error", err)
		})
	})
	startWorker("operational metrics collector", func(workerContext context.Context) error {
		return observability.RunOperationalMetrics(workerContext, executions, 15*time.Second, workQueue, completionQueue)
	})
	ready.Store(true)
	defer func() {
		ready.Store(false)
		cancelWorkers()
		workers.Wait()
	}()

	serverErrors := make(chan error, 1)
	go func() { serverErrors <- server.ListenAndServe() }()
	slog.Info("distributed Zephyr server listening", "address", config.Address, "workflow_count", len(definitions))
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
