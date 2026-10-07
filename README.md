# Zephyr

Zephyr is a workflow engine with a typed `.zephyr` DSL, Go client/worker SDKs, HTTP control plane, and RabbitMQ task transport.

## Local Operations Portal

Local development requires Go 1.27.1. Docker is optional for running the local server and is required only for PostgreSQL/RabbitMQ Testcontainers tests.

Run the server from the repository root. It compiles the `.zephyr` definitions found recursively under `workflows/` at startup, then serves the local portal and HTTP API on `127.0.0.1:8080`:

```sh
go run ./cmd/zephyr-server
```

Open `http://127.0.0.1:8080`. The server uses SQLite at `./zephyr.db` for execution history. Definitions are loaded from files at startup; this command does not upload definitions or provide a CLI for managing them.

To generate client, model, and worker scaffolding for a definition:

```sh
go run ./cmd/zephyr-gen -input workflows/checkout.zephyr -output ./generated
```

`zephyr-gen` emits Go models, worker interfaces, a typed workflow client, and `.env.example`, plus Python models/worker stubs and TypeScript interfaces. It does not deploy the generated code or run workers. For a different local address, database file, or definitions directory, use:

```sh
go run ./cmd/zephyr-server -addr 127.0.0.1:9090 -db ./zephyr-local.db -workflows ./workflows
```

Local mode can use `ZEPHYR_SERVER_TOKEN` or `-token` for static bearer authentication. The portal remains available without authentication when no token is configured. This is a local development option, not production identity management.

The local server uses an in-memory work queue, lease manager, and timer service. SQLite preserves execution history across restarts, but queued work, active leases, and scheduled deadlines are process-local. A worker must be running and configured to reach this server to execute tasks; otherwise runs with task nodes remain pending.

Task scheduling and compensation append durable publication-outbox records atomically with their workflow events. A dispatcher retries queue publication, while the in-memory queue/RabbitMQ adapter mailbox suppresses duplicate stable task IDs through successful acknowledgement; requeued, unacknowledged work remains eligible. Delivery is at-least-once, so worker side effects should still be idempotent. The local in-memory queue does not survive a server restart.

The portal supports workflow/version discovery, starting runs with JSON input, filtering and paging executions by status, and inspecting task and event history. The API exposes workflow discovery at `GET /v1/workflows`, run listing at `GET /v1/instances`, and run details/results at `GET /v1/instances/{id}`.

## Workflow Results and Idempotency

Every successful path in a workflow must end with `return OutputType { ... }`, matching the workflow's declared output type. The completed run persists this object as `result`; `fail(...)` paths remain failures and do not return a successful result. Starting a run is asynchronous and returns its ID. Retrieve the final result from `GET /v1/instances/{id}` after its status becomes `COMPLETED`.

Send an `Idempotency-Key` header when starting a workflow to make client retries safe. Keys are scoped to workflow name and version: the same key and request return the original run, while reusing that key with different input returns HTTP `409 Conflict`. Generated Go clients expose `Start...WithIdempotencyKey`, `Get...Result`, and `WaitFor...` methods.

## Distributed Server Configuration

The server keeps its default local mode: SQLite plus in-memory queues, leases, and timers. Distributed mode is opt-in and starts when both `DATABASE_URL` and `AMQP_URL` are set. It uses PostgreSQL-backed execution/lease state and RabbitMQ task and completion queues, with task publication, completion consumption, workflow recovery, and lease-expiry scanning enabled.

Supported environment variables and corresponding flags:

| Environment variable | Flag | Default / purpose |
| --- | --- | --- |
| `DATABASE_URL` | `-postgres-url` | PostgreSQL connection URL; required with `AMQP_URL` for distributed mode and required by `-migrate-only` |
| `AMQP_URL` | `-amqp-url` | RabbitMQ connection URL; required with `DATABASE_URL` for distributed mode |
| `OIDC_ISSUER_URL` | — | OIDC discovery issuer; required in distributed mode unless the explicit development-only static-auth switch is enabled |
| `OIDC_CLIENT_ID` | — | OIDC portal client ID |
| `OIDC_CLIENT_SECRET` | — | Optional secret for confidential OIDC clients; PKCE is used either way |
| `OIDC_API_AUDIENCE` | — | Expected audience for signed worker/API access tokens |
| `OIDC_REDIRECT_URL` | — | Absolute public callback URL ending in `/auth/callback` |
| `OIDC_COOKIE_HASH_KEY` | — | Base64-encoded 32-byte session-cookie signing key |
| `OIDC_COOKIE_BLOCK_KEY` | — | Base64-encoded 32-byte session-cookie encryption key |
| `OIDC_SCOPES` | — | Optional space-separated portal login scopes; defaults to workflow read/start |
| `ZEPHYR_DEV_STATIC_AUTH` | — | Must be `true` with `ZEPHYR_ENV=development` to use the local static-token path in distributed mode |
| `ZEPHYR_TRACE_SAMPLE_RATIO` | — | Parent-based trace sampling ratio from 0 to 1; defaults to `0.1` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | Optional OTLP/HTTP collector endpoint; tracing is disabled if unset |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | — | Optional traces-specific OTLP/HTTP endpoint |
| `ZEPHYR_HTTP_ADDR` | `-addr` | `127.0.0.1:8080`; use `0.0.0.0:8080` in a container |
| `ZEPHYR_TASK_QUEUE` | `-task-queue` | `zephyr-tasks` |
| `ZEPHYR_COMPLETION_QUEUE` | `-completion-queue` | `zephyr-completions` |
| `ZEPHYR_WORKFLOWS_DIR` | `-workflows` | `workflows` |
| `ZEPHYR_SERVER_TOKEN` | `-token` | Static bearer token for local mode or explicitly enabled development-only distributed mode |

If only one of the two distributed connection URLs is set, startup fails rather than silently using local mode. Apply PostgreSQL migrations once before running a distributed server:

```sh
DATABASE_URL='postgres://user:password@db-host:5432/zephyr?sslmode=require' \\
	go run ./cmd/zephyr-server -migrate-only
```

The migration command needs only `DATABASE_URL`; it exits after applying migrations.

## Docker and Compose

Build the multi-stage image from the repository root. It compiles with Go 1.27.1, runs as UID/GID 10001, and includes the `workflows/` definitions present at build time:

```sh
docker build -t zephyr:local .
```

For a containerized local-mode run with ephemeral SQLite data:

```sh
docker run --rm -p 8080:8080 \\
	-e ZEPHYR_HTTP_ADDR=0.0.0.0:8080 \\
	-e ZEPHYR_SQLITE_PATH=/tmp/zephyr.db \\
	zephyr:local
```

For a distributed smoke setup, `deploy/compose.yaml` starts local PostgreSQL and RabbitMQ, waits for their health checks, runs the one-shot migration service, then starts the server. The sample credentials are for local development only:

```sh
docker compose -f deploy/compose.yaml up --build -d
docker compose -f deploy/compose.yaml ps
curl -fsS http://127.0.0.1:8080/readyz
```

Compose binds the HTTP port to loopback and enables the explicitly development-only static-token path with a local sample token. Override `ZEPHYR_SERVER_TOKEN` before starting the stack if desired; do not reuse this mode or token in production. Stop the stack with `docker compose -f deploy/compose.yaml down`; this retains the named database and broker volumes. `down -v` also deletes those local volumes.

## Kubernetes

The provider-neutral manifests expect externally managed PostgreSQL and RabbitMQ. Build and push an image, then replace the syntactically valid all-zero digest placeholder with the exact immutable image digest in both `deploy/kubernetes/zephyr-migrate.yaml` and `deploy/kubernetes/zephyr-server.yaml`. Use the exact same digest for migration and server so they run the same binary and baked workflow bundle. CI builds and scans images but does not publish them.

```sh
IMAGE=registry.example.com/team/zephyr:2026-10-04
docker build -t "$IMAGE" .
docker push "$IMAGE"
```

Replace `IMAGE` with the registry digest reference (for example, `registry.example.com/team/zephyr@sha256:<digest>`), then update both manifests. Review CPU/memory requests and limits against measured workload. The starter ingress uses `nginx` and `zephyr.example.com`; set your TLS secret, hostname, and ingress class. Edit `zephyr-network-policy.yaml` selectors to permit your ingress, worker, and monitoring namespaces. Apply NetworkPolicy only after confirming the cluster CNI enforces it.

Create the referenced Secret in the target namespace using connection URLs and OIDC client settings for your externally managed identity provider. Register `OIDC_REDIRECT_URL` as an exact callback URL at the provider, ending in `/auth/callback`. Generate independent cookie keys with `openssl rand -base64 32`; the issuer must expose standard OIDC discovery and sign JWT access tokens for the configured API audience. Configure provider scopes/roles so portal users can receive `zephyr:workflow:read` and `zephyr:workflow:start`, and worker identities can receive `zephyr:worker:execute`. The `zephyr:admin` scope grants all API scopes.

```sh
export OIDC_ISSUER_URL=https://identity.example.com/
export OIDC_CLIENT_ID=zephyr-portal
export OIDC_API_AUDIENCE=zephyr-api
export OIDC_REDIRECT_URL=https://zephyr.example.com/auth/callback
export OIDC_COOKIE_HASH_KEY="$(openssl rand -base64 32)"
export OIDC_COOKIE_BLOCK_KEY="$(openssl rand -base64 32)"
export OIDC_SCOPES="zephyr:workflow:read zephyr:workflow:start"

kubectl create secret generic zephyr-runtime \\
	--from-literal=DATABASE_URL="$DATABASE_URL" \\
	--from-literal=AMQP_URL="$AMQP_URL" \
	--from-literal=OIDC_ISSUER_URL="$OIDC_ISSUER_URL" \
	--from-literal=OIDC_CLIENT_ID="$OIDC_CLIENT_ID" \
	--from-literal=OIDC_CLIENT_SECRET="${OIDC_CLIENT_SECRET:-}" \
	--from-literal=OIDC_API_AUDIENCE="$OIDC_API_AUDIENCE" \
	--from-literal=OIDC_REDIRECT_URL="$OIDC_REDIRECT_URL" \
	--from-literal=OIDC_COOKIE_HASH_KEY="$OIDC_COOKIE_HASH_KEY" \
	--from-literal=OIDC_COOKIE_BLOCK_KEY="$OIDC_COOKIE_BLOCK_KEY" \
	--from-literal=OIDC_SCOPES="$OIDC_SCOPES"
```

After replacing both manifest image references and configuring the Secret, apply and wait for the migration Job before creating/updating the server Deployment:

```sh
kubectl delete job zephyr-migrate --ignore-not-found
kubectl apply -f deploy/kubernetes/zephyr-migrate.yaml
kubectl wait --for=condition=complete --timeout=5m job/zephyr-migrate
kubectl apply -f deploy/kubernetes/zephyr-server.yaml
kubectl rollout status deployment/zephyr-server
kubectl port-forward service/zephyr-server 8080:8080
```

The local cluster lifecycle harness uses only the explicitly named disposable kind context and namespace:

```sh
brew install kind k6
./scripts/validate-kind.sh
```

It builds `zephyr:kind`, creates disposable PostgreSQL/RabbitMQ dependencies, applies migrations before the server, and exercises two replicas, restart, scale, rollback, pod deletion, worker-node loss, broker outage/recovery, readiness, and workflow persistence. Set `RUN_K6=true` to run the baseline benchmark (5 workflow starts/sec, 50 workers, 5 minutes by default); override `STARTS_PER_SECOND`, `WORKERS`, and `DURATION` for other measurements. Benchmark output is measured capacity, not a production capacity promise. CI runs this disposable harness and uploads a k6 summary; it never targets an external cluster.

Final production validation still requires a non-production cluster with externally managed PostgreSQL/RabbitMQ and the target ingress, network policy, monitoring, and identity configuration.

The Deployment has two replicas, readiness/liveness/startup probes, a 30-second termination grace period, and non-root/read-only-root-filesystem security settings. The Service is internal `ClusterIP`; configure TLS and any external routing at your chosen ingress or proxy. To rerun the migration Job, delete the completed Job first as shown above.

## Deployment Caveats

The distributed server stores leases and fencing tokens in PostgreSQL, serializes decider advancement with PostgreSQL advisory locks, and recovers retry/delay deadlines and expired leases from persisted state. The two-instance integration test exercises independent gateways over shared PostgreSQL and RabbitMQ. The timer heap and broker mailbox are still process-local accelerators; persisted workflow deadlines, lease rows, and task IDs are the recovery authority.

These artifacts provide a runnable distributed deployment path, not a claim of production readiness. Distributed mode requires OIDC discovery, signed access tokens with the configured audience, protected portal sessions, and endpoint scopes; static bearer auth is accepted only when explicitly enabled with `ZEPHYR_ENV=development`. The server does not implement provider logout or token refresh; sessions expire with the access token and users sign in again. Put the HTTP service behind TLS and configure the public OIDC callback URL consistently with the ingress/proxy.

Workflow definitions are baked into the image and loaded at process startup. Treat the workflow bundle as immutable for each image release, deploy the same bundle to every replica, and roll out a new image to change definitions; there is no runtime upload or synchronized reload.

PostgreSQL and RabbitMQ are external operational dependencies in Kubernetes. Provision, secure, back up, monitor, upgrade, and provide network access to them separately. The Compose credentials and images are a local smoke setup, not production settings. Kubernetes Secret references avoid embedding credentials in these manifests but do not themselves provide secret encryption, access policy, rotation, or an external secret manager.

Task and completion handling is at-least-once. Workers must make side effects idempotent; stable task IDs and completion identities suppress duplicate state transitions, but cannot make external worker side effects exactly-once. Managed RabbitMQ adapters use durable quorum queues with a per-queue dead-letter exchange and `.dlq` queue, a delivery limit of five, persistent publications, publisher confirms, and consumer prefetch of 16. A supervised connection manager reconnects with bounded exponential backoff; readiness stays false until adapter channels and required consumers are re-established. Broker outages do not require process restart. Monitor the dead-letter queues and broker quorum/replication health as part of deployment operations. Lease consumption/expiry, workflow events, and publication outbox writes commit atomically in PostgreSQL; delivery/acknowledgement remains at-least-once. Health probes verify HTTP liveness and PostgreSQL/RabbitMQ readiness; they do not establish end-to-end workflow correctness or replace operational monitoring.

## Operations

The server exposes Prometheus metrics at `/metrics`, separate from authenticated portal/API routes and readiness probes. The endpoint contains process/runtime metrics and bounded-label HTTP, workflow/task/lease state, ready queue depth, dependency, outbox-age/attempt, decision-lock/conflict, retry, and expired-lease metrics; it does not include workflow IDs, payloads, or credentials. Keep the scrape endpoint on an internal network and do not publish it through a public ingress. A starter set of Prometheus rules is in `deploy/monitoring/zephyr-alerts.yaml`; review its thresholds against your workload and route alerts to an owned on-call path.

Logs are JSON `slog` records. HTTP records include route, status, duration, and trace identifiers when present; known secret attributes, bearer values, and credential-bearing Postgres/RabbitMQ URLs are redacted. OTLP/HTTP traces are enabled when an OTLP endpoint is configured, use parent-based 10% sampling by default, and can be tuned with `ZEPHYR_TRACE_SAMPLE_RATIO`. No collector endpoint means no trace export.

### Backup And Restore

The operational target is **RPO 5 minutes / RTO 1 hour**. These are approved recovery objectives, not yet demonstrated service guarantees: configure PostgreSQL continuous WAL archiving/PITR and backup retention with the database provider, then record restore time in the target environment. Back up PostgreSQL and RabbitMQ definitions/data according to their provider guidance; the Testcontainers restore drill validates PostgreSQL execution/event/outbox consistency but does not validate a cloud provider’s PITR service.

For a PostgreSQL restore drill, restore into an isolated database and keep Zephyr servers stopped until you decide whether to promote it:

```sh
pg_dump --format=custom --no-owner --no-acl "$DATABASE_URL" > zephyr-backup.dump
pg_restore --list zephyr-backup.dump
pg_restore --no-owner --no-acl --dbname="$RESTORE_DATABASE_URL" zephyr-backup.dump
```

After restore, run the migration binary against the isolated database, verify workflow execution snapshots, event counts, idempotency rows, task/event outboxes, leases, and `/readyz`, then record elapsed recovery time. Never point live replicas at both the original and restored databases.

For a broker outage, confirm PostgreSQL is healthy, allow the managed client to reconnect, and check `/readyz`, `zephyr_dependency_ready`, quorum queue status, and DLQs before resuming manual work. Do not purge queues to recover service. Inspect and re-drive DLQ messages only after verifying their workflow/task/lease identity and worker-side idempotency.

### Retention

The default cleanup policy retains workflow event history and idempotency keys indefinitely. Delivered event/task outbox rows and completed leases are eligible after 90 days; active/expired leases, pending outbox work, workflow snapshots, and audit events are not removed. Apply schema migrations and take/verify a backup before scheduling the one-shot cleanup command. Each run deletes at most the configured batch size per table; schedule repeated runs if more records remain:

```sh
go run ./cmd/zephyr-server -cleanup-only -postgres-url "$DATABASE_URL" \
	-cleanup-retention 2160h -cleanup-batch-size 1000
```

The command is never run by normal server startup. Review deletion counts in its structured log before increasing the batch or changing the retention period.

## Verification

```sh
go test -race -count=1 ./...
go vet ./...
```

PostgreSQL and RabbitMQ integration tests use Testcontainers and require Docker.
