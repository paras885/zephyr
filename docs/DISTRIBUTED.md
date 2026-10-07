# Distributed Mode, Docker, and Compose

[← Back to README](../README.md)

## Local Server Details

For a different local address, database file, or definitions directory:

```sh
go run ./cmd/zephyr-server -addr 127.0.0.1:9090 -db ./zephyr-local.db -workflows ./workflows
```

Local mode can use `ZEPHYR_SERVER_TOKEN` or `-token` for static bearer
authentication. The portal remains available without authentication when no
token is configured — a local development option, not production identity
management.

The local server uses an in-memory work queue, lease manager, and timer
service. SQLite preserves execution history across restarts, but queued
work, active leases, and scheduled deadlines are process-local. A worker must
be running and configured to reach this server to execute tasks; otherwise
runs with task nodes remain pending.

Task scheduling and compensation append durable publication-outbox records
atomically with their workflow events. A dispatcher retries queue
publication, while the in-memory queue/RabbitMQ adapter mailbox suppresses
duplicate stable task IDs through successful acknowledgement; requeued,
unacknowledged work remains eligible. Delivery is at-least-once, so worker
side effects should still be idempotent. The local in-memory queue does not
survive a server restart.

The portal supports workflow/version discovery, starting runs with JSON
input, filtering and paging executions by status, and inspecting task and
event history. The API exposes workflow discovery at `GET /v1/workflows`,
run listing at `GET /v1/instances`, and run details/results at
`GET /v1/instances/{id}`.

## Distributed Server Configuration

The server keeps its default local mode: SQLite plus in-memory queues, leases, and timers. Distributed mode is opt-in and starts when both `DATABASE_URL` and `AMQP_URL` are set. It uses PostgreSQL-backed execution/lease state and RabbitMQ task and completion queues, with task publication, completion consumption, workflow recovery, and lease-expiry scanning enabled.

Supported environment variables and corresponding flags:

| Environment variable | Flag | Default / purpose |
| --- | --- | --- |
| `DATABASE_URL` | `-postgres-url` | PostgreSQL connection URL; required with `AMQP_URL` for distributed mode and required by `-migrate-only` |
| `AMQP_URL` | `-amqp-url` | RabbitMQ connection URL; required with `DATABASE_URL` for distributed mode |
| `OIDC_ISSUER_URL` | — | OIDC discovery issuer; required in distributed mode unless the explicit development-only static-auth switch is enabled |
| `OIDC_CLIENT_ID` | — | OIDC portal client ID |
| `OIDC_CLI_CLIENT_ID` | — | Public device-flow CLI client advertised at `/auth/config`; defaults to `zephyr-cli` (provision separately at provider) |
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

