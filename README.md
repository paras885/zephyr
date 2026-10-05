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

Set `ZEPHYR_SERVER_TOKEN` or pass `-token` to protect API routes with a static bearer token. Enter it in the portal's API Token field. The portal itself remains available without authentication. This is a minimal local option, not production identity management.

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
| `ZEPHYR_HTTP_ADDR` | `-addr` | `127.0.0.1:8080`; use `0.0.0.0:8080` in a container |
| `ZEPHYR_TASK_QUEUE` | `-task-queue` | `zephyr-tasks` |
| `ZEPHYR_COMPLETION_QUEUE` | `-completion-queue` | `zephyr-completions` |
| `ZEPHYR_WORKFLOWS_DIR` | `-workflows` | `workflows` |
| `ZEPHYR_SERVER_TOKEN` | `-token` | Empty disables API bearer authentication |

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

To enable API bearer authentication, set `ZEPHYR_SERVER_TOKEN` in the shell before starting Compose. Stop the stack with `docker compose -f deploy/compose.yaml down`; this retains the named database and broker volumes. `down -v` also deletes those local volumes.

## Kubernetes

The provider-neutral manifests expect externally managed PostgreSQL and RabbitMQ. Build and push one immutable image tag to a registry reachable by the cluster, then replace `zephyr:local` in both `deploy/kubernetes/zephyr-migrate.yaml` and `deploy/kubernetes/zephyr-server.yaml` with that image reference. Use the exact same image for both so migration and server run the same binary and baked workflow bundle:

```sh
IMAGE=registry.example.com/team/zephyr:2026-10-04
docker build -t "$IMAGE" .
docker push "$IMAGE"
```

Create the referenced Secret in the target namespace using connection URLs for your externally managed services. Set these shell variables through your normal secret-handling process first; the token is optional, but without it API bearer authentication is disabled.

```sh
kubectl create secret generic zephyr-runtime \\
	--from-literal=DATABASE_URL="$DATABASE_URL" \\
	--from-literal=AMQP_URL="$AMQP_URL" \\
	--from-literal=ZEPHYR_SERVER_TOKEN="${ZEPHYR_SERVER_TOKEN:-}"
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

The Deployment has two replicas, readiness/liveness/startup probes, a 30-second termination grace period, and non-root/read-only-root-filesystem security settings. The Service is internal `ClusterIP`; configure TLS and any external routing at your chosen ingress or proxy. To rerun the migration Job, delete the completed Job first as shown above.

## Deployment Caveats

The distributed server stores leases and fencing tokens in PostgreSQL, serializes decider advancement with PostgreSQL advisory locks, and recovers retry/delay deadlines and expired leases from persisted state. The two-instance integration test exercises independent gateways over shared PostgreSQL and RabbitMQ. The timer heap and broker mailbox are still process-local accelerators; persisted workflow deadlines, lease rows, and task IDs are the recovery authority.

These artifacts provide a runnable distributed deployment path, not a claim of production readiness. `ZEPHYR_SERVER_TOKEN` is a single static bearer token, not user identity, authorization, rotation, or audit integration; the web portal remains accessible without authentication. Put the HTTP service behind TLS and use an identity-aware edge where required.

Workflow definitions are baked into the image and loaded at process startup. Treat the workflow bundle as immutable for each image release, deploy the same bundle to every replica, and roll out a new image to change definitions; there is no runtime upload or synchronized reload.

PostgreSQL and RabbitMQ are external operational dependencies in Kubernetes. Provision, secure, back up, monitor, upgrade, and provide network access to them separately. The Compose credentials and images are a local smoke setup, not production settings. Kubernetes Secret references avoid embedding credentials in these manifests but do not themselves provide secret encryption, access policy, rotation, or an external secret manager.

Task and completion handling is at-least-once. Workers must make side effects idempotent; stable task IDs and completion identities suppress duplicate state transitions, but cannot make external worker side effects exactly-once. Managed RabbitMQ adapters use durable quorum queues with a per-queue dead-letter exchange and `.dlq` queue, a delivery limit of five, persistent publications, publisher confirms, and consumer prefetch of 16. A supervised connection manager reconnects with bounded exponential backoff; readiness stays false until adapter channels and required consumers are re-established. Broker outages do not require process restart. Monitor the dead-letter queues and broker quorum/replication health as part of deployment operations. Lease consumption/expiry, workflow events, and publication outbox writes commit atomically in PostgreSQL; delivery/acknowledgement remains at-least-once. Health probes verify HTTP liveness and PostgreSQL/RabbitMQ readiness; they do not establish end-to-end workflow correctness or replace operational monitoring.

## Verification

```sh
go test -race -count=1 ./...
go vet ./...
```

PostgreSQL and RabbitMQ integration tests use Testcontainers and require Docker.
