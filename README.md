# Zephyr

Zephyr is a workflow/saga orchestration engine built around a typed `.zephyr`
DSL. Write a workflow once, generate typed client and worker contracts in Go,
Python, or TypeScript, and run it locally or distributed across Kubernetes
with PostgreSQL and RabbitMQ.

- **Typed DSL → generated SDKs** — sequential steps, fork/join, conditionals,
  bounded fan-out, retries, and saga compensation, compiled to generated
  Go/Python/TypeScript client and worker contracts.
- **Local-first** — `go run ./cmd/zephyr-server` starts a full control plane
  backed by SQLite with no external dependencies.
- **Distributed when you need it** — opt-in PostgreSQL + RabbitMQ mode with
  multi-replica leases, recovery, and at-least-once task delivery.
- **Production-oriented deployment** — OCI image, Kubernetes manifests, and a
  Helm chart; OIDC authentication; Prometheus metrics and structured logs.

## Quick start

Requires Go 1.27.1. Docker is optional and only needed for
PostgreSQL/RabbitMQ integration tests.

```sh
go run ./cmd/zephyr-server --workflows examples/quickstart
```

This compiles the `.zephyr` files under `examples/quickstart/`, then serves
the portal and HTTP API at `http://127.0.0.1:8080` using SQLite for local
execution history. Open the portal, start a run, and watch it execute.

Generate client/model/worker scaffolding for a workflow definition:

```sh
go run ./cmd/zephyr workflow artifacts --file examples/quickstart/checkout.zephyr --output ./generated
```

This emits Go models, worker interfaces, a typed client, `.env.example`, plus
Python/TypeScript interfaces — see the [consumer CLI guide](docs/CLI.md) for
the full command set (generate, validate, publish, run, sign in).

## See it end-to-end

The [checkout example](examples/checkout/README.md) is a complete, separate
Go application — its own module, a workflow definition, generated
client/worker contracts, an HTTP API/UI, and a worker that executes tasks
through RabbitMQ. It's the reference for what a real consumer of Zephyr looks
like, including the committed `generated/` output.

```sh
docker compose -f deploy/compose.yaml -f examples/checkout/compose.yaml up --build --wait -d
```

Open the app at `http://127.0.0.1:8090` and the platform portal at
`http://127.0.0.1:8080` (enter `zephyr-compose-local-only` as the dev API
token — Compose's local-only stand-in for OIDC sign-in).

For real OIDC browser sign-in, HTTPS, and scoped service identities instead
of the dev token, see the [identity demo](examples/oidc-demo/README.md).

## Documentation

| Topic | Where |
| --- | --- |
| CLI reference (generate, validate, publish, run) | [docs/CLI.md](docs/CLI.md) |
| Workflow registration API, idempotency, results | [docs/WORKFLOWS.md](docs/WORKFLOWS.md) |
| Local server flags, distributed mode, environment variables, Docker/Compose | [docs/DISTRIBUTED.md](docs/DISTRIBUTED.md) |
| Kubernetes manifests and Helm chart | [docs/KUBERNETES.md](docs/KUBERNETES.md) |
| Metrics, logging, tracing, backup/restore | [docs/OPERATIONS.md](docs/OPERATIONS.md) |
| Checkout example (sample app + generated SDKs) | [examples/checkout](examples/checkout/README.md) |
| OIDC identity demo | [examples/oidc-demo](examples/oidc-demo/README.md) |

## Development

```sh
go test -race -count=1 ./...
go vet ./...
```

PostgreSQL and RabbitMQ integration tests use Testcontainers and require
Docker.

## License

Apache License 2.0 — see [LICENSE](LICENSE).
