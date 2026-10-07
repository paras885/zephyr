# Checkout consumer application

This is a separate Go application with its own module, workflow source, generated
contracts, HTTP API, and task implementations. It imports Zephyr's public client
and worker SDKs; it does not embed or reach into the engine.

Payment and email are simulated. There are no real charges or outgoing messages.
The application and worker share a process for simplicity; real applications can
deploy the API and workers independently.

## The boundary between platform and application

```text
Browser -> Checkout application :8090 -> generated Checkout client
                                          |
                                          v
                                  Zephyr platform :8080 -> PostgreSQL
                                          |
                                       RabbitMQ
                                          |
                      application worker (HTTP receive and heartbeat)
                                          |
                              RabbitMQ completion messages
                                          |
                                  Zephyr advances workflow
```

The Zephyr UI is the platform's operations UI, not the application's UI. It
shows workflows, runs, tasks, and events. The application keeps its API token on
the backend; its browser never receives that token.

## Run everything

Requirements: Docker with Compose. Run from the **repository root**:

```sh
docker compose -f deploy/compose.yaml -f examples/checkout/compose.yaml up --build --wait -d
```

Compose starts PostgreSQL, RabbitMQ, the migration job, the platform, and this
consumer application. The override mounts this application's workflow directory
into the platform. Relative paths in the override resolve against the first
Compose file's directory, `deploy`.

- Application: http://127.0.0.1:8090
- Platform UI: http://127.0.0.1:8080
- Development platform/API token: `zephyr-compose-local-only`

If you previously started the session-only `deploy-consumer-1` demo worker, stop
it before testing this application so it does not claim these tasks:

```sh
docker stop deploy-consumer-1
```

In the application, enter a unique order ID and an email, then click **Place
order**. Its generated client starts `Checkout` version 1. The worker receives
`AuthorizePayment`, then `SendReceipt`, and publishes completions via RabbitMQ.
The application polls the run and shows:

```json
{"id":"workflow-...","status":"COMPLETED","result":{"status":"receipt_sent"}}
```

In the platform UI, enter the development token in **Dev API Token** and select
the same run ID to inspect tasks and events. A missing/wrong token produces an
authentication message: the platform is running, but its API rejected the
browser's request. This token is not a workspace ID or a workflow-specific
configuration. Production uses OIDC sign-in and scoped access tokens instead.

Try the application's API:

```sh
curl -sS http://127.0.0.1:8090/orders \
  -H 'Content-Type: application/json' \
  -d '{"order_id":"order-002","customer_email":"customer@example.test"}'

curl -sS http://127.0.0.1:8090/orders/<returned-run-id>
```

Order ID is the start idempotency key. Submitting the same order ID and input
returns the same run. Reusing it with different input produces HTTP 409.

```sh
docker compose -f deploy/compose.yaml -f examples/checkout/compose.yaml logs -f checkout-app
docker compose -f deploy/compose.yaml -f examples/checkout/compose.yaml stop
```

Stop retains database and broker volumes. This example is loopback-only and
development-only; its application endpoint has no user authentication. Do not
expose it publicly or use the sample credentials in production.

## Workflow authoring, generation, and registration

The consumer owns [workflows/checkout.zephyr](workflows/checkout.zephyr). Generate
contracts from it, from the repository root:

```sh
go run ./cmd/zephyr workflow artifacts \
  --file examples/checkout/workflows/checkout.zephyr \
  --output examples/checkout/generated --force
```

This validates the workflow and generates:

- Go input/output types and task interfaces.
- A typed `CheckoutClient` with start, idempotent-start, result, and wait methods.
- A non-secret `.env.example` with `ZEPHYR_ENDPOINT`, `ZEPHYR_TOKEN`, and timeout.
- Python models/worker interfaces and TypeScript types.

Generation is also done during this application's Docker build. It **does not
register/upload the workflow**, issue credentials, or implement the tasks.

For runtime registration, sign in with `zephyr:workflow:register`, select
**Register workflow**, upload this source, and choose a positive version. The
portal compiles/persists it and downloads generated contracts. The unified CLI
can do this in one command after `zephyr auth login`:

```sh
zephyr workflow publish --file checkout.zephyr --version 2 --output ./generated
```

See the [CLI guide](../../docs/CLI.md) for browser authentication and all commands.
Alternatively, call the API directly:

```sh
jq -n --rawfile source examples/checkout/workflows/checkout.zephyr \
  '{source: $source, version: 2}' |
curl --fail-with-body "$ZEPHYR_ENDPOINT/v1/workflows/register" \
  -H "Authorization: Bearer $ZEPHYR_TOKEN" -H 'Content-Type: application/json' \
  --data-binary @-
```

Here `ZEPHYR_TOKEN` is a scoped OAuth access token in production, or the platform's
development token in the explicitly enabled development deployment. The response
contains `name`, `version`, and a `files` map. A read-authorized client can download
the ZIP at `GET /v1/workflows/Checkout/contracts?version=2`.

Registration is shared and durable; no platform restart is needed. Versions are
immutable (changed same-version source/definition returns 409). Generated clients
pin the registered version. Local generation can likewise pass `--version 2`;
regenerate/rebuild the consumer with those contracts to invoke version 2.
Existing runs keep their original definition snapshot. Registration does not
deploy workers or issue secrets: provision service identities and run workers
with compatible task implementations separately.

Startup file bootstrapping remains supported. Register matching source for a
bootstrapped version to enable its contract download. A Helm chart is still not
implemented; the platform provides Compose and plain Kubernetes manifests.

## Application source and configuration

The executable is [cmd/checkout/main.go](cmd/checkout/main.go). `checkoutTasks`
implements both generated worker interfaces. Replace the simulated task bodies
with application logic. Worker receive/heartbeat uses HTTP; completions use
RabbitMQ. Task operations must be idempotent because delivery is at least once.

| Variable | Purpose |
| --- | --- |
| `ZEPHYR_ENDPOINT` | Platform HTTP URL reachable from the application |
| `ZEPHYR_TOKEN` | Development bearer token matching the platform token |
| `AMQP_URL` | Broker URL reachable from the worker |
| `APP_ADDR` | Application listener, default `127.0.0.1:8090` |

For OAuth client-credentials identities instead of a development token, configure
`OAUTH_TOKEN_URL`, `OAUTH_APP_CLIENT_ID`, `OAUTH_APP_CLIENT_SECRET`,
`OAUTH_WORKER_CLIENT_ID`, and `OAUTH_WORKER_CLIENT_SECRET`, and leave `ZEPHYR_TOKEN`
unset. The SDK caches and renews each identity's access token independently.
See the [OIDC/HTTPS demo](../oidc-demo/README.md) for a complete runnable example.

The application module uses a local `replace` directive so it works without a
published SDK release. For a genuinely separate repository, use a published
Zephyr module version instead of this local replacement.

```sh
cd examples/checkout
go test -race ./...
go vet ./...
```
