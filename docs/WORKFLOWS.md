# Workflows: Registration, Idempotency, and Results

[← Back to README](../README.md)

## Integrate As A Platform Consumer

For real OIDC browser sign-in with refresh, permission-aware controls, live workflow
registration, HTTPS, and separate service identities, see the
[local production-style identity demo](../examples/oidc-demo/README.md).

The [checkout consumer application](../examples/checkout/README.md) provides a separate
Go module with a consumer-owned workflow, generated client/models/task interfaces,
an application UI/API, and a worker that publishes completions through RabbitMQ.
Run the platform and application from the repository root:

```sh
docker compose -f deploy/compose.yaml -f examples/checkout/compose.yaml up --build --wait -d
```

Open the application at `http://127.0.0.1:8090` and the platform UI at
`http://127.0.0.1:8080`. In the development platform UI, enter
`zephyr-compose-local-only` in **Dev API Token**; this matches the Compose
`ZEPHYR_SERVER_TOKEN`. A 401 means missing/invalid API authentication, not that
PostgreSQL or RabbitMQ is down. Production uses OIDC sign-in instead.
The UI's platform endpoint denotes the server serving the page; it is not a
workspace/tenant selector.

Workflows can be bootstrapped from files or registered at runtime using the
portal's **Register workflow** action or `POST /v1/workflows/register`. Registration
accepts `{ "source": "<.zephyr source>", "version": 1 }` and returns
`{ "name": "...", "version": 1, "files": { "<filename>": "<content>" } }`.
It requires `zephyr:workflow:register` in OIDC mode. Definitions and source are
durable and immutable per name/version; identical registration is idempotent,
while conflicting content returns 409. Register a new positive version for changes.
`GET /v1/workflows/{name}/contracts?version=1` downloads a ZIP with generated
Go/Python/TypeScript contracts and a non-secret configuration template (read scope).
For definitions originally bootstrapped without source, register the matching
source/version once to enable downloads.

`zephyr workflow artifacts --version 1` also generates contracts locally;
`zephyr workflow publish` registers and saves contracts in one operation. Neither
issues credentials or deploys task implementations. Configure endpoint/service
identity and run your application's workers separately. See [Kubernetes & Helm](KUBERNETES.md)
for deployment options, including a ready-made Helm chart.

## Workflow Results and Idempotency

Every successful path in a workflow must end with `return OutputType { ... }`, matching the workflow's declared output type. The completed run persists this object as `result`; `fail(...)` paths remain failures and do not return a successful result. Starting a run is asynchronous and returns its ID. Retrieve the final result from `GET /v1/instances/{id}` after its status becomes `COMPLETED`.

Send an `Idempotency-Key` header when starting a workflow to make client retries safe. Keys are scoped to workflow name and version: the same key and request return the original run, while reusing that key with different input returns HTTP `409 Conflict`. Generated Go clients expose `Start...WithIdempotencyKey`, `Get...Result`, and `WaitFor...` methods.

