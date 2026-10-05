# Zephyr Production Readiness Plan

## Purpose

This document is the execution plan for taking Zephyr from its current multi-instance foundation to an operable production service. It is intended to be used directly in a separate implementation session. Execute phases in order unless a phase explicitly allows parallel work. Do not treat container packaging or a successful two-replica smoke test as proof of production readiness.

## Current Baseline

Implementation progress: Phases 1-3 have been implemented and validated in commits `1ce29cd`, `377e73e`, and the current Phase 3 work. Phases 4-5 and the final production-readiness gate remain open; implementation does not replace provider/cluster-specific configuration or operational drills.

The repository already includes:

- Opt-in distributed `zephyr-server` mode configured with `DATABASE_URL` and `AMQP_URL`; local mode remains SQLite plus in-memory queue, lease, and timer implementations.
- PostgreSQL execution storage, migrations, task/event outboxes, shared lease/fencing records, and per-workflow advisory locks.
- RabbitMQ task and completion adapters, publisher confirms, a task-publication dispatcher, a completion consumer, and worker HTTP receive/heartbeat APIs.
- Recovery loops for pending/running workflows, retry/delay deadlines, and expired leases.
- A one-shot `-migrate-only` command, `/healthz` and `/readyz`, OCI image, local Compose setup, and provider-neutral Kubernetes manifests.
- A Testcontainers integration test that runs two gateways against shared PostgreSQL/RabbitMQ, with one instance receiving work and the other handling heartbeat/completion.

Useful entry points: [README.md](README.md), [ROADMAP.md](ROADMAP.md), [cmd/zephyr-server/distributed.go](cmd/zephyr-server/distributed.go), [pkg/store/postgres.go](pkg/store/postgres.go), [pkg/lease/lease.go](pkg/lease/lease.go), [pkg/decider/decider.go](pkg/decider/decider.go), and [test/integration/multi_instance_test.go](test/integration/multi_instance_test.go).

## Production Gaps To Close

1. Lease-state changes and workflow events currently commit in separate PostgreSQL transactions. A process/database failure between them can leave the lease and workflow task state inconsistent; idempotent reconciliation reduces impact but is weaker than atomic commit.
2. RabbitMQ connection/channel closure currently makes a replica unready; the process relies on orchestration restart rather than supervised reconnect and consumer recovery.
3. OIDC/JWT validation, endpoint scopes, and PKCE-protected portal sessions are implemented. Deployment still requires provider-specific issuer/client/audience/scope configuration, secret provisioning/rotation, TLS, and validation against the target identity provider.
4. The Kubernetes manifests have been parsed locally, but have not been applied to a real cluster or tested for rollout, drain, failure, upgrade, or rollback behavior.
5. RabbitMQ durability, queue replication/HA, dead-lettering, delivery limits, PostgreSQL backups/PITR, restore procedures, and capacity limits need explicit deployment policy and verification.
6. Operational telemetry, alerts, runbooks, threat review, load/fault testing, and security scanning are not yet established as release gates.

## Principles And Scope

- Keep the cloud deployment provider-neutral: OCI image, Kubernetes primitives, externally managed PostgreSQL and RabbitMQ.
- Preserve the HTTP worker control plane and the local SQLite development path.
- Preserve at-least-once processing. Do not claim exactly-once task side effects; require idempotent worker operations and durable event/task deduplication where state transitions are applied.
- Keep workflow definition bundles immutable per image release. All replicas in a rollout must run the same image digest and workflow bundle.
- Run schema changes as an explicit one-shot migration job, never as an uncontrolled race among server replicas.
- Use established identity/security integrations; do not grow custom token cryptography.
- Defer gRPC, CLI, and multilingual showcase work. They are not prerequisites for production readiness of the current runtime.

## Execution Order

### Phase 0: Re-establish Baseline

Before changing code, inspect current worktree edits and rerun the narrow and full gates. Do not overwrite user edits. Confirm Go, Docker, RabbitMQ/PostgreSQL Testcontainers, and access to a disposable Kubernetes cluster are available for the phases that require them.

Tasks:

- Review the current working tree and repository instructions.
- Run `go test -race -count=1 ./...` and `go vet ./...`.
- Run `docker compose -f deploy/compose.yaml up --build --wait -d`, verify `/readyz`, then tear down.
- Parse Kubernetes manifests and build the image.

Exit criteria:

- Baseline commands pass or pre-existing failures are recorded with reproduction details.
- No implementation starts on top of unexplained failures or unreviewed user changes.

### Phase 1: Make Lease And Workflow Transitions Atomic

Goal: a task completion, task failure, retry transition, lease consumption/expiry, and corresponding publication intent cannot commit in contradictory partial states.

Tasks:

- Introduce a transaction-aware execution mutation contract, such as a store callback/command API that appends a batch of workflow events and updates the matching lease row in one PostgreSQL transaction.
- Validate current lease ID, task ID, workflow/node identity, fencing token, lease state, and expiry using PostgreSQL time in that transaction.
- For success, atomically append task completion, consume the lease, update projections/snapshot, and enqueue downstream task publications.
- For retryable failure/expiry, atomically append task failure plus retry-scheduled state, consume/expire the lease, and preserve the retry deadline and publication behavior.
- For terminal failure and compensation failure, atomically persist all events that describe the state transition.
- Define behavior when the database commit succeeds but the HTTP response or broker settlement is lost. Repeated requests with the same lease/task/fencing identity must be idempotent; stale tokens must never mutate state.
- Keep local memory/SQLite behavior behind the same logical API where practical.

Required tests:

- Inject failure before commit and after commit/response loss; retry the same completion/failure.
- Race completion against expiry and assert exactly one state transition wins.
- Race heartbeat against expiry and assert PostgreSQL time/row lock is authoritative.
- Verify expired lease and task attempt move together into retry or terminal failure.
- Verify publication outbox rows commit with the transition and are not duplicated after recovery.

Exit criteria:

- No reachable code path commits a terminal/retry workflow transition while leaving the associated active lease unchanged, or vice versa.
- PostgreSQL integration tests exercise transaction rollback and concurrency, not only successful callbacks.

### Phase 2: Broker Lifecycle, Delivery Policy, And Idempotent Consumers

Goal: recover from ordinary RabbitMQ connection/channel failures without permanently unhealthy replicas, while preserving explicit at-least-once semantics.

Tasks:

- Add supervised AMQP connection/channel lifecycle with bounded reconnect backoff, queue re-declaration, publisher-confirm setup, and consumer restart.
- Ensure readiness becomes false while broker dependencies/consumers are unavailable and recovers only after workers are actually running.
- Define durable queue properties, quorum/HA expectations, prefetch limits, dead-letter exchanges/queues, delivery limits, poison-message handling, and retry ownership.
- Ensure a worker completion is durably recorded before acknowledging its broker message; repeated completion messages must be deduplicated by stable message/task/lease identity.
- Ensure task publication remains replayable after confirm loss; the stable task ID must remain the dedupe identity.
- Define shutdown behavior for in-flight receives, completions, publishes, and unacked deliveries.

Required tests:

- Kill/restart RabbitMQ during publish, confirm wait, receive, completion consumption, and acknowledgement.
- Close one channel while another remains active; verify only the affected loop reconnects.
- Deliver duplicate completion messages to different replicas and assert one workflow state transition.
- Publish an invalid/poison message and assert bounded retries/dead-letter behavior rather than infinite hot looping.
- Stop a replica with unacked work and verify recovery through lease expiry or broker redelivery without losing the workflow.

Exit criteria:

- A temporary broker outage does not require manual process intervention after the broker returns.
- Readiness, logs, metrics, and tests clearly distinguish dependency outage from process failure.
- Delivery guarantees and queue policies are documented and configured explicitly.

### Phase 3: Authentication, Authorization, And Portal Protection

Goal: production traffic is authenticated and authorized consistently across API and portal routes.

Tasks:

- Select an established OIDC/JWT provider/middleware integration and document issuer, audience, key rotation, and claim validation requirements.
- Protect portal pages and API endpoints consistently; separate health probes from user-authenticated routes without exposing operational data.
- Define authorization roles/scopes for workflow start, workflow/run reads, worker receive/heartbeat/completion, and administrative operations.
- Remove static bearer tokens from production examples; retain only an explicitly local/development mode if useful.
- Add secret sourcing/rotation expectations and ensure logs/errors never expose credentials or full DSNs.
- Add a threat model and security review for worker identity, workflow input/result sensitivity, webhook destinations, and audit-event access.

Required tests:

- Unauthenticated and insufficient-scope requests are rejected for both portal and API.
- Health/readiness checks remain usable by the orchestrator without disclosing workflow data.
- Key rotation and invalid issuer/audience/signature cases follow provider library behavior.

Exit criteria:

- No production deployment configuration can accidentally expose the portal or control plane unauthenticated.
- Authentication and authorization behavior is tested and documented; static token is clearly non-production.

### Phase 4: Operational Observability And Data Recovery

Goal: operators can detect, diagnose, and recover service, broker, database, and workflow failures.

Tasks:

- Add structured logs with request, workflow, task, lease, attempt, outbox, and broker correlation IDs while redacting credentials and sensitive payloads.
- Add metrics for workflow/task states, queue depth/age, outbox age/retries, lease expiry/reclaim, retry counts, decision-lock wait/conflicts, completion lag, and dependency health.
- Add traces for start, scheduling, publication, worker execution, heartbeat, completion, retry, compensation, and webhook/event delivery.
- Define alerts and SLOs for API availability/latency, workflow completion latency, oldest pending outbox item, expired lease backlog, retry exhaustion, and dependency outages.
- Document PostgreSQL backup/PITR, restore drills, migration rollback/forward-fix policy, RabbitMQ recovery, and workflow re-drive/manual intervention procedures.
- Add bounded retention/cleanup policies for completed leases, outbox records, events, and execution snapshots without deleting records needed for deduplication/audit.

Required tests/drills:

- Restore a PostgreSQL backup into an isolated environment and verify execution/event/outbox consistency.
- Restart all server replicas while workflows are running, retrying, delayed, and compensating; verify recovery.
- Exercise alert conditions and runbooks with controlled dependency failure.

Exit criteria:

- A named operator can follow documented runbooks to diagnose an outage, restore state, and safely resume work.
- Metrics/alerts are validated in the target monitoring environment.

### Phase 5: Cluster Validation, Capacity, And Release Gates

Goal: prove the packaged system survives realistic multi-replica deployment and controlled failure.

Tasks:

- Apply the Kubernetes Job/Deployment/Service manifests to a disposable cluster using external PostgreSQL and RabbitMQ.
- Replace mutable tags with immutable image digests; configure resource requests/limits, topology spread/anti-affinity, disruption budgets, network policies, ingress/TLS, and secret integration.
- Test startup, migration ordering, readiness/liveness, rolling deployment, rollback, scale from 2 replicas, node/pod termination, and dependency outage.
- Load-test workflow starts, worker receives/heartbeats/completions, retries, fan-out, delayed workflows, recovery scans, and outbox dispatch.
- Establish supported throughput/concurrency limits and sizing guidance for PostgreSQL pools, RabbitMQ prefetch/consumers, worker leases, recovery scan batches, and queue backlog.
- Add CI gates for unit/integration/race/vet, image build, dependency/security scanning, manifest validation, and a scheduled or release-candidate multi-instance test.

Exit criteria:

- No lost work or duplicate state transition across tested crashes, retries, redeliveries, or rolling restarts.
- The service remains ready only when it can accept work safely; it drains or explicitly abandons in-flight work according to policy.
- Capacity limits, supported configuration, recovery objectives, and known risks are documented.

## Final Production Readiness Gate

Do not declare production-ready until all are true:

- Phases 1 through 5 exit criteria pass.
- A two-or-more-replica deployment has been exercised in a real Kubernetes cluster with external PostgreSQL/RabbitMQ.
- Atomic lease/workflow transition tests pass under fault injection and races.
- Broker reconnect, redelivery, dead-letter, and duplicate-completion tests pass.
- Authentication protects both portal and API; secrets and TLS are configured through the target platform.
- Backup restore, restart recovery, rolling upgrade, and rollback drills have been recorded.
- SLOs, alerts, dashboards, runbooks, and an owner/on-call path exist.
- The release image is immutable and the deployed workflow bundle/version is recorded.

## Deferred Product Work

These roadmap items remain out of scope for production hardening unless explicitly reprioritized:

- Go payment/refund showcase worker.
- Python fraud-risk worker.
- TypeScript notification worker.
- Multilingual e-commerce workflow and end-to-end showcase.
- `zephyr-cli` commands, including start/inspect/pause/resume/audit.
- Optional gRPC control-plane adapter.