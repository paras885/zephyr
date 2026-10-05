# Zephyr Implementation Roadmap

Status values:

- `[ ]` Not started
- `[-]` In progress
- `[x]` Complete

The implementation follows the PRD milestones in order. A task is marked complete only when its running component and focused tests are present.

## Current Baseline

- [x] Go module initialized as `github.com/zephyr-workflow/zephyr`
- [x] Core packages contain executable implementation
- [x] Baseline test suite passes

## Milestone 1: Core Domain Model, In-Memory State Machine, and Reactive Decider

- [ ] Define primitive and collection data model types
- [x] Define `WorkflowDef`
- [x] Define `WorkflowInstance`
- [x] Define `TaskInstance`
- [x] Define `TaskStatus`
- [x] Define `WorkflowStatus`
- [x] Define immutable, appendable execution history/events
- [x] Implement in-memory execution store
- [x] Implement event-driven reactive decider
- [x] Implement sequential task dependencies
- [x] Implement dedicated fork node
- [x] Implement dedicated join node
- [x] Implement switch/conditional runtime with input/prior-result expressions and AND/OR/NOT
- [x] Implement bounded dynamic fan-out with nested per-block limits, empty-input barrier, and stop/drain failure policy
- [x] Execute configured saga compensations as worker tasks in reverse successful completion order
- [ ] Verify reverse-topological compensation ordering for arbitrary parallel DAGs
- [ ] Add deterministic domain tests
- [x] Add deterministic decider tests
- [x] Test conditional true/false branches, input/result references, and missing-data failure
- [x] Test fan-out bounds, empty/nested inputs, drain-on-failure, and fan-out saga compensation
- [x] Test explicit DSL failure with/without compensation and compensation failure
- [x] Test fixed-delay task retry attempts and exhaustion
- [x] Add conditional true/false, logical operator, missing-data, and skipped-branch tests
- [x] Add bounded/empty/nested fan-out, failure-drain, and fan-out-saga tests
- [x] Add explicit fail, mapped compensation, and compensation-failure tests
- [x] Add store tests
- [x] Add Milestone 1 end-to-end integration test
- [x] Make `go test ./...` pass

## Milestone 2: Queue Subsystem, Work Dispatcher, and Interruptible Timer

- [x] Define `QueueDAO` interface
- [x] Implement in-memory channel-based work dispatcher
- [x] Define shared broker/transport adapter boundary for multi-engine deployment
- [x] Implement worker engine discovery through broker discovery or service endpoint
- [x] Add RabbitMQ transport adapter
- [x] Implement task delivery to exactly one waiting worker
- [x] Implement fencing-token generation
- [x] Implement fencing-token validation
- [x] Implement interruptible resettable min-heap timer service
- [x] Implement lease manager
- [x] Support task lease expiration
- [x] Support task heartbeat deadlines
- [x] Implement dedicated workflow delay graph node
- [x] Support delay-node deadlines through the timer service
- [x] Add concurrency tests for stale worker completion rejection
- [x] Add tests proving reclaimed tasks cannot be written through by zombies
- [x] Add queue, dispatcher, fencing, and timer unit tests
- [x] Add Milestone 2 engine-worker end-to-end integration test
- [x] Add RabbitMQ/Testcontainers engine-worker end-to-end integration test

## Milestone 3: `.zephyr` DSL Lexer, Parser, and AST Compiler

- [x] Implement lexer package
- [x] Tokenize types and collections
- [x] Tokenize task declarations and policies
- [x] Tokenize workflow declarations
- [x] Tokenize sequential steps
- [x] Tokenize conditionals/switches
- [x] Tokenize fork/join blocks
- [x] Tokenize dynamic fan-out
- [x] Tokenize saga compensation
- [x] Implement recursive-descent parser
- [x] Define AST types with dedicated node types
- [x] Implement compiler type checking
- [x] Implement compiler cycle detection
- [x] Implement DAG generation
- [x] Add valid DSL test suite
- [x] Add invalid DSL test suite
- [x] Add lexer, parser, and compiler unit tests
- [x] Add Milestone 3 source-to-DAG end-to-end integration test

## Milestone 4: `zephyr-gen` Code Generator

- [x] Add `zephyr-gen` CLI
- [x] Compile `.zephyr` files through the parser/compiler
- [x] Generate type-safe Go structs
- [x] Generate Go worker interfaces
- [x] Generate typed Go workflow clients backed by the shared SDK
- [x] Generate a non-secret `.env.example` for client configuration
- [x] Generate Python Pydantic models
- [x] Generate Python worker stubs
- [x] Generate TypeScript interfaces
- [x] Add generator golden tests
- [x] Add generator unit tests
- [x] Add Milestone 4 compile-and-run generated worker end-to-end integration test

## Milestone 5: API Gateway and Worker SDK Runtime

- [x] Define workflow service contract
- [x] Define task service contract
- [x] Implement HTTP/JSON gateway
- [x] Add pluggable bearer authentication middleware and protected gateway handler
- [ ] Implement optional gRPC control-plane adapter
- [x] Implement shared Go workflow client SDK with environment-based configuration
- [x] Add generated typed-client constructors using explicit or environment configuration
- [x] Implement Go worker SDK long polling
- [x] Implement typed task unmarshaling
- [x] Implement worker auto-heartbeating
- [x] Implement worker error reporting
- [x] Implement task completion submission transport abstraction
- [x] Implement RabbitMQ task completion delivery for horizontally scaled engines
- [x] Validate completion message IDs, workflow/task/node IDs, lease IDs, and fencing tokens
- [x] Confirm RabbitMQ completion publishes and test redelivery/acknowledgement
- [x] Consume completions through lease-validated gateway operations; discard stale messages and requeue unexpected failures
- [x] Retain HTTP task completion route for local compatibility; RabbitMQ is the distributed worker path
- [x] Implement lease-token conflict handling with HTTP `409`
- [x] Implement fixed-delay task retries with fresh task IDs and retry exhaustion handling
- [x] Add client/server integration test across real task execution
- [x] Add gateway, SDK, and transport contract unit tests
- [x] Add Milestone 5 engine-gateway-worker end-to-end integration test
- [x] Add RabbitMQ/Testcontainers worker-completion end-to-end integration test
- [x] Broadcast timer deadlines independently to lease and retry consumers
- [x] Add HTTP worker integration tests for conditional branches, bounded fan-out, and saga compensation
- [x] Add gateway timer-retry delivery test with fresh task IDs and retry exhaustion
- [x] Add PostgreSQL shared lease/fencing state with cross-instance heartbeat and expiry scanning
- [x] Serialize workflow decisions across server replicas with PostgreSQL advisory locks
- [x] Atomically append compound failure, retry, and compensation events
- [x] Recover pending workflow starts, retries, delays, and compensation after restart
- [x] Support asynchronous workflow starts that return a run ID
- [x] Support idempotent starts using a workflow-scoped key and request hash; reject conflicting reuse
- [x] Persist returned output on workflow completion and expose it through run retrieval
- [x] Add generated Go client methods for idempotent start, result retrieval, and waiting

## Milestone 6: Durable PostgreSQL Storage, Showcase, and Operations Portal

- [x] Define immutable `workflow_definitions` schema
- [x] Define `workflow_executions` snapshots and append-only `workflow_events`
- [x] Define `task_executions` projection schema
- [x] Add PostgreSQL partial index for ready tasks
- [x] Implement optimistic concurrency control for event appends
- [x] Implement PostgreSQL execution DAO
- [x] Add versioned schema migration runner
- [x] Add pure-Go SQLite local-development storage
- [x] Implement pluggable workflow event publisher interface
- [x] Implement context-aware in-memory event fan-out
- [x] Implement retrying HTTP webhook publisher with event-sequence idempotency key
- [x] Persist event outbox rows atomically with workflow event appends in PostgreSQL and SQLite
- [x] Add leased outbox claims, post-publish acknowledgement, and retry release
- [x] Add dispatcher and end-to-end durable webhook retry tests
- [x] Implement replayable HTTP event streams with per-workflow `Last-Event-ID` cursors
- [x] Add PostgreSQL DAO, OCC, migration, and outbox Testcontainers coverage
- [x] Persist scheduled task publications atomically with workflow events in PostgreSQL, SQLite, and memory stores
- [x] Dispatch task publications with leased claims, retry release, and RabbitMQ publisher confirms
- [x] Deduplicate active and completed task IDs in worker-facing queue mailboxes while preserving requeue semantics
- [x] Test publication recovery after uncertain broker acceptance and persistent outbox claims across reopen
- [x] Add event-bus unit tests
- [x] Add paginated workflow/run discovery APIs and run details with task/event history
- [x] Build a SQLite-backed local operations portal and `zephyr-server` command
- [x] Verify portal workflow start, worker execution, and terminal run inspection end to end
- [x] Confirm workflow `return` result persistence and client wait/read semantics
- [x] Wire opt-in distributed `zephyr-server` mode to PostgreSQL and RabbitMQ
- [x] Add one-shot PostgreSQL migration mode, readiness/liveness routes, and supervised recovery workers
- [x] Add OCI image, local Compose smoke stack, and provider-neutral Kubernetes Job/Deployment/Service
- [x] Add two-instance PostgreSQL/RabbitMQ gateway-worker integration coverage
- [ ] Build Go payment worker and saga refund
- [ ] Build Python fraud-risk worker
- [ ] Build TypeScript notification worker
- [ ] Add end-to-end multi-language e-commerce workflow
- [ ] Add `zephyr-cli` start command
- [ ] Add `zephyr-cli` inspect command
- [ ] Add `zephyr-cli` pause command
- [ ] Add `zephyr-cli` resume command
- [ ] Add `zephyr-cli` audit command
- [ ] Add Milestone 6 multi-language showcase end-to-end integration test

## Testing Strategy

- Unit tests run without external services and cover one package/component at a time.
- Milestone integration tests cross the public component boundaries delivered by that milestone.
- Integration tests use deterministic fixtures and assert state, event history, delivery semantics, and failure behavior.
- Durable infrastructure tests use disposable real dependencies through Testcontainers rather than mocks.
- A milestone is complete only when its unit tests and end-to-end integration test pass with `go test ./...`.

## Follow-up: Provider-Neutral Cloud Deployment

- [ ] Add production identity-provider integration and authenticated portal access
- [ ] Add supervised RabbitMQ reconnect/channel and consumer recovery
- [ ] Unify shared lease transitions and workflow event writes under one crash-atomic PostgreSQL transaction boundary
- [ ] Validate the packaged two-replica Kubernetes deployment against externally managed PostgreSQL/RabbitMQ

## Decisions

- `zephyr-server` loads and compiles workflow definitions from the configured local directory at startup. Definition upload/management through a `zephyr-cli` remains planned follow-up work; the current `zephyr-gen` command generates code and does not upload definitions.
- Execution state is immutable and appendable. Current state is derived from the event history/snapshot strategy.
- Saga compensation runs only for steps that explicitly configure compensation.
- DSL control-flow constructs use dedicated AST/runtime node types.
- Workflow delays are dedicated graph nodes with explicit timer deadlines, not implicit task metadata.
- Distributed `zephyr-server` mode uses a stable HTTP endpoint, shared PostgreSQL execution/lease state, PostgreSQL advisory workflow locks, and RabbitMQ task/completion queues. Local mode remains SQLite plus in-memory queue, lease, and timer implementations. Distributed retry/delay deadlines and lease expirations are persisted and reconstructed/scanned during recovery; process-local timers are only wake-up accelerators.
- Workflow start and worker heartbeats use a stable HTTP control-plane endpoint; gRPC may be added as an optional control-plane adapter. Generated clients and worker SDKs own endpoint configuration and protocol details.
- RabbitMQ is the selected worker data plane for task delivery and completion/failure delivery. Distributed mode wires the adapters; workers do not need engine instance addresses, and heartbeats use the stable control-plane endpoint.
- Generated Go workflow clients are typed façades over the shared `pkg/client` SDK. The generator emits environment-based constructors and a non-secret `.env.example`; credentials and deployment-specific endpoints are supplied at runtime.
- HTTP authentication is pluggable. Opaque static bearer tokens are the minimal development option; production JWT/OIDC validation should use an established provider/middleware integration rather than custom token cryptography.
- Every completion carries a unique message ID, workflow/task/node IDs, lease ID, and fencing token. Engine consumers validate the lease before appending the completion event; redelivered stale messages are acknowledged without reapplying state changes.
- Event notifications use durable at-least-once delivery. Webhooks must deduplicate by `workflow_id:sequence`; stream clients resume from their last acknowledged sequence, and delivery state is committed only after publication succeeds.
- Task `retries N with backoff D` means N retries after the initial attempt, with fixed D delay; each attempt receives a fresh task ID and fencing lease. Exhaustion enters the configured saga path or terminal FAILED state.
- Fan-out uses `concurrency N` per block (default 8), collects no aggregate output, treats empty input as an empty barrier, stops admitting new items after failure, drains already-started items, compensates successful eligible items, then fails unless compensation succeeds (COMPENSATED).
- Explicit `fail(message)` requests workflow failure and runs eligible configured compensations before the final status.
- Workflow starts are asynchronous and return a run ID. A workflow `return Output { ... }` value is stored with the completed execution and is available from run-detail retrieval; generated Go clients provide result retrieval and polling helpers.
- Cloud manifests use externally managed PostgreSQL and RabbitMQ and are provider-neutral. Production identity integration, broker reconnection, stronger atomic coupling of leases/events, and an actual cluster rollout verification remain follow-up work.
- PostgreSQL integration tests use Testcontainers.

## Broker Options for Milestone 2

- **NATS JetStream**: lightweight, Go-friendly, durable streams, consumer groups, acknowledgements, and simple local deployment. Strong default candidate for work and completion queues.
- **RabbitMQ**: mature task-queue semantics, routing, acknowledgements, dead-lettering, and management tooling. Good fit when explicit work-queue behavior is more important than minimal operations.
- **Kafka or Redpanda**: excellent partitioned durability and replay at high throughput, but heavier operationally and more complex for per-task leasing and completion semantics.
- **Redis Streams**: approachable consumer groups and local setup, but adds Redis as an operational dependency and conflicts with the PRD's goal of avoiding a Redis-centered architecture.
- **PostgreSQL queue only**: matches the PRD schema and keeps infrastructure small, but requires careful claiming/notification behavior and does not provide zero database polling by itself.

RabbitMQ is the selected Milestone 2 broker. The adapter must support at-least-once delivery, competing consumers, acknowledgements, redelivery, dead-letter handling, and correlation by workflow/task ID.
