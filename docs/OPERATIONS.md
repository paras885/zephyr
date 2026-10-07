# Operations: Metrics, Logging, Backup, and Verification

[← Back to README](../README.md)

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
