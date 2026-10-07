# Kubernetes and Helm

[← Back to README](../README.md)

Two equivalent deployment paths are available; pick one:

- **Helm chart** (recommended): [deploy/helm/zephyr](../deploy/helm/zephyr/README.md) — parametrized install/upgrade, generates the same resources below from `values.yaml`.
- **Plain Kustomize/manifests**: [deploy/kubernetes](../deploy/kubernetes) — edit the YAML directly, as documented here.

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

These artifacts provide a runnable distributed deployment path, not a claim of production readiness. Distributed mode requires OIDC discovery, signed access tokens with the configured audience, protected portal sessions, and endpoint scopes; static bearer auth is accepted only when explicitly enabled with `ZEPHYR_ENV=development`. Portal access/refresh tokens are encrypted server-side in PostgreSQL; an opaque Secure HttpOnly cookie identifies the session. Refresh rotation is serialized across replicas, with an absolute eight-hour session limit; provider revocation/expiry requires sign-in again. Providers that do not issue refresh tokens retain access-token-limited sessions. Logout revokes the platform session but does not end provider SSO. Put the HTTP service behind TLS and configure the public OIDC callback URL consistently with the ingress/proxy.

Startup workflow bundles and runtime registrations share the PostgreSQL catalog. All replicas immediately resolve registered versions from that catalog; in-flight runs retain their original definition snapshots. Treat each name/version as immutable, including bootstrapped files; changes need a new version. Apply database migrations (including migration 7 for source/session storage) before starting updated replicas. Use identical cookie encryption/signing keys across replicas, protect them in a secret manager, and plan key rotation; replacing keys invalidates existing sessions.

PostgreSQL and RabbitMQ are external operational dependencies in Kubernetes. Provision, secure, back up, monitor, upgrade, and provide network access to them separately. The Compose credentials and images are a local smoke setup, not production settings. Kubernetes Secret references avoid embedding credentials in these manifests but do not themselves provide secret encryption, access policy, rotation, or an external secret manager.

Task and completion handling is at-least-once. Workers must make side effects idempotent; stable task IDs and completion identities suppress duplicate state transitions, but cannot make external worker side effects exactly-once. Managed RabbitMQ adapters use durable quorum queues with a per-queue dead-letter exchange and `.dlq` queue, a delivery limit of five, persistent publications, publisher confirms, and consumer prefetch of 16. A supervised connection manager reconnects with bounded exponential backoff; readiness stays false until adapter channels and required consumers are re-established. Broker outages do not require process restart. Monitor the dead-letter queues and broker quorum/replication health as part of deployment operations. Lease consumption/expiry, workflow events, and publication outbox writes commit atomically in PostgreSQL; delivery/acknowledgement remains at-least-once. Health probes verify HTTP liveness and PostgreSQL/RabbitMQ readiness; they do not establish end-to-end workflow correctness or replace operational monitoring.

