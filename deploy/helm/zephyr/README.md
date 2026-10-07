# Zephyr Helm chart

Installs the Zephyr server Deployment/Service, a one-shot schema migration
Job, and optional Ingress/NetworkPolicy/PodDisruptionBudget. It assumes
externally managed PostgreSQL and RabbitMQ and an external OIDC provider; it
does not install or operate those dependencies. This is an alternative to
applying [`deploy/kubernetes`](../kubernetes) directly — pick one, not both.

## Prerequisites

- A built, pushed image referenced by its exact digest (`image.digest`).
  Build and migrate must use the identical digest; see the repository root
  [Kubernetes & Helm doc](../../docs/KUBERNETES.md) for the build/push commands.
- A reachable PostgreSQL and RabbitMQ, and an OIDC provider configured with
  the portal/CLI client and API audience (or the explicitly development-only
  static-token path for a throwaway cluster).
- A Secret with the connection/OIDC values the Deployment and migration Job
  read (see `secret.name` in `values.yaml`). The chart does not create this
  Secret by default so credentials never enter `values.yaml` or Helm release
  history:

  ```sh
  kubectl create secret generic zephyr-runtime \
    --from-literal=DATABASE_URL="$DATABASE_URL" \
    --from-literal=AMQP_URL="$AMQP_URL" \
    --from-literal=OIDC_ISSUER_URL="$OIDC_ISSUER_URL" \
    --from-literal=OIDC_CLIENT_ID="$OIDC_CLIENT_ID" \
    --from-literal=OIDC_CLIENT_SECRET="${OIDC_CLIENT_SECRET:-}" \
    --from-literal=OIDC_API_AUDIENCE="$OIDC_API_AUDIENCE" \
    --from-literal=OIDC_REDIRECT_URL="$OIDC_REDIRECT_URL" \
    --from-literal=OIDC_COOKIE_HASH_KEY="$(openssl rand -base64 32)" \
    --from-literal=OIDC_COOKIE_BLOCK_KEY="$(openssl rand -base64 32)"
  ```

## Install

```sh
helm install zephyr deploy/helm/zephyr \
  --set image.repository=registry.example.com/team/zephyr \
  --set image.digest=sha256:<digest>
```

Then follow the printed `NOTES.txt`: wait for the migration Job, check
rollout status, and reach the portal.

## Upgrade

The migration Job is intentionally immutable per release (Kubernetes Jobs
cannot be updated in place). Before an upgrade that changes the image,
delete the previous Job first, exactly as with the plain manifests:

```sh
kubectl delete job zephyr-migrate --ignore-not-found
helm upgrade zephyr deploy/helm/zephyr \
  --set image.repository=registry.example.com/team/zephyr \
  --set image.digest=sha256:<new-digest>
```

## Key values

| Key | Purpose |
| --- | --- |
| `image.digest` | Exact immutable image digest (preferred over `image.tag`); shared by the Deployment and migration Job |
| `replicaCount` | Server replica count (default `2`) |
| `secret.name` | Name of the existing Secret providing `DATABASE_URL`, `AMQP_URL`, and `OIDC_*` |
| `secret.create` | Set `true` only for local/throwaway clusters to have the chart create that Secret from `secret.data` |
| `migrationJob.enabled` | Disable if you run migrations out-of-band |
| `ingress.enabled` / `ingress.host` / `ingress.tls` | Starter `nginx` Ingress; off by default |
| `networkPolicy.enabled` / `networkPolicy.namespaceSelector` | Restrict ingress to your ingress/metrics namespace; apply only after confirming your CNI enforces NetworkPolicy |
| `podDisruptionBudget.*` | Minimum availability during voluntary disruptions |
| `resources` | CPU/memory requests and limits; review against measured workload |

See `values.yaml` for the full set, including probe timing and environment
variables merged into the container.

## Validate changes

```sh
helm lint deploy/helm/zephyr --set image.digest=sha256:0000000000000000000000000000000000000000000000000000000000000000
helm template zephyr deploy/helm/zephyr --set image.digest=sha256:0000000000000000000000000000000000000000000000000000000000000000
```
