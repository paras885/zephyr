#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cluster_name="${KIND_CLUSTER_NAME:-zephyr-phase5}"
image="zephyr:kind"
namespace="zephyr-test"
local_port="${ZEPHYR_KIND_PORT:-18088}"
context="kind-${cluster_name}"
previous_context="$(kubectl config current-context 2>/dev/null || true)"
created_cluster=false
port_forward_pid=""
stopped_node=""
kind_build_dir="$root_dir/.kind-build"
keep_cluster="${KEEP_KIND_CLUSTER:-false}"

for command_name in docker kind kubectl curl jq; do
	if ! command -v "$command_name" >/dev/null 2>&1; then
		printf 'required command not found: %s\n' "$command_name" >&2
		exit 1
	fi
done

cleanup() {
	if [[ -n "$stopped_node" ]]; then
		docker start "$stopped_node" >/dev/null 2>&1 || true
	fi
	if [[ -n "$port_forward_pid" ]]; then
		kill "$port_forward_pid" 2>/dev/null || true
	fi
	if [[ "$keep_cluster" == true ]]; then
		printf 'keeping debug cluster %s (context %s, namespace %s)\n' "$cluster_name" "$context" "$namespace"
	else
		kubectl --context "$context" delete namespace "$namespace" --ignore-not-found --wait=true >/dev/null 2>&1 || true
		if [[ "$created_cluster" == true ]]; then
			kind delete cluster --name "$cluster_name"
		fi
	fi
	if [[ -n "$previous_context" ]]; then
		kubectl config use-context "$previous_context" >/dev/null 2>&1 || true
	fi
}
trap cleanup EXIT INT TERM

	if kind get clusters | grep -Fxq "$cluster_name"; then
		if [[ "${ALLOW_REUSE_KIND_CLUSTER:-false}" != "true" ]]; then
			printf 'kind cluster %q already exists; set ALLOW_REUSE_KIND_CLUSTER=true only after confirming namespace %q is disposable\n' "$cluster_name" "$namespace" >&2
			exit 1
		fi
	else
		kind create cluster --name "$cluster_name" --config "$root_dir/deploy/kubernetes/kind/cluster.yaml" --wait 120s
	created_cluster=true
fi

	case "$(uname -m)" in
		x86_64) target_arch=amd64 ;;
		arm64|aarch64) target_arch=arm64 ;;
		*) printf 'unsupported host architecture: %s\n' "$(uname -m)" >&2; exit 1 ;;
	esac
	mkdir -p "$kind_build_dir"
	(cd "$root_dir" && CGO_ENABLED=0 GOOS=linux GOARCH="$target_arch" go build -trimpath -ldflags='-s -w' -o "$kind_build_dir/zephyr-server" ./cmd/zephyr-server)
	docker build --target kind-runtime -t "$image" -f "$root_dir/Dockerfile" "$root_dir"
kind load docker-image "$image" --name "$cluster_name"
	kubectl --context "$context" delete namespace "$namespace" --ignore-not-found --wait=true
kubectl --context "$context" apply -f "$root_dir/deploy/kubernetes/kind/namespace-dependencies.yaml"
kubectl --context "$context" -n "$namespace" rollout status deployment/postgres --timeout=180s
kubectl --context "$context" -n "$namespace" rollout status deployment/rabbitmq --timeout=180s

kubectl --context "$context" apply -f "$root_dir/deploy/kubernetes/kind/zephyr.yaml"
kubectl --context "$context" -n "$namespace" wait --for=condition=complete job/zephyr-migrate --timeout=180s
kubectl --context "$context" apply -f "$root_dir/deploy/kubernetes/kind/server.yaml"
kubectl --context "$context" -n "$namespace" rollout status deployment/zephyr-server --timeout=180s

base_url="http://127.0.0.1:${local_port}"
start_port_forward() {
	kubectl --context "$context" -n "$namespace" port-forward service/zephyr-server "$local_port:8080" >"${TMPDIR:-/tmp}/zephyr-kind-port-forward.log" 2>&1 &
	port_forward_pid=$!
	curl --connect-timeout 2 --max-time 3 --retry 60 --retry-delay 1 --retry-connrefused --silent --show-error --fail "$base_url/healthz" >/dev/null
}
start_port_forward
curl --silent --show-error --fail "$base_url/readyz" >/dev/null

metrics="$(curl --silent --fail "$base_url/metrics")"
grep -q 'zephyr_dependency_ready' <<<"$metrics"
workflow_response="$(curl --silent --show-error --fail -H 'Authorization: Bearer zephyr-kind-test-only' -H 'Content-Type: application/json' -d '{"version":1,"context":{"order_id":"kind-validation","customer_email":"kind@example.test"}}' "$base_url/v1/workflows/Checkout/instances")"
workflow_id="$(jq -r '.id' <<<"$workflow_response")"
[[ -n "$workflow_id" && "$workflow_id" != "null" ]]

revision="$(kubectl --context "$context" -n "$namespace" rollout history deployment/zephyr-server | awk '/^[0-9]+[[:space:]]/ { value=$1 } END { print value }')"
kubectl --context "$context" -n "$namespace" rollout restart deployment/zephyr-server
kubectl --context "$context" -n "$namespace" rollout status deployment/zephyr-server --timeout=180s
kubectl --context "$context" -n "$namespace" scale deployment/zephyr-server --replicas=3
kubectl --context "$context" -n "$namespace" rollout status deployment/zephyr-server --timeout=180s
kubectl --context "$context" -n "$namespace" scale deployment/zephyr-server --replicas=2
kubectl --context "$context" -n "$namespace" rollout status deployment/zephyr-server --timeout=180s
server_pod="$(kubectl --context "$context" -n "$namespace" get pods -l app=zephyr-server -o jsonpath='{.items[0].metadata.name}')"
kubectl --context "$context" -n "$namespace" delete pod "$server_pod" --wait=true --grace-period=5
kubectl --context "$context" -n "$namespace" rollout status deployment/zephyr-server --timeout=180s

if [[ -n "$revision" ]]; then
	kubectl --context "$context" -n "$namespace" patch deployment/zephyr-server --type=merge -p "{\"spec\":{\"template\":{\"metadata\":{\"annotations\":{\"phase5.zephyr.dev/test-revision\":\"$(date +%s)\"}}}}}"
	kubectl --context "$context" -n "$namespace" rollout status deployment/zephyr-server --timeout=180s
	kubectl --context "$context" -n "$namespace" rollout undo deployment/zephyr-server --to-revision="$revision"
	kubectl --context "$context" -n "$namespace" rollout status deployment/zephyr-server --timeout=180s
fi

worker_node="$(kubectl --context "$context" get nodes -o name | sed 's#node/##' | grep -v control-plane | head -n 1)"
if [[ -n "$worker_node" ]]; then
	stopped_node="$worker_node"
	docker stop "$stopped_node" >/dev/null
	kubectl --context "$context" -n "$namespace" wait --for=condition=Available deployment/zephyr-server --timeout=90s
	kill "$port_forward_pid" 2>/dev/null || true
	wait "$port_forward_pid" 2>/dev/null || true
	start_port_forward
	curl --silent --show-error --fail "$base_url/healthz" >/dev/null
	docker start "$stopped_node" >/dev/null
	stopped_node=""
	kubectl --context "$context" wait --for=condition=Ready "node/$worker_node" --timeout=180s
	kubectl --context "$context" -n "$namespace" rollout status deployment/postgres --timeout=180s
	kubectl --context "$context" -n "$namespace" rollout status deployment/rabbitmq --timeout=180s
	kubectl --context "$context" -n "$namespace" rollout status deployment/zephyr-server --timeout=180s
	curl --connect-timeout 2 --max-time 3 --retry 60 --retry-delay 1 --retry-connrefused --silent --show-error --fail "$base_url/readyz" >/dev/null
fi

kubectl --context "$context" -n "$namespace" scale deployment/rabbitmq --replicas=0
kubectl --context "$context" -n "$namespace" wait --for=delete pod -l app=rabbitmq --timeout=90s
server_pod="$(kubectl --context "$context" -n "$namespace" get pods -l app=zephyr-server -o jsonpath='{.items[0].metadata.name}')"
if kubectl --context "$context" -n "$namespace" exec "$server_pod" -- wget -q -O - http://127.0.0.1:8080/readyz >/dev/null; then
	printf 'readiness remained successful while RabbitMQ was stopped\n' >&2
	exit 1
fi
kubectl --context "$context" -n "$namespace" scale deployment/rabbitmq --replicas=1
kubectl --context "$context" -n "$namespace" rollout status deployment/rabbitmq --timeout=180s
kubectl --context "$context" -n "$namespace" wait --for=condition=Ready pods -l app=zephyr-server --timeout=180s

restored_workflow="$(curl --silent --show-error --fail -H 'Authorization: Bearer zephyr-kind-test-only' "$base_url/v1/instances/$workflow_id")"
[[ "$(jq -r '.id' <<<"$restored_workflow")" == "$workflow_id" ]]
curl --silent --show-error --fail "$base_url/readyz" >/dev/null
printf 'kind validation passed: workflow=%s replicas=2 image=%s\n' "$workflow_id" "$image"

if [[ "${RUN_K6:-false}" == "true" ]]; then
	if ! command -v k6 >/dev/null 2>&1; then
		printf 'RUN_K6=true but k6 is not installed\n' >&2
		exit 1
	fi
	ZEPHYR_ENDPOINT="$base_url" ZEPHYR_TOKEN=zephyr-kind-test-only k6 run --summary-export="${K6_SUMMARY_PATH:-kind-load-summary.json}" "$root_dir/test/load/workflow-runtime.js"
fi