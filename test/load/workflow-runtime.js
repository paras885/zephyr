import http from 'k6/http';
import { check } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

const baseURL = __ENV.ZEPHYR_ENDPOINT || 'http://127.0.0.1:8080';
const token = __ENV.ZEPHYR_TOKEN || '';
const duration = __ENV.DURATION || '5m';
const startsPerSecond = Number(__ENV.STARTS_PER_SECOND || 5);
const workerCount = Number(__ENV.WORKERS || 50);

const workflowStartSuccess = new Rate('zephyr_workflow_start_success');
const heartbeatSuccess = new Rate('zephyr_worker_heartbeat_success');
const taskCompletionSuccess = new Rate('zephyr_task_completion_success');
const completedTasks = new Counter('zephyr_tasks_completed');
const workflowStartDuration = new Trend('zephyr_workflow_start_duration', true);
const taskCompletionDuration = new Trend('zephyr_task_completion_duration', true);

export const options = {
  scenarios: {
    starts: {
      executor: 'constant-arrival-rate',
      exec: 'startWorkflow',
      rate: startsPerSecond,
      timeUnit: '1s',
      duration,
      preAllocatedVUs: 20,
      maxVUs: 40,
    },
    workers: {
      executor: 'constant-vus',
      exec: 'runWorker',
      vus: workerCount,
      duration,
      gracefulStop: '35s',
    },
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

function requestHeaders() {
  const headers = { 'Content-Type': 'application/json' };
  if (token) headers.Authorization = `Bearer ${token}`;
  return headers;
}

export function startWorkflow() {
  const startedAt = Date.now();
  const response = http.post(
    `${baseURL}/v1/workflows/Checkout/instances`,
    JSON.stringify({
      version: 1,
      context: { order_id: `load-${__VU}-${__ITER}`, customer_email: 'load@example.test' },
    }),
    { headers: requestHeaders(), timeout: '15s', tags: { operation: 'workflow_start' } },
  );
  workflowStartDuration.add(Date.now() - startedAt);
	if (!response) {
    workflowStartSuccess.add(false);
    return;
  }
	const hasWorkflowID = response.status === 200 && Boolean(response.json('id'));
  const succeeded = check(response, {
    'workflow start accepted': (result) => result.status === 200,
    'workflow ID returned': () => hasWorkflowID,
  });
  workflowStartSuccess.add(succeeded);
}

export function runWorker() {
  const response = http.post(
    `${baseURL}/v1/tasks/receive`,
    JSON.stringify({ worker_id: `load-worker-${__VU}`, lease_duration_ms: 60000 }),
    { headers: requestHeaders(), timeout: '35s', tags: { operation: 'worker_receive' } },
  );
  if (!response || response.status !== 200) return;

  const delivery = response.json();
  const item = delivery.Item || delivery.item || {};
  const workflowID = item.WorkflowID || item.workflow_id;
  const taskID = item.TaskID || item.task_id;
  const nodeID = item.NodeID || item.node_id;
  const leaseID = delivery.LeaseID || delivery.lease_id;
  const leaseToken = delivery.LeaseToken || delivery.lease_token;
  const result = nodeID === 'authorize' ? { status: 'AUTHORIZED' } : { status: 'SENT' };
  const heartbeat = http.post(
    `${baseURL}/v1/tasks/heartbeat`,
    JSON.stringify({ lease_id: leaseID, lease_token: leaseToken, lease_duration_ms: 60000 }),
    { headers: requestHeaders(), timeout: '10s', tags: { operation: 'worker_heartbeat' } },
  );
  heartbeatSuccess.add(Boolean(heartbeat && heartbeat.status === 200) && check(heartbeat, { 'worker heartbeat accepted': (value) => value.status === 200 }));
  const startedAt = Date.now();
  const completion = http.post(
    `${baseURL}/v1/tasks/complete`,
    JSON.stringify({ workflow_id: workflowID, task_id: taskID, node_id: nodeID, lease_id: leaseID, lease_token: leaseToken, result }),
    { headers: requestHeaders(), timeout: '10s', tags: { operation: 'task_complete' } },
  );
  taskCompletionDuration.add(Date.now() - startedAt);
  const succeeded = Boolean(completion && completion.status === 200) && check(completion, { 'task completion accepted': (value) => value.status === 200 });
  taskCompletionSuccess.add(succeeded);
  if (succeeded) completedTasks.add(1);
}