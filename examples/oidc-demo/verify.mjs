import { createRequire } from 'node:module';
import fs from 'node:fs';
import https from 'node:https';
import assert from 'node:assert/strict';
import { fileURLToPath } from 'node:url';
import { execFileSync } from 'node:child_process';

const require = createRequire(new URL('../../test/browser/package.json', import.meta.url));
const { chromium } = require('@playwright/test');
const directory = fileURLToPath(new URL('./', import.meta.url));
const values = Object.fromEntries(fs.readFileSync(directory + '.local/demo.env', 'utf8').trim().split('\n').map(line => {
  const index = line.indexOf('=');
  return [line.slice(0, index), line.slice(index + 1)];
}));
const agent = new https.Agent({
  ca: fs.readFileSync(directory + '.local/demo.crt'),
  lookup: (_hostname, options, callback) => options.all
    ? callback(null, [{ address: '127.0.0.1', family: 4 }])
    : callback(null, '127.0.0.1', 4),
});

function request(host, path, { method = 'GET', body, headers = {} } = {}) {
  return new Promise((resolve, reject) => {
    const req = https.request(`https://${host}:8443${path}`, { agent, method, headers }, response => {
      let text = '';
      response.on('data', chunk => { text += chunk; });
      response.on('end', () => resolve({ status: response.statusCode, body: text }));
    });
    req.on('error', reject);
    req.end(body);
  });
}
async function serviceToken(id, secret) {
  const result = await request('identity.localhost', '/realms/zephyr/protocol/openid-connect/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({ grant_type: 'client_credentials', client_id: id, client_secret: secret }).toString(),
  });
  assert.equal(result.status, 200, result.body);
  return JSON.parse(result.body).access_token;
}
function claims(token) {
  return JSON.parse(Buffer.from(token.split('.')[1], 'base64url'));
}
async function placeOrder(suffix) {
  const result = await request('checkout.localhost', '/orders', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ order_id: `oidc-demo-${Date.now()}-${suffix}`, customer_email: 'consumer@example.test' }),
  });
  assert.equal(result.status, 202, result.body);
  const { id } = JSON.parse(result.body);
  for (let attempt = 0; attempt < 60; attempt++) {
    const current = await request('checkout.localhost', `/orders/${id}`);
    assert.equal(current.status, 200, current.body);
    const run = JSON.parse(current.body);
    if (run.status === 'COMPLETED') {
      assert.deepEqual(run.result, { status: 'receipt_sent' });
      return id;
    }
    assert.notEqual(run.status, 'FAILED', current.body);
    await new Promise(resolve => setTimeout(resolve, 500));
  }
  throw new Error(`Workflow ${id} did not complete`);
}

const report = { checkedAt: new Date().toISOString(), checks: [], limitations: [] };
function passed(message) { report.checks.push(message); console.log('PASS:', message); }
const browser = await chromium.launch();
try {
  // Only browser automation accepts this self-signed demo certificate.
  // Node/API verification above validates the exact generated CA.
  const operator = await browser.newContext({ ignoreHTTPSErrors: true });
  const page = await operator.newPage();
  await page.goto('https://zephyr.localhost:8443');
  await page.getByLabel('Username or email').fill('operator');
  await page.getByLabel('Password', { exact: true }).fill('demo-local-only');
  await page.getByRole('button', { name: 'Sign In', exact: true }).click();
  await page.waitForURL('https://zephyr.localhost:8443/');
  await page.getByRole('button', { name: 'Sign out' }).waitFor();
  assert.equal(await page.locator('#token-field').isVisible(), false);
  const cookies = await operator.cookies('https://zephyr.localhost:8443');
  const session = cookies.find(cookie => cookie.name === 'zephyr_oidc_session');
  assert.ok(session?.secure && session?.httpOnly);
  passed('Real Keycloak authorization-code/PKCE login creates a Secure HttpOnly portal session and hides Dev API Token');
  const beforeExpiry = await operator.request.get('https://zephyr.localhost:8443/auth/session');
  assert.equal(beforeExpiry.status(), 200);
  assert.equal((await beforeExpiry.json()).permissions['zephyr:workflow:register'], true);
  const runtimeName = `RuntimeCheckout${Date.now()}`;
  const source = fs.readFileSync(new URL('../checkout/workflows/checkout.zephyr', import.meta.url), 'utf8')
    .replace('workflow Checkout(', `workflow ${runtimeName}(`);
  await page.getByRole('button', { name: 'Register workflow', exact: true }).click();
  await page.getByLabel('Workflow source').fill(source);
  await page.getByLabel('Version', { exact: true }).fill('3');
  const archiveDownload = page.waitForEvent('download');
  await page.getByRole('button', { name: 'Register and generate contracts' }).click();
  const downloaded = await archiveDownload;
  assert.equal(downloaded.suggestedFilename(), 'workflow-contracts.zip');
  await page.getByRole('button', { name: runtimeName, exact: true }).waitFor();
  const repeated = await operator.request.post('https://zephyr.localhost:8443/v1/workflows/register', {
    headers: { Origin: 'https://zephyr.localhost:8443' }, data: { source, version: 3 },
  });
  assert.equal(repeated.status(), 200);
  const contracts = await repeated.json();
  assert.ok(contracts.files['client.go'].includes(`"${runtimeName}", 3, input`));
  const conflict = await operator.request.post('https://zephyr.localhost:8443/v1/workflows/register', {
    headers: { Origin: 'https://zephyr.localhost:8443' }, data: { source: source + '\n', version: 3 },
  });
  assert.equal(conflict.status(), 409);
  report.registeredWorkflow = runtimeName;
  passed('Portal registers a runtime workflow and downloads version-3 contracts; identical replay succeeds and immutable conflicts return 409');
  const started = await operator.request.post(`https://zephyr.localhost:8443/v1/workflows/${runtimeName}/instances`, {
    headers: { Origin: 'https://zephyr.localhost:8443' },
    data: { version: 3, context: { order_id: runtimeName, customer_email: 'runtime@example.test' } },
  });
  assert.equal(started.status(), 200);
  const runtimeRun = await started.json();
  for (let attempt = 0; attempt < 60; attempt++) {
    const current = await operator.request.get(`https://zephyr.localhost:8443/v1/instances/${runtimeRun.id}`);
    const run = await current.json();
    if (run.status === 'COMPLETED') {
      assert.deepEqual(run.result, { status: 'receipt_sent' });
      report.runtimeRun = run.id;
      break;
    }
    assert.notEqual(run.status, 'FAILED');
    await new Promise(resolve => setTimeout(resolve, 500));
  }
  assert.ok(report.runtimeRun, 'Runtime-registered workflow did not complete');
  passed('Existing consumer worker executes newly registered workflow without any platform restart');
  await page.close();

  const viewer = await browser.newContext({ ignoreHTTPSErrors: true });
  const viewerPage = await viewer.newPage();
  await viewerPage.goto('https://zephyr.localhost:8443');
  await viewerPage.getByLabel('Username or email').fill('viewer');
  await viewerPage.getByLabel('Password', { exact: true }).fill('demo-local-only');
  await viewerPage.getByRole('button', { name: 'Sign In', exact: true }).click();
  await viewerPage.waitForURL('https://zephyr.localhost:8443/');
  await viewerPage.getByRole('button', { name: 'Sign out' }).waitFor();
  assert.equal(await viewerPage.locator('#start-button').isVisible(), false);
  assert.equal(await viewerPage.locator('#register-button').isVisible(), false);
  assert.equal(await viewerPage.locator('[data-start-workflow]').count(), 0);
  const registrationDenied = await viewer.request.post('https://zephyr.localhost:8443/v1/workflows/register', {
    headers: { Origin: 'https://zephyr.localhost:8443' }, data: { source, version: 4 },
  });
  assert.equal(registrationDenied.status(), 403);
  const denied = await viewer.request.post('https://zephyr.localhost:8443/v1/workflows/Checkout/instances', {
    headers: { Origin: 'https://zephyr.localhost:8443' },
    data: { version: 1, context: { order_id: 'viewer-denied', customer_email: 'viewer@example.test' } },
  });
  assert.equal(denied.status(), 403);
  passed('Viewer has no start/registration controls and backend rejects both mutations (403)');
  await viewer.close();

  const appToken = await serviceToken('checkout-app', values.APP_CLIENT_SECRET);
  const workerToken = await serviceToken('checkout-worker', values.WORKER_CLIENT_SECRET);
  assert.ok(claims(appToken).roles.includes('zephyr:workflow:start'));
  assert.ok(!claims(appToken).roles.includes('zephyr:worker:execute'));
  assert.deepEqual(claims(workerToken).roles, ['zephyr:worker:execute']);
  const apiHeaders = token => ({ Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' });
  assert.equal((await request('zephyr.localhost', '/v1/workflows', { headers: apiHeaders(workerToken) })).status, 403);
  assert.equal((await request('zephyr.localhost', '/v1/tasks/heartbeat', {
    method: 'POST', headers: apiHeaders(appToken), body: '{}',
  })).status, 403);
  assert.equal((await request('zephyr.localhost', '/v1/workflows')).status, 401);
  assert.equal((await request('zephyr.localhost', '/metrics')).status, 404);
  passed('Application and worker have separate scoped JWT identities; invalid identity/access is rejected; proxy blocks metrics');
  report.firstRun = await placeOrder('before-expiry');
  passed('HTTPS application -> generated client -> OIDC platform -> worker -> RabbitMQ completion succeeds');

  console.log('Waiting 70 seconds to exercise access-token expiry...');
  await new Promise(resolve => setTimeout(resolve, 70_000));
  const renewed = await operator.request.get('https://zephyr.localhost:8443/auth/session');
  assert.equal(renewed.status(), 200, await renewed.text());
  assert.equal((await operator.cookies('https://zephyr.localhost:8443')).find(cookie => cookie.name === 'zephyr_oidc_session')?.value, session.value);
  assert.equal((await request('zephyr.localhost', '/v1/workflows', { headers: apiHeaders(appToken) })).status, 401);
  passed('Portal refreshes after access-token expiry without reauthorization; expired standalone API token still returns 401');
  report.secondRun = await placeOrder('after-expiry');
  passed('Application and worker obtain new service tokens automatically and still complete workflows after expiry');
  execFileSync('docker', ['compose', '--env-file', directory + '.local/demo.env', '-f', directory + 'compose.yaml', 'restart', 'server'], { stdio: 'pipe' });
  for (let attempt = 0; attempt < 60; attempt++) {
    const ready = await request('zephyr.localhost', '/readyz');
    if (ready.status === 200) break;
    await new Promise(resolve => setTimeout(resolve, 500));
  }
  assert.equal((await operator.request.get('https://zephyr.localhost:8443/auth/session')).status(), 200);
  const persistent = await operator.request.get(`https://zephyr.localhost:8443/v1/workflows/${runtimeName}?version=3`);
  assert.equal(persistent.status(), 200);
  passed('Portal session and runtime-registered workflow survive server restart');
  const loginAgain = await operator.newPage();
  await loginAgain.goto('https://zephyr.localhost:8443');
  await loginAgain.waitForURL('https://zephyr.localhost:8443/');
  await loginAgain.getByRole('button', { name: 'Sign out' }).waitFor();
  passed('Renewed session opens portal directly after restart');
  const logout = await operator.request.post('https://zephyr.localhost:8443/auth/logout', {
    headers: { Origin: 'https://zephyr.localhost:8443' },
  });
  assert.equal(logout.status(), 204);
  assert.equal((await operator.request.get('https://zephyr.localhost:8443/auth/session')).status(), 401);
  const replay = await request('zephyr.localhost', '/auth/session', {
    headers: { Cookie: `${session.name}=${session.value}` },
  });
  assert.equal(replay.status, 401);
  await loginAgain.goto('https://zephyr.localhost:8443/auth/login');
  await loginAgain.waitForURL('https://zephyr.localhost:8443/');
  passed('Portal logout clears local session, but provider SSO remains and allows immediate sign-in again');
  report.limitations.push('Portal logout is not identity-provider logout.');
  await operator.close();
  fs.writeFileSync(directory + '.local/verification-report.json', JSON.stringify(report, null, 2) + '\n');
} finally {
  agent.destroy();
  await browser.close();
}
