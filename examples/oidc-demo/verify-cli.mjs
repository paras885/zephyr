import { createRequire } from 'node:module';
import { execFileSync, spawn } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import https from 'node:https';
import assert from 'node:assert/strict';

const require = createRequire(new URL('../../test/browser/package.json', import.meta.url));
const { chromium } = require('@playwright/test');
const repository = fileURLToPath(new URL('../../', import.meta.url));
const directory = fileURLToPath(new URL('./', import.meta.url));
const workspace = fs.mkdtempSync(path.join(os.tmpdir(), 'zephyr-cli-demo-'));
const binary = path.join(workspace, 'zephyr');
const configDirectory = path.join(workspace, 'config');
fs.mkdirSync(configDirectory, { mode: 0o700 });
const env = {
  ...process.env, ZEPHYR_TOKEN: '',
  ZEPHYR_CONFIG_DIR: configDirectory,
  ZEPHYR_ENDPOINT: 'https://zephyr.localhost:8443',
  ZEPHYR_OIDC_ISSUER: '',
  ZEPHYR_OIDC_CLIENT_ID: '',
  ZEPHYR_CA_FILE: path.join(directory, '.local/demo.crt'),
};
execFileSync('go', ['build', '-o', binary, './cmd/zephyr'], { cwd: repository, stdio: 'pipe' });
const report = { checkedAt: new Date().toISOString(), checks: [] };
const passed = message => { report.checks.push(message); console.log('PASS:', message); };
const agent = new https.Agent({
  ca: fs.readFileSync(env.ZEPHYR_CA_FILE),
  lookup: (_host, options, callback) => options.all
    ? callback(null, [{ address: '127.0.0.1', family: 4 }])
    : callback(null, '127.0.0.1', 4),
});
const browser = await chromium.launch();
const active = new Set();

function launch(args, onDiagnostic) {
  const child = spawn(binary, args, { cwd: workspace, env });
  active.add(child);
  let stdout = '', stderr = '';
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => child.kill('SIGTERM'), 180_000);
    child.stdout.on('data', chunk => { stdout += chunk; });
    child.stderr.on('data', chunk => { stderr += chunk; onDiagnostic?.(stderr); });
    child.on('error', reject);
    child.on('close', code => {
      clearTimeout(timeout);
      active.delete(child);
      resolve({ code, stdout, stderr });
    });
  });
}
async function command(...args) {
  const result = await launch(args);
  assert.equal(result.code, 0, result.stderr);
  return JSON.parse(result.stdout);
}
async function signIn(username, args = ['auth', 'login', '--no-browser']) {
  const context = await browser.newContext({ ignoreHTTPSErrors: true });
  const page = await context.newPage();
  let notified = false;
  let notify, rejectLink;
  const linkReady = new Promise((resolve, reject) => { notify = resolve; rejectLink = reject; });
  const login = launch(args, diagnostic => {
    const match = diagnostic.match(/Sign in at (https:\/\/[^\s]+)/);
    if (match && !notified) { notified = true; notify(match[1]); }
  });
  login.then(result => { if (!notified) rejectLink(new Error(`No device URL: ${result.stderr}`)); });
  try {
    await page.goto(await linkReady);
    await page.getByLabel('Username or email').fill(username);
    await page.getByLabel('Password', { exact: true }).fill('demo-local-only');
    await page.getByRole('button', { name: 'Sign In', exact: true }).click();
    // Keycloak asks the user to approve the device after authentication.
    const approve = page.getByRole('button', { name: /Confirm|Allow|Yes/i });
    await approve.click();
    const result = await login;
    assert.equal(result.code, 0, result.stderr);
    const output = JSON.parse(result.stdout);
    assert.ok(output.authenticated === true || output.published === true);
  } finally { await context.close(); }
}
async function replayRefresh(refreshToken) {
  return new Promise((resolve, reject) => {
    const body = new URLSearchParams({
      grant_type: 'refresh_token', client_id: 'zephyr-cli', refresh_token: refreshToken,
    }).toString();
    const req = https.request('https://identity.localhost:8443/realms/zephyr/protocol/openid-connect/token', {
      agent, method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    }, response => {
      let body = '';
      response.on('data', chunk => { body += chunk; });
      response.on('end', () => resolve({ status: response.statusCode, body: JSON.parse(body) }));
    });
    req.on('error', reject);
    req.end(body);
  });
}

try {
  await command('workflow', 'generate');
  const file = path.join(workspace, 'workflow.zephyr');
  const name = `CLIHello${Date.now()}`;
  fs.writeFileSync(file, fs.readFileSync(file, 'utf8').replace('workflow Hello(', `workflow ${name}(`));
  assert.equal((await command('workflow', 'validate')).valid, true);
  await command('workflow', 'artifacts', '--version', '4');
  assert.ok(fs.readFileSync(path.join(workspace, 'client.go'), 'utf8').includes(`"${name}", 4, input`));
  passed('Single CLI creates/validates a sample and generates version-pinned artifacts offline');
  await signIn('operator', ['workflow', 'publish', '--version', '4', '--force', '--no-browser']);
  const credentials = JSON.parse(fs.readFileSync(path.join(configDirectory, 'session.json'), 'utf8'));
  assert.equal(credentials.issuer, 'https://identity.localhost:8443/realms/zephyr');
  assert.equal(credentials.client_id, 'zephyr-cli');
  assert.ok(credentials.token.refresh_token);
  assert.equal(fs.statSync(path.join(configDirectory, 'session.json')).mode & 0o777, 0o600);
  passed('Endpoint-only publish discovers authentication, starts browser-approved device sign-in and saves a private refreshable CLI session');
  const publication = await command('workflow', 'publish', '--version', '4', '--force');
  assert.equal(publication.published, true);
  assert.ok(fs.readFileSync(path.join(workspace, '.env.example'), 'utf8').includes(env.ZEPHYR_ENDPOINT));
  assert.ok(!fs.readFileSync(path.join(workspace, '.env.example'), 'utf8').includes(credentials.token.access_token));
  const started = await command('workflow', 'start', name, '--version', '4', '--input', '{"value":"cli-completed"}', '--idempotency-key', name);
  report.workflowName = name;
  report.run = started.id;
  const status = await command('workflow', 'status', started.id);
  assert.equal(status.status, 'COMPLETED');
  assert.deepEqual(status.result, { status: 'cli-completed' });
  assert.equal((await command('workflow', 'runs', name, '--limit', '1')).items[0].id, started.id);
  passed('CLI publishes, starts, reads full status and returns the requested number of runs');
  console.log('Waiting 70 seconds to exercise CLI refresh rotation...');
  await new Promise(resolve => setTimeout(resolve, 70_000));
  const [first, second] = await Promise.all([
    command('workflow', 'status', started.id), command('workflow', 'runs', name, '--limit', '1'),
  ]);
  assert.equal(first.status, 'COMPLETED');
  assert.equal(second.items.length, 1);
  const rotated = JSON.parse(fs.readFileSync(path.join(configDirectory, 'session.json'), 'utf8'));
  assert.notEqual(rotated.token.refresh_token, credentials.token.refresh_token);
  passed('Concurrent independent CLI processes serialize single-use refresh rotation and persist renewed credentials');
  await command('auth', 'logout');
  assert.ok(!fs.existsSync(path.join(configDirectory, 'session.json')));
  const replay = await replayRefresh(rotated.token.refresh_token);
  assert.equal(replay.status, 400);
  assert.equal(replay.body.error, 'invalid_grant');
  passed('CLI logout revokes the provider refresh token and deletes the local session');
  await signIn('viewer');
  assert.equal((await command('workflow', 'status', started.id)).status, 'COMPLETED');
  const forbidden = await launch(['workflow', 'publish', '--version', '5', '--force']);
  assert.notEqual(forbidden.code, 0);
  assert.ok(forbidden.stderr.includes('403'));
  const deniedStart = await launch(['workflow', 'start', name, '--input', '{"value":"forbidden"}']);
  assert.notEqual(deniedStart.code, 0);
  assert.ok(deniedStart.stderr.includes('403'));
  await command('auth', 'logout');
  passed('Viewer CLI session can read runs but cannot publish or start (403)');
  fs.writeFileSync(path.join(directory, '.local/cli-verification-report.json'), JSON.stringify(report, null, 2) + '\n');
} finally {
  for (const child of active) child.kill('SIGTERM');
  await browser.close();
  agent.destroy();
  fs.rmSync(workspace, { recursive: true, force: true });
}
