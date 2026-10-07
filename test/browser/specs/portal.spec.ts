import { expect, test } from '@playwright/test';

test.beforeEach(async ({ page }) => {
  await page.route('**/auth/session', (route) => route.fulfill({
    status: 200,
    contentType: 'application/json',
    body: JSON.stringify({ authenticated: true, permissions: {
      'zephyr:workflow:read': true, 'zephyr:workflow:start': true, 'zephyr:workflow:register': true,
    } }),
  }));
  await page.route('**/auth/logout', (route) => route.fulfill({ status: 204 }));
  await page.route('**/v1/**', async (route) => {
    const request = route.request();
    const path = new URL(request.url()).pathname;
    if (request.method() === 'POST' && path === '/v1/workflows/Checkout/instances') {
      await route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ id: 'workflow-browser-1' }) });
      return;
    }
    if (path === '/v1/workflows') {
      await route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify([{
        name: 'Checkout', latest_version: 1, versions: [1], task_count: 1,
      }]) });
      return;
    }
    if (path === '/v1/metrics') {
      await route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ total: 1, running: 1 }) });
      return;
    }
    if (path === '/v1/instances' || path.startsWith('/v1/workflows/Checkout/instances')) {
      await route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ items: [], total: 0 }) });
      return;
    }
    if (path === '/v1/instances/workflow-browser-1') {
      await route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({
        id: 'workflow-browser-1', workflow_name: 'Checkout', version: 1, status: 'RUNNING',
        tasks: [], events: [], context: { order_id: 'browser-test' }, next_sequence: 1,
      }) });
      return;
    }
    await route.fulfill({ status: 404, contentType: 'application/json', body: JSON.stringify({ error: 'not found' }) });
  });
});

test('renders OIDC session mode and starts a workflow', async ({ page }) => {
  await page.goto('/');
  await expect(page.getByRole('heading', { name: 'Workflow operations' })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Sign out' })).toBeVisible();
  await expect(page.locator('#token-field')).toBeHidden();
  await expect(page.getByRole('button', { name: 'Checkout', exact: true })).toBeVisible();
  await expect(page.locator('#metric-total')).toHaveText('1');

  await page.locator('#start-button').click();
  await expect(page.locator('#start-dialog')).toBeVisible();
  await page.locator('#workflow-context').fill('{"order_id":"browser-test"}');
  await page.locator('#confirm-start').click();
  await expect(page.getByText('Started Checkout')).toBeVisible();
  await expect(page.locator('#run-detail')).toContainText('workflow-browser-1');
});

test('posts logout from the authenticated portal', async ({ page }) => {
  let logoutRequest = false;
  page.on('request', (request) => {
    if (request.url().endsWith('/auth/logout') && request.method() === 'POST') logoutRequest = true;
  });
  await page.goto('/');
  await page.getByRole('button', { name: 'Sign out' }).click();
  await expect.poll(() => logoutRequest).toBe(true);
});

test('exposes operational metrics without an API token', async ({ page }) => {
  await page.goto('/');
  const response = await page.request.get('/metrics');
  expect(response.status()).toBe(200);
  const metrics = await response.text();
  expect(metrics).toContain('zephyr_http_requests_total');
  expect(metrics).toContain('go_goroutines');
});

test('development authentication is actionable on a narrow screen', async ({ page }) => {
  await page.setViewportSize({ width: 800, height: 720 });
  await page.route('**/auth/session', (route) => route.fulfill({ status: 404 }));
  await page.route('**/v1/**', (route) => route.fulfill({
    status: 401,
    contentType: 'application/json',
    body: JSON.stringify({ error: 'unauthorized' }),
  }));
  await page.goto('/');
  await expect(page.getByRole('textbox', { name: 'Development API token' })).toBeVisible();
  await expect(page.locator('#toast')).toContainText('Enter the platform ZEPHYR_SERVER_TOKEN');
  await expect(page.locator('#connection-status')).toHaveText('API not connected');
});

test('development token restores authenticated API access', async ({ page }) => {
  await page.route('**/auth/session', (route) => route.fulfill({ status: 404 }));
  await page.route('**/v1/**', async (route) => {
    if (route.request().headers()['authorization'] === 'Bearer playwright-local-token') {
      await route.fallback();
    } else {
      await route.fulfill({
        status: 401,
        contentType: 'application/json',
        body: JSON.stringify({ error: 'unauthorized' }),
      });
    }
  });
  await page.goto('/');
  const input = page.getByRole('textbox', { name: 'Development API token' });
  await input.fill('playwright-local-token');
  await input.press('Tab');
  await expect.poll(() => page.evaluate(() => sessionStorage.getItem('zephyr-token'))).toBe('playwright-local-token');
  await expect(page.locator('#workspace-name')).toHaveText('127.0.0.1:8188');
  await expect(page.locator('#connection-status')).toHaveText('Platform connected');
});

test('viewer cannot see any workflow mutation controls', async ({ page }) => {
  await page.route('**/auth/session', route => route.fulfill({
    status: 200,
    contentType: 'application/json',
    body: JSON.stringify({ authenticated: true, permissions: { 'zephyr:workflow:read': true } }),
  }));
  await page.route('**/v1/workflows/Checkout', route => route.fulfill({
    status: 200,
    contentType: 'application/json',
    body: JSON.stringify({ Name: 'Checkout', Version: 1, Nodes: {}, Start: [] }),
  }));
  await page.goto('/');
  await expect(page.locator('#start-button')).toBeHidden();
  await expect(page.locator('#register-button')).toBeHidden();
  await expect(page.locator('[data-start-workflow]')).toHaveCount(0);
  await page.getByRole('button', { name: 'Checkout', exact: true }).click();
  await expect(page.locator('.definition-start')).toBeHidden();
  await expect(page.getByRole('button', { name: 'Download contracts' })).toBeVisible();
});

test('registers a workflow and downloads contracts without restarting', async ({ page }) => {
  let submitted: { source: string; version: number } | undefined;
  await page.route('**/v1/workflows/register', async route => {
    submitted = route.request().postDataJSON();
    await route.fulfill({
      status: 200, contentType: 'application/json',
      body: JSON.stringify({ name: 'Uploaded', version: 2, files: {} }),
    });
  });
  await page.route('**/v1/workflows/Uploaded/contracts?version=2', route => route.fulfill({
    status: 200, contentType: 'application/zip',
    body: 'test contract archive',
  }));
  await page.goto('/');
  await page.getByRole('button', { name: 'Register workflow', exact: true }).click();
  await page.getByLabel('Workflow source').fill('workflow source from consumer');
  await page.getByLabel('Version', { exact: true }).fill('2');
  const download = page.waitForEvent('download');
  await page.getByRole('button', { name: 'Register and generate contracts' }).click();
  expect((await download).suggestedFilename()).toBe('workflow-contracts.zip');
  expect(submitted).toEqual({ source: 'workflow source from consumer', version: 2 });
  await expect(page.locator('#register-dialog')).toBeHidden();
});