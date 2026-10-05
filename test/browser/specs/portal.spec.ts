import { expect, test } from '@playwright/test';

test.beforeEach(async ({ page }) => {
  await page.route('**/auth/session', (route) => route.fulfill({
    status: 200,
    contentType: 'application/json',
    body: JSON.stringify({ authenticated: true }),
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