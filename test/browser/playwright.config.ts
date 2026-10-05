import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: './specs',
  fullyParallel: true,
  reporter: 'list',
  use: {
    baseURL: 'http://127.0.0.1:8188',
    browserName: 'chromium',
    viewport: { width: 1152, height: 720 },
  },
  webServer: {
    command: `cd ../.. && go run ./cmd/zephyr-server -addr 127.0.0.1:8188 -db /tmp/zephyr-playwright-${process.pid}.db -workflows workflows -token playwright-local-token`,
    url: 'http://127.0.0.1:8188/',
    reuseExistingServer: !process.env.CI,
    timeout: 120_000,
  },
});