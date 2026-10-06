import { defineConfig, devices } from "@playwright/test"

/**
 * Browser smoke tests for the pi-demo: the only gate that exercises the real browser
 * persistence path (OPFS worker, BrowserCollectionCoordinator, react-db). Chromium only —
 * Playwright's WebKit runs with zero-quota storage, so OPFS is unavailable there.
 *
 * Run with `pnpm --filter @pi-demo/web e2e`. Reuses already-running dev servers.
 */
export default defineConfig({
  testDir: "./test/e2e",
  timeout: 60_000,
  fullyParallel: false,
  workers: 1,
  reporter: "list",
  use: {
    baseURL: "http://localhost:5183",
    trace: "retain-on-failure",
  },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
  webServer: [
    {
      command: "pnpm --filter @pi-demo/server start",
      port: 3050,
      reuseExistingServer: true,
    },
    {
      command: "pnpm --filter @pi-demo/web dev",
      port: 5183,
      reuseExistingServer: true,
    },
  ],
})
