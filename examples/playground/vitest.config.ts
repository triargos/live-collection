import { defineConfig } from "vitest/config"

// Browser mode: the OPFS gate can ONLY be proven in a real browser (node has no OPFS). Playwright drives
// a headless Chromium; the same optimizeDeps exclusions as the app keep the wa-sqlite worker/wasm intact.
export default defineConfig({
  optimizeDeps: {
    exclude: ["@tanstack/browser-db-sqlite-persistence", "@journeyapps/wa-sqlite"],
  },
  test: {
    include: ["test/**/*.browser.test.ts"],
    setupFiles: ["./test/setup.ts"],
    browser: {
      enabled: true,
      provider: "playwright",
      headless: true,
      instances: [{ browser: "chromium" }],
    },
  },
})
