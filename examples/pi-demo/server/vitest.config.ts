import { defineConfig } from "vitest/config"

// Own config so a run from this package never falls back to a workspace-level config
// (the playground's suites are browser-mode and would fail in node).
export default defineConfig({
  test: {
    include: ["test/**/*.test.ts"],
    environment: "node",
  },
})
