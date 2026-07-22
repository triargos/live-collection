import { defineConfig } from "vitest/config"

// Own config so a run from this package never falls back to a workspace-level config.
export default defineConfig({
  test: {
    include: ["test/**/*.test.ts"],
    environment: "node",
  },
})
