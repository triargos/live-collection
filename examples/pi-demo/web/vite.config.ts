import { defineConfig } from "vite"
import react from "@vitejs/plugin-react"

// wa-sqlite ships a pre-built worker + wasm referenced via `new Worker(new URL(...))`;
// esbuild pre-bundling rewrites those URLs and breaks them — serve the real files instead.
export default defineConfig({
  plugins: [react()],
  optimizeDeps: {
    exclude: ["@tanstack/browser-db-sqlite-persistence", "@journeyapps/wa-sqlite"],
  },
  // main.tsx opens OPFS with a top-level await before rendering.
  build: {
    target: "esnext",
  },
  server: {
    port: 5183,
    proxy: {
      // One origin in dev: REST + catchup + the SSE stream all proxy to @pi-demo/server.
      "/api": {
        target: "http://localhost:3050",
      },
    },
  },
})
