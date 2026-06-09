import { fileURLToPath, URL } from "node:url"
import { defineConfig } from "vite"
import react from "@vitejs/plugin-react"
import tailwindcss from "@tailwindcss/vite"

// The browser OPFS persistence ships a pre-built worker (referenced via `new Worker(new URL(...))`) plus
// the wa-sqlite wasm. Pre-bundling those through esbuild rewrites the worker/wasm URLs and breaks them,
// so exclude both from optimizeDeps and let Vite serve the real package files from node_modules.
export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: {
      "@": fileURLToPath(new URL("./src", import.meta.url)),
    },
  },
  optimizeDeps: {
    exclude: ["@tanstack/browser-db-sqlite-persistence", "@journeyapps/wa-sqlite"],
  },
  // `main.tsx` opens OPFS with a top-level `await` before rendering; target a baseline that supports it.
  build: {
    target: "esnext",
  },
})
