import { createRoot } from "react-dom/client"
import { createPlayground } from "./live/playground.js"
import { PlaygroundProvider } from "./live/context.js"
import { App } from "./routes/App.js"
import "./index.css"

// OPFS must open before we render (the runtime needs the persistence value). Top-level await runs it once
// at startup; the registry/loop are then built synchronously inside createPlayground.
const playground = await createPlayground()

const root = document.getElementById("root")
if (root === null) throw new Error("missing #root element")

createRoot(root).render(
  <PlaygroundProvider value={playground}>
    <App />
  </PlaygroundProvider>,
)
