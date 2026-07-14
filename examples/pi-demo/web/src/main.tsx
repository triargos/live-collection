import { Option } from "effect"
import { createRoot } from "react-dom/client"
import { BrowserRouter } from "react-router-dom"
import { SessionGate } from "./components/SessionGate.js"
import { createApp } from "./live/collections.js"
import { getSession } from "./live/session.js"
import { App } from "./routes/App.js"
import "./index.css"

const root = document.getElementById("root")
const session = getSession()

if (root !== null) {
  if (Option.isNone(session)) {
    createRoot(root).render(<SessionGate />)
  } else {
    // OPFS only opens after a session is selected; the gate itself has no sync runtime.
    const bundle = await createApp({ session: session.value })
    createRoot(root).render(
      <BrowserRouter>
        <App bundle={bundle} />
      </BrowserRouter>,
    )
  }
}
