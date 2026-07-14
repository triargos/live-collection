import { randomSessionCode } from "@pi-demo/shared"
import { Option } from "effect"
import { type FormEvent, useState } from "react"
import { createSession, decodeSessionCode, normalizeSessionCode, setSession } from "../live/session.js"

export function SessionGate() {
  const [input, setInput] = useState("")
  const [invalid, setInvalid] = useState(false)

  const join = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    const decoded = decodeSessionCode(input)
    if (Option.isNone(decoded)) {
      setInvalid(true)
      return
    }
    setSession(decoded.value)
  }

  return (
    <main className="session-gate">
      <header className="session-gate-header">
        <span className="brand-mark">π</span>
        <div>
          <p className="eyebrow">Effect × TanStack DB</p>
          <h1>Live todos, together.</h1>
          <p className="subtitle">Share one six-character code and watch every device update live.</p>
        </div>
      </header>

      <div className="session-options">
        <section className="session-card">
          <span className="session-step">01</span>
          <h2>Start a session</h2>
          <p>Create a fresh workspace, then send its code to anyone you want to join.</p>
          <button className="primary-button gate-button" onClick={() => createSession(randomSessionCode())} type="button">
            Create session
          </button>
        </section>

        <section className="session-card">
          <span className="session-step">02</span>
          <h2>Join a session</h2>
          <p>Enter the code shown on another device. No account or password needed.</p>
          <form className="join-form" onSubmit={join}>
            <input
              aria-describedby={invalid ? "session-error" : undefined}
              aria-invalid={invalid}
              aria-label="Session code"
              autoCapitalize="characters"
              autoComplete="off"
              maxLength={6}
              onChange={(event) => {
                setInput(normalizeSessionCode(event.target.value))
                setInvalid(false)
              }}
              placeholder="ABC234"
              spellCheck={false}
              value={input}
            />
            <button className="primary-button" type="submit">Join</button>
          </form>
          {invalid && <small className="session-error" id="session-error">Enter a valid six-character code.</small>}
        </section>
      </div>
    </main>
  )
}
