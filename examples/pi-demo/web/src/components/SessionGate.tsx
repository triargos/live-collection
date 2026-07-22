import { randomSessionCode } from "@pi-demo/shared"
import { Option } from "effect"
import { LogIn, PartyPopper, Sparkles } from "lucide-react"
import { type FormEvent, useState } from "react"
import { Button } from "@/components/ui/button.js"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card.js"
import { Input } from "@/components/ui/input.js"
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
    <main className="relative flex min-h-screen items-center justify-center overflow-hidden px-4 py-12 sm:px-6">
      <div aria-hidden className="absolute left-[7%] top-[12%] animate-float text-5xl opacity-60">⭐</div>
      <div aria-hidden className="absolute bottom-[14%] right-[8%] animate-float text-5xl opacity-50 [animation-delay:-2s]">⚡</div>

      <div className="relative w-full max-w-4xl">
        <header className="mx-auto mb-10 max-w-2xl text-center">
          <div className="mx-auto mb-5 grid size-20 animate-float place-items-center rounded-3xl bg-gradient-to-br from-violet-500 to-fuchsia-500 text-5xl font-black text-white shadow-[0_8px_0_oklch(0.47_0.22_292),0_16px_40px_oklch(0.58_0.24_292/0.3)]">
            π
          </div>
          <div className="mb-3 inline-flex items-center gap-2 rounded-full border-2 border-primary/15 bg-card/80 px-4 py-1.5 text-sm font-extrabold text-primary shadow-sm backdrop-blur">
            <Sparkles className="size-4" /> Effect × TanStack DB
          </div>
          <h1 className="text-balance text-4xl font-black tracking-tight sm:text-6xl">Live quests, better together.</h1>
          <p className="mx-auto mt-4 max-w-xl text-balance text-base font-semibold text-muted-foreground sm:text-lg">
            Start a party, share the six-character invite code, and race through your quests in real time.
          </p>
        </header>

        <div className="grid gap-6 md:grid-cols-2">
          <Card className="group relative overflow-hidden transition-transform hover:-translate-y-1">
            <div aria-hidden className="absolute -right-4 -top-5 rotate-12 text-8xl opacity-10">🎉</div>
            <CardHeader>
              <span className="mb-2 grid size-12 place-items-center rounded-2xl bg-accent text-2xl shadow-sm">🚀</span>
              <CardTitle>Start a party</CardTitle>
              <CardDescription>Create a fresh quest board, then invite your crew to join the fun.</CardDescription>
            </CardHeader>
            <CardContent className="mt-auto">
              <Button className="w-full" size="lg" onClick={() => createSession(randomSessionCode())} type="button">
                <PartyPopper /> Create session
              </Button>
            </CardContent>
          </Card>

          <Card className="group relative overflow-hidden transition-transform hover:-translate-y-1">
            <div aria-hidden className="absolute -right-3 -top-6 -rotate-12 text-8xl opacity-10">🔑</div>
            <CardHeader>
              <span className="mb-2 grid size-12 place-items-center rounded-2xl bg-secondary text-2xl shadow-sm">🤝</span>
              <CardTitle>Join with a code</CardTitle>
              <CardDescription>Enter the invite from another device. No account or password needed.</CardDescription>
            </CardHeader>
            <CardContent className="mt-auto">
              <form className="grid gap-3" onSubmit={join}>
                <Input
                  aria-describedby={invalid ? "session-error" : undefined}
                  aria-invalid={invalid}
                  aria-label="Session code"
                  autoCapitalize="characters"
                  autoComplete="off"
                  className="h-14 text-center font-mono text-2xl font-black uppercase tracking-[0.3em] placeholder:tracking-[0.3em]"
                  maxLength={6}
                  onChange={(event) => {
                    setInput(normalizeSessionCode(event.target.value))
                    setInvalid(false)
                  }}
                  placeholder="ABC234"
                  spellCheck={false}
                  value={input}
                />
                <Button size="lg" type="submit"><LogIn /> Join party</Button>
                {invalid && (
                  <p className="animate-in fade-in slide-in-from-top-1 text-center text-sm font-bold text-destructive" id="session-error">
                    That code doesn't look right — try six letters or numbers.
                  </p>
                )}
              </form>
            </CardContent>
          </Card>
        </div>

        <p className="mt-9 text-center text-xs font-bold text-muted-foreground">Local-first magic · live sync · no signup</p>
      </div>
    </main>
  )
}
