import { Effect } from "effect"

/**
 * Where an entry sits in the data flow, used only for colour-coding in the panel:
 *  - `out`   a request leaving this tab (an optimistic mutation hitting the fake server)
 *  - `in`    something arriving from "the server" — a cross-tab broadcast or a catchup delta
 *  - `echo`  this tab's own write coming back through the sync loop (the self-echo)
 *  - `info`  a neutral lifecycle note (catchup ran, snapshot reconciled)
 *  - `error` a rejected mutation that rolled back
 */
export type DebugDirection = "out" | "in" | "echo" | "info" | "error"

/** One row in the network/traffic log. `payload` is shown expanded as pretty JSON. */
export interface DebugEntry {
  readonly id: number
  readonly at: number
  readonly direction: DebugDirection
  readonly channel: string
  readonly label: string
  readonly payload?: unknown
}

/**
 * A tiny observable ring buffer for the debug panel — the example app's window into the otherwise
 * invisible read/write path. The fake backend taps every mutation, echo, broadcast and catchup into it;
 * the panel renders it via `useSyncExternalStore`. Not part of the library: pure example-app glue, so a
 * plain class (no Effect service ceremony) is the right altitude. Newest entries first; capped so a long
 * session can't grow unbounded.
 */
export class DebugBus {
  #entries: ReadonlyArray<DebugEntry> = []
  #seq = 0
  readonly #listeners = new Set<() => void>()
  readonly #cap: number

  constructor(cap = 500) {
    this.#cap = cap
  }

  /** Stable identity for `useSyncExternalStore`'s subscribe. */
  readonly subscribe = (onChange: () => void): (() => void) => {
    this.#listeners.add(onChange)
    return () => {
      this.#listeners.delete(onChange)
    }
  }

  /** Stable, structurally-frozen snapshot (only changes when an entry is pushed/cleared). */
  readonly snapshot = (): ReadonlyArray<DebugEntry> => this.#entries

  /** Record one entry. Returns nothing — callers in Effect code wrap it in {@link DebugBus.tap}. */
  push(entry: Omit<DebugEntry, "id" | "at">): void {
    const full: DebugEntry = { ...entry, id: ++this.#seq, at: Date.now() }
    this.#entries = [full, ...this.#entries].slice(0, this.#cap)
    this.#emit()
  }

  clear(): void {
    this.#entries = []
    this.#emit()
  }

  /** Push from inside an Effect pipeline without leaving the Effect world. */
  tap(entry: Omit<DebugEntry, "id" | "at">): Effect.Effect<void> {
    return Effect.sync(() => this.push(entry))
  }

  #emit(): void {
    for (const listener of this.#listeners) listener()
  }
}
