/**
 * Per-tab identity, persisted in `sessionStorage`. It survives a reload (so OPFS rehydrates the same
 * database and the tab keeps its name in the debug panel) but is **unique per tab** — two tabs therefore
 * open *different* OPFS sqlite files and never contend for the single-writer OPFS lock. Cross-tab sync is
 * then carried by the shared "server" (a `localStorage` log + a `BroadcastChannel`), exactly as a real
 * backend would fan out SSE to independent browser tabs.
 */
export interface TabSession {
  readonly tabId: string
  readonly dbName: string
}

const remember = (key: string, mint: () => string): string => {
  const existing = sessionStorage.getItem(key)
  if (existing !== null) return existing
  const minted = mint()
  sessionStorage.setItem(key, minted)
  return minted
}

export const getTabSession = (): TabSession => {
  const tabId = remember("lc:playground:tabId", () => `tab-${Math.random().toString(36).slice(2, 7)}`)
  return { tabId, dbName: `playground-${tabId}.sqlite` }
}
