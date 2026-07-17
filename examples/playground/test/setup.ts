// Vitest browser mode exposes an undefined `setImmediate` property. Effect v4 checks
// property presence when selecting its scheduler, so provide the browser fallback.
if (typeof globalThis.setImmediate !== "function") {
  Object.defineProperty(globalThis, "setImmediate", {
    configurable: true,
    value: (callback: () => void) => setTimeout(callback, 0),
  })
  Object.defineProperty(globalThis, "clearImmediate", {
    configurable: true,
    value: (handle: ReturnType<typeof setTimeout>) => clearTimeout(handle),
  })
}
