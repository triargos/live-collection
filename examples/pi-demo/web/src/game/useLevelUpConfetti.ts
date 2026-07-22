import confetti from "canvas-confetti"
import { useEffect, useRef } from "react"

export const useLevelUpConfetti = (level: number): void => {
  const previousLevel = useRef<number | undefined>(undefined)

  useEffect(() => {
    const previous = previousLevel.current
    previousLevel.current = level

    // Joining a session at an existing level should not replay its celebration.
    if (previous === undefined || level <= previous) return

    const sharedOptions = {
      colors: ["#8b5cf6", "#c026d3", "#fbbf24", "#22c55e", "#38bdf8"],
      disableForReducedMotion: true,
      scalar: 1.1,
      spread: 70,
      ticks: 180,
      zIndex: 100,
    }

    void confetti({ ...sharedOptions, angle: 60, origin: { x: 0, y: 0.7 }, particleCount: 70 })
    void confetti({ ...sharedOptions, angle: 120, origin: { x: 1, y: 0.7 }, particleCount: 70 })
  }, [level])
}
