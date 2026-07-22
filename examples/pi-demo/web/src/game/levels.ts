export const XP_PER_TODO = 10

export interface LevelInfo {
  readonly level: number
  readonly title: string
  readonly emoji: string
  readonly xpIntoLevel: number
  readonly xpForNextLevel: number
  readonly progress: number
}

interface LevelDefinition {
  readonly title: string
  readonly emoji: string
  readonly startsAt: number
}

const LEVELS: ReadonlyArray<LevelDefinition> = [
  { title: "Todo Rookie", emoji: "🐣", startsAt: 0 },
  { title: "List Apprentice", emoji: "📝", startsAt: 30 },
  { title: "Checkbox Champ", emoji: "🏅", startsAt: 70 },
  { title: "Task Meister", emoji: "⚔️", startsAt: 120 },
  { title: "Productivity Wizard", emoji: "🧙", startsAt: 200 },
  { title: "Todo Legend", emoji: "👑", startsAt: 300 },
]

export const levelForXp = (xp: number): LevelInfo => {
  const safeXp = Number.isFinite(xp) ? Math.max(0, Math.floor(xp)) : 0
  let currentIndex = 0

  for (let index = 1; index < LEVELS.length; index += 1) {
    const candidate = LEVELS[index]
    if (candidate !== undefined && safeXp >= candidate.startsAt) currentIndex = index
  }

  const current = LEVELS[currentIndex] ?? LEVELS[0]
  if (current === undefined) throw new Error("At least one game level is required")

  const next = LEVELS[currentIndex + 1]
  const xpIntoLevel = safeXp - current.startsAt
  const xpForNextLevel = next === undefined ? 0 : next.startsAt - current.startsAt

  return {
    level: currentIndex + 1,
    title: current.title,
    emoji: current.emoji,
    xpIntoLevel,
    xpForNextLevel,
    progress: next === undefined ? 1 : Math.min(1, xpIntoLevel / xpForNextLevel),
  }
}
