import { useLiveQuery } from "@tanstack/react-db"
import type { AppBundle } from "@/live/collections.js"
import { levelForXp, XP_PER_TODO, type LevelInfo } from "./levels.js"

export interface GameStats {
  readonly xp: number
  readonly completedCount: number
  readonly totalCount: number
  readonly completionPercent: number
  readonly level: LevelInfo
}

export const useGameStats = (bundle: AppBundle): GameStats => {
  const todos = bundle.todosCollection(bundle.session)
  const { data: todoRows } = useLiveQuery((q) => q.from({ todo: todos }))
  const completedCount = todoRows.reduce(
    (count, todo) => count + (todo.completed ? 1 : 0),
    0,
  )
  const totalCount = todoRows.length
  const xp = completedCount * XP_PER_TODO

  return {
    xp,
    completedCount,
    totalCount,
    completionPercent: totalCount === 0 ? 0 : Math.round((completedCount / totalCount) * 100),
    level: levelForXp(xp),
  }
}
