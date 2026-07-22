import { ProjectId, projectKey } from "@pi-demo/shared"
import { useLiveQuery } from "@tanstack/react-db"
import { Copy, ListTodo, LogOut, Plus, Trash2 } from "lucide-react"
import { type FormEvent, useMemo, useState } from "react"
import { NavLink, useLocation, useNavigate } from "react-router-dom"
import { Badge } from "@/components/ui/badge.js"
import { Button } from "@/components/ui/button.js"
import { Input } from "@/components/ui/input.js"
import { Progress } from "@/components/ui/progress.js"
import { Separator } from "@/components/ui/separator.js"
import { useGameStats } from "@/game/useGameStats.js"
import { useLevelUpConfetti } from "@/game/useLevelUpConfetti.js"
import type { AppBundle } from "../live/collections.js"
import { clearSession } from "../live/session.js"

const DEFAULT_COLOR = "#8b5cf6"
const COLORS: ReadonlyArray<string> = [DEFAULT_COLOR, "#06b6d4", "#f59e0b", "#f43f5e"]

export function Sidebar({ bundle }: { readonly bundle: AppBundle }) {
  const projects = bundle.projectsCollection(bundle.session)
  const todos = bundle.todosCollection(bundle.session)
  const navigate = useNavigate()
  const location = useLocation()
  const [name, setName] = useState("")
  const [color, setColor] = useState(DEFAULT_COLOR)
  const { data: projectRows } = useLiveQuery((q) =>
    q.from({ project: projects }).orderBy(({ project }) => project.createdAt),
  )
  const { data: todoRows } = useLiveQuery((q) => q.from({ todo: todos }))
  const stats = useGameStats(bundle)
  useLevelUpConfetti(stats.level.level)

  const todoCounts = useMemo(() => {
    const counts = new Map<ProjectId, number>()
    for (const todo of todoRows) {
      counts.set(todo.projectId, (counts.get(todo.projectId) ?? 0) + 1)
    }
    return counts
  }, [todoRows])

  const addProject = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    const trimmed = name.trim()
    if (trimmed.length === 0) return

    projects.insert({
      id: ProjectId.make(crypto.randomUUID()),
      sessionId: bundle.session,
      name: trimmed,
      color,
      createdAt: new Date().toISOString(),
    })
    setName("")
  }

  const removeProject = (project: (typeof projectRows)[number]) => {
    // The server cascades this delete and publishes a separate SSE delete for every todo.
    projects.delete(projectKey(project))
    if (location.pathname === `/p/${project.id}`) navigate("/")
  }

  return (
    <aside className="relative z-10 border-b-2 border-border bg-card/90 p-4 shadow-sm backdrop-blur-xl lg:sticky lg:top-0 lg:flex lg:h-screen lg:flex-col lg:overflow-y-auto lg:border-b-0 lg:border-r-2 lg:p-5">
      <div className="flex items-center gap-3 px-1 py-2">
        <span className="grid size-12 shrink-0 place-items-center rounded-2xl bg-gradient-to-br from-violet-500 to-fuchsia-500 text-3xl font-black text-white shadow-[0_4px_0_oklch(0.47_0.22_292)]">π</span>
        <div className="min-w-0">
          <strong className="block text-lg font-bold leading-tight">Quest Party</strong>
          <small className="font-normal text-muted-foreground">live todo adventure</small>
        </div>
        <span aria-label="Live" className="ml-auto size-2.5 rounded-full bg-emerald-500 shadow-[0_0_0_4px_oklch(0.9_0.12_155)]" />
      </div>

      <div className="mt-5 rounded-xl border-2 border-primary/15 bg-secondary/55 p-3">
        <div className="mb-2 flex items-center justify-between gap-2">
          <span className="text-[0.65rem] font-medium uppercase tracking-[0.16em] text-muted-foreground">Party code</span>
          <Badge className="bg-emerald-100 text-emerald-700" variant="secondary">● Live</Badge>
        </div>
        <strong className="block text-center font-mono text-2xl font-semibold tracking-[0.22em] text-primary">{bundle.session}</strong>
        <div className="mt-3 grid grid-cols-2 gap-2">
          <Button aria-label="Copy session code" onClick={() => void navigator.clipboard.writeText(bundle.session)} size="sm" type="button" variant="outline">
            <Copy /> Copy
          </Button>
          <Button onClick={clearSession} size="sm" type="button" variant="ghost">
            <LogOut /> Leave
          </Button>
        </div>
      </div>

      <div className="mt-4 rounded-xl border-2 border-amber-200 bg-gradient-to-br from-amber-50 to-fuchsia-50 p-4 shadow-sm">
        <div className="flex items-center gap-3">
          <span className="text-3xl" aria-hidden>{stats.level.emoji}</span>
          <div className="min-w-0">
            <p className="text-xs font-semibold uppercase tracking-wider text-amber-700">Level {stats.level.level}</p>
            <p className="truncate font-bold">{stats.level.title}</p>
          </div>
          <Badge className="ml-auto border-amber-200 bg-white text-amber-700" variant="outline">⭐ {stats.xp} XP</Badge>
        </div>
        <Progress className="mt-3" value={stats.level.progress * 100} />
        <p className="mt-2 text-right text-[0.68rem] font-medium text-muted-foreground">
          {stats.level.xpForNextLevel === 0
            ? "MAX LEVEL!"
            : `${stats.level.xpIntoLevel} / ${stats.level.xpForNextLevel} XP`}
        </p>
      </div>

      <Separator className="my-5" />

      <nav className="grid gap-1" aria-label="Quests">
        <p className="mb-1 px-2 text-xs font-semibold uppercase tracking-[0.15em] text-muted-foreground">Quests</p>
        <NavLink
          className={({ isActive }) => `flex min-w-0 items-center gap-3 rounded-lg px-3 py-2.5 text-sm font-semibold transition-colors ${isActive ? "bg-primary text-primary-foreground shadow-sm" : "text-foreground hover:bg-secondary"}`}
          end
          to="/"
        >
          <ListTodo className="size-4 shrink-0" />
          <span className="min-w-0 flex-1 truncate">All quests</span>
          <Badge className="border-0 bg-card/80 text-foreground" variant="outline">{todoRows.length}</Badge>
        </NavLink>
        {projectRows.map((project) => (
          <div className="group relative" key={project.id}>
            <NavLink
              className={({ isActive }) => `flex min-w-0 items-center gap-3 rounded-lg py-2.5 pl-3 pr-16 text-sm font-semibold transition-colors ${isActive ? "bg-secondary text-secondary-foreground" : "hover:bg-muted"}`}
              to={`/p/${project.id}`}
            >
              <i className="size-3 shrink-0 rounded-full shadow-sm ring-2 ring-white" style={{ backgroundColor: project.color }} />
              <span className="min-w-0 flex-1 truncate">{project.name}</span>
            </NavLink>
            <Badge className="pointer-events-none absolute right-9 top-1/2 -translate-y-1/2" variant="secondary">{todoCounts.get(project.id) ?? 0}</Badge>
            <Button
              aria-label={`Delete ${project.name}`}
              className="absolute right-1 top-1/2 -translate-y-1/2 opacity-60 hover:text-destructive group-hover:opacity-100"
              onClick={() => removeProject(project)}
              size="icon-sm"
              type="button"
              variant="ghost"
            ><Trash2 /></Button>
          </div>
        ))}
      </nav>

      <form className="mt-5 rounded-xl border-2 border-dashed border-primary/25 bg-primary/[0.03] p-3 lg:mt-auto" onSubmit={addProject}>
        <label className="mb-2 block text-xs font-semibold uppercase tracking-wider text-muted-foreground" htmlFor="new-project">New quest line</label>
        <div className="flex gap-2">
          <Input
            className="h-9 min-w-0"
            id="new-project"
            onChange={(event) => setName(event.target.value)}
            placeholder="Quest name"
            value={name}
          />
          <Button aria-label="Add project" className="shrink-0" size="icon-sm" type="submit"><Plus /></Button>
        </div>
        <div className="mt-3 flex gap-2" aria-label="Project color">
          {COLORS.map((preset) => (
            <button
              aria-label={`Use ${preset}`}
              aria-pressed={color === preset}
              className="size-6 rounded-full border-2 border-white shadow-sm transition-transform hover:scale-110 aria-pressed:scale-110 aria-pressed:ring-2 aria-pressed:ring-primary aria-pressed:ring-offset-2"
              key={preset}
              onClick={() => setColor(preset)}
              style={{ backgroundColor: preset }}
              type="button"
            />
          ))}
        </div>
      </form>

      <p className="mt-4 text-center text-[0.65rem] font-normal text-muted-foreground">⚡ SSE + catchup · local first</p>
    </aside>
  )
}
