import { ProjectId, TodoId, todoKey } from "@pi-demo/shared"
import { eq } from "@tanstack/db"
import { useLiveQuery } from "@tanstack/react-db"
import { Option } from "effect"
import { Check, Plus, Radio, Trash2 } from "lucide-react"
import { useState, type FormEvent } from "react"
import { Badge } from "@/components/ui/badge.js"
import { Button } from "@/components/ui/button.js"
import { Card, CardContent } from "@/components/ui/card.js"
import { Input } from "@/components/ui/input.js"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select.js"
import { useGameStats } from "@/game/useGameStats.js"
import type { AppBundle } from "../live/collections.js"

interface TodoListProps {
  readonly bundle: AppBundle
  readonly projectId: Option.Option<ProjectId>
}

export function TodoList({ bundle, projectId }: TodoListProps) {
  const todos = bundle.todosCollection(bundle.session)
  const projects = bundle.projectsCollection(bundle.session)
  const [title, setTitle] = useState("")
  const [selectedProjectId, setSelectedProjectId] = useState("")
  const stats = useGameStats(bundle)

  const { data: projectRows } = useLiveQuery((q) =>
    q.from({ project: projects }).orderBy(({ project }) => project.name),
  )

  const { data: joinedTodos, isLoading } = useLiveQuery(
    (q) => {
      let query = q
        .from({ todo: todos })
        .innerJoin(
          { project: projects },
          ({ todo, project }) => eq(todo.projectId, project.id),
        )

      if (Option.isSome(projectId)) {
        query = query.where(({ todo }) => eq(todo.projectId, projectId.value))
      }

      return query
        .select(({ todo, project }) => ({
          id: todo.id,
          sessionId: todo.sessionId,
          projectId: todo.projectId,
          title: todo.title,
          completed: todo.completed,
          createdAt: todo.createdAt,
          projectName: project.name,
          projectColor: project.color,
        }))
        .orderBy(({ todo }) => todo.createdAt, "desc")
    },
    [Option.getOrElse(projectId, () => "all")],
  )

  const addTodo = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    const trimmed = title.trim()
    if (trimmed.length === 0) return

    let targetProjectId: ProjectId
    if (Option.isSome(projectId)) {
      targetProjectId = projectId.value
    } else {
      const selected = projectRows.find((project) => project.id === selectedProjectId)
      if (selected === undefined) return
      targetProjectId = selected.id
    }

    todos.insert({
      id: TodoId.make(crypto.randomUUID()),
      sessionId: bundle.session,
      projectId: targetProjectId,
      title: trimmed,
      completed: false,
      createdAt: new Date().toISOString(),
    })
    setTitle("")
  }

  const activeProject = Option.isSome(projectId)
    ? projectRows.find((project) => project.id === projectId.value)
    : undefined

  return (
    <section className="mx-auto w-full max-w-5xl px-4 py-8 sm:px-8 sm:py-10">
      <header className="mb-7 flex flex-col justify-between gap-4 sm:flex-row sm:items-start">
        <div>
          <Badge className="mb-3 gap-1.5 border-emerald-200 bg-emerald-50 text-emerald-700" variant="outline">
            <Radio className="size-3 animate-pulse" /> Live quest log
          </Badge>
          <h1 className="text-balance text-3xl font-extrabold tracking-tight sm:text-4xl">
            {activeProject === undefined ? "All quests" : activeProject.name}
          </h1>
          <p className="mt-2 font-normal text-muted-foreground">
            {stats.level.emoji} Level {stats.level.level} · {stats.level.title}. Keep the streak alive!
          </p>
        </div>
        <div className="shrink-0 rounded-2xl border-2 border-primary/15 bg-card px-4 py-2 text-center shadow-sm">
          <p className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Party score</p>
          <p className="text-xl font-bold text-primary">⭐ {stats.xp} XP</p>
        </div>
      </header>

      <div className="mb-7 grid grid-cols-3 gap-2 sm:gap-4">
        <Card className="gap-0 border-violet-200 bg-violet-50/80 py-3 shadow-[0_4px_0_oklch(0.84_0.06_292)] sm:py-4">
          <CardContent className="px-2 text-center sm:px-4">
            <span className="text-xl sm:text-2xl">⚡</span>
            <strong className="mt-1 block text-lg font-bold sm:text-2xl">{stats.xp}</strong>
            <span className="text-[0.65rem] font-medium uppercase tracking-wider text-muted-foreground sm:text-xs">XP earned</span>
          </CardContent>
        </Card>
        <Card className="gap-0 border-emerald-200 bg-emerald-50/80 py-3 shadow-[0_4px_0_oklch(0.86_0.07_155)] sm:py-4">
          <CardContent className="px-2 text-center sm:px-4">
            <span className="text-xl sm:text-2xl">✅</span>
            <strong className="mt-1 block text-lg font-bold sm:text-2xl">{stats.completedCount}/{stats.totalCount}</strong>
            <span className="text-[0.65rem] font-medium uppercase tracking-wider text-muted-foreground sm:text-xs">Quests done</span>
          </CardContent>
        </Card>
        <Card className="gap-0 border-amber-200 bg-amber-50/80 py-3 shadow-[0_4px_0_oklch(0.86_0.07_85)] sm:py-4">
          <CardContent className="px-2 text-center sm:px-4">
            <span className="text-xl sm:text-2xl">🏆</span>
            <strong className="mt-1 block text-lg font-bold sm:text-2xl">{stats.completionPercent}%</strong>
            <span className="text-[0.65rem] font-medium uppercase tracking-wider text-muted-foreground sm:text-xs">Complete</span>
          </CardContent>
        </Card>
      </div>

      <Card className="mb-7 gap-0 border-primary/25 bg-card/90 py-4 shadow-[0_5px_0_oklch(0.81_0.08_292)]">
        <CardContent className="px-4 sm:px-5">
          <form className="flex flex-col gap-3 sm:flex-row" onSubmit={addTodo}>
            <Input
              aria-label="Todo title"
              className="flex-1"
              onChange={(event) => setTitle(event.target.value)}
              placeholder="What's the next quest?"
              value={title}
            />
            {Option.isNone(projectId) && (
              <Select onValueChange={setSelectedProjectId} value={selectedProjectId}>
                <SelectTrigger aria-label="Project" className="sm:w-48">
                  <SelectValue placeholder="Choose quest line" />
                </SelectTrigger>
                <SelectContent>
                  {projectRows.map((project) => (
                    <SelectItem key={project.id} value={project.id}>{project.name}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            )}
            <Button className="shrink-0" type="submit"><Plus /> Add quest</Button>
          </form>
        </CardContent>
      </Card>

      {isLoading ? (
        <Card className="animate-pulse border-dashed py-12 text-center shadow-none">
          <CardContent><div className="mb-3 text-4xl">🌀</div><p className="font-semibold">Opening your local quest book…</p></CardContent>
        </Card>
      ) : joinedTodos.length === 0 ? (
        <Card className="border-dashed bg-card/70 py-12 text-center shadow-none">
          <CardContent>
            <div className="mb-3 text-5xl">🗺️</div>
            <strong className="text-xl font-bold">No quests here yet.</strong>
            <p className="mx-auto mt-2 max-w-sm font-normal text-muted-foreground">Add one above — it will pop up instantly on every device in the party.</p>
          </CardContent>
        </Card>
      ) : (
        <ul className="grid list-none gap-3 p-0">
          {joinedTodos.map((todo) => (
            <li
              className={`group flex items-center gap-3 rounded-xl border-2 bg-card p-3 shadow-[0_3px_0_oklch(0.85_0.05_287)] transition-all hover:-translate-y-0.5 hover:border-primary/30 sm:p-4 ${todo.completed ? "border-emerald-200 bg-emerald-50/60 opacity-75" : "border-border"}`}
              key={todo.id}
            >
              <button
                aria-label={todo.completed ? "Mark incomplete" : "Mark complete"}
                className={`grid size-10 shrink-0 place-items-center rounded-xl border-2 transition-all focus-visible:outline-none focus-visible:ring-4 focus-visible:ring-primary/20 ${todo.completed ? "animate-pop border-emerald-500 bg-emerald-500 text-white shadow-[0_3px_0_oklch(0.55_0.17_155)]" : "border-primary/25 bg-secondary/50 text-transparent hover:scale-105 hover:border-primary hover:text-primary/25"}`}
                onClick={() => todos.update(todo.id, (draft) => { draft.completed = !draft.completed })}
                type="button"
              >
                <Check className="size-5 stroke-[3]" />
              </button>
              <div className="min-w-0 flex-1">
                <span className={`block font-semibold transition-all ${todo.completed ? "text-muted-foreground line-through decoration-2" : "text-foreground"}`}>{todo.title}</span>
                <span className="mt-1 inline-flex items-center gap-1.5 text-xs font-normal text-muted-foreground">
                  <i className="size-2.5 rounded-full" style={{ backgroundColor: todo.projectColor }} /> {todo.projectName}
                </span>
              </div>
              <Button
                aria-label={`Delete ${todo.title}`}
                className="shrink-0 opacity-40 hover:text-destructive group-hover:opacity-100"
                onClick={() => todos.delete(todoKey(todo))}
                size="icon"
                type="button"
                variant="ghost"
              >
                <Trash2 />
              </Button>
            </li>
          ))}
        </ul>
      )}

      <p className="mt-8 text-center text-xs font-normal text-muted-foreground">Every move is local first, then confirmed over Effect HTTP ⚡</p>
    </section>
  )
}
