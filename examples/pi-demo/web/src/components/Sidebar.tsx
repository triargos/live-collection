import { useLiveQuery } from "@tanstack/react-db"
import { ProjectId, projectKey } from "@pi-demo/shared"
import { type FormEvent, useMemo, useState } from "react"
import { NavLink, useLocation, useNavigate } from "react-router-dom"
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
    <aside className="sidebar">
      <div className="brand">
        <span className="brand-mark">π</span>
        <div><strong>live todos</strong><small>Effect × TanStack DB</small></div>
      </div>

      <div className="session-chip">
        <span>Session</span>
        <strong>{bundle.session}</strong>
        <button
          aria-label="Copy session code"
          onClick={() => void navigator.clipboard.writeText(bundle.session)}
          type="button"
        >Copy</button>
        <button className="session-leave" onClick={clearSession} type="button">Leave</button>
      </div>

      <nav className="project-nav" aria-label="Projects">
        <NavLink className={({ isActive }) => isActive ? "nav-row active" : "nav-row"} end to="/">
          <span className="all-projects-icon">⌁</span>
          <span>All todos</span>
          <b>{todoRows.length}</b>
        </NavLink>
        <p className="nav-heading">Projects</p>
        {projectRows.map((project) => (
          <div className="project-nav-row" key={project.id}>
            <NavLink
              className={({ isActive }) => isActive ? "nav-row active" : "nav-row"}
              to={`/p/${project.id}`}
            >
              <i style={{ backgroundColor: project.color }} />
              <span>{project.name}</span>
              <b>{todoCounts.get(project.id) ?? 0}</b>
            </NavLink>
            <button
              aria-label={`Delete ${project.name}`}
              className="project-delete"
              onClick={() => removeProject(project)}
              type="button"
            >×</button>
          </div>
        ))}
      </nav>

      <form className="project-form" onSubmit={addProject}>
        <label htmlFor="new-project">New project</label>
        <div className="project-input-row">
          <input
            id="new-project"
            onChange={(event) => setName(event.target.value)}
            placeholder="Project name"
            value={name}
          />
          <button aria-label="Add project" type="submit">+</button>
        </div>
        <div className="color-picker" aria-label="Project color">
          {COLORS.map((preset) => (
            <button
              aria-label={`Use ${preset}`}
              aria-pressed={color === preset}
              key={preset}
              onClick={() => setColor(preset)}
              style={{ backgroundColor: preset }}
              type="button"
            />
          ))}
        </div>
      </form>

      <div className="sync-hint"><span /> Session {bundle.session} · live</div>
    </aside>
  )
}
