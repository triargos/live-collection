import { ProjectId, TodoId, todoKey } from "@pi-demo/shared";
import { eq } from "@tanstack/db";
import { useLiveQuery } from "@tanstack/react-db";
import { Option } from "effect";
import { useState, type FormEvent } from "react";
import type { AppBundle } from "../live/collections.js";

interface TodoListProps {
  readonly bundle: AppBundle
  readonly projectId: Option.Option<ProjectId>
}

export function TodoList({ bundle, projectId }: TodoListProps) {
  const todos = bundle.todosCollection(bundle.session)
  const projects = bundle.projectsCollection(bundle.session)
  const [title, setTitle] = useState("")
  const [selectedProjectId, setSelectedProjectId] = useState("")

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

  return (
    <section className="todo-page">
      <header className="page-header">
        <div>
          <p className="eyebrow">Live workspace</p>
          <h1>{Option.isSome(projectId) ? "Project todos" : "All todos"}</h1>
          <p className="subtitle">Every row is local first, then confirmed over Effect HTTP.</p>
        </div>
        <span className="live-pill"><span /> SSE + catchup</span>
      </header>

      <form className="todo-composer" onSubmit={addTodo}>
        <input
          aria-label="Todo title"
          onChange={(event) => setTitle(event.target.value)}
          placeholder="What needs doing?"
          value={title}
        />
        {Option.isNone(projectId) && (
          <select
            aria-label="Project"
            onChange={(event) => setSelectedProjectId(event.target.value)}
            value={selectedProjectId}
          >
            <option value="">Choose project</option>
            {projectRows.map((project) => (
              <option key={project.id} value={project.id}>{project.name}</option>
            ))}
          </select>
        )}
        <button className="primary-button" type="submit">Add todo</button>
      </form>

      {isLoading ? (
        <div className="empty-state">Opening the local database…</div>
      ) : joinedTodos.length === 0 ? (
        <div className="empty-state">
          <strong>Nothing here yet.</strong>
          <span>Add a todo above — it will appear instantly in every open window.</span>
        </div>
      ) : (
        <ul className="todo-list">
          {joinedTodos.map((todo) => (
            <li className={todo.completed ? "todo-row completed" : "todo-row"} key={todo.id}>
              <button
                aria-label={todo.completed ? "Mark incomplete" : "Mark complete"}
                className="check-button"
                onClick={() => todos.update(todo.id, (draft) => { draft.completed = !draft.completed })}
                type="button"
              >
                {todo.completed ? "✓" : ""}
              </button>
              <div className="todo-copy">
                <span className="todo-title">{todo.title}</span>
                <span className="project-badge">
                  <i style={{ backgroundColor: todo.projectColor }} /> {todo.projectName}
                </span>
              </div>
              <button
                aria-label={`Delete ${todo.title}`}
                className="icon-button"
                onClick={() => todos.delete(todoKey(todo))}
                type="button"
              >
                ×
              </button>
            </li>
          ))}
        </ul>
      )}
    </section>
  )
}
