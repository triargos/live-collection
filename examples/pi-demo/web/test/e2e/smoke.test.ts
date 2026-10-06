import { expect, test } from "@playwright/test"

// The A.3 persistence gate in a real browser: a fresh session opens its OPFS-backed
// collections, loads the seeded quest line, writes sync, and a reload hydrates from OPFS.
test("a session opens, writes, and hydrates from OPFS after reload", async ({ page }) => {
  const errors: Array<string> = []
  page.on("pageerror", (error) => errors.push(`pageerror: ${error.stack ?? error.message}`))
  page.on("console", (message) => {
    if (message.type() === "error") errors.push(`console.error: ${message.text()}`)
  })
  const withErrors = (what: string) => `${what}; browser errors:\n${errors.join("\n") || "(none)"}`

  const loading = page.getByText("Opening your local quest book…")
  const fetching = page.getByText("Fetching quests…")
  const quests = page.getByRole("navigation", { name: "Quests" })

  await page.goto("/")
  await page.getByRole("button", { name: "Create session" }).click()

  // A new session is seeded with one quest line; the app is usable once it is listed and
  // both the persisted collections and the subset ensure have settled.
  await expect(quests.getByText("First adventure")).toBeVisible({ timeout: 15_000 })
  await expect(fetching, withErrors("subset ensure never settled")).toBeHidden({ timeout: 15_000 })
  await expect(loading, withErrors("collections never became ready")).toBeHidden({ timeout: 15_000 })

  const todoTitle = `Todo ${Date.now()}`
  await page.getByLabel("Todo title").fill(todoTitle)
  await page.getByRole("combobox", { name: "Project" }).click()
  await page.getByRole("option", { name: "First adventure" }).click()
  await page.getByRole("button", { name: "Add quest" }).click()
  await expect(page.getByText(todoTitle)).toBeVisible()

  await page.reload()
  await expect(quests.getByText("First adventure")).toBeVisible({ timeout: 15_000 })
  await expect(loading, withErrors("collections never became ready after reload")).toBeHidden({ timeout: 15_000 })
  await expect(page.getByText(todoTitle)).toBeVisible()

  expect(errors).toEqual([])
})
