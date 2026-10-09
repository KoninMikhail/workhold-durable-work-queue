---
name: write-tests
description: >-
  Writing tests: unit, integration, e2e, smoke — stack and paths from project.
  Architecture-agnostic. Use when add tests, cover with tests, unit test, integration test,
  API mocks, negative cases.
---

# Write Tests

Universal workflow for writing tests. Stack (vitest, jest, pytest, …), file layout, and mock layer come **from the project**, not from this skill.

## When to apply

- The user asks to add or extend tests.
- After `fix-bug` — lock in the regression.
- Task AC require verifiable scenarios.

## When NOT to write tests

- Trivial change (typo, comment, purely visual CSS with no logic).
- Pure scaffold with no behavior.
- The user explicitly asks for no tests.
- The project has no test runner — ask the user first.

## Workflow

```
- [ ] 1. Read project conventions (test runner, folders, naming)
- [ ] 2. Find existing tests next to the module — repeat the pattern
- [ ] 3. Choose scope (see the table below)
- [ ] 4. Happy path + negative cases (from AC / edge cases)
- [ ] 5. Run the relevant project test commands
- [ ] 6. Optional: update `docs/` of the affected module — if the project has a doc structure
```

## Step 1: project conventions

| Source | What to look for |
| --- | --- |
| `package.json` / `pyproject.toml` | `test`, `vitest`, `jest`, `pytest` scripts |
| Existing `*.test.*`, `*.spec.*`, `__tests__/` | naming, location, imports |
| `docs/_ai/`, `AGENTS.md` | test utilities, shared mocks |
| CI config | which suites are required |

**Rule:** a new test should look like it was written by the same author as the neighboring tests in the repo.

## Scope

| Level | When | Examples |
| --- | --- | --- |
| **Unit** | Pure function, mapper, validator, pure logic | schemas, formatters, guards |
| **Integration** | Several modules, HTTP mock, state | API client + handler, store + effect |
| **E2E** | Critical user flow end-to-end | login, checkout — if the project has e2e |
| **Smoke** | Minimal check after a large change | "module imports", UI snapshot |

Do not duplicate e2e where unit is enough. Do not test implementation details that have no value.

## Negative cases

Take them from:

- Task AC (what must **not** happen).
- Validation boundaries (empty input, wrong format).
- API errors (4xx, 5xx, timeout) — if the project mocks HTTP.
- Access rights / unauthorized — if in the task scope.

## Mock layer

Determine from the project:

- Where fixtures live (JSON, factories).
- HTTP mocks (MSW, nock, responses, wiremock).
- Test helpers (`renderWithProviders`, `createTestStore`, …).

Do not introduce a new mock framework if the project already uses another one.

## Anti-patterns

- Tests that duplicate typing with no behavioral value.
- A snapshot of the whole DOM "just in case".
- Binding this skill's text to a specific test framework — examples only; the stack comes from the project.
- Separate skills per stack — one skill, the project supplies the details.

## Related skills

| Skill | When |
| --- | --- |
| `fix-bug` | Regression test after a fix |
| `code-review` | Check test gaps in a PR |
