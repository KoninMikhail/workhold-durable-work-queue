---
name: fix-bug
description: >-
  Fixing a bug: reproduce, localize module, minimal fix, project checks.
  Architecture-agnostic — conventions from project rules/skills. Use when fix bug, fix it,
  does not work, regression, reproduction steps, root cause.
---

# Fix Bug

Universal bug-fix workflow. **Not tied to an architecture** — conventions are read from project rules and project skills **before** edits.

## When to apply

- The user asks to fix a bug, a regression, "it does not work".
- The task has reproduction steps.
- A developer picks up a bug.

## Workflow

```
- [ ] 1. Read project conventions (see below)
- [ ] 2. Reproduce: steps from the task / the user
- [ ] 3. Localize the code area (module, component, package)
- [ ] 4. Root cause — record it briefly
- [ ] 5. Minimal fix — only what removes the cause; no scope creep
- [ ] 6. Project checks (names from package.json / Makefile / docs)
- [ ] 7. Optional tests — skill `write-tests`, if the logic is non-trivial
- [ ] 8. Commit — skill `commit-messages` (type: fix); only when the user asks
```

## Step 1: project conventions (required)

Read **before** edits, in the order they exist:

| Source | What it gives |
| --- | --- |
| `AGENTS.md` | entry, commands, stack |
| `docs/_ai/` or `memory-bank/` | code map, architecture |
| `.cursor/rules/` | coding standards, auto-commit |
| `.cursor/skills/` | project skills (scaffolding, architecture) |

Architectural constraints come **only** from the project. Do not invent layers and patterns if the project does not describe them.

## Step 6: project checks

Find scripts in `package.json`, `Makefile`, `pyproject.toml`, CI config:

| Typical name | Action |
| --- | --- |
| `typecheck`, `tsc`, `mypy` | static type check |
| `lint`, `oxlint`, `eslint`, `ruff` | linter |
| `test`, `vitest`, `pytest`, `jest` | tests (relevant scope) |
| `build` | build — if the fix touches config/types |

Run what the repository accepts. Do not invent commands.

## After the fix

Record briefly for yourself / the PR:

```markdown
## Fix

**Symptom:** …
**Root cause:** …
**What changed:** …
**How to verify:**
1. …
2. …
```

## Anti-patterns

- Edits without reproduction and without understanding the root cause.
- A large refactor "while we're here" — move it to a separate task.
- Hardcoded architectural checklists in this skill — only a router to the project layer.
- Commit/push without an explicit user request.

## Related skills

| Skill | When |
| --- | --- |
| `write-tests` | Non-trivial logic, the regression recurred |
| `commit-messages` | Commit message |
