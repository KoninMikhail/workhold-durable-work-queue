---
name: commit-messages
description: >-
  Git commit messages in Conventional Commits form for Workhold:
  type, scope, subject, body, optional footer. Use when user asks to commit,
  write commit message, commit, commit message, conventional commit.
---

# Commit Messages

## When to apply

The user asks to create a commit or write a commit message, or the agent prepares a commit after changes.

## Format

```
<type>(<scope>): <subject>

[optional body]

[optional footer]
```

### Type (required)

| type | When |
| --- | --- |
| `feat` | New functionality |
| `fix` | Bug fix |
| `refactor` | Refactoring with no behavior change |
| `test` | Tests only |
| `docs` | Documentation only |
| `chore` | Build, CI, deps, small chores |
| `perf` | Performance optimization |

### Scope (preferred)

A short identifier of the module or area: `billing`, `auth`, `reports`, `intake`, `uploader`.

If the scope is not obvious — drop the parentheses: `fix: correct …`.

### Subject

- Imperative mood, in **Russian** or **English** — as the repository does (see `git log` history)
- No trailing period
- Up to ~72 characters
- Does not repeat the type: ❌ `fix: fix login` → ✅ `fix(auth): fix redirect after logout`

### Body (if context is needed)

- What changed and why
- What was deliberately left untouched
- How to verify

### Footer

- `Refs: #…` — issue / tracker id (if any)
- `BREAKING CHANGE: …` — if it breaks a contract

## Workflow

1. `git status` and `git diff` — understand the scope of the changes
2. Split unrelated changes — **do not mix** feat + fix in one commit
3. Propose the message to the user
4. Commit — **only on an explicit user request** (see user rules on git)

## Examples

```
feat(billing): add payment status in the account area
```

```
fix(uploader): do not crash on a PDF without a text layer

Cause: NullReference in the parser for pages without text.
Check: upload sample-empty-layer.pdf.
```

```
refactor(reports): extract totals calculation into ReportTotalsService

Behavior unchanged. Covered by existing unit tests.
```

## Anti-patterns

- `wip`, `fix stuff`, `updates` with no meaning
- A commit with `--no-verify` without an explicit user request
- Secrets, tokens, `.env` in the commit
- A huge "everything at once" commit with no split
