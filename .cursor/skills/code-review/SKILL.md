---
name: code-review
description: >-
  Code review: task/AC, diff, project conventions, blockers vs suggestions format.
  Architecture-agnostic — checklist from project rules/skills. Use when code review,
  review the PR, check the PR, ready to merge, review diff.
---

# Code Review

Universal code review for the agent. Architectural blockers come **only** from project rules/skills; they are not hardcoded here.

## When to apply

- The user asks for a review of a PR, diff, branch, or set of changes.
- Before merge — "ready to merge?".
- Self-review before `create-pr`.

## Read order

```
1. Task / AC — what was supposed to change
2. git diff (or PR files) — what actually changed
3. Project conventions — AGENTS.md, docs/_ai/, .cursor/rules/, .cursor/skills/
4. Module doc — docs/README.md of the affected area (if any)
5. Tests — whether they were added for non-trivial logic
```

## Universal blockers (always)

| Blocker | Why |
| --- | --- |
| Secrets, tokens, `.env` in the diff | Security |
| Scope creep — unrelated changes | PR atomicity |
| Breaking public API without docs/migration | Consumers will break |
| Non-trivial logic without tests | Regressions (if the project expects tests) |
| Disabling the linter/hooks without justification | Quality |
| `console.log` / debug code in a production path | Noise, leaks |

## Architectural blockers (router)

Check **only** against the project layer:

| Project source | Example checks |
| --- | --- |
| `.cursor/rules/architecture/*` | layers, dependencies, module boundaries |
| `.cursor/skills/*` (architecture, scaffolding) | public API, naming, patterns |
| `docs/_ai/architecture.md` | team-specific decisions |

If there are no project rules — stay with the universal blockers plus common sense (readability, duplication, error handling).

## Response format (required)

```markdown
## Summary
1–3 sentences: what the change does and whether it matches the task.

## Blockers
- [ ] … (or "none")

## Suggestions
- … (do not block merge)

## Test gaps
- … (what QA should check / what test to add)
```

Blockers — only what **must be fixed before merge**. Everything else — Suggestions.

## Reviewer self-check

- [ ] Compared the diff with the task AC — nothing extra and nothing missing?
- [ ] Read project conventions, not only the diff?
- [ ] Separated blockers from nits/suggestions?
- [ ] Did not demand specific patterns the project does not use?

## Anti-patterns

- A review of style only, with no link to the task.
- Nitpicking as blockers (names, small style with no team rule).
- Separate skills `code-review-fe` / `code-review-be` — one skill + a router.
- Demanding architectural terminology that is not in the project rules.

## Related skills

| Skill | When |
| --- | --- |
| `write-tests` | Close test gaps |
| `create-pr` | After a clean review |
| `commit-messages` | If commits need to be split |
| project skills | Architectural checklist |
