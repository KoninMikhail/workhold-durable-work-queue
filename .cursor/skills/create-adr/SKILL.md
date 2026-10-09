---
name: create-adr
description: >-
  Creating a repo-level ADR in docs/04-architecture/adr/. Classic format, one file — one decision.
  Use when create ADR, record the decision, architecture decision, why we did it this way.
---

# Create ADR

Recording a repository-level **architectural** decision: what was chosen, what was rejected, consequences.

Shared rules: [core-rules.md](./core-rules.md).
Full policy — project rule **`adr.mdc`**.
File template: [template-repo.md](./template-repo.md).

## When an ADR

| ADR needed | ADR not needed |
| --- | --- |
| 2+ options with trade-offs | project convention |
| auth, deploy, migrations, event bus | how-to in guides |
| cross-module contract | flow description in `architecture.md` |

## Workflow

```
- [ ] Gate: is an ADR needed? — core-rules.md (if trivial → STOP)
- [ ] Read docs/04-architecture/ and existing ADRs — do not duplicate
- [ ] Next number: max in docs/04-architecture/adr/ + 1
- [ ] ADR draft in chat (template-repo.md)
- [ ] Self-check — core-rules.md
- [ ] Write docs/04-architecture/adr/<nnn>-<slug>.md
- [ ] Row in docs/04-architecture/adr/README.md
- [ ] Optional link from docs/04-architecture/*.md
```

## Intake (if unclear)

1. **Which problem/constraint** was being solved?
2. **What was accepted** — pattern, stack, API shape, deploy?
3. **What was rejected** — at least one alternative?
4. **Trade-offs** — what got harder / which debt was accepted?

## Naming

- `docs/04-architecture/adr/001-<slug>.md` — three-digit number
- Legacy: `docs/adr/` — do not create in new projects
- Body up to **~40 lines** — see [template-repo.md](./template-repo.md)

## Links after create

| From | To |
| --- | --- |
| `docs/04-architecture/*.md` | inline link to the ADR |
| `docs/04-architecture/adr/README.md` | ADR table |
| Supersede an old ADR | in the old one: `> **Superseded by** […](…)` |

## Anti-patterns

- An ADR instead of architecture.md (data flow, invariants)
- One file with unrelated decisions
- Deleting an outdated ADR — supersede only
- An ADR in `docs/02-guides/` instead of `04-architecture/adr/`

## Related rules

- rule `adr.mdc` — when an ADR is required
- rule `docs-structure.mdc` — `docs/` tree
