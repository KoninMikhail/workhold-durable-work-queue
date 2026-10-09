# Template: repo-level ADR

Path: `docs/04-architecture/adr/<nnn>-<slug>.md` (legacy: `docs/adr/`; create the folder and `README.md` if they are missing).

```markdown
# <nnn>. <short decision title>

**Status:** Accepted  
**Date:** YYYY-MM-DD  
**Scope:** <modules / team / infra — what it affects>

## Context

2–4 sentences: the problem, constraints, what happens if it is not solved.

## Decision

What was accepted — concrete and unambiguous (a list is fine).

## Alternatives considered

| Option | Why it was not chosen |
| --- | --- |
| … | … |

## Consequences

**Positive:** …

**Negative / trade-offs:** …

**Follow-up:** optional — tickets, migrations, monitoring.

## References

- Issue / PR, other ADRs, module `architecture.md`.
```

## `docs/adr/README.md` (table of contents)

```markdown
# Architecture Decision Records

| ADR | Title | Status |
| --- | --- | --- |
| [001-slug](./001-slug.md) | Short title | Accepted |
```

When **Superseded** — the Status column, and a link in the old file to the new one.
