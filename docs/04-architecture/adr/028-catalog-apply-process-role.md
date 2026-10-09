# 028. Fifth process role `apply` for catalog ensure-exists

**Status:** Accepted  
**Date:** 2026-09-21  
**Scope:** Process roles / packaging (CTRL-10); extends [ADR 015](015-single-image-multi-role.md) without rewriting it

## Context

Operators need a one-shot ensure-exists of named queues from a mounted JSON catalog,
without the HTTP admin API and without starting apply from `api`. ADR 015 fixes four CLI roles;
rewriting it for a fifth command is not allowed, so a separate ADR is required. The apply pool
must not become a third unbounded pool beside migrate and maintain.

## Decision

The fifth image command is `queue apply` (admin stays a plane of `api`, not a CLI role).
The catalog path is the absolute `QUEUE_CATALOG_PATH` (an environment variable of the apply role, not a
`DeploymentSettings` field). HTTP listeners are not started; the `api` compose service does not
`depends_on` apply. Advisory lock `QUEUAPLY` = `0x5155455541504C59`, separate
from migrate and maintain. The pool copies the migrate ceilings: replica 1, pool 2,
acquire 5s, statement 30s (`QUEUE_APPLY_*`). DEP-03 counts six
`PROCESS_ROLES` (including admin).

## Alternatives considered

| Option | Why not chosen |
| --- | --- |
| Rewrite ADR 015 | Breaks the history of an accepted decision; an extension ADR is required |
| Apply on `api` startup | Mixes one-shot work with a long-running process; violates D-12 |
| HTTP localhost admin | Bypasses the fail-closed file path; an extra trust boundary |
| YAML instead of JSON | Duplicates the contract; JSON is already in OpenAPI and the catalog |

## Consequences

**Positive:** An explicit fifth role; pool budgets stay fail-closed; the catalog stays outside the image.

**Negative / trade-offs:** Six roles raise committed connections; the operator
must mount the catalog and set `QUEUE_CATALOG_PATH`.

**Follow-up:** Plan 13-04 covers the advisory lock and the ensure-exists loop; the stub currently
validates the path, catalog, and settings, then exits `apply_failed`.

## References

- [ADR 015](015-single-image-multi-role.md)
- [ADR 026](026-optional-glitchtip-sentry-dsn.md) — opt-in Sentry after `--help`
- Phase 13 CONTEXT D-11 / D-12 / D-13 / CTRL-07 / CTRL-10
