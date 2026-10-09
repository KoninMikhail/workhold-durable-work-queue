# 015. One image with explicit process roles

**Status:** Accepted  
**Date:** 2026-09-18  
**Scope:** Packaging and deployment

## Context

queue-service needs API, migration, partition maintenance, and later a delivery relay role.
Separate images multiply release coordination; one process that runs every role
couples scaling, readiness, and failure domains.

## Decision

One versioned runtime image is published with explicit commands: `api`, `migrate`,
`maintain`, and `relay`. Phase 3 uses API, migrate, and maintain; relay is enabled
in Phase 5. The API opens the application and private-admin listeners separately.

## Alternatives considered

| Option | Why not chosen |
| --- | --- |
| Separate image per role | More artifacts and compatibility coordination |
| One monolithic process | Roles cannot be scaled, failed, or drained independently |
| Migrate in every API startup | Replica races and a connection stampede |

## Consequences

**Positive:** One release unit with independently deployable and scalable roles.

**Negative / trade-offs:** The image contains code that some roles do not use;
the entrypoint and role-specific health and security must be explicit.

**Follow-up:** Deployment manifests run a one-shot migrate,
scheduled or single-winner maintain, and N API replicas. Image secret injection is
[ADR 025](025-container-file-secrets.md).

## References

- [Deployment](../../05-operations/02-deployment.md)
- [ADR 025](025-container-file-secrets.md)
