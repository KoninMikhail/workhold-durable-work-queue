# 009. Persisted runtime control plane — operations

**Status:** Accepted  
**Date:** 2026-09-18  
**Scope:** Named queues, runtime configuration and operations API

## Context

Production queues need policy changes, pause/resume, drain, and recovery tools
without redeploying every API replica or hand-editing tables. A generic
key/value settings blob would weaken validation, compatibility, and auditing.

## Decision

Named queues persist live state and point at immutable policy versions. The
private admin API changes state or activates a policy through optimistic
`config_version`. Every change is authenticated and audited. Producer and
worker credentials cannot use admin operations.

Runtime policy and deployment configuration are separate. DDL, partition
layout, database connectivity, and hard security limits cannot be changed
through this API.

## Alternatives considered

| Option | Why not chosen |
| --- | --- |
| Environment variables only | Requires a restart and cannot vary by named queue |
| Generic `settings(key, value)` | Weak typing, validation, compatibility, and auditability |
| Direct operator SQL | Bypasses invariants, authorization, and audit |
| Put admin operations in public task API | Widens the blast radius of producer and worker credentials |

## Consequences

**Positive:** Consistent runtime changes across replicas, policy history, and
safe operator tooling.

**Negative / trade-offs:** Admin RBAC, audit retention, config-cache
invalidation, and conflict handling are required.

**Follow-up:** Phase 3 implements read-config, activate-policy, and
pause/resume. Phase 4 adds drain, dead-letter replay, and
partition-maintenance operations.

## References

- [Storage topology](../04-storage-topology.md)
- [Retry policy ADR](008-queue-retry-policy.md)
