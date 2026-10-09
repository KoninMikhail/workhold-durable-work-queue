# 008. Retry policy belongs to the named queue

**Status:** Accepted  
**Date:** 2026-09-18  
**Scope:** Task scheduling, failure and named-queue configuration

## Context

Retry behavior in an opaque payload cannot be validated or operated through
queue-service. A full scheduler in the first release would add extra states
and algorithms, and omitting scheduling fields would later require a breaking
schema and API change.

## Decision

Each task stores queue-service-visible `priority` and `available_at`. The
initial release supports only `priority=0` and immediate enqueue. Processing
failures record a stable `failure_code`; bounded retry timing and attempt
limits come from named-queue configuration. Retry policy is versioned and
configures `enabled`, `max_attempts`, backoff strategy, and delay. The initial
backoff is fixed-delay.

A task snapshots the active policy version at enqueue. Policy updates affect
new tasks. When retry is disabled, the first worker failure or lease expiry
records the attempt and moves the task to dead-lettered.

## Alternatives considered

| Option | Why not chosen |
| --- | --- |
| Retry settings inside payload | queue-service cannot safely enforce, index, or observe them |
| Per-task arbitrary retry policy | Complicates idempotency and allows unbounded operational variance |
| Omit fields until scheduling phase | Forces table and API migrations for an expected extension |
| Implement priority and cron immediately | Deep overengineering before workload evidence |

## Consequences

**Positive:** The initial implementation stays simple, and the schema and
contracts can evolve toward priority and delayed scheduling without moving
fields out of the payload.

**Negative / trade-offs:** Policy versions must be retained while tasks
reference them. Operators need an explicit future operation to migrate the
existing backlog.

**Follow-up:** Phase 4 can add exponential backoff and an explicit
administrative operation to migrate backlog policy. Per-task arbitrary
overrides stay out of scope.

## References

- [State machines](../01-state-machine.md)
- [Storage topology](../04-storage-topology.md)
