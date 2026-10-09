# 005. Fenced leases for multi-replica processing

**Status:** Accepted  
**Date:** 2026-09-18  
**Scope:** Task claims, retries and delivery relay concurrency

## Context

Many worker replicas can process one named queue. A timeout/reaper keyed only
by task ID lets an old worker complete after another replica reclaims the
task. Process-local locks and diagnostic worker IDs do not prevent stale
writes.

## Decision

Each task claim uses a new opaque token, a monotonic attempt generation, and a
workhold store expiry. Heartbeat and terminal commands validate the
current token, generation, and unexpired lease. Each claim also records
`claimed_at` from the workhold store and a diagnostic `worker_id`; neither
replaces the fence. Delivery-relay claims use a separate equivalent fencing
domain.

## Alternatives considered

| Option | Why not chosen |
| --- | --- |
| Task status + processing timestamp only | A stale worker can mutate state after reclaim |
| Stable worker ID as ownership | Identity is diagnostic, not a safe rotating capability |
| Hold a DB lock for all processing | Long transactions exhaust connections and block recovery |

## Consequences

**Positive:** Stale replicas do not mutate workhold state; API replicas
stay stateless and non-sticky.

**Negative / trade-offs:** External side effects are not fenced automatically
and still require application idempotency. Clients must handle lost-lease
conflicts.

**Follow-up:** Conformance tests cover reclaim, stale heartbeat, stale
complete, and an uncertain complete response.

## References

- [Concurrency](../02-concurrency.md)
- [Guarantees](../../01-concepts/09-guarantees.md)
