# 014. Layered admission control — protection layers

**Status:** Accepted  
**Date:** 2026-09-18  
**Scope:** PostgreSQL protection and overload behavior

## Context

Unbounded payload, complete fan-out, or active depth can exhaust PostgreSQL
before workers recover. If every limit is runtime-mutable, an admin can
accidentally exceed the tested storage envelope.

## Decision

Deployment hard ceilings limit bytes, fan-out, lease, and maximum active
depth. Versioned queue policy can only tighten allowed soft quotas.
Validation happens before write transactions; depth uses transactional
counters. Claims continue under backlog pressure while new enqueue is
throttled.

## Alternatives considered

| Option | Why not chosen |
| --- | --- |
| No limits until database failure | Uncontrolled WAL, disk, and pool exhaustion |
| All limits runtime mutable | Can exceed the tested safety envelope without a deploy review |
| Throttle claims when depth is high | Prevents the system from draining the backlog |
| Count hot rows for each enqueue | Expensive contention and scans on the correctness path |

## Consequences

**Positive:** Predictable rejection, bounded transaction cost, and safer
overload recovery.

**Negative / trade-offs:** Operators must size the ceilings; clients must
handle resource-exhausted responses.

**Follow-up:** Measured payload and request ceilings — [ADR 023](023-qualified-kernel-storage-profile.md).
Phase 4 adds adaptive rate and database-pressure controls.

## References

- [Admission control](../06-admission-control.md)
- [Storage topology](../04-storage-topology.md)
