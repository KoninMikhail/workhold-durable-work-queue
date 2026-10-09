# 002. Named queues inside a per-application instance

**Status:** Accepted  
**Date:** 2026-09-18  
**Scope:** Routing, isolation and worker subscriptions

## Context

One application can have different task types and worker pools. Routing inside
an opaque payload would let an incapable worker claim another worker's task. A
separate full workhold instance for each task type would multiply
databases and operations.

## Decision

One per-application workhold instance supports several named queues.
Producers target a name; workers claim only the queues they can process. Queue
identity and routing metadata are explicit and indexed; the business payload
stays opaque. Producer idempotency is scoped by authenticated producer
identity, named queue, and key.

## Alternatives considered

| Option | Why not chosen |
| --- | --- |
| One logical queue per instance | Cannot route heterogeneous work without inspecting the payload |
| One instance per task type | Excessive multiplication of operations and PostgreSQL |
| Route by a payload key | Couples workhold to the application schema and makes safe indexing unstable |

## Consequences

**Positive:** Worker capability matching, per-queue statistics, and future
policy are achievable without interpreting the payload.

**Negative / trade-offs:** Fairness, quotas, authorization, and indexes require
queue scope. A noisy named queue still shares the instance database.

**Follow-up:** Define naming, creation, and authorization before freezing the
physical API.

## References

- [Product boundary](../../01-concepts/07-product-boundary.md)
- [Data flow](../03-data-flow.md)
