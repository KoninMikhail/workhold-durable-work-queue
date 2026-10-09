# 017. Correctness registry retention defaults

**Status:** Accepted  
**Date:** 2026-09-18  
**Scope:** Enqueue, complete and admin replay idempotency

## Context

Correctness registries cannot grow forever, but expiry that is too early turns a
legitimate uncertain retry into a different operation. Their retry windows differ from
task and event history retention.

## Decision

Default TTLs: producer enqueue dedup 90 days, worker terminal-command replay
7 days, admin replay idempotency 30 days. TTLs are deployment-configurable within
validated bounds and are purged incrementally by the maintainer.

## Alternatives considered

| Option | Why not chosen |
| --- | --- |
| All registries 90 days | Extra growth of the complete and admin registries |
| Very short 24h/7d windows | Too weak for producer and operational retries |
| Infinite retention | Unbounded cost of the correctness index and storage |

## Consequences

**Positive:** Explicit bounded retry guarantees with a predictable registry size.

**Negative / trade-offs:** After the TTL, an old key can mean a new operation or
return not-found under the endpoint contract.

**Follow-up:** OpenAPI documents expiry behavior; metrics show purge
lag and dedup and replay hits.

## References

- [Storage topology](../04-storage-topology.md)
- [Guarantees](../../01-concepts/09-guarantees.md)
