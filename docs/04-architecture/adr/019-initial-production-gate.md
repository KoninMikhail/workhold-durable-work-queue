# 019. Initial production performance gate

**Status:** Accepted  
**Date:** 2026-09-18  
**Scope:** Phase 3 release qualification

## Context

"Fast" and "production-ready" are not testable without a reference load. Permanent SLOs
cannot be invented before implementation, but the first release still needs a concrete
acceptance gate.

## Decision

On the documented reference PostgreSQL **18.6.x** environment, at 500 claims/second,
valid operations reach 99.9% success; p99 enqueue, claim, and heartbeat are at most
100 ms; baseline complete with no or max-normal fan-out is at most 200 ms.
Maximum fan-out is measured and reported separately.

## Alternatives considered

| Option | Why not chosen |
| --- | --- |
| No numeric gate | Cannot detect regressions or qualify the claimed envelope |
| p99 claim 50 ms / 99.95% | Prematurely strict before hardware and reference data exist |
| p99 250–500 ms / 99.5% | Too weak for a local per-application queue service |

## Consequences

**Positive:** A measurable release threshold and comparable benchmark evidence.

**Negative / trade-offs:** A versioned reference environment and workload are required; this
is not a universal customer SLA.

**Follow-up:** Phase 7 re-ran this gate on PostgreSQL 18.6 without changing
thresholds (`benchmarks/results/phase-7-postgresql-18.6/`). Production SLOs
are derived after soak and load evidence.

## References

- [ADR 024](024-postgresql-18-6-runtime-engine.md) — engine of record
- [Observability](../../05-operations/03-observability.md)
- [Storage topology benchmarks](../04-storage-topology.md)
