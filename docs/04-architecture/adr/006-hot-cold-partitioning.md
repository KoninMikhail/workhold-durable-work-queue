# 006. Partition cold history, not active queue state

**Status:** Accepted  
**Date:** 2026-09-18  
**Scope:** PostgreSQL storage, retention and partition maintenance

## Context

workhold can produce about a million tasks per day and retain data for
30–90 days. Time-partitioning mutable task rows would force claim to scan old
partitions, complicate global uniqueness, and move rows on state change. A
bulk DELETE of the entire history would add extra vacuum pressure and bloat.

## Decision

Active tasks and pending delivery events stay outside time partitions.
Attempts, terminal tasks, and terminal delivery events use daily UTC RANGE
partitions on immutable workhold store timestamps. workhold-owned
maintenance creates partitions ahead of time and detaches or drops expired
ones. Correctness registries stay unpartitioned or use fixed HASH partitioning
on the full unique key.

## Alternatives considered

| Option | Why not chosen |
| --- | --- |
| RANGE partition all tasks by creation time | Claim crosses ages; global idempotency cannot be enforced naturally |
| Partition by task status | Every lifecycle transition moves rows |
| LIST partition per named queue | Unbounded catalog growth and poor multi-queue claims |
| Mandatory pg_partman/TimescaleDB | Breaks portability of a vanilla PostgreSQL deployment |

## Consequences

**Positive:** Small hot indexes, predictable retention, and no mandatory
extension.

**Negative / trade-offs:** Terminal movement and maintenance are explicit;
global dedup and replay need narrow registries; history foreign keys cannot
block partition detach.

**Follow-up:** Indexes / HASH modulus — [ADR 023](023-qualified-kernel-storage-profile.md).
Phase 7 re-validated on PostgreSQL 18.6: `UNIQUE` without the partition key on
a RANGE parent is still `0A000`; registries stay unpartitioned. Retention
operations are Phase 4.

## References

- [Storage topology](../04-storage-topology.md)
- [ADR 024](024-postgresql-18-6-runtime-engine.md) — engine of record
- [PostgreSQL 18 partitioning](https://www.postgresql.org/docs/18/ddl-partitioning.html) — PostgreSQL 18.6 still cannot put a global UNIQUE on a RANGE parent without the partition key
