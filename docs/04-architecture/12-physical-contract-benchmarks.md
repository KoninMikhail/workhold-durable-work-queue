# Physical contract benchmark hypotheses

**Status:** Accepted experiment specification (not measured results)  
**Date:** 2026-09-18  
**Tied to:** [ADR 022](adr/022-physical-contract-baseline.md),
[ADR 019](adr/019-initial-production-gate.md),
[04-storage-topology.md](04-storage-topology.md)

The document defines executable experiment specifications for Phase 3 capacity and DDL tuning.
It does **not** claim measurements were run. Benchmark evidence
may affect only an additive index/HASH ADR/migration; it cannot
override the correctness invariants below.

## Non-tunable invariants

They stay fixed regardless of benchmark outcomes:

| Invariant | Binding source |
| --- | --- |
| Correctness registry unique keys and TTL bounds/defaults | `storage-contract.md`, ADR 017/022 |
| Hot/cold boundary (active state unpartitioned; four daily UTC RANGE parents) | ADR 006, storage contract |
| Database/deployment hard payload ceiling `1048576` bytes (1 MiB) | storage contract, admission-control |
| No `DEFAULT` partition on daily RANGE parents | storage contract |
| Claim secret stays in header `X-Queue-Claim-Token`, never URL | OpenAPI, ADR 016/022 |

**Runtime payload default** (`262144` bytes) may be lowered by deployment
configuration or raised only up to the immutable hard ceiling of 1 MiB after capacity
evidence. Correctness keys, hot/cold boundaries, the 1 MiB ceiling, and no-DEFAULT
partitions are not tunable.

## Decision gates ADR 019

On the documented reference PostgreSQL environment:

| Gate | Threshold |
| --- | --- |
| Claim throughput | 500 claims/s |
| Valid-operation success | 99.9% |
| p99 enqueue / claim / heartbeat | ≤ 100 ms |
| p99 baseline complete (no/max-normal fan-out) | ≤ 200 ms |
| Maximum fan-out complete | measured and reported separately |

## Workload measurements

Each hypothesis run must state and vary:

| Dimension | Required values |
| --- | --- |
| Named queues | 1, 10, 100 |
| Daily volume envelope | up to 1 000 000 tasks/day |
| Concurrent claimers | 32 |
| Claim path mixes | ready-heavy, empty-heavy, reclaim-heavy, heartbeat-heavy |
| Idempotency storms | duplicate enqueue storms; duplicate complete/replay storms |
| Payload sizes | 1 KiB through runtime default 256 KiB (`262144`); boundary probes at immutable 1 MiB (`1048576`) |

## Explicit hypotheses

### H1 — Active-claim index order

**Question:** Does the accepted baseline
`tasks_active_claim_idx (queue_id, state_code, available_at, priority DESC, id)`
meet the ADR 019 gates on the workload matrix, or does a measured alternative column order improve
p99 claim/reclaim without weakening uniqueness?

**Tunable:** additive alternate index candidates only through an ADR + migration.  
**Non-tunable:** uniqueness and hot/cold placement of `tasks_active`.

### H2 — Correctness registry HASH partitions (0 vs fixed)

**Question:** Do unpartitioned registries (HASH partition count = 0)
stay within the WAL/autovacuum and latency gates on the 1M tasks/day envelope, or does a fixed
HASH partition count on the full unique key improve purge/lookup without breaking
global uniqueness?

**Tunable:** a fixed HASH count only after evidence + an ADR/migration.  
**Non-tunable:** unique key columns and TTL CHECK bounds.

### H3 — Runtime payload default inside the hard ceiling

**Question:** For payloads from 1 KiB through `262144`, and boundary probes up to `1048576`,
which deployment default holds the ADR 019 latency/success gates while respecting the immutable
DB CHECK ceiling?

**Tunable:** the runtime/deployment default in `1 .. 1048576`.  
**Non-tunable:** the hard ceiling `1048576`; no payload GIN on the hot path.

## Reproducibility fields (required on a run)

| Field | Purpose |
| --- | --- |
| `reference_environment` | PostgreSQL version, CPU, RAM, disk class, shared_buffers, max_connections, OS |
| `dataset_generation_seed` | Versioned integer/string seed for synthetic workloads |
| `named_queue_count` | 1 / 10 / 100 |
| `claimer_count` | Concurrent claimers (target 32) |
| `workload_mix` | ready / empty / reclaim / heartbeat / storm labels |
| `payload_bytes` | Exact size under test |
| `warmup_rules` | Duration or operation count discarded before sampling |
| `sample_rules` | Sample window, operation count, aggregation method |
| `sql_query_plan_capture` | `EXPLAIN (ANALYZE, BUFFERS)` or equivalent for claim/enqueue/complete paths |
| `schema_revision` | Storage contract / migration revision under test |
| `hypothesis_id` | H1 / H2 / H3 (or later ADR-linked IDs) |

## Required metrics

Record on every run:

- latency p50 and p99 for enqueue, claim, heartbeat, and complete;
- valid-operation success ratio;
- planner/planning time;
- touched partitions (history parents/children);
- WAL volume;
- buffer hits;
- dead tuples;
- autovacuum lag.

Raw metrics and seeds are stored with the ADR/migration that consumes them
(threat mitigate T-03.1-22).

## Deferred / excluded

- Delivery transport broker choice
- CloudEvents validation and HTTP webhook adapter configuration
- Fabricated or claimed performance results without a recorded run

This remains Phase 5 / later evidence work and is outside this physical-contract
baseline.
