# 022. Phase 3.1 physical contract baseline

**Status:** Accepted  
**Date:** 2026-09-18  
**Scope:** Phase 3.1 API/storage physical contracts; Phase 3.8 TTL consumption;
benchmark-bounded index/HASH follow-up

## Context

Phase 3.1 locks irreversible HTTP and PostgreSQL shapes before kernel implementation.
Without an accepted rationale, later phases could quietly redefine correctness
or treat performance hypotheses as permission to weaken storage
invariants. Exact catalogs live in machine artifacts; this ADR records
why these shapes were chosen and what can still be tuned.

## Decision

### Protocol (normative artifact: `openapi/queue.openapi.json`)

- HTTP/JSON OpenAPI 3.1 — machine source of truth.
- The application API is versioned under `/v1`; the private admin API is versioned under `/admin/v1`.
- Lease mutations are authorized by a public `claim_id` in the path and the secret header
  `X-Queue-Claim-Token` (`ClaimTokenHeader`).
- `GET /v1/capabilities` advertises the protocol and schema revision, TTL bounds, and
  admission limits.
- Failures use a shared `Error` envelope with stable codes and declared
  retryability.
- `CompleteRequest` reserves additive `events` and does not accept a delivery
  payload in this baseline.

### Storage (normative artifact: `docs/03-reference/02-storage-contract.md`)

- Mutable active rows (`tasks_active`, `task_payloads_active`, live registries)
  stay unpartitioned and physically separate from append-only / terminal history.
- Correctness registries stay unpartitioned; unique keys satisfy
  correctness now. Exact relation names: `admin_replay`,
  `partition_maintenance_status`, `completion_effects`.
- Four daily UTC `RANGE` parents:
  `admin_audit_log`, `task_attempts`, `tasks_terminal`, `delivery_events_terminal`.
- Initial partition premake horizon — 30 days ahead of the migration UTC day.
- Compact types follow ADR 007 (bigint identity ALWAYS, uuid public IDs,
  smallint codes, timestamptz, bytea fingerprints, bounded text/jsonb).
- Opaque payload and envelope jsonb live outside the hot lease row, with a DB hard ceiling
  of `1048576` bytes; the runtime default acceptance is `262144` bytes.
- Baseline indexes in the storage contract are accepted. They may only be added
  to or replaced through measured ADR/migration work — never silently redefined.

### Correctness registry TTL (consumed unchanged by Phase 3.8)

| Registry | Bounds (days / seconds) | Default |
| --- | --- | --- |
| enqueue dedup | 30..365 days / 2592000..31536000 s | 90 days / 7776000 s |
| terminal replay | 1..30 days / 86400..2592000 s | 7 days / 604800 s |
| admin replay | 7..90 days / 604800..7776000 s | 30 days / 2592000 s |

### Out of scope for this baseline

Delivery transport selection, CloudEvents validation, webhook adapter details and
Phase 5 Delivery Outbox runtime remain deferred **in this baseline**. Product
locks live in [ADR 018](018-http-first-delivery-relay.md) and
[ADR 021](021-cloudevents-envelope.md). This ADR does not claim any benchmark
was executed.

## Alternatives considered

| Option | Why not chosen |
| --- | --- |
| Single combined task table | Mixes hot lease updates with history and payload TOAST/WAL cost |
| Time-partitioned active state | Claim/idempotency span ages; rows move on lifecycle transitions |
| SDK-as-contract | Protocol must be language-neutral; SDK is a separate distribution |
| Claim secret in URL | Leaks through access logs, proxies and traces |
| Per-queue LIST partitions | Unbounded catalog growth; poor multi-queue claim scans |
| HASH registries in baseline | Deferred until measured evidence; unpartitioned unique keys suffice now |

## Consequences

**Positive:** Irreversible protocol and storage choices stay durable and are tied to machine
catalogs; performance tuning has explicit gates and no authority to weaken
correctness keys, hot/cold boundaries, the 1 MiB hard ceiling, or the
no-DEFAULT partition rules.

**Negative / trade-offs:** Index ordering and optional HASH registry partitions
require later measured ADR and migration work; the runtime payload default may be
lowered by deployment config, or raised only up to 1 MiB after capacity evidence.

**Follow-up:** Execute hypotheses in
[12-physical-contract-benchmarks.md](../12-physical-contract-benchmarks.md); Phase 3.8
consumes the TTL bounds above without choosing new ranges.

## Requirement mapping

| ID | Evidence in this baseline |
| --- | --- |
| STOR-01 | Active/payload/registry tables vs append-only terminal history |
| STOR-02 | Four daily UTC RANGE parents + 30-day premake |
| STOR-06 | Compact types via storage contract / ADR 007 |
| STOR-07 | Payload separation + 1048576 hard ceiling / 262144 runtime default |
| API-06 | Capabilities resource + versioning under `/v1` and `/admin/v1` |
| API-07 | OpenAPI 3.1 machine artifact + `X-Queue-Claim-Token` / Error envelope |
| QUAL-01 | Stdlib contract tests locking ADR/benchmarks/OpenAPI/storage links |

## References

- [openapi/queue.openapi.json](../../../openapi/queue.openapi.json)
- [storage-contract.md](../../03-reference/02-storage-contract.md)
- [04-storage-topology.md](../04-storage-topology.md)
- [09-client-protocol.md](../09-client-protocol.md)
- [06-admission-control.md](../06-admission-control.md)
- [ADR 006](006-hot-cold-partitioning.md), [ADR 007](007-postgresql-type-policy.md),
  [ADR 016](016-http-openapi-claim-security.md),
  [ADR 017](017-correctness-registry-retention.md),
  [ADR 019](019-initial-production-gate.md)
- [12-physical-contract-benchmarks.md](../12-physical-contract-benchmarks.md)
- [observability.md](../../05-operations/03-observability.md)
