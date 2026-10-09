# Admission control and backpressure

**Status:** Accepted architecture; numeric defaults locked by Phase 3.9 QUAL-03.

queue-service rejects work before opening a write transaction when that is possible. Hard
deployment ceilings protect PostgreSQL; softer per-queue quotas may
be versioned runtime policy.

## Hard Phase 3 baseline (QUAL-03 accepted)

| Limit | Accepted value |
| --- | --- |
| One task/event payload | 256 KiB default; deployment maximum **1 048 576 bytes (1 MiB)** |
| Whole request body | **1 048 576 bytes (1 MiB)** |
| Failure detail | 4 KiB |
| Idempotency key | 256 characters |
| Queue name / worker ID | 128 characters |
| Combined `spawn[] + events[]` | 64 items and 512 KiB |
| Requested lease | maximum 3600 seconds |
| Active tasks per queue | 100,000 |
| Active tasks per instance | 500,000 |
| Claim batch | one task, array-shaped contract |

The 1 MiB request/payload ceiling is the measured Phase 3.9 choice (candidate bundle
under `benchmarks/results/phase-3.9-candidates/`, evidence mode `synthetic`;
ADR 023). Operators may lower limits. Raising them above the accepted values requires
a new capacity measurement and an ADR. Large documents and results are stored externally and
referenced from the payload.

Depth means delayed + ready + leased rows, not retained terminal history.
Idempotent replay does not consume depth.

## Admission outcomes

- malformed/unsupported value: validation error;
- bytes or fan-out above the hard maximum: non-retryable size error;
- depth/rate quota: retryable resource-exhausted response with a retry hint;
- queue draining: reject a new external enqueue;
- queue paused: accept enqueue, but return empty claims;
- database/pool unavailable: retryable service-unavailable response;
- existing idempotency key: return the original task before depth/state evaluation.

Claims are not throttled solely because the backlog is high: consuming the backlog is the
recovery mechanism. Heartbeat may enforce a minimum interval against storms.

## Adaptive Phase 4 controls

Add only after metrics exist:

- a token-bucket enqueue rate per queue and per instance;
- an optional maximum of leased tasks per queue;
- WAL, disk, and autovacuum pressure modes;
- replay-rate and batch limits;
- overload hysteresis.

Overload progression: warning → enqueue throttle → readiness failure. queue-service
never acknowledges locally when PostgreSQL cannot commit.

## Implementation constraints

- Depth admission uses transactional counters, never `COUNT(*)` on the hot
  table.
- Counters are reconciled asynchronously and are not the authority for task state.
- DB pool acquisition and statements have bounded timeouts.
- The combined API, admin, relay, maintainer, and migrator pools fit within the
  documented PostgreSQL connection budget.
- Rejection metrics use bounded reason labels, never task IDs or
  payload data.
