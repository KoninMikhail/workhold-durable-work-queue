# Observability contract

workhold exposes low-cardinality operational telemetry. Numeric SLO targets
are set only after Phase 3 capacity benchmarks and may differ by application
class.

Initial Phase 3 release gate on a versioned reference environment:

- 500 claims/second;
- 99.9% successful valid operations;
- p99 enqueue, claim, and heartbeat at most 100 ms;
- p99 baseline complete at most 200 ms;
- maximum-fan-out complete measured/reported separately.

This is a release qualification baseline, **not** a universal external SLA.

## Required SLIs

- durable enqueue success ratio and commit latency;
- claim success/empty/error ratio and latency;
- heartbeat/complete/fail outcomes, including lease loss;
- ready/delayed/leased depth and oldest-ready age by named queue;
- wait and processing latency distributions;
- retry, lease-expiry, and dead-letter rates;
- statistics snapshot age;
- partition premake headroom and maintenance success;
- PostgreSQL pool wait, saturation, WAL/disk/autovacuum pressure;
- Delivery Outbox pending depth, publish outcomes, and oldest lag;
- bridge pending depth and lag (see [04-application-outbox-bridge.md](04-application-outbox-bridge.md)).

Empty claim is a successful no-work outcome, **not** an availability failure. Planned
pause/drain and invalid client requests are excluded from service-error ratios, but
reported separately.

Long-poll empty expiry (`tasks: []` after positive `wait_seconds`) is also a
successful no-work outcome. Closed-label metrics: `queue_long_poll_*` /
`queue_claim_wakeup_total` with labels only `result` + `process_role`; listener
connected gauge. Wake notification payload is only the queue name (no tokens /
payloads / worker ids).

## Metric label policy

Allowed bounded labels: queue, operation, result, terminal outcome, bounded
failure code, process role, and the closed `retention_window` enum
(`task_terminal`, `attempt`, `admin_audit`,
`correctness_enqueue_dedup`, `correctness_complete_replay`,
`correctness_admin_replay`, `delivery_published`, `delivery_dead_letter`).

**Forbidden:** task/event/claim IDs, worker ID, idempotency key, payload fields,
free-text failure detail, request ID, partition/relation names, DSN, and SQL.

## Logs

Structured logs include request ID, operation, queue, public task/event ID,
generation, diagnostic worker ID, config/policy version, and a stable result/error
code. Claim tokens and payload bodies are redacted by default.

Admin audit is persisted transactionally and may be mirrored to a SIEM.

Example redacted log line (no secrets or payload):

```json
{"request_id":"…","operation":"claim","queue":"billing","task_id":"…","generation":1,"worker_id":"…","result":"ok"}
```

## Traces

Trace enqueue, claim, heartbeat, complete/fail, relay, and partition maintenance.
Application handler work — outside workhold spans. Propagate W3C trace context
**without** adding payload content. Always sample errors/lease loss; sample
successful high-volume claims.

## Optional error reporting

Why DSN-presence-only and official `sentry_sdk` — [ADR 026](../04-architecture/adr/026-optional-glitchtip-sentry-dsn.md).

When `SENTRY_DSN` is non-empty, the process role initializes official
`sentry_sdk` once against a GlitchTip-compatible backend via the Sentry protocol.
Unset or empty `SENTRY_DSN` leaves the SDK uninitialized — behavior is as without
the integration; enablement is only by presence of a non-empty DSN.

`SENTRY_DSN` is a deployment secret. Do not log or send to GlitchTip:
DSN values, request bodies, claim tokens, bearer secrets, payloads, SQL, or HTTP
breadcrumbs.

Performance Monitoring, Session Replay, and Profiling are **not** enabled. This is
optional error reporting, **not** a second APM stack and **not** a replacement for Phase 4
logs/metrics/traces in this document.

## Alert principles

- alert on availability/error-budget burn, **not** isolated errors;
- alert on oldest-ready age **together with** depth;
- alert before partition horizon reaches failure window;
- alert on sustained lease expiry/DLQ/delivery-lag changes;
- alert on connection waits, disk exhaustion, and autovacuum lag;
- statistics staleness has lower severity than a correctness-path failure.

Every kernel alert maps to an executable procedure in
[05-runbooks.md](05-runbooks.md#alert--runbook-map). Retention predicates below feed
the partition/retention runbook; adaptive pressure and readiness codes feed the connection
and disk/WAL/autovacuum runbooks. Delivery Outbox lag alerts belong to Phase 5.

## Retention alert predicates (machine-testable, verifiable)

Retention health is projected from the Phase 3.8 maintenance report into aggregate telemetry
(see `workhold.observability.retention`). Fact retention is **not** a backup or
permanent archive: baseline task/attempt windows 90 days; admin audit retention
independently configurable; correctness registry defaults 90d / 7d / 30d (enqueue
dedup / complete replay / admin replay).

| Predicate | Condition | Severity | Kind |
| --- | --- | --- | --- |
| Premake approaching | `0 < premake_headroom_days <= 3` | warning | `premake_headroom_low` |
| Premake exhausted | `premake_headroom_days <= 0` | critical | `premake_headroom_exhausted` |
| Sustained maintenance failure | `outcome=failed` and `consecutive_failures >= 2` | critical | `sustained_maintenance_failure` |
| Stats staleness | statistics snapshot classified stale | warning | `stats_stale` |

Non-alerts:

- one successful no-op maintenance cycle (zero detach/drop/purge deletes);
- a single isolated maintenance failure (`consecutive_failures == 1`).

Severity order: `warning` (including stats staleness) `<` `critical`
(correctness-path premake exhaustion or sustained retention failure).

Maintenance logs and traces use the shared correlation projector
(`project_correlation` / `project_maintenance_correlation`) with request/trace ID,
operation, process role, workhold store `store_now`, and a bounded result/code only.
Payloads, claim tokens, DSNs, SQL, partition names, and free-text failure detail
excluded by construction.

`/stats` (or the protocol equivalent) is a bounded snapshot with `as_of` and freshness; it
**never** runs arbitrary scans of retained history or payload.
