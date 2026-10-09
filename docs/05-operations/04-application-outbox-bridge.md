# Application Outbox Bridge — operations contract

Operators of an app-local outbox bridge need to distinguish a live process from a process that
cannot read the application outbox or enqueue into workhold, **without** treating
temporary lag as data loss. This document is the BRDG-02 observability slice for
the supported SDK (`workhold_producer.bridge`; distribution
`workhold-producer`, optional extra `bridge-postgres`).

Normative product semantics are described in
[04-application-outbox-bridge.md](../04-architecture/10-application-outbox-bridge.md).
Metric-label and secret policy is aligned with [03-observability.md](03-observability.md) and
[01-security.md](01-security.md).

## Health and readiness

`BridgeHealth` is an immutable aggregate with `as_of` (application-DB / snapshot time)
and `freshness_seconds` (how long building the snapshot took on the process clock).

| Field | Meaning |
| --- | --- |
| `process_alive` | Process liveness — always true while the telemetry object exists |
| `app_store_reachable` / `app_store_query_ok` | Connectivity to the application outbox and success of the bounded query |
| `queue_reachable` | Recent successful workhold enqueue (cleared on timeout/transport failure) |
| `queue_compatible` | Cleared on unsupported schema / malformed immutable intent |
| `last_successful_poll_at` | Last successful app-store claim/poll |
| `last_successful_delivery_at` | Last fenced `mark_delivered` |
| `pending_count` / `pending_capped` / `pending_approximate` | Bounded depth; always declared approximate |
| `oldest_pending_lag_seconds` | `as_of - oldest pending created_at` (app-DB time base) |
| `poll_stale` | Last poll is older than the configured threshold (120s by default) |
| `empty_backlog` | No pending rows under the depth/oldest snapshots |
| `correctness_ok` | Observational only — lag **never** flips this to false |
| `ready` | Store reachable + query ok + workhold reachable/compatible + poll not stale |

Interpretation:

- **Liveness** ≠ **readiness**. A live process with an unreachable app store or workhold is
  **not** ready.
- **An empty backlog is a normal state.** Non-zero lag by itself is an SLO signal,
  **not** data loss and **not** a correctness failure. The bridge **does not promise** zero lag.
- Health and metrics are observational: sink failures **must not** change claim,
  enqueue, or lifecycle outcomes (`safe_observe` in the runner).

Depth and lag are built only from Plan 03 store methods:

1. `get_health_snapshot(depth_cap)`
2. `get_pending_depth(depth_cap)`
3. `get_oldest_pending_created_at()`

Observability code **must not** fall back to an unbounded `COUNT(*)` or history scans.

## SLI / metric names and units

Framework-neutral hooks (`BridgeTelemetry` → `BridgeMetricSink`) publish:

| Metric | Type | Unit | Labels | Meaning |
| --- | --- | --- | --- | --- |
| `bridge.pending_depth` | gauge | count | `process_role`, `operation`, `result` | Bounded pending depth (`result=empty\|approx\|capped`) |
| `bridge.oldest_pending_lag_seconds` | gauge | seconds | `process_role`, `operation`, `result` | Oldest pending lag |
| `bridge.claimed` | counter | 1 | + optional `queue` | Intents claimed in a poll |
| `bridge.delivered` | counter | 1 | + `queue`, `result=new\|replay` | Fenced deliver ack |
| `bridge.retryable_error` | counter | 1 | + `queue`, `result=<stable code>` | Retryable enqueue / uncertain |
| `bridge.permanent_conflict` | counter | 1 | + `queue`, `result=<stable code>` | Terminal operator-action conflict |
| `bridge.malformed_intent` | counter | 1 | + `queue`, `result=<stable code>` | Unsupported schema / malformed request |
| `bridge.lease_loss` | counter | 1 | + `queue` | Deliver ack lost fence |
| `bridge.lease_reclaim` | counter | 1 | + `queue` | Claim with `generation > 1` |
| `bridge.shutdown` | counter | 1 | `process_role`, `operation`, `result` | Graceful shutdown requested |

Adapters (Prometheus, OpenTelemetry, KernelMetrics) consume these hooks; the SDK
does **not** add a second metrics library.

## Allowed and forbidden labels

**Allowed metric labels:** `process_role` (bounded role of the bridge instance),
`queue` (target queue), `operation`, `result` (stable result / failure code).

**Forbidden as metric labels:** source row ID, source namespace, task ID,
idempotency key, payload fields, request ID, credentials / tokens, DSN, SQL,
partition names, free-text messages.

Logs **may** include the **public workhold task ID** and a hashed/bounded correlation
under the repository security policy. Payload fields, credentials, and raw source identities
do **not** appear in logs by default. W3C `traceparent` / `tracestate` **may** be
propagated **without** copying payload fields
(`BridgeTelemetry.project_trace_context`).

## Alert principles

Alert on **sustained oldest-pending lag together with** non-zero depth **and**
stalled progress or error outcomes (retries/conflicts). Do **not** alert on:

- an empty backlog;
- lag without depth/progress/error context;
- isolated single retries.

Default lag warning threshold: 300 seconds (`lag_warn_seconds`). Severity
for the combined predicate is `warning` / kind `bridge_lag_sustained`.
Map alerts to application runbooks (check app-store connectivity, workhold enqueue
auth/reachability, lease reclaim thrash, retention delivered rows).
Statistics/health staleness is lower severity than a correctness-path failure.

## Operator diagnostic steps

1. Read `BridgeHealth`: process alive, but `ready=false`?
2. If `app_store_reachable=false` — fix application DB / outbox connectivity.
3. If `queue_reachable=false` — fix workhold network, TLS, or auth; see
   `bridge.retryable_error` with `result=timeout|transport_error`.
4. If `queue_compatible=false` — update the bridge or stop writing unsupported
   `schema_version` / malformed intents (`bridge.malformed_intent`).
5. If `ready=true` but lag is sustained at non-zero depth — check throughput
   (`bridge.delivered` new vs replay), retries, conflicts, lease reclaim;
   confirm that app retention is **not** inflating pending scans.
6. **Never** treat lag as proof of lost business commits: a durable intent plus
   idempotent enqueue is the loss-tolerance path (see guarantees).

## Adapter notes

Wire `BridgeTelemetry` into `BridgeRunner(telemetry=...)`. Provide a
`BridgeMetricSink` / `BridgeLogSink` that forwards allowlisted labels to
the deployment's adopted telemetry backend (Phase 4 / 3.9 KernelMetrics adapters on
the workhold side; application processes use their own exporter under the same
label policy).
