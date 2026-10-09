# Admin operations

The private control plane separates routine observation, reversible operations,
dangerous bulk changes, and break-glass repair. Every mutation is authenticated,
bounded, idempotent where retryable, and audited in the same
transaction.

## Observer operations

- read queue state, config/policy version, and drain progress;
- limited inspection of task, attempt, and dead-letter;
- statistics and partition-maintenance status;
- inspection of stuck/expired task and relay leases.

`listAdminAudit` and the rest of the audit trail are **ADMIN only**, not observer.

All lists require cursor pagination, a maximum page size, and indexed/time-bounded
filters. Arbitrary payload search and SQL are **forbidden**.

## Operator operations

- explicit creation of a named queue;
- pause/resume/drain with the expected `config_version`;
- creation and activation of an immutable policy version;
- start of idempotent partition maintenance.

Drain rejects new external enqueue while claims and internal spawns continue.
Completion is explicit when active depth reaches zero.

## Replay dead-letter (re-enqueue)

Replay creates a new task with a new ID and lineage to the immutable source:

- requires an admin idempotency key and a reason;
- snapshots the currently selected retry-policy version;
- respects queue state, depth, and replay-rate limits;
- the original terminal task/attempts **never** change;
- bulk replay supports dry-run, a bounded batch, and a confirmation token.

Replay does **not** promise exactly-once and may repeat external side effects.

## Bulk cancellation

Delayed/ready tasks become cancelled. Leased tasks receive a cooperative
cancellation request. Bulk commands require indexed filters, dry-run, hard batch
limits, and per-task audit counts. Cancellation **never** creates spawns/events.

## Break-glass operations

A separate short-lived role `BREAK_GLASS` (not `ADMIN`), a mandatory ack triad
(`reason` + `incident_reference` + `risk_acknowledged=true`), and strengthened audit.
Ordinary `ADMIN` does **not** authorize break-glass operations. Credential issuance is
deployment-side only (env/secret mounts); there is **no** minting API.

JIT credentials (required): a timezone-aware `expires_at` and a non-empty
`allowed_operations` audience. Expired credentials **never**
authenticate. Missing JIT fields fail closed (`unauthenticated`).

Shipped allowlist (names as in code / OpenAPI `operationId`):

- `forceLeaseExpiry` — forced expiry of a stuck task lease **without**
  creating a new lease and **without** minting `claim_token`;
- `forceDeliveryReclaim` — stuck **publishing** delivery event → **pending**;
  `generation` and `delivery_attempt` are preserved; a claim token is not issued;
- `forceDeliveryDeadLetter` — stuck **active** delivery event → terminal
  dead-letter with a bounded `failure_code`; a claim token is not issued;
- `reconcileCounters` — reconcile non-authoritative counters;
- `raiseReplayLimit` — a temporary raise of replay limits (see below);
- `dropExpiredPartition` — force-drop named expired partition;
- `repairRegistryEntry` — repair a specific dedup/replay registry entry.

Loud detection (OPS-09): every **successful** mutation increments the
low-cardinality counter `queue_break_glass_total` with labels `operation`,
`result`, and optional `queue` only (no reason, incident_reference, tokens,
or payloads). The handler uses its own `KernelMetrics(process_role="admin")`
if `metrics=` is not injected; `create_admin_app` does not wire a shared stats registry into this
handler by default — alert on the series name, and do not assume
a single admin registry.

Durable elevation: `raiseReplayLimit` writes the Queue-store table
`break_glass_elevations`; `BulkReplayRateGate.admit` reads it on all API
workers. Auto-revert after store `expires_at`. Durable read failure →
fail-closed (`dependency_unavailable`), **not** silent unlimited. Factor
1.0–10.0; TTL 1–3600s. Hard deployment ceilings are **not** changed through the admin API.

Break-glass does **not** bypass task fencing silently. Registry repair can widen the
duplicate window and therefore requires a queue pause and explicit acknowledgement.

### Clients (SDK)

- Recovery (`replayDeadLetter`, bulk preview/execute) — `AdminClient` only, with an
  ADMIN credential on the admin listener.
- The emergency allowlist — `BreakGlassClient` only, with deployment-issued JIT
  (`expires_at` + `allowed_operations`). Ordinary ADMIN does **not** call these
  methods through the SDK and does **not** pass server authorization.
- The SDK does **not** mint JIT, does **not** issue `claim_token`, and does **not** promise
  exactly-once recovery (replay remains at-least-once).

Practical how-to: [04-admin-operations.md](../02-guides/04-admin-operations.md).

## Forbidden

- mutate terminal history in place;
- issue or impersonate worker `claim_token`s (including through break-glass);
- change DDL / partition strategy / DB credentials through the admin API;
- replay unbounded result sets;
- delete audit records through the control plane;
- treat break-glass, PITR, or replay as exactly-once recovery;
- a dual-control / four-eyes UI or self-serve minting of break-glass credentials
  inside queue-service.
