# Operations runbooks

Executable procedures for kernel alerts and recovery. Normative contracts
are described in [02-deployment.md](02-deployment.md), [03-observability.md](03-observability.md),
[07-admin-tools.md](07-admin-tools.md), [01-security.md](01-security.md), and
[storage-topology.md](../04-architecture/04-storage-topology.md).

**Principles**

- Prefer the private admin API and process roles (`api`, `migrate`, `maintain`,
  `relay`) to ad-hoc database mutations.
- **Never** invent or run operator SQL against workhold tables;
  DDL, DSN changes, and hard limits remain deployment configuration.
- workhold guarantees at-least-once execution. Do **not** treat restore, replay, or
  break-glass repair as exactly-once recovery. Consumer idempotency
  remains required.
- Retention is **not** a backup. Fact retention and registry TTL purge do **not**
  replace a base backup + WAL/PITR.
- Record an incident/ticket reference for dangerous and break-glass actions;
  after mutations, check `GET /admin/v1/audit`.
- Phase 5 Delivery Outbox lag runbooks are out of scope here; see the deferred list in
  [02-deployment.md](02-deployment.md#required-runbooks).

## Alert → runbook map

| Alert / signal (Plans 01–09) | Severity | Runbook |
| --- | --- | --- |
| `/readyz` fail: `schema_below_range` / `schema_above_range` | critical | [API not ready / schema mismatch](#api-not-ready--schema-mismatch) |
| `/readyz` fail: `postgres_unavailable` / `pool_timeout` / `statement_timeout` | critical | [Connection pressure / slow claim](#connection-pressure--slow-claim) |
| Sustained claim latency + pool wait; adaptive WARNING | warning | [Connection pressure / slow claim](#connection-pressure--slow-claim) |
| Adaptive `ENQUEUE_THROTTLE` / `/readyz` `overload_pressure` | critical | [Disk / WAL / autovacuum pressure](#disk--wal--autovacuum-pressure) |
| Sustained lease-expiry rate; reclaim storms | warning→critical | [Lease / reclaim storm](#lease--reclaim-storm) |
| Dead-letter rate / DLQ depth growth; poison tasks | warning→critical | [Poison tasks / DLQ growth](#poison-tasks--dlq-growth) |
| `premake_headroom_low` / `premake_headroom_exhausted` | warning / critical | [Partition premake / retention failure](#partition-premake--retention-failure) |
| `sustained_maintenance_failure` | critical | [Partition premake / retention failure](#partition-premake--retention-failure) |
| `stats_stale` | warning | Diagnose through [observer ops](07-admin-tools.md); lower severity than a correctness-path failure |
| Disk / WAL / autovacuum pressure snapshots | warning→critical | [Disk / WAL / autovacuum pressure](#disk--wal--autovacuum-pressure) |
| Backup/PITR restore required | critical | [PITR restore and duplicate-aware recovery](#pitr-restore-and-duplicate-aware-recovery) |
| Credential expiry / rotation window | warning | [Credential rotation](#credential-rotation) |
| `queue_break_glass_total` increment (any `operation`) | warning→critical | [Break-glass use (detection)](#break-glass-use-detection) |

## Common section template

Each runbook below describes: detection signals, severity, prerequisites,
safe diagnostics, supported private operations, stop conditions,
rollback/containment, and post-recovery verification.

---

## API not ready / schema mismatch

### Detection signals

- `/healthz` returns 200, while `/readyz` returns 503.
- Readiness `reason_code` is `schema_below_range` or `schema_above_range`.
- A rolling upgrade left API replicas ahead of or behind the migrator revision.

### Severity

Critical for serving traffic. Liveness alone is **not** enough to route
traffic.

### Prerequisites

- Migrator role credentials and the one-shot `migrate` command are available.
- A compatible image tag for expand → migrate/backfill → contract (see
  [02-deployment.md](02-deployment.md#migrations-and-rolling-upgrades)).
- An incident/ticket for production change windows.

### Safe diagnostics

1. Compare the `/readyz` body `reason_code` on all API replicas (no DSN/SQL in logs).
2. Confirm that the single migrator finished successfully before new
   replicas became ready.
3. Check the partition horizon only after schema codes have cleared; do **not**
   mix this with `partition_missing` / `partition_horizon_unsafe` (a separate runbook).

### Supported private operations

- Process role: `migrate` (one-shot Alembic upgrade).
- Observer: `GET /admin/v1/maintenance`, `GET /admin/v1/stats`.
- Do **not** use break-glass to "fix" the schema.

### Stop conditions

- Stop rolling out N+1 API pods until `/readyz` returns 200 on canaries.
- On `schema_above_range`, do not send traffic to old binaries until
  roll forward or schema contraction has been done per the deployment procedure.

### Rollback / containment

- Fail readiness and keep replicas out of the load balancer.
- Prefer a forward migrate inside the binary compatibility window; do **not** make
  manual catalog edits.

### Post-recovery verification

- `/readyz` 200 with no schema reason codes.
- Smoke test: `POST /v1/queues/{queue_name}/tasks` and `POST /v1/claims` on a canary
  queue with producer/worker credentials.
- Check admin audit only if a controlled state change was used.

---

## Connection pressure / slow claim

### Detection signals

- Rising claim/enqueue p99; pool wait and saturation SLIs
  ([03-observability.md](03-observability.md#required-slis)).
- `/readyz` `pool_timeout`, `statement_timeout`, or `postgres_unavailable`.
- Connection budget is close to `sum(role replicas × pool ceiling)` vs usable max
  ([02-deployment.md](02-deployment.md#postgresql-connection-budget)).

### Severity

Warning when latency/depth degrade but claims still succeed; critical
when readiness drops or the error budget is burning.

### Prerequisites

- Pool ceilings and replica counts per role are known.
- Adaptive pressure controller telemetry (Plan 04-03), if deployed.

### Safe diagnostics

1. Separate pool exhaustion from statement timeouts and a real Postgres outage.
2. Check `GET /admin/v1/stats` for ready/leased depth and oldest-ready age
   (bounded snapshot; **never** run arbitrary history scans).
3. Confirm that workers send heartbeat (`POST /v1/claims/{claim_id}:heartbeat`)
   and are not holding dead leases.

### Supported private operations

- Scale API/admin/relay/maintain replicas up or down within
  documented budgets.
- `POST /admin/v1/queues/{queue_name}:set-state` with the expected `config_version`
  to `paused` if intake must stop while claims drain.
- Observer lists: `GET /admin/v1/tasks`, `GET /admin/v1/attempts` (only cursor +
  page size).

### Stop conditions

- Do **not** raise hard RPS ceilings or pool sizes above deployment limits without
  capacity review.
- Do **not** open direct SQL sessions to "kill queries" as a workhold recovery step.

### Rollback / containment

- Prefer fail-fast pool acquisition (already required) and reduce intake through
  pause or the natural adaptive throttle.
- Restore replica counts only after pressure SLIs have cleared with hysteresis.

### Post-recovery verification

- Claim success ratio and p99 stay within release-baseline trends.
- `/readyz` is healthy; a single stats-freshness warning does **not** mean a correctness failure.

---

## Lease / reclaim storm

### Detection signals

- Sustained lease-expiry and reclaim rates
  ([03-observability.md](03-observability.md#alert-principles)).
- Oldest leased age grows while workers receive `lease_lost`.
- Break-glass force-expiry is used repeatedly on the same queue.

### Severity

Warning for short spikes after a deploy; critical when the expiry rate
stays elevated and ready depth / oldest-ready age grow together.

### Prerequisites

- Worker credentials and lease TTL / heartbeat policy versions are known.
- A short-lived `break_glass` role — only if force expiry is needed while preserving fencing
  ([07-admin-tools.md](07-admin-tools.md#break-glass-operations)).

### Safe diagnostics

1. `GET /admin/v1/stats` and a bounded `GET /admin/v1/tasks` for stuck leases.
2. Check clock skew and worker replica identity (`worker_id` for
   diagnostics only; see
   [01-security.md](01-security.md#worker-identity-and-claim-token)).
3. Distinguish poison tasks (DLQ growth) from a systemic reclaim.

### Supported private operations

- Policy activation: `POST /admin/v1/queues/{queue_name}/policies` then
  `.../policies/{policy_version}:activate` with `config_version`.
- Pause: `POST /admin/v1/queues/{queue_name}:set-state`.
- Break-glass: `POST /admin/v1/queues/{queue_name}/tasks/{task_id}:force-lease-expiry`
  with a reason, an incident reference, and `risk_acknowledged=true` (preserves generation
  fencing; do **not** issue claim tokens).

### Stop conditions

- Stop mass force-expiry; per-task only, and with audit.
- Do **not** impersonate claim-token holders and do **not** mutate terminal history.

### Rollback / containment

- Pause enqueue if churn is caused by intake; claims continue to drain the queue.
- Roll back the faulty worker builds; let leases expire naturally when that is safe.

### Post-recovery verification

- Lease-expiry rate returns to baseline; no unexpected DLQ spike.
- `GET /admin/v1/audit` shows only the expected break-glass rows.

---

## Poison tasks / DLQ growth

### Detection signals

- Rising dead-letter rate and DLQ depth.
- Repeated fail outcomes with bounded failure codes on one payload class.
- Replay pressure is approaching queue replay-rate limits.

### Severity

Warning when the DLQ grows slowly with stable ready depth; critical
when DLQ growth blocks business progress or replay storms threaten Postgres.

### Prerequisites

- Admin credentials for dead-letter inspection/replay; bulk ops require dry-run and
  confirmation tokens ([07-admin-tools.md](07-admin-tools.md#replay-dead-letter-re-enqueue)).
- Application owners are available for an idempotency review before bulk replay.

### Safe diagnostics

1. `GET /admin/v1/dead-letters` with cursor pagination and time-bounded filters.
2. Inspect attempts via `GET /admin/v1/attempts` — **never** search
   arbitrary payloads.
3. Confirm queue state (`active`/`paused`/`draining`) before replay.

### Supported private operations

- Single replay:
  `POST /admin/v1/queues/{queue_name}/dead-letters/{task_id}:replay`
  (admin idempotency key + reason; new task ID + lineage).
- Bulk:
  `POST .../bulk:preview-replay` then `.../bulk:execute-replay`.
- Bulk cancel delayed/ready poison intake:
  `.../bulk:preview-cancel` / `.../bulk:execute-cancel`.
- Temporary break-glass:
  `POST /admin/v1/queues/{queue_name}:raise-replay-limit` (`raiseReplayLimit`)
  on acknowledgement — a durable row in `break_glass_elevations`, factor 1.0–10.0,
  TTL 1–3600s, auto-revert after the store `expires_at`; durable read failure
  fail-closed (`dependency_unavailable`).
- Stuck Delivery Outbox (publishing):
  `POST .../delivery-events/{event_id}:force-reclaim` (`forceDeliveryReclaim`)
  or `:force-dead-letter` (`forceDeliveryDeadLetter`) — **without** `claim_token`.

### Stop conditions

- Stop unbounded replay; hard batch limits and dry-run are mandatory.
- Do **not** mutate original terminal tasks/attempts in place.
- Do **not** promise exactly-once side effects after replay.
- Do **not** use break-glass delivery ops as exactly-once recovery.

### Rollback / containment

- Pause the queue if poison producers are still running.
- Prefer cancelling delayed/ready bad work over quiet history edits.

### Post-recovery verification

- DLQ growth rate falls; replayed tasks either complete or re-enter the DLQ with
  understandable causes.
- Audit rows are present for every replay/cancel batch.

---

## Partition premake / retention failure

### Detection signals

- Retention alerts: `premake_headroom_low`, `premake_headroom_exhausted`,
  `sustained_maintenance_failure`
  ([03-observability.md](03-observability.md#retention-alert-predicates-machine-testable-verifiable)).
- `/readyz` `partition_missing` or `partition_horizon_unsafe`.
- The maintainer lost the advisory-lock / failed `maintain` cycles.

### Severity

`premake_headroom_low` and `stats_stale` are the warning class.
`premake_headroom_exhausted`, sustained maintenance failure, and readiness partition
codes — critical.

### Prerequisites

- Single-winner `maintain` role; do **not** run concurrent maintainers.
- Understand that retention windows (90d baselines; registry TTL 90/7/30) do
  **not** replace backup
  ([storage-topology.md](../04-architecture/04-storage-topology.md)).

### Safe diagnostics

1. `GET /admin/v1/maintenance` for last success, headroom, and failure counts.
2. Correlate with maintenance logs through OPS-08 allowlisted fields only (no
   partition names, SQL, or payloads in metrics).
3. Confirm that disk pressure is **not** the root cause (see
   [Disk / WAL / autovacuum pressure](#disk--wal--autovacuum-pressure)).

### Supported private operations

- `POST /admin/v1/maintenance:run` (idempotent partition maintenance).
- Process role: `maintain`.
- Break-glass only for a named expired partition:
  `POST /admin/v1/partitions/{partition_name}:force-drop` with acknowledgement.
- Registry repair (pause-gated):
  `POST /admin/v1/queues/{queue_name}/registry:repair`.

### Stop conditions

- Stop when headroom is exhausted and readiness is failing — do **not** keep
  accepting serving writes that need missing future partitions.
- Do **not** detach/drop partitions through hand-written SQL.

### Rollback / containment

- Keep the API unready until the horizon is restored.
- If force-drop was used, confirm that only expired partitions were targeted
  and that audit recorded the incident reference.

### Post-recovery verification

- Premake headroom is above the warning threshold; `/readyz` has no partition codes.
- The sustained failure counter resets after consecutive successful maintain cycles.
- Reminder: successful no-op maintain cycles are normal; retention is still
  **not** a backup.

---

## Disk / WAL / autovacuum pressure

### Detection signals

- PressureSnapshot peaks for disk, WAL, or autovacuum lag (Plan 04-01 / 04-03).
- Adaptive modes: WARNING → `ENQUEUE_THROTTLE` → readiness `overload_pressure`,
  while claims remain allowed.
- Protocol hints: `resource_exhausted` / `enqueue_throttled_pressure` or
  `dependency_unavailable` / `readiness_pressure`.

### Severity

Warning in adaptive WARNING; critical on enqueue throttle or readiness failure.

### Prerequisites

- Deployment disk/WAL capacity and autovacuum settings are the platform DBAs' responsibility.
- Soft adaptive rates **never** raise hard enqueue RPS ceilings.

### Safe diagnostics

1. Check PressureSnapshot freshness; on stale/unavailable snapshots, degrade
   conservatively (do **not** clear overload on noisy samples).
2. `GET /admin/v1/stats` for depth while claims continue.
3. Check whether partition detach/drop backlog or table bloat correlates.

### Supported private operations

- The adaptive gate runs automatically; operators **may** pause queues via
  `POST /admin/v1/queues/{queue_name}:set-state`.
- Run maintain if retention detach reduces disk:
  `POST /admin/v1/maintenance:run`.
- Break-glass `POST ...:reconcile-counters` only for non-authoritative counters
  after storage recovery — it **never** replaces fixing disk problems.

### Stop conditions

- Stop adding API replicas that increase connection and I/O load.
- Stop advancing intake until thresholds are clear and hold samples have passed.

### Rollback / containment

- Keep claims draining under throttle; block new enqueue through adaptive mode
  or an explicit pause.
- Expand disk / clear WAL at the infrastructure level; workhold **cannot**
  invent vacuum SQL for operators.

### Post-recovery verification

- Overload mode returns to NORMAL only after the clear-ratio + hold samples.
- `/readyz` no longer reports `overload_pressure`.
- Enqueue and claim SLIs recover without a DLQ storm.

---

## PITR restore and duplicate-aware recovery

### Detection signals

- Corruption, an irreversible operator error, or an RPO-driven restore decision.
- Failed partial-recovery attempts (**must not** leave split registries).

### Severity

Critical. Treat it as a full trust-boundary restore of one workhold database.

### Prerequisites

- A valid base backup + WAL continuous archive matching the application RPO/RTO
  ([02-deployment.md](02-deployment.md#backup-and-restore)).
- Safe handling of backup media (no credential/token/payload extraction into
  tickets or chat).
- The ability to stop workers/relay and block producer intake.
- An incident/ticket for the restore window.

### Safe diagnostics

1. Confirm the restore target is the single workhold database for this application trust
   boundary ([01-security.md](01-security.md)).
2. Choose the restore point; do **not** attempt a table-level or registry-only restore.
3. Inventory the process roles that must stay down: API intake,
   workers, relay, maintain (migrator only as required by procedure).

### Supported private operations

After the database is restored by deployment tooling (not admin SQL):

- Check readiness: `/readyz` (schema + partition horizon).
- Observer: `GET /admin/v1/maintenance`, `GET /admin/v1/stats`,
  `GET /admin/v1/audit`.
- Break-glass: `POST /admin/v1/queues/{queue_name}:reconcile-counters` for
  **non-authoritative** counters only.
- Named-queue pause/resume via `POST /admin/v1/queues/{queue_name}:set-state` when
  a controlled reopen is needed.
- Do **not** use registry repair to recreate missing correctness windows
  instead of an incomplete restore.

### Recovery procedure (required order)

1. **Stop workers and relay**; fail readiness / block intake so there are no
   split-brain writes during the restore.
2. **Restore one consistent workhold database**, including:
   - active scheduling state (`tasks_active`, payloads, claim registry);
   - correctness registries (`enqueue_dedup`, `complete_replay`, admin replay
     registry as deployed);
   - terminal and attempt history partitions;
   - `admin_audit_log` and named-queue config/policy rows.
3. **Verify the schema** (Alembic revision inside the binary window) and the **partition premake
   horizon**.
4. **Reconcile only non-authoritative counters** when needed; **never**
   rebuild authoritative registries from guesswork.
5. **Resume** API/workers/relay, expecting **at-least-once** re-execution and
   possible duplicate deliveries; consumers must remain idempotent.

### Stop conditions

- Abort if the backup set cannot restore registries and audit together with active
  state — a partial registry restore is **forbidden**.
- Abort operator-written SQL "fixes" during the restore.
- Do **not** claim exactly-once recovery after PITR.

### Rollback / containment

- Keep traffic blocked until the schema/horizon check has passed.
- If the wrong restore point was chosen, restore again to a consistent point
  rather than editing individual rows.

### Post-recovery verification

- `/readyz` 200; maintenance headroom is in the safe zone.
- Spot-check audit continuity and queue `config_version` through admin GET APIs.
- Application owners confirm duplicate-aware replay of unfinished work.
- Explicit reminder: **retention is not a backup**; TTL purge does not remove the need for
  PITR.

---

## Break-glass use (detection)

### Detection signals

- An increment of `queue_break_glass_total` (labels: `operation`, `result`, optional
  `queue` only) — the primary OPS-09 detection signal. Example query:
  `sum(increase(queue_break_glass_total[15m])) by (operation, result)`.
- OPS-08 correlation (`incident_ref_hash` / `target_id`) in admin diagnostics —
  additive to the metric, not a replacement.
- The handler process publishes the counter from its own
  `KernelMetrics(process_role="admin")` by default if `metrics=` is not injected; do not
  promise a shared admin registry that `create_admin_app` does not wire.

### Severity

Warning on any unexpected break-glass use outside an incident window;
critical on a series of ops with no ticket / no known change window.

### Prerequisites

- The ops allowlist in [07-admin-tools.md](07-admin-tools.md#break-glass-operations) is known
  (`forceLeaseExpiry`, `forceDeliveryReclaim`, `forceDeliveryDeadLetter`,
  `reconcileCounters`, `raiseReplayLimit`, `dropExpiredPartition`,
  `repairRegistryEntry`).
- JIT `BREAK_GLASS` credentials with a timezone-aware `expires_at` and an audience.

### Safe diagnostics

1. Match the `operation` label to the repair expected for the incident.
2. `GET /admin/v1/audit` — only the expected break-glass rows; redacted correlation.
3. For `raiseReplayLimit`, confirm that the elevation in `break_glass_elevations`
   has a finite `expires_at` (not sticky unlimited).

### Supported private operations

See the shipped catalog in [07-admin-tools.md](07-admin-tools.md#break-glass-operations). Delivery
reclaim/dead-letter and durable elevation are shipped, not deferred.

### Stop conditions

- Stop repeated break-glass calls without a new incident reference.
- Do **not** mint `claim_token`; do **not** treat break-glass as exactly-once.
- Do **not** escalate into a dual-control UI or SQL/DDL through the control plane.

### Rollback / containment

- Revoke break-glass credentials after the incident window.
- Wait for the elevation TTL to auto-revert; if in doubt, pause enqueue and do not
  raise the factor above 10.0.

### Post-recovery verification

- The counter rate has returned to zero outside the incident window.
- Audit and elevation rows match the incident window.

---

## Credential rotation

### Detection signals

- Approaching credential expiry; auth failures (`unauthenticated` /
  `permission_denied`).
- Short-lived break-glass credentials are approaching `expires_at`.

### Severity

Warning during a planned overlap; critical if every active
credential for the role stops working.

### Prerequisites

- Deployment-issued overlapping old/new credentials
  ([01-security.md](01-security.md#rotation-and-audit)).
- Separate admin/break-glass listeners and principals.

### Safe diagnostics

1. Identify which service principal fails (producer, worker, admin, break_glass,
   migrator/maintainer).
2. Confirm the TLS and private-network path; do **not** log bearer tokens or claim
   tokens.

### Supported private operations

- Roll out overlapping credentials; restart pods so they pick up secret mounts.
- Issue short-lived break-glass credentials with a timezone-aware `expires_at` and a
  non-empty `allowed_operations` audience only when emergency repair is needed.
  Expired credentials never authenticate; `ADMIN` does **not** authorize break-glass.
- The admin API does **not** change DDL or DB URLs and does **not** mint break-glass tokens.

### Stop conditions

- Stop rotating every role at once without overlap.
- Stop pasting tokens into runbooks, tickets, or metrics.

### Rollback / containment

- Keep the previous credential valid until the new one is confirmed on canaries.
- Revoke break-glass immediately after the incident window.

### Post-recovery verification

- Canary enqueue/claim/admin read succeeds with the new credentials.
- Old credentials are revoked after the overlap; audit shows only the expected admin actors.
