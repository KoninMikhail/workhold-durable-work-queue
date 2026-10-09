# Kernel chaos testing

Reproducible fault-injection matrix for queue-service kernel correctness (QUAL-04
kernel slice). Relay duplicate-publication chaos is deferred until Phase 5.

Normative sources: [concurrency.md](../04-architecture/02-concurrency.md),
[admission-control.md](../04-architecture/06-admission-control.md),
[storage-topology.md](../04-architecture/04-storage-topology.md),
[02-deployment.md](02-deployment.md), [07-admin-tools.md](07-admin-tools.md),
[03-observability.md](03-observability.md), [05-runbooks.md](05-runbooks.md).

## Environment

Use the Phase 3 Docker/PostgreSQL reference stack — do not introduce a separate
chaos service:

```powershell
docker compose -f docker-compose.dev.yml up -d postgres
$env:TEST_DATABASE_URL = 'postgresql+psycopg://queue:queue@127.0.0.1:5432/queue'
  # Chaos kernel suite
uv run pytest tests/chaos/kernel -q
```

Isolation: each test uses a disposable Alembic schema (`qch_*`,
function-scoped) and tears it down. PostgreSQL restarts target only the compose
`postgres` service and wait for readiness before assertions. Cleanup fixture-scoped.
RC-RESTORE-PITR uses the harness `logical_snapshot` / `logical_restore` of the whole
schema `qch_*` (registries + audit), not partial table-level deletes — aligned
with `02-deployment.md` one-consistent-DB restore.

Duration class: **short–medium** (seconds to a few minutes). Scenarios that
restart PostgreSQL dominate wall time.

## Failure classification

| Class | Signal | Action |
| --- | --- | --- |
| Environmental | Docker unavailable, `TEST_DATABASE_URL` unset, compose restart timeout, `pg_isready` never recovers | Fix local/CI infra; do not treat this as an invariant failure |
| Invariant | Scenario assertion on fencing, idempotency, atomicity, audit, redaction, or terminal counts | Product defect — investigate with the scenario ID + evidence bundle |

Evidence fields per scenario: injection timestamp, protocol outcome, database
counts/invariants, telemetry notes (redacted), recovery assertion. Captured
text **never** contains claim tokens, payload bodies, or DSNs.

## Scenario matrix

| ID | Category | Fault | Invariants / recovery |
| --- | --- | --- | --- |
| RT-API-BEFORE-COMMIT | runtime | Kill API backend before enqueue commit | No task until same-key retry creates exactly one |
| RT-API-AFTER-COMMIT | runtime | Drop committed enqueue response | Exactly one task; same-key replay returns it |
| RT-WORKER-LEASE | runtime | Worker death + lease expiry | Stale complete fenced; reclaim succeeds |
| RT-WORKER-TERMINAL | runtime | API process restart after complete | Idempotent replay; no duplicate terminals/spawns |
| RT-PG-ENQUEUE | database | PostgreSQL restart around enqueue | Committed rows survive; no uncommitted success |
| RT-PG-CLAIM-HB-COMPLETE | database | PostgreSQL restart mid claim/HB/complete | Lifecycle continues without split-brain |
| RT-PG-MAINT-ADMIN | database | PostgreSQL restart around admin set-state | Committed state survives; retry safe |
| RT-RACE-PAUSE-CLAIM | race | Pause vs claim | Claims empty while paused; enqueue allowed; resume restores claims |
| RT-RACE-DRAIN-ENQUEUE-SPAWN | race | Drain vs enqueue/spawn | External enqueue rejected; spawn + claims continue |
| RT-RACE-CANCEL-EXPIRY-COMPLETE | race | Cancel vs complete | At most one terminal for the task |
| RT-RACE-RETRY-LEASE-EXPIRY | race | Fail vs forced lease expiry | Stale fail fenced; current lease single-winner |
| RC-RESTORE-PITR | restore | Logical `qch_*` schema snapshot → mutate → `logical_restore` | Registries/audit restored; readiness/horizon; duplicate-aware resume; counters reconcilable |
| RC-INTERRUPT-REPLAY | admin | Drop-committed-response mid replay | Same admin idempotency → `replayed=true`; source terminal immutable |
| RC-INTERRUPT-BULK-CANCEL | admin | Drop-committed-response mid bulk execute | Bounded batch; idempotent retry where promised |
| RC-INTERRUPT-MAINTENANCE | admin | Drop-committed-response mid maintenance | Completes or skipped without hang/corruption |
| RC-INTERRUPT-BREAK-GLASS | admin | Pre-commit backend kill mid force-lease-expiry | Audited; no claim token minted; fencing preserved |
| RC-INTERRUPT-BG-DELIVERY-RECLAIM | admin | Pre-commit kill mid `forceDeliveryReclaim` | Idempotent/bounded retry; no claim token; fencing preserved |
| RC-INTERRUPT-BG-ELEVATION-WRITE | admin | Pre-commit kill mid durable `raiseReplayLimit` write | Elevation absent or single durable row; no sticky unlimited; factor 1.0–10.0 / TTL 1–3600s |
| RC-PRESSURE-CLAIM-DRAIN | recovery | Live `AdaptivePressureController` on enqueue path + PG restart | Enqueue/readiness throttled; `claims_allowed` stays true; claims drain |
| RC-NO-LEAKAGE | security | Evidence/log capture with secrets in play | No token/payload/DSN leakage |

### Explicitly excluded (Phase 5)

| ID | Reason |
| --- | --- |
| RT-RELAY-DUP-PUBLISH | Delivery Outbox relay duplicate publication — Phase 5 QUAL-04 relay slice |

## Harness location

- `tests/chaos/kernel/conftest.py` — fixtures
- `tests/chaos/kernel/harness.py` — scenario matrix + fault injection
- `tests/chaos/kernel/test_runtime_failures.py` — runtime/database/race
- `tests/chaos/kernel/test_recovery_failures.py` — restore/admin/pressure/redaction
- `tests/chaos/kernel/test_break_glass_interrupt.py` — delivery reclaim + elevation
  write pre-commit interrupt (`RC-INTERRUPT-BG-DELIVERY-RECLAIM`,
  `RC-INTERRUPT-BG-ELEVATION-WRITE`)

## CI / local commands

```powershell
  # Chaos kernel suite
uv run pytest tests/chaos/kernel -q

  # Full suite
uv run pytest
```
