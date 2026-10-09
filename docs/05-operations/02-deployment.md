# Deployment and recovery

## Process roles

queue-service defines roles independently of image count:

- **API:** producer/worker protocol;
- **Admin:** private control plane;
- **Relay:** Delivery Outbox publication;
- **Maintainer:** partition premake, retention, and registry purge;
- **Migrator:** one-shot Alembic upgrade.

One versioned image exposes the commands `api`, `migrate`, `maintain`, `relay`,
and `apply` ([ADR 015](../04-architecture/adr/015-single-image-multi-role.md);
the fifth role is [ADR 028](../04-architecture/adr/028-catalog-apply-process-role.md)).
Runtime engine — PostgreSQL 18.6.x
([ADR 024](../04-architecture/adr/024-postgresql-18-6-runtime-engine.md)).
API/Admin share the `api` binary but use separate listeners, credentials, and
network policy. Relay is introduced with the Delivery Outbox. Maintainer is single-winner through
an advisory lock. `apply` is a one-shot ensure-exists for named queues from a mounted
JSON (`QUEUE_CATALOG_PATH`); the catalog is **not baked into the image**, it is **not** GitOps
reconcile of live state, and it is **not** part of `api` startup / `/readyz`.

All five CLI commands (`api`, `migrate`, `maintain`, `relay`, `apply`) honor the same
opt-in error reporting: a non-empty `SENTRY_DSN` (or `SENTRY_DSN_FILE` in the
image) initializes `sentry_sdk` when the role starts; unset/empty leaves the SDK off
([ADR 026](../04-architecture/adr/026-optional-glitchtip-sentry-dsn.md)).

### File secrets and image ENTRYPOINT

The runtime image uses exec-form `ENTRYPOINT ["/app/entrypoint.sh"]` and
`CMD ["--help"]` ([ADR 025](../04-architecture/adr/025-container-file-secrets.md)).
The entrypoint materializes allowlisted secrets from `NAME_FILE`
paths (see [01-security.md — File secrets](01-security.md#file-secrets-_file)),
then `exec queue` with the remaining arguments. In Compose/Kubernetes keep
`command: ["api"]` (or `migrate` / `maintain` / `relay` / `apply`) as the role name —
do **not** use `docker run … queue api` as the image command; the entrypoint already
invokes `queue`.

`NAME_FILE` values must be absolute paths. The process runs as uid
`10001`; that user must be able to read the mounted files. The default Compose secret mode
(`0444`) and Kubernetes `defaultMode` `0644` are acceptable. Mode `0400` is operator
guidance and requires matching ownership / `fsGroup`. The entrypoint follows symlinks
(Kubernetes Secret volumes use `..data`).

Production Compose example (docs only — do **not** attach Compose `secrets:` to the
local development stack; that stack keeps inline env):

```yaml
services:
  api:
    image: queue:<version>
    command: ["api"]
    environment:
      DATABASE_URL_FILE: /run/secrets/database_url
      QUEUE_API_PRINCIPALS_MANIFEST_FILE: /run/secrets/api_principals.json
    secrets:
      - database_url
      - api_principals
secrets:
  database_url:
    file: ./secrets/database_url
  api_principals:
    file: ./secrets/api_principals.json
```

Kubernetes Secret volume example (docs only):

```yaml
env:
  - name: DATABASE_URL_FILE
    value: /run/secrets/queue/database_url
  - name: QUEUE_API_PRINCIPALS_MANIFEST_FILE
    value: /run/secrets/queue/api_principals.json
volumeMounts:
  - name: queue-secrets
    mountPath: /run/secrets/queue
    readOnly: true
volumes:
  - name: queue-secrets
    secret:
      secretName: queue-secrets
      defaultMode: 0440
```

Do **not** add a `secrets:` section to `docker-compose.dev.yml`; development
stays inline-env only.

The manifest is one bounded secret JSON for `PRODUCER`, `WORKER`, `OBSERVER`, and
`ADMIN`; schema and placeholders:
[service-principal-manifest.schema.json](../04-architecture/schemas/service-principal-manifest.schema.json),
[service-principal-manifest.example.json](../08-examples/service-principal-manifest.example.json).
The mount must be a regular file, readable by uid `10001`, non-empty, and reachable at an
absolute path. `QUEUE_API_PRINCIPALS_MANIFEST_FILE` and the unsuffixed env are exclusive;
the entrypoint materializes the JSON without printing its contents and removes `_FILE` before `exec`.

A non-empty manifest has exclusive precedence: legacy
`QUEUE_API_BEARER_TOKEN(_PREVIOUS)` is not merged and is not accepted. An invalid
manifest stops the API before listeners/engines. Only an absent or blank
manifest enables the legacy ADMIN fallback; it does not grant enqueue/claim/lease grants.
Exact queue scopes do not support wildcard/prefix. Rotation is performed by
overlapping generations of one stable principal, then removing the old generation.

## Named queue catalog: apply (ensure-exists)

Optional one-shot `queue apply` creates missing named queues from a
JSON file at the absolute `QUEUE_CATALOG_PATH` (apply role env, not
`DeploymentSettings`, not `QUEUE_CATALOG_PATH_FILE`). The operator mounts the file
(Compose volume / ConfigMap). The `Dockerfile` does **not** `COPY` catalogs into the image —
the catalog is **not baked into the image**. The process under uid `10001` must be able to read the path.

This is **ensure-exists**, not GitOps reconcile: existing queues (any state /
policy) are skipped; names removed from the file are **not** deleted and are **not**
drained. Pause / drain / activate policy is admin HTTP only. `api` does **not**
`depends_on` apply; startup and `/readyz` do not read the catalog.

Compose (docs/dev stack):

```bash
docker compose -f docker-compose.dev.yml --profile apply run --rm apply
```

Kubernetes (docs-only; there is no chart in the repo): a Job with a ConfigMap volume, readable by
uid `10001`, `command: ["apply"]`, after the migrate Job; the api Deployment does not wait for
apply.

Pool budget: `docker-compose.dev.yml` records the comment
`10 reserved + 14 committed` (six `PROCESS_ROLES` × replica × pool, including
`QUEUE_APPLY_*`). Catalog example:
[named-queue-catalog.example.json](../08-examples/named-queue-catalog.example.json);
schema:
[named-queue-catalog.schema.json](../04-architecture/schemas/named-queue-catalog.schema.json).

Partial apply: after validation, a mid-catalog create failure can leave earlier
creates committed; recovery is to run `queue apply` again.

## Migrations and rolling upgrades

- a single migrator runs before new replicas become ready;
- API pods do **not** run migrations on startup;
- use expand → migrate/backfill → contract;
- schema and protocol versions are independent;
- N and N+1 binaries must tolerate the rollout schema;
- avoid long `ACCESS EXCLUSIVE` operations on active tables;
- future daily partitions receive the same indexes/constraints as the parent.

Readiness checks PostgreSQL, a compatible schema, and the partition premake horizon.
Liveness checks only that the process/event loop is alive. Statistics, relay
destination, and retention lag do **not** make the Work Queue API unready while the
correctness path is working.

## Graceful shutdown

Process shutdown is **not** a persisted queue drain:

1. fail readiness and stop accepting new requests;
2. finish bounded in-flight API transactions;
3. workers stop claiming and finish within grace or let leases expire;
4. relay stops claiming and finishes/abandons current publications;
5. do **not** mutate named queue state only because one replica terminates.

## PostgreSQL connection budget

```text
usable = max_connections - admin/monitoring/migration reserve
sum(role replicas × pool ceiling) <= usable
```

API, admin, relay, maintainer, migrator, and apply have separate bounded pools.
Pool acquisition and statements fail fast. Broker/network publication **never**
holds a DB transaction. PgBouncer is optional; any use of advisory locks or
LISTEN must match the pooling mode.

## Backup and restore

Production uses a PostgreSQL base backup + WAL/PITR according to the application
RPO/RTO. Logical dumps are suitable only for small/dev instances.

Restore procedure (detail and verification steps:
[05-runbooks.md — PITR](05-runbooks.md#pitr-restore-and-duplicate-aware-recovery)):

1. stop workers/relay and block intake;
2. restore **one consistent** queue-service database including active state,
   correctness registries (`enqueue_dedup`, `complete_replay`, admin replay),
   terminal/attempt history, and `admin_audit_log`;
3. verify the schema and the partition horizon;
4. reconcile **only** non-authoritative counters when needed;
5. resume while expecting at-least-once task execution and duplicate-aware
   recovery (event republish when relay deployed).

A partial registry-only or table-level restore is **forbidden**. Retention is **not** a
backup. Restoring to an earlier point can repeat work or delivery; consumer
idempotency remains required. Do **not** treat PITR as exactly-once recovery.

## Claim long polling (WORK-17)

Live capability: `long_polling=true`, `max_wait_seconds=20`, `batch_claim=false`,
`max_claim_tasks=1`. This is **not** push delivery: PostgreSQL `LISTEN/NOTIFY` is only a
wake hint; eligibility remains on each claim attempt. Fallback reconciliation is
**1 s**.

Operator bounds (see `.env.example`):

| Env | Default | Meaning |
| --- | --- | --- |
| `QUEUE_CLAIM_MAX_WAIT_SECONDS` | 20 | Server hard ceiling for `wait_seconds` |
| `QUEUE_CLAIM_WAIT_FALLBACK_SECONDS` | 1.0 | Reconciliation on a missed wake |
| `QUEUE_CLAIM_CANCELLATION_PROBE_SECONDS` | 0.25 | Disconnect / shutdown probe |
| `QUEUE_CLAIM_MAX_OUTSTANDING_WAITS` | 64 | Per-replica waiter semaphore; the 65th → retryable `resource_exhausted` |

Each API replica holds **one** dedicated autocommit LISTEN connection outside the
SQLAlchemy pool (see the connection budget). An outstanding wait does **not** hold a
pooled checkout between claim attempts.

**Production preflight:** reverse-proxy upstream idle/response timeout **≥ 30 s**
(server max 20 + 10 s). Client budgets: read = wait+5, total = wait+10.
`ConsumerSupervisor` default wait = **15 s** after the capability preflight.
If the proxy floor is not proven, do not enable the capability in the deployment (fail-closed).

A real disconnect and API shutdown cancel the wait without handing out work and without
exactly-once / distributed-transaction promises. Fencing and at-least-once
claim semantics do not change.

## Required runbooks

Executable procedures: [05-runbooks.md](05-runbooks.md).

Kernel (Phase 4):

- [API not ready / schema mismatch](05-runbooks.md#api-not-ready--schema-mismatch);
- [connection exhaustion and slow claim](05-runbooks.md#connection-pressure--slow-claim);
- [lease/reclaim storm](05-runbooks.md#lease--reclaim-storm);
- [poison tasks and dead-letter growth](05-runbooks.md#poison-tasks--dlq-growth);
- [partition premake/retention failure](05-runbooks.md#partition-premake--retention-failure);
- [disk/WAL/autovacuum pressure](05-runbooks.md#disk--wal--autovacuum-pressure);
- [PITR restore and duplicate-aware recovery](05-runbooks.md#pitr-restore-and-duplicate-aware-recovery);
- [credential rotation](05-runbooks.md#credential-rotation).

Deferred (Phase 5 Delivery Outbox):

- Delivery Outbox lag (not implemented in Phase 4 kernel runbooks).
