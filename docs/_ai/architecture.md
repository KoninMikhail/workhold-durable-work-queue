# Architecture — Memory Bank snapshot

## Intent

**Workhold - Durable work queue** (`queue-service`). Delivery is into each application,
not a shared cluster: one versioned multi-role image at the application, with one or
several replicas.

The core is a durable Work Queue with named queues (named task streams inside
one instance; [named-queues.md](../01-concepts/04-named-queues.md)) and
competing consumers. Multi-replica
correctness is provided by a rotating claim token, lease generation, and queue-service-store
expiry. Each claim writes `claimed_at` and a diagnostic `worker_id`. Queued work
is cancelled immediately; leased work is cancelled cooperatively. Tasks have `priority` (default `0`, `-32768`…`32767`, strict integer; higher wins among **due** candidates) and
`available_at`; scheduling — bounded one-shot future
`available_at` (default/max horizon **86400** s via `QUEUE_SCHEDULE_HORIZON_SECONDS`,
range **0..86400**). Omitted/null/past/current → immediate; future within horizon
→ `delayed` until queue-service-store due; claim selects due `delayed`/`ready` directly
to `leased` (no promotion job). Retry-scheduled work shares the same due-claim
path. Structured failure codes and the versioned retry policy belong to the named queue.
Policy is configurable and can be disabled; the task keeps the enqueue-time version.
Claim order among eligible: `priority DESC`, `available_at ASC`, `id ASC`; due gate and unexpired leases are not preempted. Migration `1201` rebuilds priority-first claim index (blocking); downgrade fail-closed if any `priority <> 0`. Bridge rollout: server migrate with capability false → deploy SDK/bridge → `priority=true`. Execution at-least-once. Out of scope: cron/recurrence, per-task retry override, bands/aging/fairness/weighted queues, lease preemption, inspection priority filters, Delivery Outbox scheduling.

Bounded consumer long polling (Phase 20.1): live `long_polling=true`,
`max_wait_seconds=20`, `batch_claim=false` / `max_tasks=1`. LISTEN/NOTIFY — wake
hint only (not push). Supervisor default wait 15 s; fallback 1 s; client
read/total wait+5 / wait+10; proxy upstream ≥ 30 s. One dedicated listener
connection per API replica; default 64 outstanding waiters.

Runtime control plane: persisted `active|paused|draining` queue state, immutable
policy versions, optimistic `config_version`, private admin RBAC and audit.
Deployment/DDL/hard security settings are not changed through the runtime API.

Runtime semantics: queues are created by admin explicitly or by one-shot `queue apply`;
paused accepts enqueue and does not claim, draining does not accept external enqueue
and continues claim/internal spawn. Leased cancel completes through idempotent
`ack_cancel`. Stable errors contain retryability. The claim contract is array-shaped from the start, MVP `max_tasks=1`; bounded
`wait_seconds` 0..20 when `long_polling=true`. queue-service stores operational
outcome/attempts/lineage, but not the business result.

Production contracts: [admission](../04-architecture/06-admission-control.md),
[errors](../04-architecture/07-error-model.md), [inspection](../04-architecture/08-task-inspection.md),
[client protocol](../04-architecture/09-client-protocol.md), and
[operations](../05-operations/README.md).

Accepted packaging/protocol: one image with `api|migrate|maintain|relay|apply`
([ADR 015](../04-architecture/adr/015-single-image-multi-role.md);
apply extension [ADR 028](../04-architecture/adr/028-catalog-apply-process-role.md));
mounted catalog + `queue apply` is ensure-exists only (not image-baked, not
GitOps reconcile, not api startup) —
[02-deployment.md](../05-operations/02-deployment.md);
the image materializes allowlisted file secrets from `*_FILE` into `NAME` before
exec'ing the role (`docker/entrypoint.sh`,
[ADR 025](../04-architecture/adr/025-container-file-secrets.md));
runtime engine is PostgreSQL 18.6.x
([ADR 024](../04-architecture/adr/024-postgresql-18-6-runtime-engine.md));
optional error reporting is official `sentry_sdk` when `SENTRY_DSN` is non-empty
([ADR 026](../04-architecture/adr/026-optional-glitchtip-sentry-dsn.md));
HTTP/JSON OpenAPI 3.1 on stdlib `ThreadingHTTPServer` + pure ASGI
([ADR 027](../04-architecture/adr/027-stdlib-http-asgi-runtime.md));
public claim ID in path + secret token header; correctness
registry TTL 90d/7d/30d; HTTP webhook is the first pluggable delivery adapter;
CloudEvents 1.0 JSON structured mode is the delivery envelope; role-split
Python clients ship as `queue-service-client-core` plus
`queue-service-producer` / `queue-service-consumer` / `queue-service-admin`
([ADR 029](../04-architecture/adr/029-role-split-python-clients.md)). The
unreleased `queue-client` / `queue_service_client` prototype is removed.

HTTP service principals are deployment-configured through one bounded mounted
JSON secret manifest (`QUEUE_API_PRINCIPALS_MANIFEST_FILE`): explicit
producer/worker/observer/admin roles, overlapping credential generations and
exact named-queue scopes. If the manifest is absent, legacy
`QUEUE_API_BEARER_TOKEN` generations remain an ADMIN-only compatibility
fallback and never receive producer/worker grants.

Complete atomically closes the task and can create:

- `spawn[]` — new Work Queue tasks ([follow-up.md](../01-concepts/05-follow-up.md));
- `events[]` — pending Delivery Outbox records.

Who sends outbound HTTP and why two recipients go through one webhook —
[12-delivery-outbox.md](../01-concepts/12-delivery-outbox.md). Public HTTP
complete does not accept `events[]` yet (`delivery_events` is disabled).

queue-service has its own PostgreSQL. An application with a business DB uses an
app-local outbox bridge; a DB-less application enqueues directly and does not
introduce its own business DB.

Accepted architecture: [state machines](../04-architecture/01-state-machine.md),
[concurrency](../04-architecture/02-concurrency.md), [data flow](../04-architecture/03-data-flow.md),
and [storage topology](../04-architecture/04-storage-topology.md). Active claim state
is not partitioned by time; attempts and terminal history use
queue-service-maintained daily UTC RANGE partitions. Old reference drafts are not normative.

Do not carry v1 parser-queue contracts into this document. Patterns:
[outbox](../01-concepts/10-transactional-outbox.md), [inbox](../01-concepts/11-inbox.md).

## Current code

Package `queue_service` implements the kernel runtime: API (`api/`), intake, claims,
leases, completion, delivery relay, maintenance, observability, security, and
storage models on `queue_service.db.Base`. Process roles: `api`, `migrate`,
`maintain`, `relay`, `apply` (`roles/`; catalog ensure-exists —
[02-deployment.md](../05-operations/02-deployment.md)). The Python SDK is role-split
packages: `queue-service-client-core` plus
`queue-service-producer` / `queue-service-consumer` / `queue-service-admin`
([ADR 029](../04-architecture/adr/029-role-split-python-clients.md)).

The detailed tree is [codebase-map.md](codebase-map.md). Normative docs are
[../04-architecture/](../04-architecture/README.md).
