# Project brief

**Workhold - Durable work queue** (personal project).
Runtime and package name: `queue-service` / `queue_service`.

The same tool is delivered to **each application** as one versioned
multi-role container image next to that application's replicas. It is not a shared bus
for the whole platform.

The goal is a reusable durable Work Queue for different application types. One instance
supports several named queues (named task streams inside the
instance, not separate deployments), competing workers, and fenced leases.
What a named queue is — [named-queues.md](../01-concepts/04-named-queues.md). `complete` can
atomically create follow-up tasks (`spawn[]`) and separately record outbound events
(`events[]`) in the Delivery Outbox. queue-service has its own PostgreSQL. An application
does not have to have its own business DB.

If the application has a business DB, it uses app-local outbox → relay →
idempotent enqueue. There is no distributed transaction with the queue-service DB. The concrete
delivery transport and the physical API/schema are not fixed yet.

Storage target: up to 1 million tasks/day and hundreds of claims/s per instance. Mutable active
state is separate from daily-partitioned attempts/terminal history; retention is 30–90 days.

Production boundary includes explicit queue creation, pause/drain semantics,
layered admission control, stable retryable errors, task/attempt inspection,
separate service principals, migration/readiness/backup and audited recovery.
queue-service does not store the application business result.

Packaging/protocol: one image with `api|migrate|maintain|relay|apply`, HTTP/JSON OpenAPI
3.1, public claim ID + secret header token. The first Delivery Relay adapter is an HTTP
webhook; the envelope is CloudEvents 1.0 JSON structured mode. The Python SDK is role-split
packages in this repo: `queue-service-client-core` plus
`queue-service-producer` / `queue-service-consumer` / `queue-service-admin`
([ADR 029](../04-architecture/adr/029-role-split-python-clients.md)).

This is a rethink (nominally v2) of an earlier file-parsers queue line (`parsers-queue-service`, forks for PDF/Excel, and `parsers-queue-client`). v1 contracts (`POST /jobs`, `JOB_KIND`, `minio_path`, the `jobs` model) are **not** the specification of this repository.

Concepts: [overview](../01-concepts/01-overview.md), [outbox](../01-concepts/10-transactional-outbox.md), [inbox](../01-concepts/11-inbox.md).

**Boundaries**

- In scope: named Work Queues, multi-replica leases, bounded retry/DLQ,
  atomic spawn + delivery intent, observability and retention.
- Sources of truth: [product boundary](../01-concepts/07-product-boundary.md),
  [guarantees](../01-concepts/09-guarantees.md), [architecture](../04-architecture/README.md).
- Old [formats](../03-reference/03-formats.md), [storage](../03-reference/04-storage.md),
  and [HTTP](../03-reference/05-http-api.md) — superseded proposals.
- Physical catalog: [storage-contract](../03-reference/02-storage-contract.md) and OpenAPI.
- Out of scope: carrying over v1 parser-specific logic as-is; a mandatory database for every application.
