# AGENTS

Entry point for AI agents.

**Product:** Workhold - Durable work queue. Runtime/package: `queue-service` / `queue_service`.

1. Read `docs/_ai/README.md` and the Memory Bank files in order.
2. Follow `.cursor/rules/` (Graphify, roles, workflow, docs).
3. Sources of product truth — `docs/01-concepts/07-product-boundary.md`,
   `09-guarantees.md` and `docs/04-architecture/`. Workhold (`queue-service`) — per-app durable Work Queue:
   named queues, fenced leases, distinct `spawn[]` tasks and Delivery Outbox
   `events[]`. queue-service always owns its PostgreSQL; a client business DB
   is optional. App with business DB uses app-local outbox bridge. Do not promise
   exactly-once or a distributed transaction. `docs/03-reference/03-formats.md`,
   `04-storage.md`, `05-http-api.md` — superseded proposals, not an implementation contract.

Sources of truth:

| Topic | Where |
| --- | --- |
| Compressed context | `docs/_ai/` |
| Reading path (≈15 min) | `docs/00-onboarding/01-reading-path.md` |
| Why the service exists, and how it differs from v1 | `docs/01-concepts/01-overview.md` |
| Why Workhold rather than a broker | `docs/01-concepts/02-why-queue.md` |
| How it works | `docs/01-concepts/03-how-it-works.md` |
| Boundary and guarantees | `docs/01-concepts/07-product-boundary.md`, `docs/01-concepts/09-guarantees.md` |
| How-to guides | `docs/02-guides/` |
| FAQ | `docs/06-faq/` |
| Troubleshooting | `docs/07-troubleshooting/` |
| Examples | `docs/08-examples/` |
| State machines / concurrency / ADR | `docs/04-architecture/` |
| Storage topology / partitioning / types | `docs/04-architecture/04-storage-topology.md`, ADR 006–007 |
| Runtime semantics / errors / limits | `docs/04-architecture/05-runtime-semantics.md`, `07-error-model.md`, `06-admission-control.md` |
| Protocol / SDK / packaging | `docs/04-architecture/09-client-protocol.md`, ADR 015–020 |
| Security / deploy / observability / tools | `docs/05-operations/` |
| Outbox and inbox patterns | `docs/01-concepts/10-transactional-outbox.md`, `docs/01-concepts/11-inbox.md` |
| Stack and commands | `docs/_ai/tech-stack.md`, `docs/03-reference/01-commands.md` |
| Old design proposals | `docs/03-reference/03-formats.md`, `04-storage.md`, `05-http-api.md` |
| Local setup / Alembic | `docs/00-onboarding/02-local-setup.md` |
| Agent rules | `.cursor/rules/` |
| Skills | `.cursor/skills/` |
