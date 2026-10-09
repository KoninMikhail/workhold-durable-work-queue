# Memory Bank

Compressed repository context for an agent. Product: **Workhold - Durable work queue**
(runtime/package: `queue-service` / `queue_service`). Read it before a task, in this order.

| # | File | When to read |
| --- | --- | --- |
| 1 | [project-brief.md](project-brief.md) | What this repository is |
| 2 | [tech-stack.md](tech-stack.md) | Stack and commands |
| 3 | [architecture.md](architecture.md) | How the code is structured and what is fixed |
| 4 | [codebase-map.md](codebase-map.md) | Where things are |

| Topic | Where to read in detail |
| --- | --- |
| Reading path (≈15 min) | [../00-onboarding/01-reading-path.md](../00-onboarding/01-reading-path.md) |
| Why the service exists, v1 | [../01-concepts/01-overview.md](../01-concepts/01-overview.md) |
| Why queue-service rather than a broker | [../01-concepts/02-why-queue.md](../01-concepts/02-why-queue.md) |
| How it works | [../01-concepts/03-how-it-works.md](../01-concepts/03-how-it-works.md) |
| Product boundary and terms | [product boundary](../01-concepts/07-product-boundary.md), [glossary](../01-concepts/06-glossary.md), [named queue](../01-concepts/04-named-queues.md), [follow-up / spawn](../01-concepts/05-follow-up.md) |
| Guarantees and use cases | [guarantees](../01-concepts/09-guarantees.md), [use cases](../01-concepts/08-use-cases.md) |
| Transactional outbox | [../01-concepts/10-transactional-outbox.md](../01-concepts/10-transactional-outbox.md) |
| Inbox | [../01-concepts/11-inbox.md](../01-concepts/11-inbox.md) |
| Delivery Outbox / who sends HTTP | [../01-concepts/12-delivery-outbox.md](../01-concepts/12-delivery-outbox.md) |
| How-to guides | [../02-guides/](../02-guides/) |
| Client SDK (sync/async, ergonomics) | [../02-guides/06-client-sdk-ergonomics.md](../02-guides/06-client-sdk-ergonomics.md) |
| FAQ | [../06-faq/](../06-faq/) |
| Troubleshooting | [../07-troubleshooting/](../07-troubleshooting/) |
| Examples | [../08-examples/](../08-examples/) |
| Architecture, storage, and ADRs | [architecture](../04-architecture/README.md), [storage topology](../04-architecture/04-storage-topology.md) |
| Production operations | [../05-operations/README.md](../05-operations/README.md) |
| First run | [../00-onboarding/02-local-setup.md](../00-onboarding/02-local-setup.md) |
| Commands, env, Alembic | [../03-reference/01-commands.md](../03-reference/01-commands.md) |
| Physical schema (tables/columns) | [../03-reference/02-storage-contract.md](../03-reference/02-storage-contract.md) |
| Old design proposals | [formats](../03-reference/03-formats.md), [storage](../03-reference/04-storage.md), [HTTP](../03-reference/05-http-api.md) — not normative |
| Docs contents | [../README.md](../README.md) |

Knowledge graph: `graphify-out/` (local, gitignored).
