# Concepts

[Documentation](../README.md) › **Concepts**

How the product is structured at the level of meaning — without schema and API.

Start with the [reading path](../00-onboarding/01-reading-path.md) (~15 minutes).

| # | File | About |
| --- | --- | --- |
| 1 | [01-overview.md](01-overview.md) | Why Workhold exists, how it is delivered, how it differs from v1 |
| 2 | [02-why-queue.md](02-why-queue.md) | Comparison with RabbitMQ and Kafka; when they complement / compete |
| 3 | [03-how-it-works.md](03-how-it-works.md) | End-to-end lifecycle enqueue → claim → complete / spawn / events |
| 4 | [04-named-queues.md](04-named-queues.md) | Named queue: the name of a task stream inside an instance |
| 5 | [05-follow-up.md](05-follow-up.md) | Follow-up / spawn: the next task on complete, not a subtask |
| 6 | [06-glossary.md](06-glossary.md) | Canonical terms for Work Queue, spawn, event, lease, and outbox |
| 7 | [07-product-boundary.md](07-product-boundary.md) | What the product guarantees, what it owns, and what it is not |
| 8 | [08-use-cases.md](08-use-cases.md) | Scenarios for producer, worker, relay, operator, and the app-local bridge |
| 9 | [09-guarantees.md](09-guarantees.md) | What workhold promises on each stretch of the path |
| 10 | [10-transactional-outbox.md](10-transactional-outbox.md) | Publication pattern: dual-write, relay, what it provides |
| 11 | [11-inbox.md](11-inbox.md) | Reception pattern: redelivery, idempotency |
| 12 | [12-delivery-outbox.md](12-delivery-outbox.md) | Who sends HTTP, the CloudEvents contract, two recipients X and Y |
| — | [FAQ](../06-faq/README.md) | Typical questions (one question — one file) |

---

← [Contents](../README.md) · [Reading path](../00-onboarding/01-reading-path.md) · [Overview](01-overview.md) →
