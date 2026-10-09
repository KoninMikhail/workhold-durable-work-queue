# Guides

[Documentation](../README.md) › **Guides**

Practical how-to: enqueue, claim, admin named queues, and choosing an integration. These are step recipes, not FAQ, troubleshooting, or narrative examples.

Start with the [reading path](../00-onboarding/01-reading-path.md) (~15 minutes).

| # | File | About |
| --- | --- | --- |
| 1 | [01-integrate-application.md](01-integrate-application.md) | DB-less enqueue versus app-local outbox bridge |
| 2 | [02-producer-enqueue.md](02-producer-enqueue.md) | Enqueue a task on a named queue, idempotency key, inspect |
| 3 | [03-worker-claim-complete.md](03-worker-claim-complete.md) | Claim, heartbeat, complete / fail / `ack_cancel` |
| 4 | [04-admin-queues.md](04-admin-queues.md) | Create a queue, pause / drain, policy version |
| 4a | [04-admin-operations.md](04-admin-operations.md) | AdminClient recovery + BreakGlassClient (separate tokens) |
| 5 | [05-app-outbox-bridge.md](05-app-outbox-bridge.md) | Install extras, bridge imports, producer-only credentials |
| 6 | [06-client-sdk-ergonomics.md](06-client-sdk-ergonomics.md) | Sync/async install, bearer tokens, TLS/timeouts, pagination, retry, instrumentation, test kit, codecs |

---

← [Contents](../README.md) · [First integration](01-integrate-application.md) →
