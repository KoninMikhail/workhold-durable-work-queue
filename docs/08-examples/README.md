# Examples

[Documentation](../README.md) › **Examples**

Application usage scenarios. Steps are in [02-guides](../02-guides/README.md). Start with the [reading path](../00-onboarding/01-reading-path.md).

| # | File | Scenario |
| --- | --- | --- |
| 1 | [01-dbless-app.md](01-dbless-app.md) | Application without a database + competing workers |
| 2 | [02-business-db-bridge.md](02-business-db-bridge.md) | App-local outbox → bridge enqueue |
| 3 | [03-complete-and-spawn.md](03-complete-and-spawn.md) | Order in `orders` → follow-up invoice in `billing` |
| 4 | [04-complete-and-events.md](04-complete-and-events.md) | Complete + events[]: one webhook, two `type` values (X and Y) |
| 5 | [05-retry-then-dead-letter.md](05-retry-then-dead-letter.md) | Retry → dead letter |
| 6 | [06-cooperative-cancel.md](06-cooperative-cancel.md) | Cooperative cancel |
| 7 | [07-named-queue-catalog.md](07-named-queue-catalog.md) | Mount + `workhold apply` catalog ensure-exists |
| 8 | [08-consumer-long-polling.md](08-consumer-long-polling.md) | Bounded claim long polling (capability-gated) |

---

← [Troubleshooting](../07-troubleshooting/README.md) · [Contents](../README.md)
