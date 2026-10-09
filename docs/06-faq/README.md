# FAQ

[Documentation](../README.md) › **FAQ**

Answers to common misconceptions about Workhold. One question — one file.
The page stands on its own: the mechanism, the example, and "what not to do" live here,
not behind a link. Links at the bottom of the page are only when you need deeper
architecture or an ADR.

The reading path is ≈15 minutes — in [reading-path.md](../00-onboarding/01-reading-path.md).

| # | Question | File |
| --- | --- | --- |
| 1 | Why Workhold if RabbitMQ or Kafka is already there? | [01-why-not-rabbitmq-kafka.md](01-why-not-rabbitmq-kafka.md) |
| 2 | Is this a shared platform bus? | [02-shared-platform-bus.md](02-shared-platform-bus.md) |
| 3 | What is a named queue? | [03-what-is-named-queue.md](03-what-is-named-queue.md) |
| 4 | Does the application need its own database? | [04-application-db.md](04-application-db.md) |
| 5 | Is there exactly-once? | [05-exactly-once.md](05-exactly-once.md) |
| 6 | Does Workhold store the business result? | [06-business-result.md](06-business-result.md) |
| 7 | Is this a workflow/DAG engine? | [07-workflow-dag.md](07-workflow-dag.md) |
| 8 | What is a follow-up / spawn? | [08-what-is-follow-up.md](08-what-is-follow-up.md) |
| 9 | How does `spawn[]` differ from `events[]`? | [09-spawn-vs-events.md](09-spawn-vs-events.md) |
| 10 | How does `pause` differ from `drain`? | [10-pause-vs-drain.md](10-pause-vs-drain.md) |
| 11 | Can you enqueue into a queue that does not exist? | [11-missing-named-queue.md](11-missing-named-queue.md) |
| 12 | Why are `claim_id` and `claim_token` separate? | [12-claim-id-vs-claim-token.md](12-claim-id-vs-claim-token.md) |
| 13 | What happens if the worker dies? | [13-worker-dies.md](13-worker-dies.md) |
| 14 | Who makes the outbound HTTP call — the queue or the application? | [14-who-sends-outbound-http.md](14-who-sends-outbound-http.md) |
| 15 | How do you send one event to X and another to Y? | [15-events-to-two-services.md](15-events-to-two-services.md) |

---

← [Contents](../README.md) · [Troubleshooting](../07-troubleshooting/README.md)
