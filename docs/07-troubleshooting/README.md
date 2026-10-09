# Troubleshooting

[Documentation](../README.md) › **Troubleshooting**

One symptom, one file. On the page: what you see, why the service is built
this way, what to do step by step, and what not to do. This is not a bug
report in an ADR: diagnose the symptom here first.

The ≈15-minute reading path is in [reading-path.md](../00-onboarding/01-reading-path.md).

| # | File | Symptom |
| --- | --- | --- |
| 1 | [01-enqueue-response-lost.md](01-enqueue-response-lost.md) | Enqueue response lost |
| 2 | [02-idempotency-key-conflict.md](02-idempotency-key-conflict.md) | Idempotency key conflict |
| 3 | [03-lease-lost.md](03-lease-lost.md) | lease_lost on heartbeat/complete |
| 4 | [04-side-effect-then-lease-lost.md](04-side-effect-then-lease-lost.md) | Side effect already happened, lease lost |
| 5 | [05-complete-response-lost.md](05-complete-response-lost.md) | Complete response lost |
| 6 | [06-paused-empty-claim.md](06-paused-empty-claim.md) | Pause: empty claim |
| 7 | [07-draining-enqueue-rejected.md](07-draining-enqueue-rejected.md) | Drain: new enqueue rejected |
| 8 | [08-dead-letter.md](08-dead-letter.md) | Task in dead letter |
| 9 | [09-cancel-vs-complete.md](09-cancel-vs-complete.md) | Cancel and complete race |
| 10 | [10-relay-duplicate-publish.md](10-relay-duplicate-publish.md) | Duplicate delivery publish |

---

← [FAQ](../06-faq/README.md) · [Examples](../08-examples/README.md)
