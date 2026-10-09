# Example: consumer long polling

[Documentation](../README.md) › [Examples](README.md) › **Long polling**

Bounded claim wait for a worker without busy polling. This is **not** push delivery and
**not** exactly-once: wake through PostgreSQL LISTEN/NOTIFY — only a hint;
at-least-once and fencing are preserved.

## Capability

Authenticated `GET /v1/capabilities`:

- `long_polling=true`
- `max_wait_seconds=20`
- `batch_claim=false` / `max_claim_tasks=1`

## Raw claim

```bash
curl -sS -X POST "https://queue.example/v1/claims" \
  -H "Authorization: Bearer <worker-secret>" \
  -H "Content-Type: application/json" \
  -d '{
    "queues": ["orders"],
    "max_tasks": 1,
    "lease_seconds": 60,
    "wait_seconds": 15,
    "worker_id": "pool-a/replica-1"
  }'
```

- `wait_seconds=0` — immediate claim;
- `1..20` — bounded wait; empty expiry → HTTP 200 `tasks: []`;
- client read budget = wait+5, total = wait+10;
- reverse-proxy upstream idle/response timeout ≥ 30 s.

## SDK

```bash
pip install workhold-consumer
pip install "workhold-consumer[async]"
```

Sync: `ConsumerClient.claim(..., wait_seconds=15)` / `ConsumerSupervisor` after
capability guard. Async: `AsyncConsumerClient` /
`AsyncConsumerSupervisor` from `workhold_consumer.async_*` modules.
Default supervisor wait = **15** after one-time preflight (`wait_seconds=0`
disables long poll). Explicit WORKER bearer token; see
[06-client-sdk-ergonomics.md](../02-guides/06-client-sdk-ergonomics.md).

## Operator notes

See [deployment — Claim long polling](../05-operations/02-deployment.md#claim-long-polling-work-17):
one listener connection per API replica, default 64 waiters, fallback 1 s.
