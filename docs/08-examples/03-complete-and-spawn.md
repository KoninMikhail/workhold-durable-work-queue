# Complete atomically creates spawn[]

[Documentation](../README.md) › [Examples](README.md) › **spawn[]**

**Who:** the `orders` named queue worker after successful order processing.

**What workhold stores:** the original task in `orders` → succeeded and a new
follow-up task in `billing` in one transaction (`spawn[]`).

**What is idempotent in the app:** a repeat complete with the same claim and the same body
does not create a second billing-task.

## Scenario

The instance has two named queues: `orders` and `billing`. Complete of a task in
`orders` enqueues a new task in `billing`. The `orders` worker does not wait for billing.

1. Producer: enqueue into `orders` with payload `{"order_id":"42"}`.
2. Worker `orders`: claim, processing, complete with `spawn[]` into another
   named queue — `billing`.
3. Worker `billing`: claim this new task as ordinary work.

`generation` is taken from the claim response. The `events` field on complete is still
reserved and is not placed in the body.

```bash
curl -sS -X POST \
  "https://queue.example/v1/claims/{claim_id}:complete" \
  -H "Authorization: Bearer <secret>" \
  -H "X-Queue-Claim-Token: CLAIM_TOKEN_FROM_RESPONSE" \
  -H "Content-Type: application/json" \
  -d '{
    "generation": 1,
    "spawn": [
      {
        "queue_name": "billing",
        "idempotency_key": "order-42-invoice",
        "payload": {"order_id": "42"},
        "priority": 0
      }
    ]
  }'
```

What a follow-up is: [follow-up.md](../01-concepts/05-follow-up.md).
Claim/complete steps: [worker-claim-complete.md](../02-guides/03-worker-claim-complete.md).
