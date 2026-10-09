# Complete writes events[] to the Delivery Outbox

[Documentation](../README.md) › [Examples](README.md) › **events[]**

**Who:** worker + Delivery Relay.

**What workhold stores:** `events[]` as pending Delivery Outbox records together with
complete; the relay publishes at-least-once after commit.

**What is idempotent in the app:** downstream by event ID (inbox).

## One webhook, two meanings

The relay does not send “this event to X, that one to Y”. Both POSTs go to one sink.
A different `type` lets the sink decide whom to call next.

```text
worker complete
  events:
    - type: com.app.order.billed     data: { "order_id": "42" }
    - type: com.app.order.notified   data: { "order_id": "42" }
            ↓
relay → POST https://sink.example/hooks/queue   (one deployment URL)
            ↓
sink: billed → service X, notified → service Y
```

If X and Y are **work** with retries like tasks, not Outbox: `spawn[]` into
two named queues. See [complete-and-spawn.md](03-complete-and-spawn.md).

Meaning and contract: [12-delivery-outbox.md](../01-concepts/12-delivery-outbox.md).
Worker steps: [worker-claim-complete.md](../02-guides/03-worker-claim-complete.md).
Public HTTP complete does not accept `events[]` yet (`delivery_events` is disabled).
