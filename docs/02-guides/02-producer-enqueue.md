# Producer: enqueue a task

[Documentation](../README.md) › [Guides](README.md) › **Producer**

How an application enqueues work on a named queue and inspects its own task.
Paths and headers come from the current OpenAPI (`openapi/queue.openapi.json`), not from
the superseded `http-api.md`.

## 1. Choose a named queue and a key

1. The named queue is already created by admin (see [04-admin-queues.md](04-admin-queues.md)).
   Enqueue does not create a queue “by typo”.
2. Take a stable **idempotency key** for one business enqueue attempt.
3. The payload is opaque application JSON. queue-service does not interpret it.

## 2. Enqueue

`POST /v1/queues/{queue_name}/tasks`

Required header: `Idempotency-Key`.

Request body: `payload` and `priority` are required. The `available_at` field sets **one-shot** delayed
availability; cron, calendar recurrence, and per-task retry override are out of scope.

### `priority` — bounded static priority

| Rule | Behavior |
| --- | --- |
| default | `0` if the field is omitted (see OpenAPI) |
| range | inclusive `-32768` … `32767` (signed `smallint`) |
| type | strict integer; strings, fractions, and implicit coercion → `validation_failed` |
| polarity | **Higher numeric value → higher priority** only among **due** candidates; not named bands |
| due gate | due eligibility `(delayed\|ready AND available_at <= store time) OR (leased AND lease expired)` is **before** sorting by `priority` |

A repeated `enqueue` with the same `Idempotency-Key` but a different normalized body (including `priority`) → `idempotency_conflict`.

Application-outbox intent schema **major 1** passes `priority` in `enqueue_request`; intents with `priority: 0` stay valid after capability `priority=true` is activated (see [bridge rollout](../04-architecture/10-application-outbox-bridge.md#capability-rollout-priority)).

### `available_at` — semantics

Authoritative time is **queue-service-store** (`transaction_timestamp()` in PostgreSQL),
not the client or worker clock.

| Value | Behavior |
| --- | --- |
| omitted / JSON `null` | queue-service-store “now” at commit; the task is immediately claimable (`ready`) |
| aware past or current | Immediate availability (equivalent to “now”) |
| aware future within the horizon | Task stays `delayed` until `available_at`; claim without a promotion job |
| naive datetime | `validation_failed`, non-retryable |
| future beyond the horizon | `validation_failed`, non-retryable |

Deployment sets the horizon: `QUEUE_SCHEDULE_HORIZON_SECONDS` (default and
absolute maximum **86400** seconds; allowed range **0..86400**).
A deployment may only **tighten** the ceiling, not expand it.

### Install

```bash
# Sync producer (HTTP only — no PostgreSQL driver, no httpx)
pip install queue-service-producer

# Async producer (optional httpx via core[async])
pip install "queue-service-producer[async]"

# App-local outbox bridge that needs a concrete Postgres driver
pip install "queue-service-producer[bridge-postgres]"
```

Import package: `queue_service_producer` (not the removed `queue-client` /
`queue_service_client` prototype). Pass an explicit **producer** bearer token —
no auth-provider discovery; admin/worker tokens belong to other role clients.

TLS / timeouts: `ClientConfig` (`verify_tls=True` by default, optional
`ca_cert_path`, connect/read/total budgets). Shared ergonomics (retry,
instrumentation redaction, codecs, capability guards, test kit):
[06-client-sdk-ergonomics.md](06-client-sdk-ergonomics.md).

### Migration from `queue-client`

| Was | Now |
| --- | --- |
| `pip install queue-client` | `pip install queue-service-producer` |
| `from queue_service_client import ProducerClient` | `from queue_service_producer import ProducerClient` |
| `queue_service_client.bridge` | `queue_service_producer.bridge` (+ optional `[bridge-postgres]`) |

Paths stay OpenAPI-authoritative: `POST /v1/queues/{queue_name}/tasks`,
`POST /v1/queues/{queue_name}/submissions:resolve`, `GET /v1/tasks/{task_id}`,
`POST /v1/tasks/{task_id}:cancel`, `GET /v1/capabilities`.

### `queue-service-producer` (aware `datetime`)

```python
from datetime import datetime, timezone, timedelta
from queue_service_producer import HttpJsonTransport, ProducerClient

client = ProducerClient(
    HttpJsonTransport("https://queue.example"),
    bearer_token="...",  # PRODUCER credential — never admin
)

# Immediate — omit or None
client.enqueue("orders", idempotency_key="k1", payload={"order_id": "42"})

# Delay by 30 minutes (aware UTC)
run_at = datetime.now(timezone.utc) + timedelta(minutes=30)
client.enqueue(
    "orders",
    idempotency_key="k2",
    payload={"order_id": "43"},
    available_at=run_at,
)

# Optional nonzero priority (default 0)
client.enqueue(
    "orders",
    idempotency_key="k3",
    payload={"order_id": "44"},
    priority=100,
)
```

The SDK requires a timezone-aware `datetime` (`tzinfo` and `utcoffset()` are not `None`)
and serializes RFC 3339. A naive value is rejected on the client before HTTP.

### HTTP (RFC 3339)

```bash
curl -sS -X POST "https://queue.example/v1/queues/orders/tasks" \
  -H "Authorization: Bearer <secret>" \
  -H "Idempotency-Key: order-42-place" \
  -H "Content-Type: application/json" \
  -d '{"payload":{"order_id":"42"},"priority":0}'

# Delayed enqueue (aware RFC 3339)
curl -sS -X POST "https://queue.example/v1/queues/orders/tasks" \
  -H "Authorization: Bearer <secret>" \
  -H "Idempotency-Key: order-43-delayed" \
  -H "Content-Type: application/json" \
  -d '{"payload":{"order_id":"43"},"priority":0,"available_at":"2026-09-20T12:00:00+00:00"}'

# Nonzero priority (optional)
curl -sS -X POST "https://queue.example/v1/queues/orders/tasks" \
  -H "Authorization: Bearer <secret>" \
  -H "Idempotency-Key: order-44-urgent" \
  -H "Content-Type: application/json" \
  -d '{"payload":{"order_id":"44"},"priority":100}'
```

### Application-outbox bridge

Bridge intent schema (major **1**) passes `available_at` as a JSON **string**
or **null** from the immutable app-outbox row — not as a Python `datetime`.
The producer SDK and the bridge are different serialization boundaries; queue-service validates aware
RFC 3339 at the HTTP boundary.

The response after commit contains `task` and the `replayed` flag:

- the same key and the same body → the original task, `replayed: true`;
- the same key and a different body → rejection (fingerprint conflict).

Replay idempotency is guaranteed within a bounded window (see OpenAPI /
the dedup TTL ADR).

## 3. Inspect your own task

`GET /v1/tasks/{task_id}`

Substitute `task_id` from the enqueue response:

```bash
curl -sS "https://queue.example/v1/tasks/{task_id}" \
  -H "Authorization: Bearer <secret>"
```

The producer sees operational status and lineage, not the “business result”
of processing. The application stores the work result.

## 4. Cancellation (if needed)

The producer can request cancellation through the colon-form cancel operation in
OpenAPI (`{task_id}:cancel` on the task resource). A delayed/ready task
becomes terminal immediately; a leased task becomes terminal through cooperative cancellation on the
worker side (`ack_cancel`).

## Explicitly out of scope

- cron / calendar recurrence;
- per-task retry override;
- scheduling Delivery Outbox `events[]`.

## Next

- Worker lifecycle: [03-worker-claim-complete.md](03-worker-claim-complete.md)
- Meaning without HTTP: [how-it-works.md](../01-concepts/03-how-it-works.md)
- Guarantees: [guarantees.md](../01-concepts/09-guarantees.md)
