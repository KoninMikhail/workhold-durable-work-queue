# Application outbox bridge (producer package)

[Documentation](../README.md) › [Guides](README.md) › **Outbox bridge**

How an application with a business database delivers intents from the **app-local outbox** into
workhold through the bridge in the `workhold-producer` package.

Normative semantics: [10-application-outbox-bridge.md](../04-architecture/10-application-outbox-bridge.md).
Operator view: [04-application-outbox-bridge.md](../05-operations/04-application-outbox-bridge.md).

## Install

```bash
# HTTP producer only
pip install workhold-producer

# Bridge + tested Postgres driver for injected PEP 249 factories
pip install "workhold-producer[bridge-postgres]"
```

| Extra | What it adds |
| --- | --- |
| *(none)* | `ProducerClient` + bridge modules; `PostgresOutboxStore` stays driver-injected |
| `bridge-postgres` | `psycopg[binary]` for apps that want the documented factory helper |

Import root: `workhold_producer.bridge` (not `queue_service_client.bridge`).

## Credentials

Bridge uses a **producer** bearer token for:

- `POST /v1/queues/{queue_name}/tasks` (`enqueueTask`)
- `POST /v1/queues/{queue_name}/submissions:resolve` (`resolveSubmission`)
- `GET /v1/tasks/{task_id}` (`getTask`) when inspecting after delivery
- `GET /v1/capabilities` (`getCapabilities`) for capability gates

Do **not** configure admin or worker tokens on the bridge process. Admin
control-plane and claim/complete belong to other role clients.

## Minimal wiring

```python
from workhold_producer import HttpJsonTransport, ProducerClient
from workhold_producer.bridge import BridgeRunner
from workhold_producer.bridge.postgres_store import PostgresOutboxStore

producer = ProducerClient(
    HttpJsonTransport("https://queue.example"),
    bearer_token="...",  # PRODUCER only
)
store = PostgresOutboxStore(connect_factory=...)  # PEP 249; driver via extra
runner = BridgeRunner(store=store, producer=producer, ...)
runner.run_forever()
```

Idempotency keys remain `bridge:v1:…` (byte-stable). Crash-after-enqueue
replay must still yield one Queue task for the same outbox row.

## Migration from `queue-client`

1. Depend on `workhold-producer` (add `[bridge-postgres]` if you used the
   Postgres store convenience).
2. Replace imports `queue_service_client.bridge` → `workhold_producer.bridge`.
3. Replace `queue_service_client.ProducerClient` → `workhold_producer.ProducerClient`.
4. Keep OpenAPI paths and intent schema major 1; no admin token in bridge config.

## Next

- Producer enqueue: [02-producer-enqueue.md](02-producer-enqueue.md)
- Choose DB-less vs bridge: [01-integrate-application.md](01-integrate-application.md)
- Example: [02-business-db-bridge.md](../08-examples/02-business-db-bridge.md)
