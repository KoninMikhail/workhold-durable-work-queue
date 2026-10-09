# Business DB and the app-local outbox bridge

[Documentation](../README.md) › [Examples](README.md) › **Business DB**

**Who:** an application with a business transaction and a bridge.

**What queue-service stores:** one task per outbox row through a deterministic
idempotency key; enqueue is eventual.

**What is idempotent in the app:** mark delivered outbox only after durable enqueue.

## Install

```bash
pip install "queue-service-producer[bridge-postgres]"
pip install "queue-service-producer[async]"   # optional
```

Base `queue-service-producer` is enough if you inject your own PEP 249 factory
without the documented Postgres driver extra.

## Imports

```python
from queue_service_producer import ProducerClient
from queue_service_producer.bridge import BridgeRunner
from queue_service_producer.bridge.postgres_store import PostgresOutboxStore
```

Use a **producer** bearer token only. Bridge calls OpenAPI
`POST /v1/queues/{queue_name}/tasks` (and resolve/inspect/capabilities as needed);
it does not use admin credentials.

Steps: [integrate-application.md](../02-guides/01-integrate-application.md),
[05-app-outbox-bridge.md](../02-guides/05-app-outbox-bridge.md).
