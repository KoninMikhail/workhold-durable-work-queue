# Business DB and the app-local outbox bridge

[Documentation](../README.md) › [Examples](README.md) › **Business DB**

**Who:** an application with a business transaction and a bridge.

**What workhold stores:** one task per outbox row through a deterministic
idempotency key; enqueue is eventual.

**What is idempotent in the app:** mark delivered outbox only after durable enqueue.

## Install

```bash
pip install "workhold-producer[bridge-postgres]"
pip install "workhold-producer[async]"   # optional
```

Base `workhold-producer` is enough if you inject your own PEP 249 factory
without the documented Postgres driver extra.

## Imports

```python
from workhold_producer import ProducerClient
from workhold_producer.bridge import BridgeRunner
from workhold_producer.bridge.postgres_store import PostgresOutboxStore
```

Use a **producer** bearer token only. Bridge calls OpenAPI
`POST /v1/queues/{queue_name}/tasks` (and resolve/inspect/capabilities as needed);
it does not use admin credentials.

Steps: [integrate-application.md](../02-guides/01-integrate-application.md),
[05-app-outbox-bridge.md](../02-guides/05-app-outbox-bridge.md).
