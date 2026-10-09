# A DB-less application enqueues work

[Documentation](../README.md) › [Examples](README.md) › **DB-less**

**Who:** a producer and several worker replicas without their own business DB.
The queue store is queue-service PostgreSQL; a client database is not created.

**What queue-service stores:** named-queue tasks in the Work Queue, fenced lease, attempts;
at-least-once redelivery after lease loss.

**What is idempotent in the app:** the handler of the external effect (inbox or natural key).

Install:

```bash
pip install queue-service-producer
pip install queue-service-consumer
# optional async
pip install "queue-service-producer[async]" "queue-service-consumer[async]"
```

```python
from queue_service_producer import ProducerClient, HttpJsonTransport
from queue_service_consumer import ConsumerClient

producer = ProducerClient(HttpJsonTransport("https://queue.example"), bearer_token=producer_secret)
worker = ConsumerClient(HttpJsonTransport("https://queue.example"), bearer_token=worker_secret)
```

Pass **explicit** role bearer tokens (no auth provider). Worker claim uses a
separate consumer credential — not the producer token. Ergonomics:
[06-client-sdk-ergonomics.md](../02-guides/06-client-sdk-ergonomics.md).

Steps: [integrate-application.md](../02-guides/01-integrate-application.md),
[producer-enqueue.md](../02-guides/02-producer-enqueue.md),
[worker-claim-complete.md](../02-guides/03-worker-claim-complete.md).
