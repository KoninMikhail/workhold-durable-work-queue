# A DB-less application enqueues work

[Documentation](../README.md) › [Examples](README.md) › **DB-less**

**Who:** a producer and several worker replicas without their own business DB.
The queue store is workhold PostgreSQL; a client database is not created.

**What workhold stores:** named-queue tasks in the Work Queue, fenced lease, attempts;
at-least-once redelivery after lease loss.

**What is idempotent in the app:** the handler of the external effect (inbox or natural key).

Install:

```bash
pip install workhold-producer
pip install workhold-consumer
# optional async
pip install "workhold-producer[async]" "workhold-consumer[async]"
```

```python
from workhold_producer import ProducerClient, HttpJsonTransport
from workhold_consumer import ConsumerClient

producer = ProducerClient(HttpJsonTransport("https://queue.example"), bearer_token=producer_secret)
worker = ConsumerClient(HttpJsonTransport("https://queue.example"), bearer_token=worker_secret)
```

Pass **explicit** role bearer tokens (no auth provider). Worker claim uses a
separate consumer credential — not the producer token. Ergonomics:
[06-client-sdk-ergonomics.md](../02-guides/06-client-sdk-ergonomics.md).

Steps: [integrate-application.md](../02-guides/01-integrate-application.md),
[producer-enqueue.md](../02-guides/02-producer-enqueue.md),
[worker-claim-complete.md](../02-guides/03-worker-claim-complete.md).
