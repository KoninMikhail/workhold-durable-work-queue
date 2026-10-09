# Cooperative cancel of a leased task

[Documentation](../README.md) › [Examples](README.md) › **Cancel**

**Who:** the producer requests cancel; the worker confirms `ack_cancel`.

**What queue-service stores:** cancel requested on the leased task; terminal cancelled without
spawn[] / events[].

**What is idempotent in the app:** checkpoints respect cancellation on heartbeat.

Steps: [worker-claim-complete.md](../02-guides/03-worker-claim-complete.md),
[producer-enqueue.md](../02-guides/02-producer-enqueue.md).
