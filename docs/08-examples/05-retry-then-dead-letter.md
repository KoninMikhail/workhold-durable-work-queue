# Retry, then dead letter

[Documentation](../README.md) › [Examples](README.md) › **Retry / DLQ**

**Who:** the worker reports failure_code; named queue policy bounds attempts.

**What queue-service stores:** attempt history, delayed availability, then dead letter
on exhaustion or disabled retry.

**What is idempotent in the app:** a repeated effect on at-least-once reclaim.

Steps: [worker-claim-complete.md](../02-guides/03-worker-claim-complete.md).
