# Glossary

[Documentation](../README.md) › [Concepts](README.md) › **Glossary**

Canonical product terms. API field names and physical tables are set later in the reference documentation.

## Service and deploy

| Term | Meaning |
| --- | --- |
| Workhold | Product name: **Workhold - Durable work queue**. Per-application durable Work Queue plus Delivery Outbox in this repository |
| queue-service | Runtime and package name of Workhold (`queue_service` import / distribution) |
| queue-service instance | One queue-service deploy for one application trust boundary, on queue-service-owned PostgreSQL |
| queue-service store | The required queue-service PostgreSQL: tasks, leases, Delivery Outbox; not the application's business DB |
| Business DB | Optional application DB; needed only if there is business state committed together with the enqueue intent |
| Service principal | Authenticated identity of a producer, worker, relay, observer, or admin |

## Work and queues

| Term | Meaning |
| --- | --- |
| Named queue | A named task stream inside one instance: the producer enqueues a task under a specific name, and the worker claims only the queues it can process. Not the whole service, not a pub/sub topic, and not a separate deploy. See [04-named-queues.md](04-named-queues.md) |
| named-queue state | Runtime mode `active`, `paused`, or `draining` with explicit intake/claim semantics |
| Task | One durable unit of work for one logical handler |
| Payload | The application's JSON body, which queue-service stores without interpreting business fields |
| Task metadata | queue-service-visible routing and operations fields outside the payload |
| Priority | A queue-service-visible scheduling value; default `0`, range `-32768..32767`; a higher number is earlier among due tasks |
| Available at | A timestamp in the queue-service-store before which the task cannot be claimed; the initial enqueue is "now" |
| Idempotency key | The producer's stable key: a repeat enqueue returns the original task |

## Claim and lease

| Term | Meaning |
| --- | --- |
| Claim | Successful acquisition of one task by a worker |
| Claim ID | Public identifier for lease resource paths and correlation |
| Lease | The claim holder's time-limited right to heartbeat or finish the task |
| Claim token | A secret header capability, separate from the claim ID; fenced mutations of the lease |
| Claimed at | A timestamp in the queue-service-store when the current attempt captured the task |
| Worker ID | Stable diagnostic identity of the worker replica that made the claim |
| Attempt | Append-only record of claim token, generation, `claimed_at`, worker ID, and outcome |

## Outcomes and policy

| Term | Meaning |
| --- | --- |
| Failure code | A stable machine-readable identifier of an attempt failure; detail is diagnostic text |
| Retry policy | Versioned named queue configuration: retry disabled, or attempt and backoff limits |
| Retryable failure | An attempt outcome after which the task may become available again |
| Dead letter | A task parked after a non-retryable failure or after the retry policy is exhausted |
| Dead-lettered | Terminal state of a task or delivery-event associated with a dead letter |
| Terminal task | A task that is no longer claimed: succeeded, dead-lettered, or cancelled |
| Protocol result | Replayable queue-service metadata from a terminal command; not the application's business result |
| Admission control | Hard and soft limits that reject unsafe work before PostgreSQL is exhausted |

## Delivery and integration

| Term | Meaning |
| --- | --- |
| Spawn | Follow-up task created atomically on complete of another task. See [05-follow-up.md](05-follow-up.md) |
| Event | An outbound record for delivery outside the Work Queue |
| Delivery outbox | The queue-service-owned log of events awaiting asynchronous relay |
| Delivery relay | The process that claims Delivery Outbox records and publishes them to an external channel. The first adapter is one HTTP webhook per instance; the event does not choose the URL. See [12-delivery-outbox.md](12-delivery-outbox.md) |
| App-local outbox | An outbox in the application's business DB, written together with the business change |
| Integration bridge | A relay from the app-local outbox into an idempotent queue-service enqueue |
| Inbox | A consumer record of already applied message IDs, committed together with the business effect |

## Terms that are intentionally distinguished

| This | Do not confuse with |
| --- | --- |
| **spawn** — work for one competing consumer | **event** is delivered by the Delivery Outbox |
| **claim token** protects queue-service state | It does not make external side effects exactly-once |
| **queue-service store** — the service's required PostgreSQL | **Business DB** — the client's optional DB; queue tables do not live in it |
| **Delivery Outbox** | **app-local outbox**. They have different transaction boundaries and owners |
| Named queue | A pub/sub topic. One logical worker processes one task, with possible at-least-once redelivery |

## Do not use

| Do not write | Why |
| --- | --- |
| `job` as a canonical term | It carries the parser-v1 meaning |
| `outbox task` | Write `spawn` or `event` |
| `exactly-once processing` | queue-service does not provide this across external effects |
| `global bus` | queue-service is deployed per application |

---

← [Follow-up](05-follow-up.md) · [Product boundary](07-product-boundary.md) →
