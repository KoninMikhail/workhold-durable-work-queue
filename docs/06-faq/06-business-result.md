# Does Workhold store the business result of a task?

[Documentation](../README.md) › [FAQ](README.md) › **Business result**

**In short.** No. queue-service stores the operational state of the task
(attempts, the attempt outcome, lineage) and is not a store or an API
of the application's business result. The processing result lives with the application.

## What the queue stores, and what it does not

| queue-service stores | Does not store |
| --- | --- |
| That the task succeeded / dead-lettered / cancelled | The invoice amount, the report text, the file URL |
| Which `failure_code` the worker wrote | A decoding of "why the business considers this an error" |
| Payload as opaque JSON | The schema and meaning of payload fields |
| Who spawned whom (lineage) | An ownership tree where "the parent waits for the children" |
| Protocol result complete (replayable metadata) | A UI answer of "here is the result of the work" |

The payload is application data. The queue stores it and hands it to the worker, but does not
read it as business meaning and does not expose it outward as "the result of the work" for
reports and search.

If an invoice number is needed after the order is processed, the worker writes it to the
business database, object storage, or another service owned by the
application. In complete you can put `spawn[]` (the next piece of work) or
`events[]` (an intent to notify). That is not "save the result in the queue".

## Why

Otherwise queue-service turns into the application's database: a result schema,
search, permissions, retention "like the domain". That breaks the product boundary and
pulls parser fields such as `minio_path` / `job_kind` back into the core.

Retention of attempt history and terminal outcomes is operational
(tens of days), not an archive of business data.

## Common mistakes

| Mistake | What to do instead |
| --- | --- |
| "I'll read complete and show the amount in the UI" | Store the amount in your own store; the queue is only the status of the work |
| "I'll put the PDF in the complete payload" | The worker writes the PDF where the content lives; the queue holds a link in the payload of the next task or event |
| "The queue will validate the result schema" | A schema registry and payload validation are outside the product |

---

Product boundary: [product-boundary.md](../01-concepts/07-product-boundary.md).
