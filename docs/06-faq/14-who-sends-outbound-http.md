# Who makes the outbound HTTP call — the queue or the application?

[Documentation](../README.md) › [FAQ](README.md) › **who sends HTTP**

**In short.** It depends on whether this is **the work of the task** or a **notification
after complete**. The application's worker does the work, into the service that
it knows. The `relay` role of the same image sends the notification, to one webhook
from the deploy. The queue is not an HTTP proxy and does not read a URL from the payload.

## Three different calls

They are easy to mix into one "someone calls somewhere".

| What happens | Who sends the HTTP call | Where |
| --- | --- | --- |
| Process the task: a parser, someone else's API, disk | the application's worker | the service that this worker knows |
| Tell the outside that the task finished | the `relay` role | one webhook of the deploy, CloudEvents |
| Enqueue / claim / close a task | producer / worker | the queue-service API |

While the worker holds the lease, the external effect is its own. `relay` is not
needed for that. Complete without `events[]` is normal: "the work is done, and there is
nobody to notify".

There is no need to write a separate application that "reads the Outbox and publishes".
The `relay` process of the same image publishes (`api` + `relay` + `migrate` /
`maintain`). Another process is needed only for the app-local outbox: the bridge from
the business database *into* enqueue, not outward.

## Why the queue itself does not call X

If the recipient URL lives in the payload or in the event, the task itself says
where to hit. That is SSRF: a compromised producer points the relay at
internal addresses. Therefore:

- where to send notifications is set by the operator (`QUEUE_DELIVERY_WEBHOOK_URL`
  and the allowlist), not by the worker and not by the event body;
- how to send is set by the deploy (HTTPS, timeout, bearer, TLS, circuit breaker);
- what is in the body is CloudEvents: the application sets `type` / `source`,
  and the queue assigns `id`.

`type` is routing by meaning ("the order is paid"), not a URL.

## How to choose

| You need | Who calls |
| --- | --- |
| Charge money / call MinIO / call an internal API | the worker inside the lease, or `spawn[]` into the queue whose worker does it |
| Tell billing and the notifier that the order is closed | `events[]` → relay → your sink → they then call the services |
| Simply close the task | complete, with no outbound HTTP from the queue |

The relay itself does not split two different recipients:
[15-events-to-two-services.md](15-events-to-two-services.md).

Public HTTP complete does not accept `events[]` yet (`delivery_events`
= false). The field is reserved. The current path for the work is the worker plus `spawn[]`.
