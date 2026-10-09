# How does spawn[] differ from events[]?

[Documentation](../README.md) › [FAQ](README.md) › **spawn vs events**

**In short.** Both appear on a successful complete, in one
queue-service transaction, but they are different resources. `spawn[]` is the next piece of work for a
worker. `events[]` is an intent to notify outward through the Delivery Outbox.
The relay does not perform business work and does not split recipients by address.

## Two resources

| | `spawn[]` | `events[]` |
| --- | --- | --- |
| What it is | Follow-up tasks | Delivery Outbox records |
| Who takes it | Competing workers, as ordinary work | The `relay` role of the same image |
| When it is visible outside | Immediately, as work queue tasks | After commit; publication is at-least-once |
| Where it "goes" | Into the target named queue (it must exist) | To one webhook of the deploy |
| Retries like those of tasks | Yes: lease, fail, the queue's dead letter | Their own: relay backoff, delivery dead letter |
| Creates work | Yes | No, a notification only |

Both either appear together with the succeeded source task, or they do not appear.
A retry of the same complete with the same claim and body does not create second spawns
or second events.

You can mix them: in one complete, `spawn[]` (work) and `events[]`
(a notification). They are still different rows in the store.

## How to choose

| You need | You put |
| --- | --- |
| Someone must *do* the work (call X, issue an invoice) | `spawn[]` into the queue whose worker can do it |
| Someone must *learn* that the step finished | `events[]` with a clear `type` |
| Neither work nor a notification | complete without either field is normal |

Do not put "go to the payment API and charge the money" into `events[]`. The relay
hits one webhook and that is the end of its work. The charge is either
the current worker inside the lease, or a follow-up task.

Public HTTP complete does not accept `events[]` yet: the capability
`delivery_events` = false, and the key is reserved. The model and the `relay` role
already exist; turning the field on in the API is a separate step. An empty complete and
`spawn[]` are the current normal path.

## Two recipients, X and Y

The relay itself does not send one event to X and another to Y. Both events go to
one webhook. If these are notifications, the sink looks at `type`. If this is
work, use `spawn[]` into two queues. A walkthrough with examples:
[15-events-to-two-services.md](15-events-to-two-services.md).

---

What a follow-up is: [08-what-is-follow-up.md](08-what-is-follow-up.md).
Who sends HTTP: [14-who-sends-outbound-http.md](14-who-sends-outbound-http.md).
