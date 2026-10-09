# Does the application need its own database?

[Documentation](../README.md) › [FAQ](README.md) › **Application database**

**In short.** No, not necessarily. workhold has its own PostgreSQL — that is
the required store of the service. The application needs a business database only if there is
its own business state that must change together with the intent to enqueue
a task. Queue tables do not go into the business database.

## Two different stores

| Store | Whose | What is stored |
| --- | --- | --- |
| workhold PostgreSQL | the queue service | tasks, leases, attempts, Delivery Outbox |
| Application business database | the application | orders, invoices, domain statuses |

workhold always owns its database: DDL, migrations, schema. This is not
"queue tables inside your database", as in the v1 parser queue. Direct
application access to workhold tables is unnecessary and would tie you to the schema again.

A dedicated physical PostgreSQL server is not required: the service database can
sit on a shared cluster. Ownership and migrations still stay with
workhold.

## If there is no business database

The producer calls the API directly: enqueue with an idempotency key and a payload.
There is nothing to say about atomicity of "business change + enqueue" —
there is no external business state in its own database. workhold guarantees only
its own records.

A business database is not created "for the sake of the queue". The transport is broker-style:
the queue stores the work itself.

## If there is a business database

You cannot atomically make two writes in different databases: change an order in your
database and enqueue a task in workhold PostgreSQL. There is no distributed transaction
between them.

If you commit the order and then crash before enqueue, the work is lost.
If enqueue succeeded and the order commit rolled back, the queue holds a task
about an order that "did not exist".

The pattern: in the same transaction as the business change, write a row to the
app-local outbox. A separate bridge (integration bridge) retries the
idempotent enqueue into workhold. The business change and the *intent*
to enqueue the task are atomic in your database; delivery into the queue is eventual.
A bridge retry with the same key returns the existing task, not a second one.

The bridge is the supported way to integrate with a business database, not a required
component for applications without their own database.

## Common mistakes

| Mistake | Why that is wrong |
| --- | --- |
| "We must create an application database to use the queue" | No. Enqueue goes to the API |
| "We'll put tasks in our business database" | The queue store belongs to workhold |
| "We'll wrap two databases in a distributed transaction" | The product does not promise that |

---

Outbox pattern: [10-transactional-outbox.md](../01-concepts/10-transactional-outbox.md).
