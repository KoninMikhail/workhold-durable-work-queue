# Is this a shared bus for the whole platform?

[Documentation](../README.md) › [FAQ](README.md) › **Platform bus**

**In short.** No. One workhold instance serves one application
and one trust boundary. The image is deployed next to the replicas of that application.
Several named queues inside the instance are normal. A shared
multi-tenant bus for the whole platform is not.

## What this means in practice

Each application has its own workhold deploy and its own PostgreSQL for the service
(a dedicated physical server is not required: the database can sit on a
shared cluster, but DDL, migrations, and table ownership stay with
that application's workhold).

The producer and the worker of the `orders` application talk to *their* instance. They do not
enqueue tasks into a neighboring product's instance and do not read its Outbox.
Secrets, payload, and attempt history are not mixed between applications.

Several API and relay replicas on one store of the same application are normal.
Several named queues inside that store are normal too: they are streams
of work (`orders`, `billing`), not separate deploys and not "the platform bus".

## Why not a shared bus

A shared bus for the whole platform mixes the trust boundary: who may enqueue,
who sees the payload, whose retry policy it is, whose dead letter it is. workhold
is intentionally per-application so that:

- secrets and payload do not cross the application boundary;
- pause, drain, and retry of one product do not stall another;
- an operator repairs and backs up one application's store, not "the whole platform".

Two workhold deploys "so that each one has its own webhook" are not a way
to split recipients inside one application. One instance belongs
to one trust boundary. How to split services X and Y inside an application
is in [15-events-to-two-services.md](15-events-to-two-services.md).

## Common mistakes

| Mistake | Why that is wrong |
| --- | --- |
| "We'll run one workhold for all products" | That is the forbidden shared bus |
| "Each task type is a separate instance" | PostgreSQL and operations multiply; task types are named queues |
| "A named queue is a platform topic" | A named queue lives inside one instance of one application |

---

Deploy boundary and store ownership: [product-boundary.md](../01-concepts/07-product-boundary.md).
