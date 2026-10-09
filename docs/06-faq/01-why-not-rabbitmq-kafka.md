# Why Workhold if RabbitMQ or Kafka is already there?

[Documentation](../README.md) › [FAQ](README.md) › **Broker**

**In short.** queue-service is not a replacement for a broker. It is the work queue of one
application: named queues, a fenced lease, and an atomic `complete` with `spawn[]` and
`events[]`. RabbitMQ and Kafka transport messages. They do not own claim,
lease, and atomic spawn in one store.

## Different jobs

A broker answers "how do you deliver bytes from A to B". queue-service
answers "how several replicas of one application safely
work through tasks, not lose the next piece of work on complete, and not make
an external effect the core of the queue".

| | queue-service | RabbitMQ | Kafka |
| --- | --- | --- | --- |
| Model | Work Queue + Delivery Outbox | message broker | event log |
| Fenced lease | yes: one active claim, a new token on claim | competing consumers without a fenced lease as the core | consumer groups / offsets |
| Idempotent enqueue | yes, a key + a body fingerprint | usually on the application side | usually on the application side |
| `complete` + `spawn[]` + `events[]` in one transaction | yes | not as a task kernel | not as a task kernel |
| Pub/sub, log replay | not a product goal | pub/sub | replay / log |
| Deploy | next to one application | often shared | often shared |

One task in queue-service is meant for one logical handler.
Several workers compete for it. This is not fan-out "to every subscriber"
and not replay of a topic from an arbitrary offset.

## When a broker is still needed

queue-service owns enqueue, claim, lease, complete, `spawn[]`, and the
Delivery Outbox. If, after complete, already committed
events must be distributed onto someone else's bus, a broker can be a **channel after commit**.
It must not become a second place where a task is claimed and acked.

Right now the first Delivery Relay adapter is an HTTP webhook. An adapter into a broker
may appear later and does not change the Work Queue model: an event still
does not choose the recipient's address.

## When queue-service is not needed

| You only need… | Use |
| --- | --- |
| Pub/sub or log replay without a task queue | a broker / the log directly |
| A DAG, join, compensation, human task | a separate workflow product |
| Two claim/ack cores side by side ("both the queue and a broker as a task queue") | do not do that |

Do not run RabbitMQ "as a task queue" and queue-service "as a second
task queue" on the same flow of work. You get two sources
of truth about the lease and two different at-least-once behaviors.

---

More on the product boundary: [why-queue.md](../01-concepts/02-why-queue.md).
