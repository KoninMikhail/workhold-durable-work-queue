# Can you enqueue a task into a named queue that does not exist?

[Documentation](../README.md) › [FAQ](README.md) › **No queue**

**In short.** No. An admin creates named queues explicitly. Enqueue
does **not** create a queue "from a typo": a request for an unknown name must
end in an error, not a silent backlog in a random queue.

## Why it is this way

If enqueue created the queue itself, a typo of `ordres` instead of `orders`
would produce a live queue with no workers. Tasks would pile up unnoticed: the producer
gets success, the handler looks at `orders` and sees nothing. That is worse than
an explicit error at enqueue time.

The same applies to `spawn[]`: the target queue must exist in advance.
An unknown name on complete is an error, as with an ordinary enqueue. A worker
that has already done the application work must not learn about a typo
in the spawn name after the effect has already gone out. The target
queue name is checked in code and in tests; the queue is created in admin before rollout.

## What to do

1. An admin creates the queue through the private admin plane (name, retry policy,
   initial state).
2. The producer uses that same name on enqueue.
3. In the claim, the worker lists only the names it knows how to process.

If enqueue fails with "the queue does not exist", that is not "retry until it appears".
Either the typo was fixed, or an operator created the queue. A silent retry with the
same wrong name will fail again.

The idempotency key does not create a queue. The key applies only inside
a name that already exists.

## Common mistakes

| Mistake | Why that is wrong |
| --- | --- |
| "The first enqueue will create the queue, like a topic in Kafka" | It will not. This is a work queue, not a log |
| "In dev it is fine; we'll create it in prod later" | In dev too, a typo would grow an invisible backlog if auto-create existed |
| "A spawn into a queue that does not exist yet is fine; it will be created" | No. complete will be rejected |

---

What a named queue is: [03-what-is-named-queue.md](03-what-is-named-queue.md).
Create a queue: [admin-queues.md](../02-guides/04-admin-queues.md).
