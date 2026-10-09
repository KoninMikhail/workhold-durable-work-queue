# What is a follow-up?

[Documentation](../README.md) › [FAQ](README.md) › **Follow-up**

**In short.** A new independent work queue task that the worker creates
on a successful `complete` of the current task. In the API this is `spawn[]`. The source task
is already `succeeded` on complete; competing workers take the follow-up
immediately as ordinary work. It is not "a subtask inside the parent".

## How it works

1. The worker holds a lease on the source task.
2. On a successful complete it passes `spawn[]` — zero or more new tasks
   into the target named queues.
3. In one transaction, workhold sets the source to succeeded and the follow-ups
   appear in the work queue. There is no window of "the parent is closed and the next
   piece of work is lost".
4. A retry of the same complete with the same claim and the same body returns the same
   spawned tasks, with no duplicates.
5. The target queue must exist in advance. An unknown name is an error,
   as with an ordinary enqueue.

The source → spawn link is written as lineage. That is not waiting for children and not
a tracker hierarchy.

## Example

The instance has two queues: `orders` and `billing`. The `orders` worker processed
order `42` and on complete spawns a task into `billing` with payload
`{"order_id":"42"}`. The `billing` worker takes it as ordinary work.
The `orders` queue no longer holds that work and does not wait for the invoice.

You can spawn several tasks, into different names, in the same complete.

## How to talk about it

| In conversation | In the product |
| --- | --- |
| parent | source task |
| subtask | follow-up task / spawn |
| create when closing the parent | atomically spawn on complete |

In a tracker a "subtask" lives inside the parent and often does not release it.
A follow-up is the next independent unit of work.

## Do not confuse them

| This | Not this |
| --- | --- |
| `spawn[]` — work for a worker | `events[]` — an intent to deliver outward through the Delivery Outbox |
| The next task in a named queue | A workflow / DAG with a join and waiting for children |
| A required step | No: complete without `spawn[]` is normal |

If you really need to call services X and Y with retries like those of tasks,
that is `spawn[]` into two queues, not two events. The difference:
[09-spawn-vs-events.md](09-spawn-vs-events.md).

---

Step-by-step scenario: [complete-and-spawn.md](../08-examples/03-complete-and-spawn.md).
