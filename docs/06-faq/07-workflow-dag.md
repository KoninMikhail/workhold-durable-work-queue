# Is this a workflow or DAG engine?

[Documentation](../README.md) › [FAQ](README.md) › **Workflow / DAG**

**In short.** No. queue-service is a durable work queue with competing
workers, not a process orchestrator. There are no joins, compensation, human
tasks, or a built-in DAG model. `spawn[]` creates the next independent
piece of work, but it does not "wait for all children".

## What the queue can do that an orchestrator cannot

An orchestrator holds a graph: step B waits for A and C, rolls back
with compensation on error, and a human approves a step. queue-service does not model that.

What exists:

- the worker closes the current task through `complete`;
- in the same transaction it may create zero or more new tasks (`spawn[]`);
- competing workers claim the new tasks immediately as ordinary work;
- the source → spawn link exists as lineage (who produced whom), not as
  an ownership tree.

What does not exist:

- "wait until all children succeeded";
- a join of several branches into one;
- saga / compensation;
- a human task and built-in process timers;
- a condition "if A failed, start B".

If an invoice must be issued after an order, `complete` in `orders` spawns
a task in `billing`. The `orders` worker **does not wait** for billing. If billing must
depend on two independent steps, the application writes that join
(its own status, its own inbox, its own next enqueue), not the queue.

## When spawn is enough, and when another product is needed

| You need | Where |
| --- | --- |
| The next unit of work after success | `spawn[]` |
| To notify an external service that a step finished | `events[]`, not a workflow |
| A graph with join / compensation / a human | a separate workflow product |

Do not emulate a DAG on top of the queue as "the parent stays open until the children close".
On complete the parent is already succeeded. A hanging parent that waits for children
is a different model, and the queue does not support it.

---

What a follow-up is: [08-what-is-follow-up.md](08-what-is-follow-up.md).
