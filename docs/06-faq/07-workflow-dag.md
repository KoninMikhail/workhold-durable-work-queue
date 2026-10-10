# Is this Temporal or a workflow/DAG engine?

[Documentation](../README.md) › [FAQ](README.md) › **Workflow / DAG**

**In short.** No. workhold is a durable work queue with competing
workers, not a process orchestrator such as Temporal. There are no workflow
replay, joins, compensation, human tasks, or a built-in DAG model. `spawn[]`
creates the next independent piece of work, but it does not "wait for all
children".

## Different units of durability

Workhold durably owns an independent task: its queue state, fenced lease,
attempts, and terminal outcome. Temporal durably owns a Workflow Execution:
its Event History is replayed to reconstruct workflow state, while Activities
perform external I/O.

| | workhold | Temporal |
| --- | --- | --- |
| Durable unit | Independent queue task | Workflow Execution |
| Worker code | Ordinary handler for a claimed payload | Deterministic workflow code plus Activities |
| Coordination | Independent `spawn[]` follow-ups | Timers, signals, child workflows, waits, and multi-step control flow |
| Recovery | Lease expiry and another claim | Event History and workflow replay |
| External effects | At-least-once worker execution; require idempotency | Activities may execute more than once; require idempotency |

An orchestrator can hold a graph: step B waits for A and C, compensates after
an error, or waits for a human decision. workhold does not model that.

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
| A graph with joins, signals, compensation, or long-lived process state | Temporal or another workflow product |

Do not emulate a DAG on top of the queue as "the parent stays open until the children close".
On complete the parent is already succeeded. A hanging parent that waits for children
is a different model, and the queue does not support it.

---

Detailed comparison: [why-queue.md](../01-concepts/02-why-queue.md).
What a follow-up is: [08-what-is-follow-up.md](08-what-is-follow-up.md).
