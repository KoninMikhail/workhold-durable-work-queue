# How does pause differ from drain?

[Documentation](../README.md) › [FAQ](README.md) › **pause vs drain**

**In short.** These are different runtime states of one named queue. `paused`
accepts new tasks and stops handing them out. `draining` closes
new external enqueue and keeps working through what was already accepted. Existing
leases are not revoked when the state changes. There is no automatic transition after drain.

## Table

| | `active` | `paused` | `draining` |
| --- | --- | --- | --- |
| External enqueue / bridge | yes | yes | no (a new key) |
| Retry of an already committed key | yes | yes | yes — returns the source task |
| Internal `spawn[]` from an accepted complete | yes | yes | yes |
| Claim | yes | an empty successful response | yes |
| Heartbeat / complete / fail | by the lease rules | by the lease rules | by the lease rules |
| Cancel | yes | yes | yes |
| Delivery relay | independent | independent | independent |

`paused`: the backlog may grow, and processing is stopped. This is "stop the machine,
keep accepting deliveries".

`draining`: new external tasks are not accepted, but workers finish
clearing the queue. Internal `spawn[]` is allowed so that a valid complete is not
blocked after the worker has already done the application work.
Otherwise you would get: the work is done, complete is rejected, and the next
task is lost.

## When to turn which on

| Situation | State |
| --- | --- |
| Fix the handler without losing incoming enqueues | `paused` |
| Take the queue off intake and finish the tail before shutdown | `draining` |
| Ordinary work | `active` |

Drain of the work queue counts as finished when delayed + ready + leased
tasks equal zero. The delivery-event backlog is counted separately and does not block
finishing the drain of the task queue.

After drain the queue does not become `active` or `paused` by itself. The operator explicitly
chooses the next state (`draining → active` or `draining → paused`).
Transitions go with the expected `config_version` and write an audit record.

A state change does **not** break leases that were already handed out. A worker that holds
a task keeps doing heartbeat / complete / fail while the token is alive. Shutting down
the process is not the same as changing the persisted state of the queue.

## What the developer will see

- Under `paused`, claim is not an error: a successful empty list comes back, with
  state metadata. Do not retry the claim as an API failure.
- Under `draining`, a new enqueue with a new key is rejected. A retry
  of the same key and the same body after an already successful commit is not rejected:
  the source task is returned.

Symptom walkthrough: [paused claim](../07-troubleshooting/06-paused-empty-claim.md),
[draining enqueue](../07-troubleshooting/07-draining-enqueue-rejected.md).
