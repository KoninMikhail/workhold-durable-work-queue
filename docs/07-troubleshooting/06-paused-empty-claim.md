# Pause: claim returns an empty list

[Documentation](../README.md) › [Troubleshooting](README.md) › **Paused claim**

**What you see.** The named queue definitely has tasks (enqueue succeeds, the
inspection backlog grows), and claim returns an empty `tasks: []`. It looks
as if workers "do not see" the work.

**This is expected when `paused`.** A claim during pause is a successful empty
response with named-queue state metadata, not an operational error.

## Why

`paused` means: enqueues are accepted, processing is stopped. The backlog
grows on purpose. Processing was stopped (a handler rollout, an incident, a
manual stop), and producers were left running.

An empty claim during pause is not a 5xx and not "the named queue is gone".
A worker that treats an empty list as an error and shouts into an alert will
be wrong for the whole pause.

Pause does not revoke an existing lease. Whoever already holds a task keeps
doing heartbeat / complete / fail while the token is alive. Pause cuts off
*new* dispatch, not work already in progress.

Internal `spawn[]` from an accepted complete is also allowed during pause:
complete must not fail because the named queue is stopped. Newly spawned
tasks simply wait in the backlog until the named queue is `active` again.

## What to do

1. Check the admin state of the named queue and `config_version`. If it is
   `paused`, this is not a claim bug.
2. Wait until an operator sets `active` again, or switch it yourself
   (expected version + audit) if you are that operator.
3. The worker should sit out an empty claim quietly: sleep / long poll
   (when one appears), without retrying it as a failure.
4. Do not "fix" a pause with a new enqueue into another named queue, and do
   not spin up temporary instances.

If the named queue is `active` and claim is still empty, this is a different
symptom: no ready tasks, everything is leased, `available_at` is in the
future, the worker claims the wrong name, or admission/limits. Check task
inspection and the name in the claim request.

## What not to do

| Do not | Why |
| --- | --- |
| Retry claim as if it were a 5xx | The response succeeded |
| Assume enqueue is stopped too | During pause, enqueue is allowed |
| Restart workers "so they see the tasks" | They will see them after `active` |
| Confuse this with drain | During drain, claim continues. See [07](07-draining-enqueue-rejected.md) |

How pause differs from drain: [10-pause-vs-drain.md](../06-faq/10-pause-vs-drain.md).
