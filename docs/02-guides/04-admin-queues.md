# Admin: named queues

[Documentation](../README.md) › [Guides](README.md) › **Admin**

The private control plane `/admin/v1` is separate from the public `/v1`. Admin has a separate
credential and, in production, a separate listener / base URL. An observer reads through
the same admin plane and the public `/v1` with **its own** OBSERVER credential.

**Never put admin credentials in application producer/consumer pods.**
A producer and a consumer receive only their own role tokens and only the public base URL.
Admin/observer clients (`workhold-admin`) live on the control/ops planes.

SDK:

- public base URL → application plane (`/v1/...`)
- admin base URL → private control plane (`/admin/v1/...`)
- `ObserverClient(public, bearer_token=observer, admin_transport=admin)`
- `AdminClient(public, bearer_token=admin, admin_transport=admin)`

Optimistic config: `set_queue_state` / `activate_queue_policy` require
`expected_config_version`. On conflict, reread the queue and retry —
the SDK does not perform a hidden read-modify-write. Do not change queue tables directly.

## 1. Create a named queue

`POST /admin/v1/queues`

Queues are created explicitly. Enqueue of an unknown name is rejected.

```bash
curl -sS -X POST "https://queue-admin.example/admin/v1/queues" \
  -H "Authorization: Bearer <admin-secret>" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: create-orders-1" \
  -d '{
    "name": "orders",
    "initial_policy": {
      "enabled": true,
      "max_attempts": 5,
      "backoff_strategy": "fixed",
      "retry_delay_seconds": 30
    }
  }'
```

List / card: `GET /admin/v1/queues`, `GET /admin/v1/queues/{queue_name}`.

### Bootstrap from a mounted catalog (`workhold apply`)

An alternative to a manual `POST` at deploy is a one-shot `workhold apply` with an absolute
`QUEUE_CATALOG_PATH` (mounted JSON). This is the same create mutation: ensure-exists,
skip if the queue already exists (any state / policy). The catalog is **not baked into the
image**; this is **not** a GitOps reconcile of live state and **not** `api` startup /
`/readyz`. Names removed from the file are **not** deleted and are **not** drained.
An invalid file fails closed, with no writes. On a partial apply, earlier creates
may have committed; recovery is to run `workhold apply` again.

Pause / drain / policy activation remain **admin HTTP only** (below). Deployment
and Compose: [02-deployment.md](../05-operations/02-deployment.md); example —
[07-named-queue-catalog.md](../08-examples/07-named-queue-catalog.md).

## 2. Pause and drain

`POST /admin/v1/queues/{queue_name}:set-state`

The body requires `expected_config_version` (optimistic concurrency) and `state`:
`active` | `paused` | `draining`.

- **paused** — enqueue is still allowed; claim stops.
- **draining** — external enqueue is rejected; claim and internal `spawn[]`
  continue until depth reaches zero.

```bash
curl -sS -X POST \
  "https://queue-admin.example/admin/v1/queues/orders:set-state" \
  -H "Authorization: Bearer <admin-secret>" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: pause-orders-1" \
  -d '{"expected_config_version": 3, "state": "paused"}'
```

A version mismatch is a `config_version` conflict; reread the queue and
retry with the current value.

## 3. Policy: create and activate

1. `POST /admin/v1/queues/{queue_name}/policies` — an immutable policy version
   (`enabled`, `max_attempts`, `backoff_strategy`, `retry_delay_seconds`).
2. `POST /admin/v1/queues/{queue_name}/policies/{policy_version}:activate`
   with `expected_config_version`.

New enqueues snapshot the active policy. Tasks already in flight stay on
their own version.

Admin operations do not replace the worker lease API and do not issue a
`claim_token` to a worker.

## Next

- Producer / worker: [02-producer-enqueue.md](02-producer-enqueue.md),
  [03-worker-claim-complete.md](03-worker-claim-complete.md)
- Pause vs drain: [pause-vs-drain.md](../06-faq/10-pause-vs-drain.md)
- Operations: [admin-tools.md](../05-operations/07-admin-tools.md)
