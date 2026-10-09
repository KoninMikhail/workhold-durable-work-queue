# Security contract

One queue-service instance serves one application trust boundary. Named queues
are routing units, not strong tenant isolation. Unrelated applications
use separate instances.

## Service principals

- **producer:** enqueue, inspect, and cancel on allowed queues;
- **worker:** claim and mutations of the current lease on allowed queues;
- **bridge:** idempotent enqueue from the app-local outbox;
- **relay:** claim/publish acknowledgement Delivery Outbox;
- **observer:** metrics and limited operational inspect;
- **admin:** private runtime control and recovery (not break-glass);
- **break_glass:** short-lived emergency repair; does **not** inherit grants from
  `ADMIN`, and the reverse is also true;
- **migrator/maintainer:** schema and partition roles owned by queue-service.

Credentials map to a stable principal ID and explicit operation/queue scopes.
Producer identity is part of enqueue idempotency scope. Producer, worker,
and admin credentials are **never** interchangeable. `BREAK_GLASS` requires a timezone-aware
`expires_at` and a non-empty `allowed_operations`; expired credentials never
authenticate. Minting break-glass tokens through admin HTTP is **forbidden** —
only deployment-issued secrets.

The MVP may use deployment-issued bearer tokens or mTLS. queue-service does not
implement a human IAM platform. Production traffic uses only TLS and private-network
by default.

## Worker identity and claim token

`worker_id` is derived from the authenticated principal plus a validated replica
identifier. This is diagnostic, not authorization.

Each claim has a public `claim_id` for resource paths/correlation and a
separate secret `claim_token`. The token is an ephemeral bearer capability for a single
lease:

- random; rotated on every claim and reclaim;
- sent only in a protected header, **never** in URL/query strings or logs;
- returned only to the holding worker;
- valid only with a matching generation and an unexpired lease.

General inspection and the admin API redact it.

## Admin plane

- a separate credential and preferably a separate listener/network policy;
- optimistic `config_version` for runtime changes;
- an audit row is committed with every mutation;
- no arbitrary SQL, DDL, DB URL, or hard-limit updates;
- dangerous operations require the admin role plus a reason; break-glass is a separate JIT role
  (`BREAK_GLASS`) with an ack triad and an operation audience; see
  [07-admin-tools.md](07-admin-tools.md#break-glass-operations);
- break-glass does **not** mint a worker `claim_token`, does **not** promise exactly-once
  recovery, and does **not** provide a dual-control UI.

## Payload handling

queue-service treats the payload as application-owned opaque JSON:

- hard size limit;
- no payload logging or metric labels by default;
- no arbitrary payload indexes/search;
- storage encryption is the deployment's responsibility;
- secrets must be references, not embedded values;
- the payload expires with the configured task/event retention.

## Rotation and audit

The deployment supports overlapping old/new credentials during rotation. Admin
changes, claims/attempts, cancellations, and replays record the authenticated actor,
queue-service-store time, and request correlation. Audit retention is independent of
task-result retention.

`SENTRY_DSN` is a deployment secret on par with `DATABASE_URL` and bearer tokens.
Do not log the DSN, do not include it in traces/errors, and do not send it to GlitchTip
together with payloads, claim tokens, or bearer secrets. Do not use a live DSN in
documentation, fixtures, or examples.

### HTTP principal manifest

The production API receives HTTP principals from a single secret JSON manifest:
`QUEUE_API_PRINCIPALS_MANIFEST` on the host or
`QUEUE_API_PRINCIPALS_MANIFEST_FILE` in the container. The v1 contract is described by the schema
[service-principal-manifest.schema.json](../04-architecture/schemas/service-principal-manifest.schema.json),
and placeholders are in
[service-principal-manifest.example.json](../08-examples/service-principal-manifest.example.json).

Each entry sets a unique stable `principal_id`, an uppercase role
`PRODUCER|WORKER|OBSERVER|ADMIN`, exact `queue_scopes`, and a non-empty list of
`credentials` (`generation_id` + opaque `secret`). Producer, worker, and observer
must have at least one queue; ADMIN must have an empty list. A scope is not
a glob or a prefix: `orders` does not allow `orders.audit`. Relay,
break-glass, migrator, and maintainer are not issued by this HTTP manifest.

For overlap rotation, add a new generation to the same principal/role, roll out
credentials, then remove the old generation. `principal_id` stays the same. Reusing a
generation ID, a principal declaration, or a secret across different identities is forbidden.
Unknown keys/roles, malformed queue names, and empty/invalid/oversized JSON cause
a fail-closed API startup before listeners and database engines; diagnostics contain
only the env name and the JSON field path, not the source JSON and not bearer values.

A non-empty manifest is the only source of API credentials:
`QUEUE_API_BEARER_TOKEN` and the optional previous generation are not merged with it and
are not accepted. Legacy tokens remain only as an absent-manifest fallback, always
authenticate as `ADMIN`, and do not receive producer/worker grants.

### File secrets (`*_FILE`)

Why injection happens in the entrypoint rather than in Python — [ADR 025](../04-architecture/adr/025-container-file-secrets.md).

The runtime image entrypoint materializes allowlisted deployment secrets from sibling
`NAME_FILE` paths before the process role starts:

- `DATABASE_URL` ← `DATABASE_URL_FILE`
- `QUEUE_API_PRINCIPALS_MANIFEST` ← `QUEUE_API_PRINCIPALS_MANIFEST_FILE`
- `QUEUE_API_BEARER_TOKEN` ← `QUEUE_API_BEARER_TOKEN_FILE`
- `QUEUE_API_BEARER_TOKEN_PREVIOUS` ← `QUEUE_API_BEARER_TOKEN_PREVIOUS_FILE`
- `SENTRY_DSN` ← `SENTRY_DSN_FILE` (optional opt-in error reporting only)

`SENTRY_DSN_FILE` is resolved **only** by the container entrypoint (`entrypoint.sh`,
DEP-05). Host / `uv run queue` uses `SENTRY_DSN` directly; `from_environ`
on the host does not read `SENTRY_DSN_FILE`.

For each allowlisted name, `NAME` and `NAME_FILE` are exclusive: startup fails closed
if both are set, or if `NAME_FILE` is missing, unreadable, empty, or not an absolute
path. The entrypoint reads the file contents into the env var; it does **not** shell-source the file.
File contents and resolved values **never** appear in entrypoint logs (only
names on errors).

Manifest rotation uses multiple `credentials` of one principal. Legacy
rotation overlap uses the current bearer plus an optional previous bearer only when
the manifest is absent. If neither the previous `NAME` nor its `NAME_FILE` is set,
there is no overlap. The same exclusivity and fail-closed
rules apply to every `_FILE`. This is injection of deployment secrets, not a new secret store.
