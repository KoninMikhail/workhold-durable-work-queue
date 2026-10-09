# Client protocol and SDK boundary

**Status:** Accepted architecture; exact HTTP paths belong to Phase 3 OpenAPI.

The machine-readable protocol and the black-box conformance suite are normative. SDKs are
optional thin adapters and cannot become the only specification.

The primary protocol is HTTP/JSON REST with OpenAPI 3.1. Application resources use
`/v1`; private admin resources use `/admin/v1`.

## Batch-ready claim

The first claim contract is array-shaped:

```json
{
  "queues": ["default"],
  "max_tasks": 1,
  "lease_seconds": 60,
  "wait_seconds": 0,
  "worker_id": "worker-pool-a/replica-7"
}
```

The response always contains `tasks: []`, even when empty. Each item carries the task,
payload, policy version, and a claim with a public ID, secret token, generation,
`claimed_at`, and expiry. Lease-operation paths contain the public `claim_id`;
the secret `claim_token` is passed in a protected header and never in URL/query/logs.
Metadata includes workhold store server time and the recommended heartbeat interval.

The MVP validates `max_tasks=1`. Bounded long polling is enabled: `wait_seconds`
is a strict integer `0..20` (server max = advertised `max_wait_seconds=20`).
`batch_claim` stays `false`. A zero `wait_seconds` is an immediate claim;
a positive wait requires authenticated `long_polling=true`.

An empty response after expiry is a successful `tasks: []`, **not** a client timeout.
Transport timeout and cancel remain separate outcomes.

`ConsumerSupervisor` / `AsyncConsumerSupervisor` after capability preflight
use a default wait of **15** s (an explicit `0` disables long poll). Client
budgets: read = wait+5, total = wait+10. The reverse-proxy upstream idle/response
timeout in production must be **≥ 30 s** (server max + 10 s).

## Worker behavior

1. Claim from explicitly subscribed queues.
2. Start the heartbeat at the server-recommended interval, with jitter.
3. Process outside workhold transactions.
4. Observe cooperative cancellation on heartbeat and at application checkpoints.
5. Complete, fail, or acknowledge a requested cancellation with an idempotent body
   fingerprint.
6. On `lease_lost`, stop every workhold mutation and hand the loss to application code.
7. On shutdown, stop claiming; finish within grace or let the lease expire.

SDK convenience loops must not hide lease loss, automatically retry a different
terminal body, or claim more work than the handler's capacity.

## SDK surfaces

Role-split public distributions (role split: [ADR 029](adr/029-role-split-python-clients.md); names: [ADR 030](adr/030-workhold-distribution-names.md)):

- producer (`workhold_producer.ProducerClient`): enqueue, resolve
  submission, producer-authorized inspect, and cancel; optional
  `workhold_producer.bridge` via producer extra;
- consumer (`workhold_consumer.ConsumerClient` /
  `ConsumerSupervisor`): claim, heartbeat, complete, fail, `ack_cancel`, and
  an optional supervised loop (wire identifiers remain `WorkerBearer` /
  `worker_id`);
- admin package (`workhold_admin`): separately credentialed
  `ObserverClient`, `AdminClient`, and `BreakGlassClient` for observer reads,
  routine admin, and break-glass repair.

Shared transport, errors, and models live in the dependency-only
`workhold-client-core` (`_workhold_client_core`) without operation
client classes. Credentials and packages are separated enough that application
pods do not receive admin capabilities. Dual base URLs: public `/v1` and private
`/admin/v1`.

Machine-checked ownership of every authenticated OpenAPI `operationId` is
recorded in [`packages/client-operation-ownership.json`](../../packages/client-operation-ownership.json)
and enforced by `tools/check_client_operation_ownership.py`. That manifest is a
release/guardrail artifact — **not** normative over OpenAPI or conformance.

The unreleased `queue-client` / `queue_service_client` prototype is migration
input only and is removed before role-client release; no facade ships.

## Compatibility

- a breaking protocol change creates a new major version;
- additive fields are ignored by tolerant older clients;
- state and error enums are extensible, with an explicit unknown fallback;
- deprecated operations publish a support window and a sunset date;
- the server advertises the protocol version and the enabled optional capabilities;
- the schema migration version and the protocol version are independent.

The MVP does not require complex feature-negotiation headers. The capabilities resource
is sufficient until independently deployable clients need stronger
negotiation.

## Conformance

Every client implementation must pass protocol tests for enqueue
idempotency, claim fencing, stale heartbeat and complete, uncertain complete
replay, cancellation, queue state gates, structured failure, and authorization.
Tests run against a real workhold/PostgreSQL instance, not SDK mocks.
