# Error model

**Status:** Accepted semantic contract

Errors are protocol-independent. HTTP status is a coarse projection; clients act
on the stable lowercase `code` and the explicit `retryable` flag.

```json
{
  "code": "lease_lost",
  "message": "claim is no longer current",
  "retryable": false,
  "retry_after_ms": null,
  "request_id": "uuid",
  "details": {}
}
```

Messages are diagnostic and may change. Details are bounded and never
contain a payload or a full claim token.

## Stable codes

| Code | Meaning | Retry same operation |
| --- | --- | --- |
| `validation_failed` | Malformed or unsupported request | no |
| `payload_too_large` | Hard byte/count limit | no |
| `idempotency_key_required` | Enqueue omitted stable key | no |
| `idempotency_conflict` | Same key/claim with different fingerprint | no |
| `queue_not_found` | Explicitly configured queue does not exist | no |
| `queue_draining` | New external enqueue is closed | yes, after state change |
| `task_not_found` | Task absent, expired from retention or out of scope | no |
| `claim_not_found` | Claim token never existed or replay retention expired | no |
| `lease_lost` | Claim expired or was superseded | no |
| `task_already_terminal` | Another terminal outcome won | no |
| `cancel_race_lost` | Completion/failure lost to cancellation | no |
| `config_version_conflict` | Admin optimistic version is stale | after reread |
| `permission_denied` | Valid principal lacks operation/queue scope | no |
| `unauthenticated` | Missing or invalid credential | after credential refresh |
| `resource_exhausted` | Depth/rate/pool budget reached | yes, with backoff |
| `dependency_unavailable` | PostgreSQL or required dependency unavailable | yes |
| `internal_error` | Unexpected server failure | bounded retry |

A processing `failure_code` sent by the worker is attempt data, not an API error.
A valid fail command returns success with the resulting retry or dead-letter
state.

## Not errors

- enqueue with the same scoped key and fingerprint returns the original task;
- a complete or fail replay with the same claim and fingerprint returns the stored result;
- a claim with no available work returns an empty array;
- a claim against a paused queue returns an empty array plus queue-state metadata;
- `ack_cancel` with the current claim and the same body returns the stored cancelled
  result; complete or fail after the cancellation is recorded returns `cancel_race_lost`.

## HTTP projection

- validation: `400`;
- unauthenticated/forbidden: `401`/`403`;
- unknown resource: `404`;
- idempotency, lease, and terminal races: `409`;
- queue draining state conflict: `409`, `retryable=true`, with a conservative
  `Retry-After` hint;
- stale admin `If-Match`/version: `412`;
- hard size limit: `413`;
- resource exhaustion: `429` with `Retry-After`;
- dependency unavailable: `503`;
- internal error: `500`.

Clients retry an uncertain enqueue with the same idempotency key and
an uncertain complete or fail with the same claim and body. They never retry a
lost lease.
