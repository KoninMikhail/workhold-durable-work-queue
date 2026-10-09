# Application outbox bridge compatibility

**Status:** Accepted architecture contract (Phase 6 Plan 07)  
**Requirement:** BRDG-02 (compatibility slice)  
**Companion:** [10-application-outbox-bridge.md](10-application-outbox-bridge.md), [09-client-protocol.md](09-client-protocol.md)  
**Executable matrix:** `tests/conformance/bridge/compatibility_matrix.yaml`

This document is the upgrade / downgrade / support-window contract for the
supported application-outbox bridge. It does **not** invent an independent bridge
versioning system; support is derived from the Phase 3.1 workhold `Capabilities`
advertisement and the Phase 6 intent schema major.

## Ownership and sequence

1. **Probe workhold first.** Before claiming any app-outbox row, the bridge fetches
   `GET /v1/capabilities` (or an injected equivalent) and classifies the response
   with `BridgeCompatibility`.
2. **Fail closed.** Protocol-major mismatch, missing durable idempotent-enqueue
   semantics, malformed bodies, unavailable discovery, and unknown required
   capability state block polling. The runner makes **zero** app-store `claim`
   calls and **zero** workhold enqueues.
3. **Then poll.** Only a `SUPPORTED` result permits `claim` → enqueue → ack.
4. **Re-check on resume.** Every `poll_once` (including after incompatibility or
   dependency recovery) re-runs the gate so a rolling workhold upgrade cannot
   silently resume delivery.

Operators observe incompatibility through Plan 06 health: `queue_compatible=false`
and `ready=false` after `note_capability_mismatch`, while pending intents remain
in the application outbox for rollback.

## Distinct version axes

These identifiers are **not** interchangeable:

| Axis | Source | Current supported value |
| --- | --- | --- |
| workhold protocol major | `Capabilities.protocol_major` (OpenAPI const) | `1` |
| workhold schema revision | `Capabilities.schema_revision` | `"0001"` |
| Bridge package version | `queue-client` distribution version | independently released |
| Intent schema major/minor | Persisted app-outbox envelope | major `1` (minor additive) |

Schema migration revision inside workhold storage, OpenAPI schema revision, bridge
package version, and app intent schema version must remain distinct in logs,
health notes, and support matrices.

## Durable idempotent enqueue

The required capability for bridge delivery is **durable idempotent enqueue**.
It is satisfied when:

- `Capabilities.enqueue_dedup_ttl_seconds > 0` (authoritative Phase 3.1 signal), or
- an explicit `durable_idempotent_enqueue=true` additional property is present.

Absence, zero TTL, or an explicit `false` fails closed before any claim.

## Additive and unknown handling

- **Additive intent fields** (`intent_extensions` / unknown keys under major 1)
  are ignored by tolerant bridges; the immutable enqueue request
  (`target_queue`, `enqueue_request`, identity) is unchanged.
- **Unknown extensible workhold error codes** map to explicit `UNKNOWN` handling —
  never crashes and never treated as success.
- Unknown additive fields on a valid capabilities document are tolerated when
  required semantics are present.

## Supported vs rejected combinations

Declarative rows live in `compatibility_matrix.yaml`. Binary outcomes:

| Combination | Outcome |
| --- | --- |
| Intent 1.0 / 1.1-additive + workhold protocol 1 + durable enqueue present | supported |
| Intent major 2 + workhold protocol 1 | rejected (preserve pending) |
| Intent 1.x + workhold protocol 2 | rejected |
| Intent 1.x + workhold protocol 1 + durable enqueue missing/unknown | rejected |
| Discovery unavailable / malformed capabilities | rejected (fail closed) |
| Mixed current + older_tolerant replicas on intent 1.x | supported; same `bridge:v1:` key |

## Rolling upgrade

Mixed old/new **supported** bridge replicas must:

- compute the same Plan 02 `bridge:v1:` idempotency key for the same identity;
- create at most one workhold task identity under that key across ≥50 replay cycles;
- never rewrite persisted intent to force compatibility.

## Downgrade and rollback

- Downgrade is **rejected** when persisted intents require unsupported semantics
  (for example intent schema major 2 on a major-1 bridge).
- Rollback guidance: leave pending rows untouched; restore a supported bridge /
  workhold pair; resume polling only after capabilities classify as `SUPPORTED`.
- The bridge must **never** mutate durable intent to force a downgrade.

## Support windows and sunset

Deprecation and sunset dates for workhold protocol majors and client distributions
follow the Phase 3.1 / 3.9 support policy published with the OpenAPI contract and
qualified client releases. This document does not invent separate sunset dates.
When workhold publishes a protocol-major bump or removes durable enqueue semantics,
bridges must fail closed before delivery and operators must roll forward or
restore a supported pair.

## Detection via Plan 06 health

| Signal | Meaning |
| --- | --- |
| `queue_compatible=false` | Compatibility gate failed (`note_capability_mismatch`) |
| `ready=false` | Bridge must not be treated as delivery-ready |
| Pending depth / oldest lag | Intents preserved in app DB — not data loss by itself |
| Logs `bridge compatibility gate blocked poll` | Exact reason + status for runbooks |
