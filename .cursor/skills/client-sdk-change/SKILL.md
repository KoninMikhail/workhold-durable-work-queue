---
name: client-sdk-change
description: >-
  Change the Workhold Python clients under packages/: workhold-client-core and the
  producer, consumer, admin role packages, sync and async surfaces, the client
  operation ownership manifest, release gate. Use when SDK change, client package,
  ProducerClient, ConsumerClient, AdminClient, ObserverClient, BreakGlassClient,
  async client, outbox bridge, packages/.
---

# Client SDK Change

Clients follow the protocol; OpenAPI stays authoritative
([ADR 012](../../../docs/04-architecture/adr/012-protocol-first-clients.md),
[ADR 029](../../../docs/04-architecture/adr/029-role-split-python-clients.md),
[ADR 030](../../../docs/04-architecture/adr/030-workhold-distribution-names.md)).
Read `docs/02-guides/06-client-sdk-ergonomics.md` and
`docs/04-architecture/09-client-protocol.md` before changing a surface.

## Packages

| Distribution | Import | Owns |
| --- | --- | --- |
| `workhold-client-core` | `_workhold_client_core`, `workhold_client_testing` | Transport, errors, wire models, retry classes, test kit; no operation clients |
| `workhold-producer` | `workhold_producer` | `ProducerClient`; extras `async`, `bridge-postgres` |
| `workhold-consumer` | `workhold_consumer` | `ConsumerClient`, claims; extra `async` |
| `workhold-admin` | `workhold_admin` | `AdminClient`, `ObserverClient`, `BreakGlassClient`; extra `async` |

Role packages depend only on a compatible exact-minor `workhold-client-core`,
never on each other (`tests/sdk/test_role_package_boundaries.py`). Shared code goes to core.

## Workflow

```
- [ ] 1. Confirm the server operation exists in openapi/queue.openapi.json (else skill api-contract-change)
- [ ] 2. Implement sync and async surfaces together
- [ ] 3. Update the ownership manifest (both copies)
- [ ] 4. Tests in tests/sdk/<role>/ and, for live behavior, tests/conformance/
- [ ] 5. Ownership check + release gate (skill run-checks)
- [ ] 6. Update docs/02-guides/06-client-sdk-ergonomics.md if the surface changed
```

## Ownership manifest

`packages/client-operation-ownership.json` maps each authenticated `operationId`
to `clients`, `credential_roles`, `sync_async_surfaces`, and `retry_class`.

- Allowed client classes and retry classes are fixed in
  `tools/check_client_operation_ownership.py` (`KNOWN_CLIENT_CLASSES`,
  `KNOWN_RETRY_CLASSES`). A new value is a design decision — ask.
- An operation in several clients must be listed in
  `approved_multi_client_operations`.
- A bundled copy ships in the core wheel:
  `packages/workhold-client-core/src/_workhold_client_core/data/client-operation-ownership.json`.
  It is a plain file, not a link — keep it **byte-identical** to the root copy
  (`tests/sdk/core/test_retry_helpers.py` compares their retry classes only, so
  other drift is not caught by tests).

Retry classes are audited: never retry an operation outside its declared class.

## Versions and release

- Do **not** bump versions or edit `packages/CHANGELOG.md`; release-please does it
  ([ADR 031](../../../docs/04-architecture/adr/031-separate-server-and-client-release-trains.md)).
- Core and the three role packages share one coordinated version
  (`tools/sync_coordinated_version.py`); the gate rejects partial sets.
- A `feat`/`fix`/`perf`/`refactor` commit releases clients only if it touches
  `packages/`. Keep client and server changes in separate commits when both change.

## Checks

```bash
uv run pytest tests/sdk -q
uv run python tools/check_client_operation_ownership.py
uv run python tools/client_release_gate.py
```

Plus `tests/conformance/` for live client behavior (needs PostgreSQL, skill `run-checks`).

## Related skills

| Skill | When |
| --- | --- |
| `api-contract-change` | The server contract changes too |
| `run-checks` | Verification |
| `commit-messages` | Commit message and release train |
