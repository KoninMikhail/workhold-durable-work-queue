---
name: api-contract-change
description: >-
  Add or change a Workhold HTTP operation or wire shape: openapi/queue.openapi.json,
  Operation enum and role grants, ASGI routes, error model, contract tests,
  client ownership manifest. Use when new endpoint, change request/response,
  new error code, operationId, OpenAPI, HTTP API, admin endpoint, capabilities.
---

# API Contract Change

`openapi/queue.openapi.json` (OpenAPI 3.1) is the machine source of the HTTP
contract ([ADR 016](../../../docs/04-architecture/adr/016-http-openapi-claim-security.md)).
`docs/03-reference/05-http-api.md` is a superseded proposal — never a source.

Read first: `docs/04-architecture/07-error-model.md`,
`docs/04-architecture/09-client-protocol.md` § Compatibility,
`docs/01-concepts/09-guarantees.md`. A change that weakens a guarantee is a
product decision — stop and ask.

## Workflow

```
- [ ] 1. Classify: additive (new optional field, new operation) or breaking
- [ ] 2. Edit openapi/queue.openapi.json
- [ ] 3. Keep the operation catalog in sync (table below)
- [ ] 4. Implement the route and service
- [ ] 5. Update contract tests and conformance
- [ ] 6. Update clients if the operation is client-owned (skill client-sdk-change)
- [ ] 7. Docs + run checks (skill run-checks)
```

## Compatibility

- Additive only by default: older clients tolerate unknown response fields and
  enums (`x-queue-compatibility` in the spec, asserted by tests).
- Optional fields allow JSON `null` (`nullable: true`).
- A breaking protocol change means a new major version — do not do it inside a
  task; return it to planning.

## Catalog sync (all must agree)

| Place | What |
| --- | --- |
| `openapi/queue.openapi.json` | `operationId`, path, method, `security` scheme, responses |
| `src/workhold/security/authorization.py` | `Operation` enum value = `operationId`; `ROLE_OPERATION_GRANTS` |
| `src/workhold/api/security.py` | Loads the catalog at import; raises `OpenAPI operationIds drift from Operation enum` on mismatch. Paths `/v1…` = application plane, `/admin/v1…` = admin plane |
| `tests/contracts/test_openapi_contract.py` | `PUBLIC_OPERATIONS` / `ADMIN_OPERATIONS` exact catalog, security matrix, status matrices |
| `x-queue-conformance-skeleton.applies_to_operation_ids` | Sorted list of all operationIds |
| `packages/client-operation-ownership.json` | Every authenticated operation mapped to clients, credential roles, retry class |

## Where code goes

| Plane | Files |
| --- | --- |
| Application `/v1` | `src/workhold/api/v1/`, `api/routes/`, `api/schemas/`, `api/application.py` |
| Admin `/admin/v1` | `src/workhold/api/admin_*.py` |
| Business logic | Service package next to the area (`intake/`, `application/`, `operations/`, `delivery/`) — not in the route |

Errors use the stable envelope and codes from `07-error-model.md`; a new code is
added to the spec and the error model doc together.

## Tests

| Suite | Purpose |
| --- | --- |
| `tests/contracts/test_openapi_contract.py` | Spec structure, catalog, schemas |
| `tests/contracts/test_client_operation_ownership.py`, `tools/check_client_operation_ownership.py` | Manifest ↔ OpenAPI ↔ grants |
| `tests/test_api_security_boundaries.py`, `tests/test_authorization.py` | Plane and role access |
| `tests/conformance/` | Black-box HTTP behavior (needs PostgreSQL) |

## Docs

Update the numbered page that describes the behavior (guides, `05-operations/07-admin-tools.md`
for admin ops). Security-plane or protocol decisions with alternatives → ADR.

## Related skills

| Skill | When |
| --- | --- |
| `client-sdk-change` | Operation is exposed in a role client |
| `alembic-migration` | The change needs new storage |
| `run-checks` | Verification |
| `create-adr` | Protocol or security decision |
