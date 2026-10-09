#!/usr/bin/env python3
"""Deterministic checker for role-split client operation ownership (SDK-08).

Compares OpenAPI authenticated operations, ROLE_OPERATION_GRANTS, and the
checked-in manifest. OpenAPI remains authoritative; the manifest is not
normative over OpenAPI.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OPENAPI = ROOT / "openapi" / "queue.openapi.json"
DEFAULT_MANIFEST = ROOT / "packages" / "client-operation-ownership.json"

# Locked client surfaces from phase 15-CONTEXT / ADR 029.
KNOWN_CLIENT_CLASSES: frozenset[str] = frozenset(
    {
        "ProducerClient",
        "ConsumerClient",
        "ObserverClient",
        "AdminClient",
        "BreakGlassClient",
    }
)

# Audited retry classes (SDK-11). Hints alone never authorize unsafe retries.
KNOWN_RETRY_CLASSES: frozenset[str] = frozenset(
    {
        "safe-read",
        "same-idempotency-key",
        "same-resource-identity",
        "same-terminal-body",
        "never",
    }
)

# Duplicate ownership allowed only where OpenAPI accepts multiple principals
# (15-CONTEXT operation ownership).
APPROVED_MULTI_CLIENT_OPERATIONS: frozenset[str] = frozenset(
    {
        "getCapabilities",
        "getTask",
        "getQueue",
        "getStats",
        "getMaintenanceStatus",
        "listInspectionTasks",
        "listInspectionAttempts",
        "listDeadLetters",
    }
)

# OpenAPI security scheme → ServiceRole value. ClaimTokenHeader is AND-only and
# ignored for role mapping. Break-glass endpoints still declare AdminBearer on
# the admin plane; ROLE_OPERATION_GRANTS maps those operationIds to BREAK_GLASS.
_SCHEME_TO_ROLE: dict[str, str] = {
    "ProducerBearer": "PRODUCER",
    "WorkerBearer": "WORKER",
    "ObserverBearer": "OBSERVER",
    "AdminBearer": "ADMIN",
}

_CLIENT_TO_ROLE: dict[str, str] = {
    "ProducerClient": "PRODUCER",
    "ConsumerClient": "WORKER",
    "ObserverClient": "OBSERVER",
    "AdminClient": "ADMIN",
    "BreakGlassClient": "BREAK_GLASS",
}

# HTTP operations only — deployment process roles are out of client scope.
# Keys use ServiceRole.name (PRODUCER, WORKER, …), not wire values (producer).
_HTTP_GRANT_ROLES: frozenset[str] = frozenset(
    {"PRODUCER", "WORKER", "OBSERVER", "ADMIN", "BREAK_GLASS"}
)


def load_openapi_authenticated_operations(path: Path) -> dict[str, dict[str, Any]]:
    """Return operationId → {method, path, security, schemes, roles_from_security}."""
    doc = json.loads(path.read_text(encoding="utf-8"))
    global_security = doc.get("security")
    out: dict[str, dict[str, Any]] = {}
    for path_key, methods in doc.get("paths", {}).items():
        if not isinstance(methods, dict):
            continue
        for method, op in methods.items():
            if method.startswith("x-") or not isinstance(op, dict):
                continue
            op_id = op.get("operationId")
            if not op_id:
                continue
            security = op.get("security", global_security)
            if security is None or security == []:
                # Unauthenticated / empty security — not a client ownership entry.
                continue
            schemes: set[str] = set()
            for requirement in security:
                if not isinstance(requirement, dict):
                    continue
                schemes.update(requirement.keys())
            schemes.discard("ClaimTokenHeader")
            roles: set[str] = set()
            for scheme in schemes:
                role = _SCHEME_TO_ROLE.get(scheme)
                if role is not None:
                    roles.add(role)
            out[op_id] = {
                "method": method.upper(),
                "path": path_key,
                "security": security,
                "schemes": frozenset(schemes),
                "roles_from_security": frozenset(roles),
            }
    return out


def load_manifest(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _http_role_grants() -> dict[str, frozenset[str]]:
    """Map ServiceRole.name → frozenset of HTTP operationId strings."""
    # Import lazily so the CLI works without editable install edge-cases.
    from queue_service.security.authorization import (  # noqa: PLC0415
        Operation,
        ROLE_OPERATION_GRANTS,
    )

    deployment = {
        Operation.APPLY_MIGRATIONS.value,
        Operation.RUN_PARTITION_MAINTENANCE.value,
    }
    grants: dict[str, frozenset[str]] = {}
    for role, ops in ROLE_OPERATION_GRANTS.items():
        if role.name not in _HTTP_GRANT_ROLES:
            continue
        http_ops = frozenset(op.value for op in ops if op.value not in deployment)
        grants[role.name] = http_ops
    for name in _HTTP_GRANT_ROLES:
        grants.setdefault(name, frozenset())
    return grants


def _expected_security_roles(op_id: str, openapi_meta: dict[str, Any], grants: dict[str, frozenset[str]]) -> frozenset[str]:
    """Roles OpenAPI + grants jointly imply for an operation.

    Break-glass ops declare AdminBearer on the wire but are granted only to
    BREAK_GLASS — substitute ADMIN→BREAK_GLASS for those operationIds.
    """
    roles = set(openapi_meta["roles_from_security"])
    if op_id in grants.get("BREAK_GLASS", frozenset()):
        roles.discard("ADMIN")
        roles.add("BREAK_GLASS")
    return frozenset(roles)


def check_ownership(
    openapi_ops: dict[str, dict[str, Any]],
    manifest: dict[str, Any],
    *,
    grants: dict[str, frozenset[str]] | None = None,
) -> list[str]:
    """Return human-readable error strings; empty list means pass."""
    errors: list[str] = []
    role_grants = grants if grants is not None else _http_role_grants()

    if "operations" not in manifest or not isinstance(manifest["operations"], list):
        return ["manifest missing operations array"]

    approved = set(manifest.get("approved_multi_client_operations", []))
    if approved != APPROVED_MULTI_CLIENT_OPERATIONS:
        errors.append(
            "approved_multi_client_operations mismatch: "
            f"manifest={sorted(approved)} expected={sorted(APPROVED_MULTI_CLIENT_OPERATIONS)}"
        )

    seen_ids: set[str] = set()
    entries_by_id: dict[str, dict[str, Any]] = {}
    for entry in manifest["operations"]:
        if not isinstance(entry, dict):
            errors.append(f"invalid operation entry: {entry!r}")
            continue
        op_id = entry.get("operationId")
        if not isinstance(op_id, str) or not op_id:
            errors.append(f"operation entry missing operationId: {entry!r}")
            continue
        if op_id in seen_ids:
            errors.append(f"duplicate manifest entry for operationId {op_id}")
            continue
        seen_ids.add(op_id)
        entries_by_id[op_id] = entry

        clients = entry.get("clients")
        roles = entry.get("credential_roles")
        if not isinstance(clients, list) or not clients:
            errors.append(f"{op_id}: clients must be a non-empty list")
            continue
        if not isinstance(roles, list) or not roles:
            errors.append(f"{op_id}: credential_roles must be a non-empty list")
            continue
        if len(clients) != len(set(clients)):
            errors.append(f"{op_id}: duplicate client class in clients")
        if len(roles) != len(set(roles)):
            errors.append(f"{op_id}: duplicate role in credential_roles")

        for client in clients:
            if client not in KNOWN_CLIENT_CLASSES:
                errors.append(f"{op_id}: unknown client class {client}")

        if len(clients) > 1 and op_id not in APPROVED_MULTI_CLIENT_OPERATIONS:
            errors.append(
                f"{op_id}: unapproved duplicate ownership across clients {clients}"
            )

        # Client ↔ credential_role pairing must be consistent and 1:1 by role.
        expected_roles_from_clients = {_CLIENT_TO_ROLE[c] for c in clients if c in _CLIENT_TO_ROLE}
        role_set = set(roles)
        if expected_roles_from_clients != role_set:
            errors.append(
                f"{op_id}: client/role mismatch clients={clients} "
                f"credential_roles={roles}"
            )

        retry_class = entry.get("retry_class")
        if not isinstance(retry_class, str) or retry_class not in KNOWN_RETRY_CLASSES:
            errors.append(
                f"{op_id}: retry_class must be one of {sorted(KNOWN_RETRY_CLASSES)}"
            )
        elif "BreakGlassClient" in clients and retry_class != "never":
            errors.append(
                f"{op_id}: break-glass operations must use retry_class never"
            )

    missing = sorted(set(openapi_ops) - seen_ids)
    for op_id in missing:
        errors.append(f"missing operation in manifest: {op_id}")

    stale = sorted(seen_ids - set(openapi_ops))
    for op_id in stale:
        errors.append(f"stale operation in manifest (not in OpenAPI): {op_id}")

    # ROLE_OPERATION_GRANTS ↔ manifest credential_roles
    grant_ops: dict[str, set[str]] = {role: set(ops) for role, ops in role_grants.items()}
    for op_id, entry in entries_by_id.items():
        if op_id not in openapi_ops:
            continue
        roles = set(entry.get("credential_roles") or [])
        for role in roles:
            if op_id not in grant_ops.get(role, set()):
                errors.append(
                    f"{op_id}: role/security mismatch — credential_role {role} "
                    f"not granted in ROLE_OPERATION_GRANTS"
                )
        for role, ops in grant_ops.items():
            if op_id in ops and role not in roles:
                errors.append(
                    f"{op_id}: role/security mismatch — ROLE_OPERATION_GRANTS "
                    f"grants {role} but manifest omits it"
                )

        expected_sec = _expected_security_roles(op_id, openapi_ops[op_id], role_grants)
        if roles != expected_sec:
            errors.append(
                f"{op_id}: role/security mismatch — OpenAPI+grants imply "
                f"{sorted(expected_sec)} but manifest has {sorted(roles)}"
            )

        # Break-glass exclusivity
        if "BreakGlassClient" in (entry.get("clients") or []):
            if entry.get("clients") != ["BreakGlassClient"]:
                errors.append(
                    f"{op_id}: break-glass operations must map only to BreakGlassClient"
                )
            if roles != {"BREAK_GLASS"}:
                errors.append(
                    f"{op_id}: break-glass operations must use credential_role BREAK_GLASS only"
                )

        # Observer must not own mutations (non-GET)
        if "ObserverClient" in (entry.get("clients") or []):
            method = openapi_ops[op_id]["method"]
            if method != "GET":
                errors.append(
                    f"{op_id}: observer entries must not include mutations "
                    f"(method={method})"
                )

    return errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Check client operation ownership manifest against OpenAPI and grants."
    )
    parser.add_argument(
        "--openapi",
        type=Path,
        default=DEFAULT_OPENAPI,
        help="Path to queue OpenAPI document",
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=DEFAULT_MANIFEST,
        help="Path to client-operation-ownership.json",
    )
    args = parser.parse_args(argv)

    openapi_ops = load_openapi_authenticated_operations(args.openapi)
    manifest = load_manifest(args.manifest)
    errors = check_ownership(openapi_ops, manifest)
    if errors:
        print("FAIL: client operation ownership check", file=sys.stderr)
        for err in errors:
            print(f"  - {err}", file=sys.stderr)
        return 1
    print(
        f"OK: {len(openapi_ops)} authenticated operations match "
        f"{args.manifest.name} and ROLE_OPERATION_GRANTS"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
