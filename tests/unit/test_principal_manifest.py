"""Strict deployment service-principal manifest tests (OPS-06 / SEC-01..04)."""

from __future__ import annotations

import json
import traceback

import pytest

from workhold.security.authorization import (
    AuthorizationContext,
    AuthorizationDenied,
    Authorizer,
    Operation,
)
from workhold.security.credentials import BearerCredentialAuthenticator
from workhold.security.principal_manifest import (
    PRINCIPAL_MANIFEST_MAX_BYTES,
    parse_principal_manifest,
)
from workhold.security.principals import Principal, ServiceRole
from workhold.settings import SettingsValidationError


SENTINEL = "MANIFEST_SECRET_SENTINEL_9z8y"


def _manifest() -> dict[str, object]:
    return {
        "schema_version": 1,
        "principals": [
            {
                "principal_id": "producer-orders",
                "role": "PRODUCER",
                "queue_scopes": ["orders"],
                "credentials": [
                    {"generation_id": "old", "secret": "old-token"},
                    {"generation_id": "current", "secret": SENTINEL},
                ],
            },
            {
                "principal_id": "worker-orders",
                "role": "WORKER",
                "queue_scopes": ["orders"],
                "credentials": [
                    {"generation_id": "worker-current", "secret": "worker-token"}
                ],
            },
            {
                "principal_id": "observer-audit",
                "role": "OBSERVER",
                "queue_scopes": ["orders.audit"],
                "credentials": [
                    {"generation_id": "observer-current", "secret": "observer-token"}
                ],
            },
            {
                "principal_id": "admin-ops",
                "role": "ADMIN",
                "queue_scopes": [],
                "credentials": [
                    {"generation_id": "admin-current", "secret": "admin-token"}
                ],
            },
        ],
    }


def _raw(payload: object) -> bytes:
    return json.dumps(payload).encode("utf-8")


def test_valid_manifest_builds_rotating_bindings_and_exact_immutable_scopes() -> None:
    parsed = parse_principal_manifest(_raw(_manifest()))

    assert len(parsed.bindings) == 5
    producer = [
        binding
        for binding in parsed.bindings
        if binding.principal_id == "producer-orders"
    ]
    assert {binding.generation_id for binding in producer} == {"old", "current"}
    assert {binding.role for binding in producer} == {ServiceRole.PRODUCER}
    assert parsed.queue_scopes == {
        "producer-orders": frozenset({"orders"}),
        "worker-orders": frozenset({"orders"}),
        "observer-audit": frozenset({"orders.audit"}),
        "admin-ops": frozenset(),
    }
    with pytest.raises(TypeError):
        parsed.queue_scopes["producer-orders"] = frozenset()  # type: ignore[index]

    authenticator = BearerCredentialAuthenticator.from_bindings(parsed.bindings)
    old = authenticator.authenticate("Bearer old-token")
    current = authenticator.authenticate(f"Bearer {SENTINEL}")
    assert isinstance(old, Principal)
    assert isinstance(current, Principal)
    assert old == current

    authorizer = Authorizer(parsed.queue_scopes)
    assert isinstance(
        authorizer.authorize(current, Operation.ENQUEUE_TASK, queue_name="orders"),
        AuthorizationContext,
    )
    assert isinstance(
        authorizer.authorize(current, Operation.ENQUEUE_TASK, queue_name="other"),
        AuthorizationDenied,
    )
    assert isinstance(
        authorizer.authorize(current, Operation.CLAIM_TASKS, queue_name="orders"),
        AuthorizationDenied,
    )
    admin = authenticator.authenticate("Bearer admin-token")
    assert isinstance(admin, Principal)
    assert isinstance(
        authorizer.authorize(admin, Operation.GET_QUEUE, queue_name="orders"),
        AuthorizationContext,
    )
    assert isinstance(
        authorizer.authorize(admin, Operation.ENQUEUE_TASK, queue_name="orders"),
        AuthorizationDenied,
    )


@pytest.mark.parametrize("role", ["RELAY", "BREAK_GLASS", "MIGRATOR", "MAINTAINER", "producer"])
def test_unsupported_or_non_uppercase_roles_fail(role: str) -> None:
    payload = _manifest()
    payload["principals"][0]["role"] = role  # type: ignore[index]
    with pytest.raises(SettingsValidationError, match=r"\$\.principals\[0\]\.role"):
        parse_principal_manifest(_raw(payload))


@pytest.mark.parametrize(
    ("mutation", "path"),
    [
        (lambda p: p.update(extra=True), r"\$: unknown property"),
        (
            lambda p: p["principals"][0].update(extra=True),
            r"\$\.principals\[0\]: unknown property",
        ),
        (
            lambda p: p["principals"][0]["credentials"][0].update(extra=True),
            r"\.credentials\[0\]: unknown property",
        ),
        (
            lambda p: p["principals"][0].update(queue_scopes=["Orders"]),
            r"\.queue_scopes\[0\]",
        ),
        (
            lambda p: p["principals"][0].update(credentials=[]),
            r"\.credentials",
        ),
    ],
)
def test_strict_shape_and_queue_validation_fail(
    mutation: object, path: str
) -> None:
    payload = _manifest()
    mutation(payload)  # type: ignore[operator]
    with pytest.raises(SettingsValidationError, match=path):
        parse_principal_manifest(_raw(payload))


def test_non_admin_scopes_required_and_admin_scopes_forbidden() -> None:
    payload = _manifest()
    payload["principals"][0]["queue_scopes"] = []  # type: ignore[index]
    with pytest.raises(SettingsValidationError, match="non-empty"):
        parse_principal_manifest(_raw(payload))

    payload = _manifest()
    payload["principals"][3]["queue_scopes"] = ["orders"]  # type: ignore[index]
    with pytest.raises(SettingsValidationError, match="ADMIN"):
        parse_principal_manifest(_raw(payload))


@pytest.mark.parametrize("duplicate", ["principal", "generation", "secret"])
def test_ambiguous_identity_or_credential_reuse_fails_without_leak(
    duplicate: str,
) -> None:
    payload = _manifest()
    principals = payload["principals"]  # type: ignore[assignment]
    if duplicate == "principal":
        principals[1]["principal_id"] = principals[0]["principal_id"]
    elif duplicate == "generation":
        principals[1]["credentials"][0]["generation_id"] = "old"
    else:
        principals[1]["credentials"][0]["secret"] = SENTINEL

    with pytest.raises(SettingsValidationError) as exc_info:
        parse_principal_manifest(_raw(payload))
    rendered = repr(exc_info.value) + str(exc_info.value)
    assert SENTINEL not in rendered
    assert json.dumps(payload) not in rendered


@pytest.mark.parametrize(
    "raw",
    [
        b"",
        b" \r\n",
        b"{not-json",
        b"\xff",
        _raw({"schema_version": 2, "principals": []}),
        b"x" * (PRINCIPAL_MANIFEST_MAX_BYTES + 1),
    ],
    ids=["empty", "blank", "invalid-json", "invalid-utf8", "version", "oversized"],
)
def test_invalid_bytes_fail_closed_without_source_disclosure(raw: bytes) -> None:
    with pytest.raises(SettingsValidationError) as exc_info:
        parse_principal_manifest(raw)
    assert SENTINEL not in str(exc_info.value)


def test_secret_is_redacted_from_all_parsed_representations() -> None:
    parsed = parse_principal_manifest(_raw(_manifest()))
    rendered = repr(parsed) + str(parsed) + repr(parsed.bindings)
    assert SENTINEL not in rendered


def test_invalid_json_source_is_absent_from_formatted_traceback() -> None:
    raw = f'{{"secret":"{SENTINEL}",'.encode()
    with pytest.raises(SettingsValidationError) as exc_info:
        parse_principal_manifest(raw)
    rendered = "".join(
        traceback.format_exception(
            type(exc_info.value),
            exc_info.value,
            exc_info.value.__traceback__,
        )
    )
    assert SENTINEL not in rendered
