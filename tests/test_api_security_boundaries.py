"""Black-box application/admin plane isolation and security boundaries (03.2-06).

Covers OPS-06 / SEC-01 / SEC-02 / SEC-03 / SEC-05 at the HTTP transport edge:
separate ASGI compositions, authenticate-then-authorize before lookup, and
sanitized diagnostics that never echo credentials, claim tokens, or payloads.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest

from workhold.api.admin import create_admin_app
from workhold.api.application import SKELETON_CODE, create_application_app
from workhold.api.security import (
    APPLICATION_OPERATIONS,
    ADMIN_OPERATIONS,
    ListenerBind,
    load_openapi_operation_catalog,
    plane_for_operation,
)
from workhold.security.authorization import Authorizer, Operation
from workhold.security.credentials import (
    BearerCredentialAuthenticator,
    CredentialBinding,
)
from workhold.security.principals import ServiceRole
from workhold.settings import Secret

REPO_ROOT = Path(__file__).resolve().parents[1]
OPENAPI_PATH = REPO_ROOT / "openapi" / "queue.openapi.json"

CREDENTIAL_SENTINEL = "super-secret-credential-xyz-03-2-06"
CLAIM_TOKEN_SENTINEL = "claim-token-leak-sentinel-03-2-06"
PAYLOAD_SENTINEL = "payload-body-leak-sentinel-03-2-06"

PRODUCER_TOKEN = "tok-producer-plane"
WORKER_TOKEN = "tok-worker-plane"
ADMIN_TOKEN = "tok-admin-plane"
OBSERVER_TOKEN = "tok-observer-plane"


class LookupProbe:
    """Detects handler/resource lookup execution."""

    def __init__(self) -> None:
        self.calls = 0

    def mark(self) -> None:
        self.calls += 1


def _bindings() -> tuple[CredentialBinding, ...]:
    return (
        CredentialBinding(
            principal_id="producer-1",
            role=ServiceRole.PRODUCER,
            generation_id="g1",
            secret=Secret(PRODUCER_TOKEN),
        ),
        CredentialBinding(
            principal_id="worker-1",
            role=ServiceRole.WORKER,
            generation_id="g1",
            secret=Secret(WORKER_TOKEN),
        ),
        CredentialBinding(
            principal_id="admin-1",
            role=ServiceRole.ADMIN,
            generation_id="g1",
            secret=Secret(ADMIN_TOKEN),
        ),
        CredentialBinding(
            principal_id="observer-1",
            role=ServiceRole.OBSERVER,
            generation_id="g1",
            secret=Secret(OBSERVER_TOKEN),
        ),
        # Sentinel credential present so accidental logging would expose it.
        CredentialBinding(
            principal_id="sentinel-principal",
            role=ServiceRole.PRODUCER,
            generation_id="g-sentinel",
            secret=Secret(CREDENTIAL_SENTINEL),
        ),
    )


def _authenticator() -> BearerCredentialAuthenticator:
    return BearerCredentialAuthenticator.from_bindings(_bindings())


def _authorizer() -> Authorizer:
    return Authorizer(
        queue_scopes={
            "producer-1": frozenset({"orders"}),
            "worker-1": frozenset({"orders"}),
            "admin-1": frozenset({"orders"}),
            "observer-1": frozenset({"orders"}),
            "sentinel-principal": frozenset({"orders"}),
        }
    )


def _apps(*, probe: LookupProbe | None = None):
    authn = _authenticator()
    authz = _authorizer()
    application = create_application_app(
        authenticator=authn,
        authorizer=authz,
        bind=ListenerBind(host="127.0.0.1", port=8080),
        lookup_probe=probe,
    )
    admin = create_admin_app(
        authenticator=authn,
        authorizer=authz,
        bind=ListenerBind(host="127.0.0.1", port=8081),
        lookup_probe=probe,
    )
    return application, admin


def _asgi_http_call(
    app: Any,
    *,
    method: str,
    path: str,
    headers: Mapping[str, str] | None = None,
    body: bytes = b"",
) -> tuple[int, dict[str, str], bytes]:
    """Minimal stdlib ASGI HTTP client (no httpx/starlette)."""

    header_list = [
        (k.lower().encode("latin-1"), v.encode("latin-1"))
        for k, v in (headers or {}).items()
    ]
    if body and not any(k == b"content-length" for k, _ in header_list):
        header_list.append((b"content-length", str(len(body)).encode("latin-1")))

    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": method.upper(),
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("utf-8"),
        "query_string": b"",
        "headers": header_list,
        "client": ("127.0.0.1", 12345),
        "server": ("test", 80),
    }

    request_body = body
    body_sent = False
    status_code = 500
    response_headers: dict[str, str] = {}
    response_chunks: list[bytes] = []

    async def receive() -> dict[str, Any]:
        nonlocal body_sent
        if not body_sent:
            body_sent = True
            return {
                "type": "http.request",
                "body": request_body,
                "more_body": False,
            }
        return {"type": "http.disconnect"}

    async def send(message: dict[str, Any]) -> None:
        nonlocal status_code, response_headers
        if message["type"] == "http.response.start":
            status_code = int(message["status"])
            response_headers = {
                k.decode("latin-1").lower(): v.decode("latin-1")
                for k, v in message.get("headers", [])
            }
        elif message["type"] == "http.response.body":
            chunk = message.get("body", b"")
            if chunk:
                response_chunks.append(chunk)

    asyncio.run(app(scope, receive, send))
    return status_code, response_headers, b"".join(response_chunks)


def _error(body: bytes) -> dict[str, Any]:
    payload = json.loads(body.decode("utf-8"))
    assert isinstance(payload, dict)
    return payload


def test_openapi_contract_present_with_security_schemes() -> None:
    assert OPENAPI_PATH.is_file(), "Phase 3.1 OpenAPI contract must exist"
    catalog = load_openapi_operation_catalog(OPENAPI_PATH)
    assert len(catalog) >= 20
    spec = json.loads(OPENAPI_PATH.read_text(encoding="utf-8"))
    schemes = spec["components"]["securitySchemes"]
    for name in ("ProducerBearer", "WorkerBearer", "ObserverBearer", "AdminBearer"):
        assert name in schemes


def test_every_openapi_operation_has_exactly_one_plane_mapping() -> None:
    catalog = load_openapi_operation_catalog(OPENAPI_PATH)
    seen: dict[str, str] = {}
    for operation_id, path, _method in catalog:
        plane = plane_for_operation(operation_id, path)
        assert plane in {"application", "admin"}
        if operation_id in seen and seen[operation_id] != plane:
            pytest.fail(f"operation {operation_id} mapped to multiple planes")
        seen[operation_id] = plane

    http_ops = {op.value for op in Operation} - {
        Operation.APPLY_MIGRATIONS.value,
        Operation.RUN_PARTITION_MAINTENANCE.value,
    }
    catalog_ids = {operation_id for operation_id, _, _ in catalog}
    assert catalog_ids == http_ops
    assert catalog_ids == APPLICATION_OPERATIONS | ADMIN_OPERATIONS
    assert APPLICATION_OPERATIONS.isdisjoint(ADMIN_OPERATIONS)

    # Multiply-mapped / unmapped must fail closed.
    with pytest.raises(ValueError):
        plane_for_operation("notARealOperation", "/v1/nowhere")
    with pytest.raises(ValueError):
        plane_for_operation("getCapabilities", "/admin/v1/queues")


def test_unauthenticated_returns_401_before_lookup() -> None:
    probe = LookupProbe()
    application, _admin = _apps(probe=probe)
    status, _headers, body = _asgi_http_call(
        application,
        method="GET",
        path="/v1/capabilities",
    )
    assert status == 401
    err = _error(body)
    assert err["code"] == "unauthenticated"
    assert err["retryable"] is False
    assert "request_id" in err
    assert probe.calls == 0


def test_invalid_credentials_return_401_envelope() -> None:
    probe = LookupProbe()
    application, _admin = _apps(probe=probe)
    status, _headers, body = _asgi_http_call(
        application,
        method="GET",
        path="/v1/capabilities",
        headers={"Authorization": "Bearer totally-wrong-token"},
    )
    assert status == 401
    err = _error(body)
    assert err["code"] == "unauthenticated"
    assert "totally-wrong-token" not in body.decode("utf-8")
    assert probe.calls == 0


def test_insufficient_role_returns_403_before_lookup() -> None:
    probe = LookupProbe()
    application, _admin = _apps(probe=probe)
    status, _headers, body = _asgi_http_call(
        application,
        method="POST",
        path="/v1/queues/orders/tasks",
        headers={
            "Authorization": f"Bearer {WORKER_TOKEN}",
            "Idempotency-Key": "k1",
            "Content-Type": "application/json",
        },
        body=b'{"payload":{"x":1}}',
    )
    assert status == 403
    err = _error(body)
    assert err["code"] == "permission_denied"
    assert err["retryable"] is False
    assert probe.calls == 0


def test_queue_scope_denial_returns_403_before_lookup() -> None:
    probe = LookupProbe()
    application, _admin = _apps(probe=probe)
    status, _headers, body = _asgi_http_call(
        application,
        method="POST",
        path="/v1/queues/billing/tasks",
        headers={
            "Authorization": f"Bearer {PRODUCER_TOKEN}",
            "Idempotency-Key": "k1",
            "Content-Type": "application/json",
        },
        body=json.dumps({"payload": {"secret": PAYLOAD_SENTINEL}}).encode("utf-8"),
    )
    assert status == 403
    err = _error(body)
    assert err["code"] == "permission_denied"
    assert PAYLOAD_SENTINEL not in body.decode("utf-8")
    assert probe.calls == 0


def test_authorized_application_request_reaches_handler() -> None:
    probe = LookupProbe()
    application, _admin = _apps(probe=probe)
    status, _headers, body = _asgi_http_call(
        application,
        method="GET",
        path="/v1/capabilities",
        headers={"Authorization": f"Bearer {PRODUCER_TOKEN}"},
    )
    assert status == 200
    assert probe.calls == 1
    payload = json.loads(body.decode("utf-8"))
    assert payload["scheduling"] is True
    assert "code" not in payload


def test_authenticated_capabilities_returns_200_openapi_const_payload() -> None:
    """Live GET /v1/capabilities after production composition (Plan 05 + 10)."""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session, sessionmaker

    from tests.contracts.test_openapi_contract import CAPABILITIES_CONSTS

    engine = create_engine("sqlite:///:memory:")
    factory: sessionmaker[Session] = sessionmaker(bind=engine, expire_on_commit=False)
    authn = _authenticator()
    authz = _authorizer()
    application = create_application_app(
        authenticator=authn,
        authorizer=authz,
        bind=ListenerBind(host="127.0.0.1", port=8080),
        session_factory=factory,
        schedule_horizon_seconds=86400,
    )

    producer_auth = {"Authorization": f"Bearer {PRODUCER_TOKEN}"}
    worker_auth = {"Authorization": f"Bearer {WORKER_TOKEN}"}
    enqueue_status, _, enqueue_body = _asgi_http_call(
        application,
        method="POST",
        path="/v1/queues/orders/tasks",
        headers={
            **producer_auth,
            "Idempotency-Key": "k-capabilities",
            "Content-Type": "application/json",
        },
        body=b'{"payload":{"x":1}}',
    )
    assert enqueue_status != 501
    assert SKELETON_CODE not in enqueue_body.decode("utf-8")

    claim_status, _, claim_body = _asgi_http_call(
        application,
        method="POST",
        path="/v1/claims",
        headers={**worker_auth, "Content-Type": "application/json"},
        body=b'{"queue_names":["orders"],"worker_id":"worker-1"}',
    )
    assert claim_status != 501
    assert claim_status != 403
    assert SKELETON_CODE not in claim_body.decode("utf-8")

    claim_payload = json.loads(claim_body.decode("utf-8"))
    claim_id = claim_payload["claims"][0]["claim_id"] if claim_payload.get("claims") else str(uuid.uuid4())
    complete_status, _, complete_body = _asgi_http_call(
        application,
        method="POST",
        path=f"/v1/claims/{claim_id}:complete",
        headers={**worker_auth, "Content-Type": "application/json"},
        body=b'{"spawn":[]}',
    )
    assert complete_status != 501
    assert complete_status != 403
    assert SKELETON_CODE not in complete_body.decode("utf-8")

    probe = LookupProbe()
    application_with_probe = create_application_app(
        authenticator=authn,
        authorizer=authz,
        bind=ListenerBind(host="127.0.0.1", port=8080),
        session_factory=factory,
        schedule_horizon_seconds=86400,
        lookup_probe=probe,
    )
    status, headers, body = _asgi_http_call(
        application_with_probe,
        method="GET",
        path="/v1/capabilities",
        headers=producer_auth,
    )
    assert status == 200
    assert probe.calls == 1
    assert "x-request-id" in headers
    payload = json.loads(body.decode("utf-8"))
    assert payload == CAPABILITIES_CONSTS
    assert payload["priority"] is True
    assert payload["schema_revision"] == "0001"
    assert payload["protocol_major"] == 1


@pytest.mark.parametrize(
    ("max_wait_seconds", "expect_long_polling"),
    (
        (0, False),
        (10, True),
        (20, True),
    ),
)
def test_authenticated_capabilities_advertise_deployment_max_wait(
    max_wait_seconds: int,
    expect_long_polling: bool,
) -> None:
    """Live capabilities fail-closed at 0 and mirror positive deployment ceilings."""

    from workhold.api.v1.capabilities import live_capabilities_for

    expected = live_capabilities_for(max_wait_seconds=max_wait_seconds)
    assert expected["long_polling"] is expect_long_polling
    assert expected["max_wait_seconds"] == max_wait_seconds
    assert expected["batch_claim"] is False
    assert expected["max_claim_tasks"] == 1

    application = create_application_app(
        authenticator=_authenticator(),
        authorizer=_authorizer(),
        bind=ListenerBind(host="127.0.0.1", port=8080),
        max_wait_seconds=max_wait_seconds,
    )
    status, _headers, body = _asgi_http_call(
        application,
        method="GET",
        path="/v1/capabilities",
        headers={"Authorization": f"Bearer {PRODUCER_TOKEN}"},
    )
    assert status == 200
    payload = json.loads(body.decode("utf-8"))
    assert payload == expected
    assert payload["long_polling"] is expect_long_polling
    assert payload["max_wait_seconds"] == max_wait_seconds
    assert payload["batch_claim"] is False
    assert payload["max_claim_tasks"] == 1


def test_authorized_admin_request_reaches_handler() -> None:
    probe = LookupProbe()
    _application, admin = _apps(probe=probe)
    status, _headers, body = _asgi_http_call(
        admin,
        method="GET",
        path="/admin/v1/queues",
        headers={"Authorization": f"Bearer {ADMIN_TOKEN}"},
    )
    assert status == 501
    assert probe.calls == 1
    payload = json.loads(body.decode("utf-8"))
    assert payload["code"] == "skeleton_operation_unsupported"


def test_application_plane_cannot_reach_admin_handlers() -> None:
    probe = LookupProbe()
    application, _admin = _apps(probe=probe)
    status, _headers, body = _asgi_http_call(
        application,
        method="GET",
        path="/admin/v1/queues",
        headers={"Authorization": f"Bearer {ADMIN_TOKEN}"},
    )
    assert status == 404
    err = _error(body)
    assert err["code"] in {"task_not_found", "queue_not_found"}
    assert probe.calls == 0


def test_admin_plane_does_not_expose_application_handlers() -> None:
    probe = LookupProbe()
    _application, admin = _apps(probe=probe)
    status, _headers, body = _asgi_http_call(
        admin,
        method="GET",
        path="/v1/capabilities",
        headers={"Authorization": f"Bearer {PRODUCER_TOKEN}"},
    )
    assert status == 404
    err = _error(body)
    assert err["code"] in {"task_not_found", "queue_not_found"}
    assert probe.calls == 0


def test_producer_denied_on_admin_plane_even_with_admin_path() -> None:
    probe = LookupProbe()
    _application, admin = _apps(probe=probe)
    status, _headers, body = _asgi_http_call(
        admin,
        method="GET",
        path="/admin/v1/queues",
        headers={"Authorization": f"Bearer {PRODUCER_TOKEN}"},
    )
    assert status == 403
    assert _error(body)["code"] == "permission_denied"
    assert probe.calls == 0


def test_security_diagnostics_redact_secrets_and_keep_request_id(
    caplog: pytest.LogCaptureFixture,
) -> None:
    probe = LookupProbe()
    application, _admin = _apps(probe=probe)
    with caplog.at_level(logging.INFO, logger="workhold.api.security"):
        status, _headers, body = _asgi_http_call(
            application,
            method="POST",
            path="/v1/queues/orders/tasks",
            headers={
                "Authorization": f"Bearer {CREDENTIAL_SENTINEL}",
                "Idempotency-Key": "k-sentinel",
                "Content-Type": "application/json",
                "X-Queue-Claim-Token": CLAIM_TOKEN_SENTINEL,
            },
            body=json.dumps({"payload": {"leak": PAYLOAD_SENTINEL}}).encode("utf-8"),
        )
    assert status in {401, 403, 501}
    err = _error(body)
    request_id = err["request_id"]
    assert request_id
    combined = body.decode("utf-8") + "\n".join(
        record.getMessage() for record in caplog.records
    )
    assert request_id in combined or any(
        request_id in str(getattr(record, "request_id", ""))
        or request_id in record.getMessage()
        for record in caplog.records
    )
    for sentinel in (CREDENTIAL_SENTINEL, CLAIM_TOKEN_SENTINEL, PAYLOAD_SENTINEL):
        assert sentinel not in combined
    assert probe.calls in {0, 1}


def test_listener_binds_are_distinct_on_plane_apps() -> None:
    application, admin = _apps()
    assert application.bind == ListenerBind(host="127.0.0.1", port=8080)
    assert admin.bind == ListenerBind(host="127.0.0.1", port=8081)
    assert application.bind != admin.bind
