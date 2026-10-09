"""Black-box admin create/read conformance (Phase 03.3-03).

Covers WORK-01 / WORK-12 / CTRL-03 / CTRL-04 / CTRL-05 / CTRL-07 / CTRL-08 at the
private `/admin/v1` HTTP edge: OpenAPI-shaped create/read, separate admin auth,
deployment-field rejection, and secret/internal-key redaction.
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session, sessionmaker

from workhold.api.admin import create_admin_app
from workhold.api.security import ListenerBind
from workhold.infrastructure.postgres.queue_control_repository import (
    QueueControlRepository,
)
from workhold.security.authorization import Authorizer
from workhold.security.credentials import (
    BearerCredentialAuthenticator,
    CredentialBinding,
)
from workhold.security.principals import ServiceRole
from workhold.settings import Secret
from tests.conformance.harness import ConformanceHarness, ObservedResponse

pytest_plugins = ["tests.integration.conftest", "tests.conformance.admin_client_helpers"]

REPO_ROOT = Path(__file__).resolve().parents[2]
OPENAPI_PATH = REPO_ROOT / "openapi" / "queue.openapi.json"

ADMIN_TOKEN = "tok-admin-create-read"
ADMIN_OTHER_TOKEN = "tok-admin-other-scope"
PRODUCER_TOKEN = "tok-producer-create-read"
WORKER_TOKEN = "tok-worker-create-read"

ADMIN_PRINCIPAL = "admin-create-read"
ADMIN_OTHER_PRINCIPAL = "admin-other-scope"
BASE_QUEUE_NAME = "orders.intake"
OTHER_QUEUE = "billing.intake"

DEPLOYMENT_ONLY_FIELDS = (
    "database_url",
    "ddl",
    "partition_layout",
    "pool_size",
    "listener_tls_mode",
    "credential",
    "hard_limit",
    "max_connections",
    "search_path",
)


def _unique_queue_name(prefix: str = BASE_QUEUE_NAME) -> str:
    # OpenAPI pattern: ^[a-z0-9][a-z0-9._-]*$
    return f"{prefix}.{uuid.uuid4().hex[:12]}"


def _bindings() -> tuple[CredentialBinding, ...]:
    return (
        CredentialBinding(
            principal_id=ADMIN_PRINCIPAL,
            role=ServiceRole.ADMIN,
            generation_id="g1",
            secret=Secret(ADMIN_TOKEN),
        ),
        CredentialBinding(
            principal_id=ADMIN_OTHER_PRINCIPAL,
            role=ServiceRole.ADMIN,
            generation_id="g1",
            secret=Secret(ADMIN_OTHER_TOKEN),
        ),
        CredentialBinding(
            principal_id="producer-create-read",
            role=ServiceRole.PRODUCER,
            generation_id="g1",
            secret=Secret(PRODUCER_TOKEN),
        ),
        CredentialBinding(
            principal_id="worker-create-read",
            role=ServiceRole.WORKER,
            generation_id="g1",
            secret=Secret(WORKER_TOKEN),
        ),
    )


@pytest.fixture
def queue_name() -> str:
    return _unique_queue_name()


@pytest.fixture
def authorizer(queue_name: str) -> Authorizer:
    return Authorizer(
        queue_scopes={
            ADMIN_PRINCIPAL: frozenset({queue_name, BASE_QUEUE_NAME}),
            ADMIN_OTHER_PRINCIPAL: frozenset({OTHER_QUEUE}),
            "producer-create-read": frozenset({queue_name, BASE_QUEUE_NAME}),
            "worker-create-read": frozenset({queue_name, BASE_QUEUE_NAME}),
        }
    )


@pytest.fixture
def admin_session_factory(migrated_schema) -> Iterator[sessionmaker[Session]]:
    _conn, schema = migrated_schema
    database_url = os.environ.get("TEST_DATABASE_URL", "").strip()
    if not database_url:
        pytest.fail("TEST_DATABASE_URL is required for admin create/read conformance")
    engine = create_engine(database_url, pool_pre_ping=True)

    @event.listens_for(engine, "connect")
    def _set_search_path(dbapi_connection, _connection_record) -> None:  # noqa: ANN001
        previous = dbapi_connection.autocommit
        dbapi_connection.autocommit = True
        try:
            cursor = dbapi_connection.cursor()
            cursor.execute(f'SET search_path TO "{schema}"')
            cursor.close()
        finally:
            dbapi_connection.autocommit = previous

    factory = sessionmaker(bind=engine, expire_on_commit=False)
    try:
        yield factory
    finally:
        engine.dispose()


@pytest.fixture
def admin_app(
    admin_session_factory: sessionmaker[Session],
    authorizer: Authorizer,
) -> Any:
    return create_admin_app(
        authenticator=BearerCredentialAuthenticator.from_bindings(_bindings()),
        authorizer=authorizer,
        bind=ListenerBind(host="127.0.0.1", port=18081),
        session_factory=admin_session_factory,
        repository=QueueControlRepository(),
        deployment_retry_delay_ceiling_seconds=86_400,
    )


def _asgi_http_call(
    app: Any,
    *,
    method: str,
    path: str,
    headers: Mapping[str, str] | None = None,
    body: bytes = b"",
) -> tuple[int, dict[str, str], bytes]:
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


def _create_body(*, name: str, **extra: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "name": name,
        "initial_policy": {
            "enabled": True,
            "max_attempts": 3,
            "backoff_strategy": "fixed",
            "retry_delay_seconds": 5,
        },
    }
    body.update(extra)
    return body


def _admin_headers(*, idempotency_key: str | None = "idem-create-1") -> dict[str, str]:
    headers = {
        "Authorization": f"Bearer {ADMIN_TOKEN}",
        "Content-Type": "application/json",
    }
    if idempotency_key is not None:
        headers["Idempotency-Key"] = idempotency_key
    return headers


def _assert_error(
    body: bytes,
    *,
    code: str,
    retryable: bool,
) -> dict[str, Any]:
    payload = json.loads(body.decode("utf-8"))
    assert isinstance(payload, dict)
    assert payload["code"] == code
    assert payload["retryable"] is retryable
    assert "request_id" in payload
    assert isinstance(payload.get("details"), dict)
    return payload


def _assert_no_secrets_or_internal_keys(payload: Any) -> None:
    text = json.dumps(payload, default=str)
    for forbidden in (
        ADMIN_TOKEN,
        PRODUCER_TOKEN,
        WORKER_TOKEN,
        "password",
        "secret",
        '"id":',
        "active_policy_version_id",
        "state_code",
        "backoff_strategy_code",
        "key_hash",
        "DATABASE_URL",
    ):
        assert forbidden not in text


def test_create_request_fixture_rejects_deployment_fields() -> None:
    harness = ConformanceHarness(
        openapi_path=OPENAPI_PATH,
        base_url="http://127.0.0.1:9",
    )
    for field in DEPLOYMENT_ONLY_FIELDS:
        with pytest.raises(Exception):
            harness.validate_request_fixture(
                "createQueue",
                _create_body(name=BASE_QUEUE_NAME, **{field: "must-reject"}),
            )


def test_authorized_admin_create_and_read_match_openapi(
    admin_app: Any,
    queue_name: str,
) -> None:
    harness = ConformanceHarness(
        openapi_path=OPENAPI_PATH,
        base_url="http://127.0.0.1:9",
    )
    create_body = _create_body(name=queue_name)
    harness.validate_request_fixture("createQueue", create_body)

    status, headers, raw = _asgi_http_call(
        admin_app,
        method="POST",
        path="/admin/v1/queues",
        headers=_admin_headers(idempotency_key=f"idem-{uuid.uuid4().hex}"),
        body=json.dumps(create_body).encode("utf-8"),
    )
    assert status == 201
    created = json.loads(raw.decode("utf-8"))
    assert created["replayed"] is False
    assert "admin_replay_expires_at" in created
    queue = created["queue"]
    assert queue["name"] == queue_name
    assert queue["state"] == "active"
    assert queue["config_version"] == 1
    assert queue["active_policy"]["version"] == 1
    assert queue["active_policy"]["enabled"] is True
    assert queue["active_policy"]["max_attempts"] == 3
    assert queue["active_policy"]["backoff_strategy"] == "fixed"
    assert queue["active_policy"]["retry_delay_seconds"] == 5
    _assert_no_secrets_or_internal_keys(created)

    create_findings = harness._validate_response(  # noqa: SLF001 - schema gate
        "createQueue",
        ObservedResponse(
            status=status,
            headers=headers,
            body_text=raw.decode("utf-8"),
            body_json=created,
            content_type=headers.get("content-type"),
        ),
    )
    assert create_findings == [], create_findings

    status, headers, raw = _asgi_http_call(
        admin_app,
        method="GET",
        path=f"/admin/v1/queues/{queue_name}",
        headers={"Authorization": f"Bearer {ADMIN_TOKEN}"},
    )
    assert status == 200
    read = json.loads(raw.decode("utf-8"))
    assert read["queue_id"] == queue["queue_id"]
    assert read["name"] == queue_name
    assert read["config_version"] == 1
    assert read["active_policy"]["version"] == 1
    _assert_no_secrets_or_internal_keys(read)

    read_findings = harness._validate_response(  # noqa: SLF001
        "getQueue",
        ObservedResponse(
            status=status,
            headers=headers,
            body_text=raw.decode("utf-8"),
            body_json=read,
            content_type=headers.get("content-type"),
        ),
    )
    assert read_findings == [], read_findings


def test_unauthenticated_create_returns_401(admin_app: Any, queue_name: str) -> None:
    status, _headers, body = _asgi_http_call(
        admin_app,
        method="POST",
        path="/admin/v1/queues",
        headers={
            "Content-Type": "application/json",
            "Idempotency-Key": "idem-unauth",
        },
        body=json.dumps(_create_body(name=queue_name)).encode("utf-8"),
    )
    assert status == 401
    _assert_error(body, code="unauthenticated", retryable=False)


def test_producer_and_worker_denied_on_create_and_read(
    admin_app: Any,
    queue_name: str,
) -> None:
    for token in (PRODUCER_TOKEN, WORKER_TOKEN):
        status, _headers, body = _asgi_http_call(
            admin_app,
            method="POST",
            path="/admin/v1/queues",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Idempotency-Key": f"idem-{token}",
            },
            body=json.dumps(_create_body(name=queue_name)).encode("utf-8"),
        )
        assert status == 403
        _assert_error(body, code="permission_denied", retryable=False)

        status, _headers, body = _asgi_http_call(
            admin_app,
            method="GET",
            path=f"/admin/v1/queues/{queue_name}",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert status == 403
        _assert_error(body, code="permission_denied", retryable=False)


def test_wrong_queue_admin_denied_on_read(admin_app: Any, queue_name: str) -> None:
    status, _headers, raw = _asgi_http_call(
        admin_app,
        method="POST",
        path="/admin/v1/queues",
        headers=_admin_headers(idempotency_key=f"idem-seed-{uuid.uuid4().hex}"),
        body=json.dumps(_create_body(name=queue_name)).encode("utf-8"),
    )
    assert status == 201, raw.decode("utf-8")

    status, _headers, body = _asgi_http_call(
        admin_app,
        method="GET",
        path=f"/admin/v1/queues/{queue_name}",
        headers={"Authorization": f"Bearer {ADMIN_OTHER_TOKEN}"},
    )
    assert status == 403
    _assert_error(body, code="permission_denied", retryable=False)


def test_unknown_queue_returns_queue_not_found(admin_app: Any, queue_name: str) -> None:
    status, _headers, body = _asgi_http_call(
        admin_app,
        method="GET",
        path=f"/admin/v1/queues/{queue_name}",
        headers={"Authorization": f"Bearer {ADMIN_TOKEN}"},
    )
    assert status == 404
    err = _assert_error(body, code="queue_not_found", retryable=False)
    assert err["retryable"] is False


def test_duplicate_create_returns_idempotency_conflict(
    admin_app: Any,
    queue_name: str,
) -> None:
    body = _create_body(name=queue_name)
    first = _asgi_http_call(
        admin_app,
        method="POST",
        path="/admin/v1/queues",
        headers=_admin_headers(idempotency_key=f"idem-dup-a-{uuid.uuid4().hex}"),
        body=json.dumps(body).encode("utf-8"),
    )
    assert first[0] == 201

    status, _headers, raw = _asgi_http_call(
        admin_app,
        method="POST",
        path="/admin/v1/queues",
        headers=_admin_headers(idempotency_key=f"idem-dup-b-{uuid.uuid4().hex}"),
        body=json.dumps(body).encode("utf-8"),
    )
    assert status == 409
    _assert_error(raw, code="idempotency_conflict", retryable=False)


def test_undeclared_and_deployment_fields_rejected(
    admin_app: Any,
    queue_name: str,
) -> None:
    for field in (*DEPLOYMENT_ONLY_FIELDS, "extra_payload", "events"):
        status, _headers, raw = _asgi_http_call(
            admin_app,
            method="POST",
            path="/admin/v1/queues",
            headers=_admin_headers(idempotency_key=f"idem-bad-{field}-{uuid.uuid4().hex}"),
            body=json.dumps(_create_body(name=queue_name, **{field: True})).encode(
                "utf-8"
            ),
        )
        assert status == 400, field
        err = _assert_error(raw, code="validation_failed", retryable=False)
        _assert_no_secrets_or_internal_keys(err)


def test_missing_idempotency_key_rejected(admin_app: Any, queue_name: str) -> None:
    status, _headers, raw = _asgi_http_call(
        admin_app,
        method="POST",
        path="/admin/v1/queues",
        headers=_admin_headers(idempotency_key=None),
        body=json.dumps(_create_body(name=queue_name)).encode("utf-8"),
    )
    assert status == 400
    _assert_error(raw, code="idempotency_key_required", retryable=False)


def test_authorized_admin_create_and_read_via_admin_client(
    admin_http_url: str,
    queue_name: str,
) -> None:
    """Same create/read success path through typed AdminClient."""
    from tests.conformance.admin_client_helpers import make_admin_client, retry_policy_draft

    client = make_admin_client(admin_http_url, bearer_token=ADMIN_TOKEN)
    created = client.create_queue(
        queue_name,
        initial_policy=retry_policy_draft(),
        idempotency_key=f"idem-sdk-{uuid.uuid4().hex}",
    )
    assert created.queue.name == queue_name
    assert created.queue.config_version == 1
    fetched = client.get_queue(queue_name)
    assert fetched.name == queue_name
    assert fetched.config_version == 1
