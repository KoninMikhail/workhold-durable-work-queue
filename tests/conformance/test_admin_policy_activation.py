"""Black-box admin policy create/activate conformance (Phase 03.3-04).

Covers WORK-12 / CTRL-02 / CTRL-03 / CTRL-04 / CTRL-05 at the private `/admin/v1`
edge: immutable policy create, optimistic activation, auth boundaries, and
OpenAPI response/error shapes.
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

from queue_service.api.admin import create_admin_app
from queue_service.api.security import ListenerBind
from queue_service.infrastructure.postgres.queue_control_repository import (
    QueueControlRepository,
)
from queue_service.security.authorization import Authorizer
from queue_service.security.credentials import (
    BearerCredentialAuthenticator,
    CredentialBinding,
)
from queue_service.security.principals import ServiceRole
from queue_service.settings import Secret
from tests.conformance.harness import ConformanceHarness, ObservedResponse

pytest_plugins = ["tests.integration.conftest", "tests.conformance.admin_client_helpers"]

REPO_ROOT = Path(__file__).resolve().parents[2]
OPENAPI_PATH = REPO_ROOT / "openapi" / "queue.openapi.json"

ADMIN_TOKEN = "tok-admin-policy"
ADMIN_OTHER_TOKEN = "tok-admin-policy-other"
PRODUCER_TOKEN = "tok-producer-policy"
WORKER_TOKEN = "tok-worker-policy"

ADMIN_PRINCIPAL = "admin-policy"
ADMIN_OTHER_PRINCIPAL = "admin-policy-other"
BASE_QUEUE_NAME = "orders.policy"
OTHER_QUEUE = "billing.policy"


def _unique_queue_name(prefix: str = BASE_QUEUE_NAME) -> str:
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
            principal_id="producer-policy",
            role=ServiceRole.PRODUCER,
            generation_id="g1",
            secret=Secret(PRODUCER_TOKEN),
        ),
        CredentialBinding(
            principal_id="worker-policy",
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
            "producer-policy": frozenset({queue_name, BASE_QUEUE_NAME}),
            "worker-policy": frozenset({queue_name, BASE_QUEUE_NAME}),
        }
    )


@pytest.fixture
def admin_session_factory(migrated_schema) -> Iterator[sessionmaker[Session]]:
    _conn, schema = migrated_schema
    database_url = os.environ.get("TEST_DATABASE_URL", "").strip()
    if not database_url:
        pytest.fail("TEST_DATABASE_URL is required for admin policy conformance")
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
        bind=ListenerBind(host="127.0.0.1", port=18082),
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


def _create_queue_body(*, name: str) -> dict[str, Any]:
    return {
        "name": name,
        "initial_policy": {
            "enabled": True,
            "max_attempts": 3,
            "backoff_strategy": "fixed",
            "retry_delay_seconds": 5,
        },
    }


def _policy_body(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "enabled": False,
        "max_attempts": 1,
        "backoff_strategy": "fixed",
        "retry_delay_seconds": 0,
    }
    body.update(overrides)
    return body


def _admin_headers(*, idempotency_key: str | None = "idem-policy-1") -> dict[str, str]:
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


def _seed_queue(admin_app: Any, queue_name: str) -> dict[str, Any]:
    status, _headers, raw = _asgi_http_call(
        admin_app,
        method="POST",
        path="/admin/v1/queues",
        headers=_admin_headers(idempotency_key=f"idem-seed-{uuid.uuid4().hex}"),
        body=json.dumps(_create_queue_body(name=queue_name)).encode("utf-8"),
    )
    assert status == 201, raw.decode("utf-8")
    return json.loads(raw.decode("utf-8"))["queue"]


def test_authorized_create_and_activate_match_openapi(
    admin_app: Any,
    queue_name: str,
) -> None:
    harness = ConformanceHarness(
        openapi_path=OPENAPI_PATH,
        base_url="http://127.0.0.1:9",
    )
    queue = _seed_queue(admin_app, queue_name)
    assert queue["config_version"] == 1
    assert queue["active_policy"]["version"] == 1

    create_body = _policy_body()
    harness.validate_request_fixture("createQueuePolicy", create_body)
    status, headers, raw = _asgi_http_call(
        admin_app,
        method="POST",
        path=f"/admin/v1/queues/{queue_name}/policies",
        headers=_admin_headers(idempotency_key=f"idem-create-{uuid.uuid4().hex}"),
        body=json.dumps(create_body).encode("utf-8"),
    )
    assert status == 200, raw.decode("utf-8")
    created = json.loads(raw.decode("utf-8"))
    assert created["replayed"] is False
    assert "admin_replay_expires_at" in created
    assert created["queue"]["config_version"] == 1
    assert created["queue"]["active_policy"]["version"] == 1
    _assert_no_secrets_or_internal_keys(created)
    create_findings = harness._validate_response(  # noqa: SLF001
        "createQueuePolicy",
        ObservedResponse(
            status=status,
            headers=headers,
            body_text=raw.decode("utf-8"),
            body_json=created,
            content_type=headers.get("content-type"),
        ),
    )
    assert create_findings == [], create_findings

    activate_body = {"expected_config_version": 1}
    harness.validate_request_fixture("activateQueuePolicy", activate_body)
    status, headers, raw = _asgi_http_call(
        admin_app,
        method="POST",
        path=f"/admin/v1/queues/{queue_name}/policies/2:activate",
        headers=_admin_headers(idempotency_key=f"idem-act-{uuid.uuid4().hex}"),
        body=json.dumps(activate_body).encode("utf-8"),
    )
    assert status == 200, raw.decode("utf-8")
    activated = json.loads(raw.decode("utf-8"))
    assert activated["replayed"] is False
    assert activated["queue"]["config_version"] == 2
    assert activated["queue"]["active_policy"]["version"] == 2
    assert activated["queue"]["active_policy"]["enabled"] is False
    assert activated["queue"]["active_policy"]["max_attempts"] == 1
    assert activated["queue"]["active_policy"]["retry_delay_seconds"] == 0
    _assert_no_secrets_or_internal_keys(activated)
    activate_findings = harness._validate_response(  # noqa: SLF001
        "activateQueuePolicy",
        ObservedResponse(
            status=status,
            headers=headers,
            body_text=raw.decode("utf-8"),
            body_json=activated,
            content_type=headers.get("content-type"),
        ),
    )
    assert activate_findings == [], activate_findings


def test_stale_activate_returns_412_config_version_conflict(
    admin_app: Any,
    queue_name: str,
) -> None:
    _seed_queue(admin_app, queue_name)
    status, _headers, raw = _asgi_http_call(
        admin_app,
        method="POST",
        path=f"/admin/v1/queues/{queue_name}/policies",
        headers=_admin_headers(idempotency_key=f"idem-create-{uuid.uuid4().hex}"),
        body=json.dumps(_policy_body(enabled=True, max_attempts=2, retry_delay_seconds=3)).encode(
            "utf-8"
        ),
    )
    assert status == 200, raw.decode("utf-8")

    status, _headers, body = _asgi_http_call(
        admin_app,
        method="POST",
        path=f"/admin/v1/queues/{queue_name}/policies/2:activate",
        headers=_admin_headers(idempotency_key=f"idem-stale-{uuid.uuid4().hex}"),
        body=json.dumps({"expected_config_version": 99}).encode("utf-8"),
    )
    assert status == 412
    _assert_error(body, code="config_version_conflict", retryable=True)

    status, _headers, raw = _asgi_http_call(
        admin_app,
        method="GET",
        path=f"/admin/v1/queues/{queue_name}",
        headers={"Authorization": f"Bearer {ADMIN_TOKEN}"},
    )
    assert status == 200
    queue = json.loads(raw.decode("utf-8"))
    assert queue["config_version"] == 1
    assert queue["active_policy"]["version"] == 1


def test_unauthenticated_activate_returns_401(admin_app: Any, queue_name: str) -> None:
    _seed_queue(admin_app, queue_name)
    status, _headers, body = _asgi_http_call(
        admin_app,
        method="POST",
        path=f"/admin/v1/queues/{queue_name}/policies/1:activate",
        headers={
            "Content-Type": "application/json",
            "Idempotency-Key": "idem-unauth",
        },
        body=json.dumps({"expected_config_version": 1}).encode("utf-8"),
    )
    assert status == 401
    _assert_error(body, code="unauthenticated", retryable=False)


def test_producer_and_worker_denied_on_create_and_activate(
    admin_app: Any,
    queue_name: str,
) -> None:
    _seed_queue(admin_app, queue_name)
    for token in (PRODUCER_TOKEN, WORKER_TOKEN):
        status, _headers, body = _asgi_http_call(
            admin_app,
            method="POST",
            path=f"/admin/v1/queues/{queue_name}/policies",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Idempotency-Key": f"idem-{token}-create",
            },
            body=json.dumps(_policy_body()).encode("utf-8"),
        )
        assert status == 403
        _assert_error(body, code="permission_denied", retryable=False)

        status, _headers, body = _asgi_http_call(
            admin_app,
            method="POST",
            path=f"/admin/v1/queues/{queue_name}/policies/1:activate",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Idempotency-Key": f"idem-{token}-act",
            },
            body=json.dumps({"expected_config_version": 1}).encode("utf-8"),
        )
        assert status == 403
        _assert_error(body, code="permission_denied", retryable=False)


def test_wrong_queue_admin_denied_on_activate(admin_app: Any, queue_name: str) -> None:
    _seed_queue(admin_app, queue_name)
    status, _headers, body = _asgi_http_call(
        admin_app,
        method="POST",
        path=f"/admin/v1/queues/{queue_name}/policies/1:activate",
        headers={
            "Authorization": f"Bearer {ADMIN_OTHER_TOKEN}",
            "Content-Type": "application/json",
            "Idempotency-Key": f"idem-other-{uuid.uuid4().hex}",
        },
        body=json.dumps({"expected_config_version": 1}).encode("utf-8"),
    )
    assert status == 403
    _assert_error(body, code="permission_denied", retryable=False)


def test_missing_idempotency_key_rejected_on_create(
    admin_app: Any,
    queue_name: str,
) -> None:
    _seed_queue(admin_app, queue_name)
    status, _headers, raw = _asgi_http_call(
        admin_app,
        method="POST",
        path=f"/admin/v1/queues/{queue_name}/policies",
        headers=_admin_headers(idempotency_key=None),
        body=json.dumps(_policy_body()).encode("utf-8"),
    )
    assert status == 400
    _assert_error(raw, code="idempotency_key_required", retryable=False)


def test_authorized_create_and_activate_via_admin_client(
    admin_http_url: str,
    queue_name: str,
) -> None:
    """Policy create/activate success path through typed AdminClient."""
    from queue_service_admin.models import BackoffStrategy, RetryPolicyDraft
    from tests.conformance.admin_client_helpers import make_admin_client, retry_policy_draft

    client = make_admin_client(admin_http_url, bearer_token=ADMIN_TOKEN)
    created = client.create_queue(
        queue_name,
        initial_policy=retry_policy_draft(),
        idempotency_key=f"idem-sdk-seed-{uuid.uuid4().hex}",
    )
    assert created.queue.config_version == 1
    draft = RetryPolicyDraft(
        enabled=False,
        max_attempts=1,
        backoff_strategy=BackoffStrategy("fixed"),
        retry_delay_seconds=0,
    )
    policy = client.create_queue_policy(
        queue_name,
        draft,
        idempotency_key=f"idem-sdk-policy-{uuid.uuid4().hex}",
    )
    assert policy.queue.active_policy.version == 1
    activated = client.activate_queue_policy(
        queue_name,
        2,
        expected_config_version=1,
        idempotency_key=f"idem-sdk-act-{uuid.uuid4().hex}",
    )
    assert activated.queue.config_version == 2
    assert activated.queue.active_policy.version == 2
    assert activated.queue.active_policy.enabled is False
