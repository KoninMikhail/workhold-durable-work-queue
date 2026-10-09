"""Black-box admin pause/resume conformance (Phase 03.3-05).

Covers CTRL-01..05 / CTRL-09 at the private `/admin/v1` edge: pause/resume only,
optimistic config_version, auth boundaries, and no drain admin route.
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

ADMIN_TOKEN = "tok-admin-state"
ADMIN_OTHER_TOKEN = "tok-admin-state-other"
PRODUCER_TOKEN = "tok-producer-state"
WORKER_TOKEN = "tok-worker-state"

ADMIN_PRINCIPAL = "admin-state"
ADMIN_OTHER_PRINCIPAL = "admin-state-other"
BASE_QUEUE_NAME = "orders.state"
OTHER_QUEUE = "billing.state"


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
            principal_id="producer-state",
            role=ServiceRole.PRODUCER,
            generation_id="g1",
            secret=Secret(PRODUCER_TOKEN),
        ),
        CredentialBinding(
            principal_id="worker-state",
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
            "producer-state": frozenset({queue_name, BASE_QUEUE_NAME}),
            "worker-state": frozenset({queue_name, BASE_QUEUE_NAME}),
        }
    )


@pytest.fixture
def admin_session_factory(migrated_schema) -> Iterator[sessionmaker[Session]]:
    _conn, schema = migrated_schema
    database_url = os.environ.get("TEST_DATABASE_URL", "").strip()
    if not database_url:
        pytest.fail("TEST_DATABASE_URL is required for admin state conformance")
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
        bind=ListenerBind(host="127.0.0.1", port=18085),
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


def _admin_headers(*, idempotency_key: str | None = "idem-state-1") -> dict[str, str]:
    headers = {
        "Authorization": f"Bearer {ADMIN_TOKEN}",
        "Content-Type": "application/json",
    }
    if idempotency_key is not None:
        headers["Idempotency-Key"] = idempotency_key
    return headers


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


def _assert_error(body: bytes, *, code: str, retryable: bool) -> dict[str, Any]:
    payload = json.loads(body.decode("utf-8"))
    assert isinstance(payload, dict)
    assert payload["code"] == code
    assert payload["retryable"] is retryable
    assert "request_id" in payload
    return payload


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


def test_authorized_pause_and_resume_match_openapi(
    admin_app: Any,
    queue_name: str,
) -> None:
    harness = ConformanceHarness(
        openapi_path=OPENAPI_PATH,
        base_url="http://127.0.0.1:9",
    )
    queue = _seed_queue(admin_app, queue_name)
    assert queue["state"] == "active"
    assert queue["config_version"] == 1

    pause_body = {"expected_config_version": 1, "state": "paused"}
    harness.validate_request_fixture("setQueueState", pause_body)
    status, headers, raw = _asgi_http_call(
        admin_app,
        method="POST",
        path=f"/admin/v1/queues/{queue_name}:set-state",
        headers=_admin_headers(idempotency_key=f"idem-pause-{uuid.uuid4().hex}"),
        body=json.dumps(pause_body).encode("utf-8"),
    )
    assert status == 200, raw.decode("utf-8")
    paused = json.loads(raw.decode("utf-8"))
    findings = harness._validate_response(  # noqa: SLF001
        "setQueueState",
        ObservedResponse(
            status=status,
            headers=headers,
            body_text=raw.decode("utf-8"),
            body_json=paused,
            content_type=headers.get("content-type"),
        ),
    )
    assert findings == [], findings
    assert paused["queue"]["state"] == "paused"
    assert paused["queue"]["config_version"] == 2

    resume_body = {"expected_config_version": 2, "state": "active"}
    status, headers, raw = _asgi_http_call(
        admin_app,
        method="POST",
        path=f"/admin/v1/queues/{queue_name}:set-state",
        headers=_admin_headers(idempotency_key=f"idem-resume-{uuid.uuid4().hex}"),
        body=json.dumps(resume_body).encode("utf-8"),
    )
    assert status == 200, raw.decode("utf-8")
    resumed = json.loads(raw.decode("utf-8"))
    findings = harness._validate_response(  # noqa: SLF001
        "setQueueState",
        ObservedResponse(
            status=status,
            headers=headers,
            body_text=raw.decode("utf-8"),
            body_json=resumed,
            content_type=headers.get("content-type"),
        ),
    )
    assert findings == [], findings
    assert resumed["queue"]["state"] == "active"
    assert resumed["queue"]["config_version"] == 3


def test_admin_draining_via_set_state_is_accepted(
    admin_app: Any, queue_name: str
) -> None:
    """ADR 010: draining is a first-class runtime state via set-state (not rejected)."""
    harness = ConformanceHarness(
        openapi_path=OPENAPI_PATH,
        base_url="http://127.0.0.1:9",
    )
    _seed_queue(admin_app, queue_name)
    drain_body = {"expected_config_version": 1, "state": "draining"}
    harness.validate_request_fixture("setQueueState", drain_body)
    status, headers, raw = _asgi_http_call(
        admin_app,
        method="POST",
        path=f"/admin/v1/queues/{queue_name}:set-state",
        headers=_admin_headers(idempotency_key=f"idem-drain-{uuid.uuid4().hex}"),
        body=json.dumps(drain_body).encode("utf-8"),
    )
    assert status == 200, raw.decode("utf-8")
    drained = json.loads(raw.decode("utf-8"))
    findings = harness._validate_response(  # noqa: SLF001
        "setQueueState",
        ObservedResponse(
            status=status,
            headers=headers,
            body_text=raw.decode("utf-8"),
            body_json=drained,
            content_type=headers.get("content-type"),
        ),
    )
    assert findings == [], findings
    assert drained["queue"]["state"] == "draining"
    assert drained["queue"]["config_version"] == 2

    status, _headers, raw = _asgi_http_call(
        admin_app,
        method="GET",
        path=f"/admin/v1/queues/{queue_name}",
        headers={"Authorization": f"Bearer {ADMIN_TOKEN}"},
    )
    assert status == 200, raw.decode("utf-8")
    body = json.loads(raw.decode("utf-8"))
    assert body["state"] == "draining"
    assert body["config_version"] == 2


def test_no_dedicated_drain_admin_route(admin_app: Any, queue_name: str) -> None:
    _seed_queue(admin_app, queue_name)
    for path in (
        f"/admin/v1/queues/{queue_name}:drain",
        f"/admin/v1/queues/{queue_name}/drain",
        f"/admin/v1/queues/{queue_name}:start-drain",
    ):
        status, _headers, _raw = _asgi_http_call(
            admin_app,
            method="POST",
            path=path,
            headers=_admin_headers(idempotency_key=f"idem-route-{uuid.uuid4().hex}"),
            body=b"{}",
        )
        assert status in {404, 405, 501}


def test_stale_set_state_returns_412(admin_app: Any, queue_name: str) -> None:
    _seed_queue(admin_app, queue_name)
    status, _headers, raw = _asgi_http_call(
        admin_app,
        method="POST",
        path=f"/admin/v1/queues/{queue_name}:set-state",
        headers=_admin_headers(idempotency_key=f"idem-stale-{uuid.uuid4().hex}"),
        body=json.dumps(
            {"expected_config_version": 99, "state": "paused"}
        ).encode("utf-8"),
    )
    assert status == 412, raw.decode("utf-8")
    _assert_error(raw, code="config_version_conflict", retryable=True)


def test_producer_worker_and_wrong_queue_denied(
    admin_app: Any,
    queue_name: str,
) -> None:
    _seed_queue(admin_app, queue_name)
    body = json.dumps(
        {"expected_config_version": 1, "state": "paused"}
    ).encode("utf-8")
    for token in (PRODUCER_TOKEN, WORKER_TOKEN, ADMIN_OTHER_TOKEN):
        status, _headers, raw = _asgi_http_call(
            admin_app,
            method="POST",
            path=f"/admin/v1/queues/{queue_name}:set-state",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Idempotency-Key": f"idem-{token}-{uuid.uuid4().hex}",
            },
            body=body,
        )
        assert status == 403, raw.decode("utf-8")
        _assert_error(raw, code="permission_denied", retryable=False)


def test_unauthenticated_set_state_returns_401(
    admin_app: Any,
    queue_name: str,
) -> None:
    _seed_queue(admin_app, queue_name)
    status, _headers, raw = _asgi_http_call(
        admin_app,
        method="POST",
        path=f"/admin/v1/queues/{queue_name}:set-state",
        headers={
            "Content-Type": "application/json",
            "Idempotency-Key": "idem-unauth",
        },
        body=json.dumps(
            {"expected_config_version": 1, "state": "paused"}
        ).encode("utf-8"),
    )
    assert status == 401, raw.decode("utf-8")
    _assert_error(raw, code="unauthenticated", retryable=False)


def test_authorized_pause_and_resume_via_admin_client(
    admin_http_url: str,
    queue_name: str,
) -> None:
    """Pause/resume success path through typed AdminClient."""
    from tests.conformance.admin_client_helpers import make_admin_client, retry_policy_draft

    client = make_admin_client(admin_http_url, bearer_token=ADMIN_TOKEN)
    created = client.create_queue(
        queue_name,
        initial_policy=retry_policy_draft(),
        idempotency_key=f"idem-sdk-seed-{uuid.uuid4().hex}",
    )
    assert created.queue.config_version == 1
    paused = client.set_queue_state(
        queue_name,
        "paused",
        expected_config_version=1,
        idempotency_key=f"idem-sdk-pause-{uuid.uuid4().hex}",
    )
    assert paused.queue.state.value == "paused"
    assert paused.queue.config_version == 2
    resumed = client.set_queue_state(
        queue_name,
        "active",
        expected_config_version=2,
        idempotency_key=f"idem-sdk-resume-{uuid.uuid4().hex}",
    )
    assert resumed.queue.state.value == "active"
    assert resumed.queue.config_version == 3
