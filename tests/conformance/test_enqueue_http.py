"""Black-box producer HTTP enqueue conformance (Phase 03.4-05).

Covers WORK-02 / WORK-10 / WORK-13 / OPS-04 / API-02 / API-03 at the public
`/v1` edge: OpenAPI-shaped durable enqueue, replay success, conflicts, admission,
auth scope, and redaction.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from collections.abc import Iterator, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import create_engine, event, func, select
from sqlalchemy.orm import Session, sessionmaker

from workhold.api.application import create_application_app
from workhold.api.security import ListenerBind
from workhold.domain.queue_control import (
    AdminRequestMetadata,
    BackoffStrategy,
    ConfigVersion,
    CreateQueueMutation,
    QueueState,
    RetryPolicyDraft,
    SetQueueStateMutation,
)
from workhold.infrastructure.postgres.queue_control_repository import (
    QueueControlRepository,
)
from workhold.intake.depth import DepthCeilings
from workhold.priority import PRIORITY_MAX
from workhold.intake.service import EnqueueService
from workhold.scheduling import SchedulingPolicy
from workhold.storage.models import QueueCounter, TaskPayloadActive
from workhold.security.authorization import Authorizer
from workhold.security.credentials import (
    BearerCredentialAuthenticator,
    CredentialBinding,
)
from workhold.security.principals import ServiceRole
from workhold.settings import Secret
from workhold.storage.models import EnqueueDedup, Queue, QueueCounter, TaskActive
from tests.conformance.harness import ConformanceHarness, ObservedResponse

pytest_plugins = ["tests.integration.conftest"]

REPO_ROOT = Path(__file__).resolve().parents[2]
OPENAPI_PATH = REPO_ROOT / "openapi" / "queue.openapi.json"

PRODUCER_TOKEN = "tok-producer-enqueue-http"
PRODUCER_OTHER_TOKEN = "tok-producer-enqueue-other"
WORKER_TOKEN = "tok-worker-enqueue-http"
ADMIN_TOKEN = "tok-admin-enqueue-http"

PRODUCER_PRINCIPAL = "producer-enqueue-http"
PRODUCER_OTHER_PRINCIPAL = "producer-enqueue-other"
WORKER_PRINCIPAL = "worker-enqueue-http"
ADMIN_PRINCIPAL = "admin-enqueue-http"

BASE_QUEUE_NAME = "orders.enqueue"
OTHER_QUEUE = "billing.enqueue"
PAYLOAD_SENTINEL = "SECRET_PAYLOAD_SHOULD_NEVER_LEAK"
CREDENTIAL_SENTINEL = PRODUCER_TOKEN


def _unique_queue_name(prefix: str = BASE_QUEUE_NAME) -> str:
    return f"{prefix}.{uuid.uuid4().hex[:12]}"


def _bindings() -> tuple[CredentialBinding, ...]:
    return (
        CredentialBinding(
            principal_id=PRODUCER_PRINCIPAL,
            role=ServiceRole.PRODUCER,
            generation_id="g1",
            secret=Secret(PRODUCER_TOKEN),
        ),
        CredentialBinding(
            principal_id=PRODUCER_OTHER_PRINCIPAL,
            role=ServiceRole.PRODUCER,
            generation_id="g1",
            secret=Secret(PRODUCER_OTHER_TOKEN),
        ),
        CredentialBinding(
            principal_id=WORKER_PRINCIPAL,
            role=ServiceRole.WORKER,
            generation_id="g1",
            secret=Secret(WORKER_TOKEN),
        ),
        CredentialBinding(
            principal_id=ADMIN_PRINCIPAL,
            role=ServiceRole.ADMIN,
            generation_id="g1",
            secret=Secret(ADMIN_TOKEN),
        ),
    )


@pytest.fixture
def queue_name() -> str:
    return _unique_queue_name()


@pytest.fixture
def authorizer(queue_name: str) -> Authorizer:
    return Authorizer(
        queue_scopes={
            PRODUCER_PRINCIPAL: frozenset({queue_name, BASE_QUEUE_NAME}),
            PRODUCER_OTHER_PRINCIPAL: frozenset({OTHER_QUEUE}),
            WORKER_PRINCIPAL: frozenset({queue_name, BASE_QUEUE_NAME}),
            ADMIN_PRINCIPAL: frozenset({queue_name, BASE_QUEUE_NAME}),
        }
    )


@pytest.fixture
def session_factory(migrated_schema) -> Iterator[sessionmaker[Session]]:
    _conn, schema = migrated_schema
    database_url = os.environ.get("TEST_DATABASE_URL", "").strip()
    if not database_url:
        pytest.fail("TEST_DATABASE_URL is required for enqueue HTTP conformance")
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
def app(
    session_factory: sessionmaker[Session],
    authorizer: Authorizer,
) -> Any:
    service = EnqueueService(
        session_factory=session_factory,
        depth_ceilings=DepthCeilings(
            queue_active_depth=100,
            instance_active_depth=500,
            retry_after_ms=250,
        ),
    )
    return create_application_app(
        authenticator=BearerCredentialAuthenticator.from_bindings(_bindings()),
        authorizer=authorizer,
        bind=ListenerBind(host="127.0.0.1", port=18091),
        session_factory=session_factory,
        enqueue_service=service,
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


def _admin_meta() -> AdminRequestMetadata:
    return AdminRequestMetadata(
        actor_id=ADMIN_PRINCIPAL,
        request_id=str(uuid.uuid4()),
        idempotency_key=f"admin-idem-{uuid.uuid4().hex}",
    )


def _seed_queue(session: Session, *, name: str) -> Queue:
    QueueControlRepository().create_named_queue(
        session,
        CreateQueueMutation(
            name=name,
            initial_policy=RetryPolicyDraft(
                enabled=True,
                max_attempts=3,
                backoff_strategy=BackoffStrategy.FIXED,
                retry_delay_seconds=5,
            ),
            metadata=_admin_meta(),
        ),
    )
    session.commit()
    return session.execute(select(Queue).where(Queue.name == name)).scalar_one()


def _set_state(
    session: Session,
    *,
    queue_name: str,
    state: QueueState,
    expected_config_version: int,
) -> None:
    QueueControlRepository().set_queue_state(
        session,
        queue_name=queue_name,
        mutation=SetQueueStateMutation(
            expected_config_version=ConfigVersion(value=expected_config_version),
            state=state,
            metadata=_admin_meta(),
        ),
    )
    session.commit()


def _enqueue_body(*, payload: Any | None = None, **extra: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "payload": payload if payload is not None else {"n": 1},
        "priority": 0,
    }
    body.update(extra)
    return body


def _producer_headers(
    *,
    idempotency_key: str | None = "idem-enqueue-1",
    token: str = PRODUCER_TOKEN,
) -> dict[str, str]:
    headers = {
        "Authorization": f"Bearer {token}",
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
    assert "message" in payload
    return payload


def _assert_no_secrets_or_payload(payload: Any, *, raw_response: bytes = b"") -> None:
    text = json.dumps(payload, default=str)
    combined = text + raw_response.decode("utf-8", errors="replace")
    for forbidden in (
        CREDENTIAL_SENTINEL,
        PRODUCER_OTHER_TOKEN,
        WORKER_TOKEN,
        ADMIN_TOKEN,
        PAYLOAD_SENTINEL,
        "password",
        "Bearer ",
    ):
        assert forbidden not in combined


def _intake_counts(session: Session, queue_name: str) -> tuple[int, int, int]:
    queue = session.execute(select(Queue).where(Queue.name == queue_name)).scalar_one_or_none()
    if queue is None:
        return (0, 0, 0)
    tasks = int(
        session.scalar(
            select(func.count()).select_from(TaskActive).where(TaskActive.queue_id == queue.id)
        )
        or 0
    )
    dedup = int(
        session.scalar(
            select(func.count())
            .select_from(EnqueueDedup)
            .where(EnqueueDedup.queue_id == queue.id)
        )
        or 0
    )
    counters = session.execute(
        select(QueueCounter).where(QueueCounter.queue_id == queue.id)
    ).scalar_one_or_none()
    depth = 0 if counters is None else int(counters.ready_count) + int(counters.delayed_count)
    return tasks, dedup, depth


def _validate_enqueue_response(
    harness: ConformanceHarness,
    *,
    status: int,
    headers: Mapping[str, str],
    body: bytes,
) -> dict[str, Any]:
    payload = json.loads(body.decode("utf-8"))
    findings = harness._validate_response(  # noqa: SLF001 - schema gate
        "enqueueTask",
        ObservedResponse(
            status=status,
            headers=dict(headers),
            body_text=body.decode("utf-8"),
            body_json=payload,
            content_type=headers.get("content-type", "application/json"),
        ),
    )
    assert findings == [], findings
    return payload


def test_enqueue_request_fixture_is_closed() -> None:
    harness = ConformanceHarness(
        openapi_path=OPENAPI_PATH,
        base_url="http://127.0.0.1:9",
    )
    # Closed request validation + JsonValue oneOf: prefer a string payload fixture.
    harness.validate_request_fixture("enqueueTask", _enqueue_body(payload="ok"))
    with pytest.raises(Exception):
        harness.validate_request_fixture(
            "enqueueTask",
            {"payload": "ok", "priority": 0, "spawn": []},
        )


def test_authorized_enqueue_success_and_matching_replay(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    harness = ConformanceHarness(openapi_path=OPENAPI_PATH, base_url="http://127.0.0.1:9")
    session = session_factory()
    try:
        _seed_queue(session, name=queue_name)
    finally:
        session.close()

    path = f"/v1/queues/{queue_name}/tasks"
    body = _enqueue_body(payload={"order_id": 42})
    raw = json.dumps(body, separators=(",", ":")).encode("utf-8")
    headers = _producer_headers(idempotency_key="idem-success-1")

    with caplog.at_level(logging.INFO):
        status, resp_headers, resp_body = _asgi_http_call(
            app, method="POST", path=path, headers=headers, body=raw
        )

    assert status == 201
    assert "x-request-id" in resp_headers
    assert "location" in resp_headers
    first = _validate_enqueue_response(
        harness, status=status, headers=resp_headers, body=resp_body
    )
    assert first["replayed"] is False
    task_id = first["task"]["task_id"]
    assert first["task"]["queue_name"] == queue_name
    assert first["task"]["producer_id"] == PRODUCER_PRINCIPAL
    assert first["task"]["priority"] == 0
    assert first["task"]["retry_policy_version"] >= 1
    assert first["task"]["state"] in {"ready", "delayed"}
    assert resp_headers["location"] == f"/v1/tasks/{task_id}"
    _assert_no_secrets_or_payload(first, raw_response=resp_body)

    status2, resp_headers2, resp_body2 = _asgi_http_call(
        app, method="POST", path=path, headers=headers, body=raw
    )
    assert status2 == 200
    second = _validate_enqueue_response(
        harness, status=status2, headers=resp_headers2, body=resp_body2
    )
    assert second["replayed"] is True
    assert second["task"]["task_id"] == task_id
    _assert_no_secrets_or_payload(second, raw_response=resp_body2)

    session = session_factory()
    try:
        tasks, dedup, _depth = _intake_counts(session, queue_name)
        assert tasks == 1
        assert dedup == 1
    finally:
        session.close()

    assert PAYLOAD_SENTINEL not in caplog.text
    assert CREDENTIAL_SENTINEL not in caplog.text


def test_changed_fingerprint_conflict(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    harness = ConformanceHarness(openapi_path=OPENAPI_PATH, base_url="http://127.0.0.1:9")
    session = session_factory()
    try:
        _seed_queue(session, name=queue_name)
    finally:
        session.close()

    path = f"/v1/queues/{queue_name}/tasks"
    headers = _producer_headers(idempotency_key="idem-conflict-1")
    first_body = json.dumps(_enqueue_body(payload={"a": 1}), separators=(",", ":")).encode()
    status, resp_headers, resp_body = _asgi_http_call(
        app, method="POST", path=path, headers=headers, body=first_body
    )
    assert status == 201
    first = _validate_enqueue_response(
        harness, status=status, headers=resp_headers, body=resp_body
    )

    conflict_body = json.dumps(_enqueue_body(payload={"a": 2}), separators=(",", ":")).encode()
    status2, resp_headers2, resp_body2 = _asgi_http_call(
        app, method="POST", path=path, headers=headers, body=conflict_body
    )
    assert status2 == 409
    err = _assert_error(resp_body2, code="idempotency_conflict", retryable=False)
    findings = harness._validate_response(  # noqa: SLF001
        "enqueueTask",
        ObservedResponse(
            status=status2,
            headers=dict(resp_headers2),
            body_text=resp_body2.decode("utf-8"),
            body_json=err,
            content_type=resp_headers2.get("content-type", "application/json"),
        ),
    )
    assert findings == [], findings
    assert "retry-after" in resp_headers2
    session = session_factory()
    try:
        tasks, dedup, _ = _intake_counts(session, queue_name)
        assert tasks == 1
        assert dedup == 1
        assert first["task"]["task_id"]
    finally:
        session.close()


@pytest.mark.parametrize(
    ("case", "expected_status", "expected_code", "retryable", "expect_retry_after"),
    [
        ("missing_key", 400, "idempotency_key_required", False, False),
        ("invalid_priority_out_of_range", 400, "validation_failed", False, False),
        ("naive_available_at", 400, "validation_failed", False, False),
        ("over_horizon_available_at", 400, "validation_failed", False, False),
        ("unknown_queue", 404, "queue_not_found", False, False),
        ("draining", 409, "queue_draining", True, True),
        ("oversized_payload", 413, "payload_too_large", False, False),
        ("oversized_body", 413, "payload_too_large", False, False),
        ("depth_exhausted", 429, "resource_exhausted", True, True),
    ],
)
def test_negative_enqueue_cases(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
    case: str,
    expected_status: int,
    expected_code: str,
    retryable: bool,
    expect_retry_after: bool,
) -> None:
    harness = ConformanceHarness(openapi_path=OPENAPI_PATH, base_url="http://127.0.0.1:9")
    session = session_factory()
    try:
        if case != "unknown_queue":
            queue = _seed_queue(session, name=queue_name)
            if case == "draining":
                _set_state(
                    session,
                    queue_name=queue_name,
                    state=QueueState.DRAINING,
                    expected_config_version=1,
                )
            if case == "depth_exhausted":
                # Rebuild app with ceiling=1 after seeding one reserved unit via a prior enqueue
                # through a tight service is done in the call site below.
                _ = queue
    finally:
        session.close()

    path = f"/v1/queues/{queue_name}/tasks"
    headers = _producer_headers(idempotency_key=f"idem-neg-{case}")
    body_obj = _enqueue_body(payload={"case": case})
    raw = json.dumps(body_obj, separators=(",", ":")).encode("utf-8")

    if case == "missing_key":
        headers = _producer_headers(idempotency_key=None)
    elif case == "invalid_priority_out_of_range":
        body_obj = _enqueue_body(payload={"case": case}, priority=PRIORITY_MAX + 1)
        raw = json.dumps(body_obj, separators=(",", ":")).encode("utf-8")
    elif case == "naive_available_at":
        body_obj = _enqueue_body(
            payload={"case": case},
            available_at="2026-09-18T12:00:00",
        )
        raw = json.dumps(body_obj, separators=(",", ":")).encode("utf-8")
    elif case == "over_horizon_available_at":
        future = (datetime.now(tz=UTC) + timedelta(days=2)).isoformat().replace(
            "+00:00", "Z"
        )
        body_obj = _enqueue_body(payload={"case": case}, available_at=future)
        raw = json.dumps(body_obj, separators=(",", ":")).encode("utf-8")
    elif case == "unknown_queue":
        # Authorized scope includes BASE_QUEUE_NAME, but it is never seeded → 404.
        path = f"/v1/queues/{BASE_QUEUE_NAME}/tasks"
    elif case == "oversized_payload":
        # Default admission ceiling is 256 KiB for payload JSON bytes.
        body_obj = _enqueue_body(payload={"blob": "x" * 300_000})
        raw = json.dumps(body_obj, separators=(",", ":")).encode("utf-8")
    elif case == "oversized_body":
        # Whole-request ceiling is 1 MiB; craft a body larger than that.
        raw = b'{"payload":"' + (b"y" * 1_100_000) + b'","priority":0}'
        headers = _producer_headers(idempotency_key=f"idem-neg-{case}")
    elif case == "depth_exhausted":
        tight = EnqueueService(
            session_factory=session_factory,
            depth_ceilings=DepthCeilings(
                queue_active_depth=1,
                instance_active_depth=500,
                retry_after_ms=250,
            ),
        )
        app = create_application_app(
            authenticator=BearerCredentialAuthenticator.from_bindings(_bindings()),
            authorizer=Authorizer(
                queue_scopes={
                    PRODUCER_PRINCIPAL: frozenset({queue_name, BASE_QUEUE_NAME}),
                    PRODUCER_OTHER_PRINCIPAL: frozenset({OTHER_QUEUE}),
                    WORKER_PRINCIPAL: frozenset({queue_name, BASE_QUEUE_NAME}),
                    ADMIN_PRINCIPAL: frozenset({queue_name, BASE_QUEUE_NAME}),
                }
            ),
            bind=ListenerBind(host="127.0.0.1", port=18092),
            session_factory=session_factory,
            enqueue_service=tight,
        )
        fill_raw = json.dumps(
            _enqueue_body(payload={"fill": True}), separators=(",", ":")
        ).encode()
        fill_status, _, _ = _asgi_http_call(
            app,
            method="POST",
            path=path,
            headers=_producer_headers(idempotency_key="idem-fill-depth"),
            body=fill_raw,
        )
        assert fill_status == 201

    before = (0, 0, 0)
    session = session_factory()
    try:
        if case not in {"unknown_queue"}:
            before = _intake_counts(session, queue_name)
    finally:
        session.close()

    status, resp_headers, resp_body = _asgi_http_call(
        app, method="POST", path=path, headers=headers, body=raw
    )
    assert status == expected_status
    err = _assert_error(resp_body, code=expected_code, retryable=retryable)
    findings = harness._validate_response(  # noqa: SLF001
        "enqueueTask",
        ObservedResponse(
            status=status,
            headers=dict(resp_headers),
            body_text=resp_body.decode("utf-8"),
            body_json=err,
            content_type=resp_headers.get("content-type", "application/json"),
        ),
    )
    assert findings == [], findings
    if expect_retry_after:
        assert "retry-after" in resp_headers
        assert int(resp_headers["retry-after"]) >= 0
        assert err.get("retry_after_ms") is not None
        assert int(err["retry_after_ms"]) >= 0
    _assert_no_secrets_or_payload(err, raw_response=resp_body)

    session = session_factory()
    try:
        if case == "depth_exhausted":
            tasks, dedup, depth = _intake_counts(session, queue_name)
            assert tasks == 1
            assert dedup == 1
            assert depth == 1
        elif case == "unknown_queue":
            # Authorized path never seeded — no queue row and no intake rows for it.
            missing = session.execute(
                select(Queue).where(Queue.name == BASE_QUEUE_NAME)
            ).scalar_one_or_none()
            assert missing is None
            assert _intake_counts(session, BASE_QUEUE_NAME) == (0, 0, 0)
        else:
            assert _intake_counts(session, queue_name) == before
    finally:
        session.close()


def test_unauthenticated_and_unauthorized_leave_state_unchanged(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    harness = ConformanceHarness(openapi_path=OPENAPI_PATH, base_url="http://127.0.0.1:9")
    session = session_factory()
    try:
        _seed_queue(session, name=queue_name)
        before = _intake_counts(session, queue_name)
    finally:
        session.close()

    path = f"/v1/queues/{queue_name}/tasks"
    raw = json.dumps(_enqueue_body(payload={"x": 1}), separators=(",", ":")).encode()

    status, headers, body = _asgi_http_call(
        app,
        method="POST",
        path=path,
        headers={
            "Content-Type": "application/json",
            "Idempotency-Key": "idem-unauth",
        },
        body=raw,
    )
    assert status == 401
    err = _assert_error(body, code="unauthenticated", retryable=False)
    findings = harness._validate_response(  # noqa: SLF001
        "enqueueTask",
        ObservedResponse(
            status=status,
            headers=dict(headers),
            body_text=body.decode("utf-8"),
            body_json=err,
            content_type=headers.get("content-type", "application/json"),
        ),
    )
    assert findings == [], findings

    status2, headers2, body2 = _asgi_http_call(
        app,
        method="POST",
        path=path,
        headers=_producer_headers(idempotency_key="idem-other-scope", token=PRODUCER_OTHER_TOKEN),
        body=raw,
    )
    assert status2 == 403
    err2 = _assert_error(body2, code="permission_denied", retryable=False)
    findings2 = harness._validate_response(  # noqa: SLF001
        "enqueueTask",
        ObservedResponse(
            status=status2,
            headers=dict(headers2),
            body_text=body2.decode("utf-8"),
            body_json=err2,
            content_type=headers2.get("content-type", "application/json"),
        ),
    )
    assert findings2 == [], findings2

    status3, headers3, body3 = _asgi_http_call(
        app,
        method="POST",
        path=path,
        headers=_producer_headers(idempotency_key="idem-worker", token=WORKER_TOKEN),
        body=raw,
    )
    assert status3 == 403
    _assert_error(body3, code="permission_denied", retryable=False)

    # Malformed JSON body after auth should not mutate.
    status4, headers4, body4 = _asgi_http_call(
        app,
        method="POST",
        path=path,
        headers=_producer_headers(idempotency_key="idem-malformed"),
        body=b"{not-json",
    )
    assert status4 == 400
    err4 = _assert_error(body4, code="validation_failed", retryable=False)
    findings4 = harness._validate_response(  # noqa: SLF001
        "enqueueTask",
        ObservedResponse(
            status=status4,
            headers=dict(headers4),
            body_text=body4.decode("utf-8"),
            body_json=err4,
            content_type=headers4.get("content-type", "application/json"),
        ),
    )
    assert findings4 == [], findings4

    session = session_factory()
    try:
        assert _intake_counts(session, queue_name) == before
    finally:
        session.close()


def test_in_horizon_future_enqueue_persists_delayed_with_counters(
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    harness = ConformanceHarness(openapi_path=OPENAPI_PATH, base_url="http://127.0.0.1:9")
    session = session_factory()
    try:
        _seed_queue(session, name=queue_name)
    finally:
        session.close()

    future_at = datetime.now(tz=UTC) + timedelta(minutes=30)
    future_iso = future_at.isoformat().replace("+00:00", "Z")
    path = f"/v1/queues/{queue_name}/tasks"
    body = _enqueue_body(payload={"delayed": True}, available_at=future_iso)
    raw = json.dumps(body, separators=(",", ":")).encode("utf-8")
    headers = _producer_headers(idempotency_key="idem-delayed-success")

    app = create_application_app(
        authenticator=BearerCredentialAuthenticator.from_bindings(_bindings()),
        authorizer=Authorizer(
            queue_scopes={
                PRODUCER_PRINCIPAL: frozenset({queue_name, BASE_QUEUE_NAME}),
                PRODUCER_OTHER_PRINCIPAL: frozenset({OTHER_QUEUE}),
                WORKER_PRINCIPAL: frozenset({queue_name, BASE_QUEUE_NAME}),
                ADMIN_PRINCIPAL: frozenset({queue_name, BASE_QUEUE_NAME}),
            }
        ),
        bind=ListenerBind(host="127.0.0.1", port=18093),
        session_factory=session_factory,
        enqueue_service=EnqueueService(session_factory=session_factory),
    )

    status, resp_headers, resp_body = _asgi_http_call(
        app, method="POST", path=path, headers=headers, body=raw
    )
    assert status == 201
    payload = _validate_enqueue_response(
        harness, status=status, headers=resp_headers, body=resp_body
    )
    assert payload["replayed"] is False
    assert payload["task"]["state"] == "delayed"
    assert payload["task"]["available_at"] == future_iso
    _assert_no_secrets_or_payload(payload, raw_response=resp_body)

    session = session_factory()
    try:
        queue = session.execute(select(Queue).where(Queue.name == queue_name)).scalar_one()
        task = session.execute(
            select(TaskActive).where(TaskActive.task_id == payload["task"]["task_id"])
        ).scalar_one()
        assert int(task.state_code) == 1
        assert abs((task.available_at - future_at).total_seconds()) < 1.0
        counter = session.get(QueueCounter, int(queue.id))
        assert counter is not None
        assert int(counter.delayed_count) == 1
        assert int(counter.ready_count) == 0
        assert session.scalar(
            select(func.count()).select_from(TaskPayloadActive).where(
                TaskPayloadActive.task_id == task.id
            )
        ) == 1
    finally:
        session.close()


def test_matching_replay_survives_tighter_service_horizon(
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    session = session_factory()
    try:
        _seed_queue(session, name=queue_name)
    finally:
        session.close()

    future_at = datetime.now(tz=UTC) + timedelta(hours=2)
    future_iso = future_at.isoformat().replace("+00:00", "Z")
    path = f"/v1/queues/{queue_name}/tasks"
    body = _enqueue_body(payload={"replay": "horizon"}, available_at=future_iso)
    raw = json.dumps(body, separators=(",", ":")).encode("utf-8")
    headers = _producer_headers(idempotency_key="idem-replay-horizon")

    wide_service = EnqueueService(
        session_factory=session_factory,
        scheduling_policy=SchedulingPolicy(horizon_seconds=86_400),
    )
    app = create_application_app(
        authenticator=BearerCredentialAuthenticator.from_bindings(_bindings()),
        authorizer=Authorizer(
            queue_scopes={
                PRODUCER_PRINCIPAL: frozenset({queue_name, BASE_QUEUE_NAME}),
                PRODUCER_OTHER_PRINCIPAL: frozenset({OTHER_QUEUE}),
                WORKER_PRINCIPAL: frozenset({queue_name, BASE_QUEUE_NAME}),
                ADMIN_PRINCIPAL: frozenset({queue_name, BASE_QUEUE_NAME}),
            }
        ),
        bind=ListenerBind(host="127.0.0.1", port=18094),
        session_factory=session_factory,
        enqueue_service=wide_service,
    )
    status, _, resp_body = _asgi_http_call(
        app, method="POST", path=path, headers=headers, body=raw
    )
    assert status == 201
    first = json.loads(resp_body.decode("utf-8"))
    task_id = first["task"]["task_id"]

    tight_service = EnqueueService(
        session_factory=session_factory,
        scheduling_policy=SchedulingPolicy(horizon_seconds=3_600),
    )
    tight_app = create_application_app(
        authenticator=BearerCredentialAuthenticator.from_bindings(_bindings()),
        authorizer=Authorizer(
            queue_scopes={
                PRODUCER_PRINCIPAL: frozenset({queue_name, BASE_QUEUE_NAME}),
                PRODUCER_OTHER_PRINCIPAL: frozenset({OTHER_QUEUE}),
                WORKER_PRINCIPAL: frozenset({queue_name, BASE_QUEUE_NAME}),
                ADMIN_PRINCIPAL: frozenset({queue_name, BASE_QUEUE_NAME}),
            }
        ),
        bind=ListenerBind(host="127.0.0.1", port=18095),
        session_factory=session_factory,
        enqueue_service=tight_service,
    )
    status2, _, resp_body2 = _asgi_http_call(
        tight_app, method="POST", path=path, headers=headers, body=raw
    )
    assert status2 == 200
    replay = json.loads(resp_body2.decode("utf-8"))
    assert replay["replayed"] is True
    assert replay["task"]["task_id"] == task_id

    changed_iso = (future_at + timedelta(seconds=1)).isoformat().replace("+00:00", "Z")
    changed_raw = json.dumps(
        _enqueue_body(payload={"replay": "horizon"}, available_at=changed_iso),
        separators=(",", ":"),
    ).encode("utf-8")
    status3, _, resp_body3 = _asgi_http_call(
        tight_app, method="POST", path=path, headers=headers, body=changed_raw
    )
    assert status3 == 409
    _assert_error(resp_body3, code="idempotency_conflict", retryable=False)


def test_success_response_omits_payload_and_credentials(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    session = session_factory()
    try:
        _seed_queue(session, name=queue_name)
    finally:
        session.close()

    path = f"/v1/queues/{queue_name}/tasks"
    body = _enqueue_body(payload={"secret": PAYLOAD_SENTINEL})
    raw = json.dumps(body, separators=(",", ":")).encode("utf-8")
    status, headers, resp = _asgi_http_call(
        app,
        method="POST",
        path=path,
        headers=_producer_headers(idempotency_key="idem-redact"),
        body=raw,
    )
    assert status == 201
    payload = json.loads(resp.decode("utf-8"))
    _assert_no_secrets_or_payload(payload, raw_response=resp)
    assert "payload" not in payload.get("task", {}) or payload["task"].get("payload") is None
