"""Black-box worker HTTP claimTasks conformance (Phase 03.5-02).

Covers WORK-03 / OPS-04 / API-01 / API-08 at the public `/v1` edge: OpenAPI-shaped
batch claim, empty/paused success, lease/admission bounds, auth scope, and
claim_id vs claim_token separation with redaction.
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
from workhold.application.claim_service import ClaimService
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
from workhold.intake.service import EnqueueService
from workhold.security.authorization import Authorizer
from workhold.security.credentials import (
    BearerCredentialAuthenticator,
    CredentialBinding,
)
from workhold.security.principals import ServiceRole
from workhold.settings import Secret
from workhold.storage.models import ClaimRegistry, Queue, TaskActive, TaskAttempt
from tests.conformance.harness import ConformanceHarness, ObservedResponse

pytest_plugins = ["tests.integration.conftest"]

REPO_ROOT = Path(__file__).resolve().parents[2]
OPENAPI_PATH = REPO_ROOT / "openapi" / "queue.openapi.json"

PRODUCER_TOKEN = "tok-producer-claim-http"
WORKER_TOKEN = "tok-worker-claim-http"
WORKER_OTHER_TOKEN = "tok-worker-claim-other"
ADMIN_TOKEN = "tok-admin-claim-http"

PRODUCER_PRINCIPAL = "producer-claim-http"
WORKER_PRINCIPAL = "worker-claim-http"
WORKER_OTHER_PRINCIPAL = "worker-claim-other"
ADMIN_PRINCIPAL = "admin-claim-http"

BASE_QUEUE_NAME = "orders.claim"
OTHER_QUEUE = "billing.claim"
PAYLOAD_SENTINEL = "SECRET_PAYLOAD_SHOULD_NEVER_LEAK"
CLAIM_PATH = "/v1/claims"
REPLICA_ID = "pool-a/replica-7"


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
            principal_id=WORKER_PRINCIPAL,
            role=ServiceRole.WORKER,
            generation_id="g1",
            secret=Secret(WORKER_TOKEN),
        ),
        CredentialBinding(
            principal_id=WORKER_OTHER_PRINCIPAL,
            role=ServiceRole.WORKER,
            generation_id="g1",
            secret=Secret(WORKER_OTHER_TOKEN),
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
            PRODUCER_PRINCIPAL: frozenset({queue_name, BASE_QUEUE_NAME, OTHER_QUEUE}),
            WORKER_PRINCIPAL: frozenset({queue_name, BASE_QUEUE_NAME}),
            WORKER_OTHER_PRINCIPAL: frozenset({OTHER_QUEUE}),
            ADMIN_PRINCIPAL: frozenset({queue_name, BASE_QUEUE_NAME}),
        }
    )


@pytest.fixture
def session_factory(migrated_schema) -> Iterator[sessionmaker[Session]]:
    _conn, schema = migrated_schema
    database_url = os.environ.get("TEST_DATABASE_URL", "").strip()
    if not database_url:
        pytest.fail("TEST_DATABASE_URL is required for claim HTTP conformance")
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
    enqueue_service = EnqueueService(
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
        bind=ListenerBind(host="127.0.0.1", port=18092),
        session_factory=session_factory,
        enqueue_service=enqueue_service,
        claim_service=ClaimService(session_factory=session_factory),
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


def _enqueue_ready(
    app: Any,
    *,
    queue_name: str,
    payload: Any,
    idempotency_key: str,
) -> str:
    path = f"/v1/queues/{queue_name}/tasks"
    body = json.dumps(
        {"payload": payload, "priority": 0},
        separators=(",", ":"),
    ).encode("utf-8")
    headers = {
        "Authorization": f"Bearer {PRODUCER_TOKEN}",
        "Content-Type": "application/json",
        "Idempotency-Key": idempotency_key,
    }
    status, _hdrs, resp = _asgi_http_call(app, method="POST", path=path, headers=headers, body=body)
    assert status == 201, resp.decode("utf-8", errors="replace")
    return json.loads(resp.decode("utf-8"))["task"]["task_id"]


def _claim_body(
    *,
    queues: list[str],
    max_tasks: int = 1,
    lease_seconds: int = 60,
    wait_seconds: int = 0,
    worker_id: str = REPLICA_ID,
) -> dict[str, Any]:
    return {
        "queues": queues,
        "max_tasks": max_tasks,
        "lease_seconds": lease_seconds,
        "wait_seconds": wait_seconds,
        "worker_id": worker_id,
    }


def _worker_headers(*, token: str = WORKER_TOKEN) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


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


def _validate_claim_response(
    harness: ConformanceHarness,
    *,
    status: int,
    headers: Mapping[str, str],
    body: bytes,
) -> dict[str, Any]:
    payload = json.loads(body.decode("utf-8"))
    findings = harness._validate_response(  # noqa: SLF001 - schema gate
        "claimTasks",
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


def _claim_counts(session: Session, queue_name: str) -> tuple[int, int, int]:
    queue = session.execute(select(Queue).where(Queue.name == queue_name)).scalar_one_or_none()
    if queue is None:
        return (0, 0, 0)
    leased = int(
        session.scalar(
            select(func.count())
            .select_from(TaskActive)
            .where(TaskActive.queue_id == queue.id, TaskActive.state_code == 3)
        )
        or 0
    )
    task_ids = select(TaskActive.task_id).where(TaskActive.queue_id == queue.id)
    attempts = int(
        session.scalar(
            select(func.count()).select_from(TaskAttempt).where(TaskAttempt.task_id.in_(task_ids))
        )
        or 0
    )
    registries = int(
        session.scalar(
            select(func.count())
            .select_from(ClaimRegistry)
            .where(ClaimRegistry.task_id.in_(task_ids))
        )
        or 0
    )
    return leased, attempts, registries


def test_claim_request_fixture_is_closed() -> None:
    harness = ConformanceHarness(
        openapi_path=OPENAPI_PATH,
        base_url="http://127.0.0.1:9",
    )
    harness.validate_request_fixture(
        "claimTasks",
        _claim_body(queues=["orders.claim"]),
    )
    with pytest.raises(Exception):
        harness.validate_request_fixture(
            "claimTasks",
            _claim_body(queues=["orders.claim"], max_tasks=2),
        )


def test_authorized_claim_returns_array_shaped_tasks_with_separated_credentials(
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

    task_id = _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"secret": PAYLOAD_SENTINEL, "n": 1},
        idempotency_key="idem-claim-ready-1",
    )

    body = json.dumps(
        _claim_body(queues=[queue_name], lease_seconds=90),
        separators=(",", ":"),
    ).encode("utf-8")

    with caplog.at_level(logging.INFO):
        status, resp_headers, resp_body = _asgi_http_call(
            app,
            method="POST",
            path=CLAIM_PATH,
            headers=_worker_headers(),
            body=body,
        )

    assert status == 200
    assert "x-request-id" in resp_headers
    payload = _validate_claim_response(
        harness, status=status, headers=resp_headers, body=resp_body
    )
    assert isinstance(payload["tasks"], list)
    assert len(payload["tasks"]) == 1
    claimed = payload["tasks"][0]
    assert claimed["task"]["task_id"] == task_id
    assert claimed["task"]["queue_name"] == queue_name
    assert claimed["task"]["state"] == "leased"
    assert claimed["task"]["payload"] == {"secret": PAYLOAD_SENTINEL, "n": 1}
    assert claimed["task"]["retry_policy_version"] >= 1
    claim = claimed["claim"]
    assert claim["claim_id"] != claim["claim_token"]
    uuid.UUID(claim["claim_id"])
    uuid.UUID(claim["claim_token"])
    assert claim["generation"] == 1
    assert claim["worker_id"].startswith(f"{WORKER_PRINCIPAL}/")
    assert claim["worker_id"].endswith(REPLICA_ID)
    assert claim["cancel_requested"] is False
    assert isinstance(payload["server_time"], str)
    assert payload["recommended_heartbeat_seconds"] >= 1
    assert payload["queue_states"][queue_name] == "active"

    # Public claim_id may appear; secret token and opaque payload must not leak
    # into logs/traces/metrics-style diagnostics captured by the test logger.
    assert claim["claim_token"] not in caplog.text
    assert PAYLOAD_SENTINEL not in caplog.text
    assert WORKER_TOKEN not in caplog.text
    assert claim["claim_token"] not in CLAIM_PATH
    assert "?" not in CLAIM_PATH

    session = session_factory()
    try:
        leased, attempts, registries = _claim_counts(session, queue_name)
        assert leased == 1
        assert attempts == 1
        assert registries == 1
        task = session.execute(
            select(TaskActive).where(TaskActive.task_id == uuid.UUID(task_id))
        ).scalar_one()
        assert task.worker_id == claim["worker_id"]
        assert str(task.current_claim_id) == claim["claim_id"]
    finally:
        session.close()


def test_no_work_and_paused_return_successful_empty_arrays(
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

    empty_body = json.dumps(
        _claim_body(queues=[queue_name]),
        separators=(",", ":"),
    ).encode("utf-8")
    status, resp_headers, resp_body = _asgi_http_call(
        app,
        method="POST",
        path=CLAIM_PATH,
        headers=_worker_headers(),
        body=empty_body,
    )
    assert status == 200
    empty = _validate_claim_response(
        harness, status=status, headers=resp_headers, body=resp_body
    )
    assert empty["tasks"] == []
    assert empty["queue_states"][queue_name] == "active"
    assert isinstance(empty["server_time"], str)
    assert empty["recommended_heartbeat_seconds"] >= 1

    session = session_factory()
    try:
        leased, attempts, registries = _claim_counts(session, queue_name)
        assert (leased, attempts, registries) == (0, 0, 0)
        queue = session.execute(select(Queue).where(Queue.name == queue_name)).scalar_one()
        _set_state(
            session,
            queue_name=queue_name,
            state=QueueState.PAUSED,
            expected_config_version=int(queue.config_version),
        )
    finally:
        session.close()

    # Enqueue under pause is allowed; claim must stay empty without mutation.
    _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"n": 2},
        idempotency_key="idem-claim-paused-1",
    )
    status2, resp_headers2, resp_body2 = _asgi_http_call(
        app,
        method="POST",
        path=CLAIM_PATH,
        headers=_worker_headers(),
        body=empty_body,
    )
    assert status2 == 200
    paused = _validate_claim_response(
        harness, status=status2, headers=resp_headers2, body=resp_body2
    )
    assert paused["tasks"] == []
    assert paused["queue_states"][queue_name] == "paused"

    session = session_factory()
    try:
        leased, attempts, registries = _claim_counts(session, queue_name)
        assert leased == 0
        assert attempts == 0
        assert registries == 0
        ready = int(
            session.scalar(
                select(func.count())
                .select_from(TaskActive)
                .where(TaskActive.queue_id == queue.id, TaskActive.state_code == 2)
            )
            or 0
        )
        assert ready == 1
    finally:
        session.close()


@pytest.mark.parametrize(
    ("mutate", "expected_code"),
    [
        (lambda b: {**b, "max_tasks": 2}, "validation_failed"),
        (lambda b: {**b, "max_tasks": True}, "validation_failed"),
        (lambda b: {**b, "max_tasks": 1.0}, "validation_failed"),
        (lambda b: {**b, "wait_seconds": 21}, "validation_failed"),
        (lambda b: {**b, "wait_seconds": -1}, "validation_failed"),
        (lambda b: {**b, "wait_seconds": False}, "validation_failed"),
        (lambda b: {**b, "wait_seconds": 0.0}, "validation_failed"),
        (lambda b: {**b, "queues": []}, "validation_failed"),
        (lambda b: {**b, "queues": ["a", "a"]}, "validation_failed"),
        (lambda b: {**b, "queues": ["BadName"]}, "validation_failed"),
        (lambda b: {**b, "lease_seconds": 0}, "validation_failed"),
        (lambda b: {**b, "lease_seconds": 3601}, "validation_failed"),
        (lambda b: {**b, "lease_seconds": 1.5}, "validation_failed"),
        (lambda b: {**b, "worker_id": "x" * 129}, "validation_failed"),
        (lambda b: {**b, "queues": ["x" * 129]}, "validation_failed"),
    ],
)
def test_invalid_claim_bounds_rejected_before_write(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
    mutate: Any,
    expected_code: str,
) -> None:
    harness = ConformanceHarness(openapi_path=OPENAPI_PATH, base_url="http://127.0.0.1:9")
    session = session_factory()
    try:
        _seed_queue(session, name=queue_name)
        before = _claim_counts(session, queue_name)
    finally:
        session.close()

    base = _claim_body(queues=[queue_name])
    try:
        body_obj = mutate(base)
    except Exception:  # pragma: no cover - mutate is pure
        body_obj = base
    raw = json.dumps(body_obj, separators=(",", ":")).encode("utf-8")
    status, resp_headers, resp_body = _asgi_http_call(
        app,
        method="POST",
        path=CLAIM_PATH,
        headers=_worker_headers(),
        body=raw,
    )
    assert status == 400
    err = _assert_error(resp_body, code=expected_code, retryable=False)
    findings = harness._validate_response(  # noqa: SLF001
        "claimTasks",
        ObservedResponse(
            status=status,
            headers=dict(resp_headers),
            body_text=resp_body.decode("utf-8"),
            body_json=err,
            content_type=resp_headers.get("content-type", "application/json"),
        ),
    )
    assert findings == [], findings

    session = session_factory()
    try:
        assert _claim_counts(session, queue_name) == before
    finally:
        session.close()


def test_oversized_claim_body_rejected_before_write(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    session = session_factory()
    try:
        _seed_queue(session, name=queue_name)
        before = _claim_counts(session, queue_name)
    finally:
        session.close()

    # Exceed 1 MiB whole-body ceiling without relying on JSON field expansion.
    raw = b'{"queues":["' + queue_name.encode("utf-8") + b'"],"max_tasks":1,'
    raw += b'"lease_seconds":60,"wait_seconds":0,"worker_id":"w","pad":"'
    raw += b"x" * (1_048_576)
    raw += b'"}'
    assert len(raw) > 1_048_576

    status, _hdrs, resp_body = _asgi_http_call(
        app,
        method="POST",
        path=CLAIM_PATH,
        headers=_worker_headers(),
        body=raw,
    )
    assert status == 413
    _assert_error(resp_body, code="payload_too_large", retryable=False)

    session = session_factory()
    try:
        assert _claim_counts(session, queue_name) == before
    finally:
        session.close()


def test_unauthorized_queue_scope_rejected_and_authorized_identity_bound(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    harness = ConformanceHarness(openapi_path=OPENAPI_PATH, base_url="http://127.0.0.1:9")
    session = session_factory()
    try:
        _seed_queue(session, name=queue_name)
        _seed_queue(session, name=OTHER_QUEUE)
    finally:
        session.close()

    _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"n": 3},
        idempotency_key="idem-claim-auth-1",
    )

    # Worker scoped only to OTHER_QUEUE cannot claim queue_name.
    denied_body = json.dumps(
        _claim_body(queues=[queue_name]),
        separators=(",", ":"),
    ).encode("utf-8")
    status, resp_headers, resp_body = _asgi_http_call(
        app,
        method="POST",
        path=CLAIM_PATH,
        headers=_worker_headers(token=WORKER_OTHER_TOKEN),
        body=denied_body,
    )
    assert status == 403
    err = _assert_error(resp_body, code="permission_denied", retryable=False)
    findings = harness._validate_response(  # noqa: SLF001
        "claimTasks",
        ObservedResponse(
            status=status,
            headers=dict(resp_headers),
            body_text=resp_body.decode("utf-8"),
            body_json=err,
            content_type=resp_headers.get("content-type", "application/json"),
        ),
    )
    assert findings == [], findings

    session = session_factory()
    try:
        leased, attempts, registries = _claim_counts(session, queue_name)
        assert (leased, attempts, registries) == (0, 0, 0)
    finally:
        session.close()

    # Authorized worker binds claim to principal-derived diagnostic identity.
    ok_body = json.dumps(
        _claim_body(queues=[queue_name], worker_id="replica-auth"),
        separators=(",", ":"),
    ).encode("utf-8")
    status2, resp_headers2, resp_body2 = _asgi_http_call(
        app,
        method="POST",
        path=CLAIM_PATH,
        headers=_worker_headers(),
        body=ok_body,
    )
    assert status2 == 200
    ok = _validate_claim_response(
        harness, status=status2, headers=resp_headers2, body=resp_body2
    )
    assert len(ok["tasks"]) == 1
    assert ok["tasks"][0]["claim"]["worker_id"] == f"{WORKER_PRINCIPAL}/replica-auth"


def test_unauthenticated_claim_rejected(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    session = session_factory()
    try:
        _seed_queue(session, name=queue_name)
    finally:
        session.close()

    body = json.dumps(_claim_body(queues=[queue_name]), separators=(",", ":")).encode()
    status, _hdrs, resp_body = _asgi_http_call(
        app,
        method="POST",
        path=CLAIM_PATH,
        headers={"Content-Type": "application/json"},
        body=body,
    )
    assert status == 401
    _assert_error(resp_body, code="unauthenticated", retryable=False)
