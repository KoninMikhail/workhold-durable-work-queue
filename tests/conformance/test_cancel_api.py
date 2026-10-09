"""Black-box producer HTTP cancelTask conformance (Phase 03.6-04).

Covers WORK-08: immediate cancel for delayed/ready, cooperative cancel request
for leased work, queue-state gates, idempotency, and no spawn/event side effects.
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from collections.abc import Iterator, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
from sqlalchemy import create_engine, event, func, select
from sqlalchemy.orm import Session, sessionmaker

from queue_service.api.application import create_application_app
from queue_service.api.security import ListenerBind
from queue_service.application.claim_service import ClaimService
from queue_service.application.lease_service import LeaseService
from queue_service.domain.queue_control import (
    AdminRequestMetadata,
    BackoffStrategy,
    ConfigVersion,
    CreateQueueMutation,
    QueueState,
    RetryPolicyDraft,
    SetQueueStateMutation,
)
from queue_service.infrastructure.postgres.queue_control_repository import (
    QueueControlRepository,
)
from queue_service.intake.depth import DepthCeilings
from queue_service.intake.service import EnqueueService
from queue_service.security.authorization import Authorizer
from queue_service.security.credentials import (
    BearerCredentialAuthenticator,
    CredentialBinding,
)
from queue_service.security.principals import ServiceRole
from queue_service.settings import Secret
from queue_service.storage.models import (
    ClaimRegistry,
    DeliveryEventActive,
    Queue,
    QueueCounter,
    TaskActive,
    TaskAttempt,
    TaskTerminal,
)
from tests.conformance.harness import ConformanceHarness, ObservedResponse

pytest_plugins = ["tests.integration.conftest"]

REPO_ROOT = Path(__file__).resolve().parents[2]
OPENAPI_PATH = REPO_ROOT / "openapi" / "queue.openapi.json"

PRODUCER_TOKEN = "tok-producer-cancel-http"
PRODUCER_OTHER_TOKEN = "tok-producer-cancel-other"
WORKER_TOKEN = "tok-worker-cancel-http"
ADMIN_TOKEN = "tok-admin-cancel-http"

PRODUCER_PRINCIPAL = "producer-cancel-http"
PRODUCER_OTHER_PRINCIPAL = "producer-cancel-other"
WORKER_PRINCIPAL = "worker-cancel-http"
ADMIN_PRINCIPAL = "admin-cancel-http"

BASE_QUEUE_NAME = "orders.cancel"
OTHER_QUEUE = "billing.cancel"
PAYLOAD_SENTINEL = "CANCEL_SECRET_PAYLOAD_SHOULD_NEVER_LEAK"
CLAIM_PATH = "/v1/claims"
REPLICA_ID = "pool-cancel/replica-1"
CLAIM_TOKEN_HEADER = "X-Queue-Claim-Token"

_STATE_DELAYED = 1
_STATE_READY = 2
_STATE_LEASED = 3
_TERMINAL_DEAD = 11
_TERMINAL_CANCELLED = 12
_OUTCOME_ACTIVE = 1


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
            PRODUCER_PRINCIPAL: frozenset({queue_name, BASE_QUEUE_NAME, OTHER_QUEUE}),
            PRODUCER_OTHER_PRINCIPAL: frozenset({queue_name, BASE_QUEUE_NAME}),
            WORKER_PRINCIPAL: frozenset({queue_name, BASE_QUEUE_NAME}),
            ADMIN_PRINCIPAL: frozenset({queue_name, BASE_QUEUE_NAME}),
        }
    )


@pytest.fixture
def session_factory(migrated_schema) -> Iterator[sessionmaker[Session]]:
    _conn, schema = migrated_schema
    database_url = os.environ.get("TEST_DATABASE_URL", "").strip()
    if not database_url:
        pytest.fail("TEST_DATABASE_URL is required for cancel HTTP conformance")
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
        bind=ListenerBind(host="127.0.0.1", port=18097),
        session_factory=session_factory,
        enqueue_service=enqueue_service,
        claim_service=ClaimService(session_factory=session_factory),
        lease_service=LeaseService(session_factory=session_factory),
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
        "client": ("127.0.0.1", 50000),
        "server": ("127.0.0.1", 18097),
    }
    status_box: dict[str, int] = {}
    header_box: dict[str, str] = {}
    body_chunks: list[bytes] = []

    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": body, "more_body": False}

    async def send(message: dict[str, Any]) -> None:
        if message["type"] == "http.response.start":
            status_box["status"] = int(message["status"])
            header_box.clear()
            for raw_k, raw_v in message.get("headers", []):
                header_box[raw_k.decode("latin-1").lower()] = raw_v.decode("latin-1")
        elif message["type"] == "http.response.body":
            body_chunks.append(message.get("body", b"") or b"")

    asyncio.run(app(scope, receive, send))
    return status_box["status"], header_box, b"".join(body_chunks)


def _admin_meta() -> AdminRequestMetadata:
    return AdminRequestMetadata(
        actor_id=ADMIN_PRINCIPAL,
        request_id=str(uuid.uuid4()),
        idempotency_key=f"admin-idem-{uuid.uuid4().hex}",
    )


def _seed_queue(
    session: Session,
    *,
    name: str,
) -> Queue:
    QueueControlRepository().create_named_queue(
        session,
        CreateQueueMutation(
            name=name,
            initial_policy=RetryPolicyDraft(
                enabled=True,
                max_attempts=3,
                backoff_strategy=BackoffStrategy.FIXED,
                retry_delay_seconds=0,
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
    token: str = PRODUCER_TOKEN,
) -> str:
    path = f"/v1/queues/{queue_name}/tasks"
    body = json.dumps(
        {"payload": payload, "priority": 0},
        separators=(",", ":"),
    ).encode("utf-8")
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Idempotency-Key": idempotency_key,
    }
    status, _hdrs, resp = _asgi_http_call(app, method="POST", path=path, headers=headers, body=body)
    assert status == 201, resp.decode("utf-8", errors="replace")
    return json.loads(resp.decode("utf-8"))["task"]["task_id"]


def _enqueue_delayed(
    app: Any,
    *,
    queue_name: str,
    payload: Any,
    idempotency_key: str,
    available_at: datetime,
    token: str = PRODUCER_TOKEN,
) -> str:
    path = f"/v1/queues/{queue_name}/tasks"
    body = json.dumps(
        {
            "payload": payload,
            "priority": 0,
            "available_at": available_at.isoformat().replace("+00:00", "Z"),
        },
        separators=(",", ":"),
    ).encode("utf-8")
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Idempotency-Key": idempotency_key,
    }
    status, _hdrs, resp = _asgi_http_call(app, method="POST", path=path, headers=headers, body=body)
    assert status == 201, resp.decode("utf-8", errors="replace")
    payload_json = json.loads(resp.decode("utf-8"))
    assert payload_json["task"]["state"] == "delayed"
    return payload_json["task"]["task_id"]


def _producer_headers(*, token: str = PRODUCER_TOKEN) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def _worker_headers(
    *,
    token: str = WORKER_TOKEN,
    claim_token: str | None = None,
) -> dict[str, str]:
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    if claim_token is not None:
        headers[CLAIM_TOKEN_HEADER] = claim_token
    return headers


def _cancel_path(task_id: str) -> str:
    return f"/v1/tasks/{task_id}:cancel"


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


def _validate_cancel_response(
    harness: ConformanceHarness,
    *,
    status: int,
    headers: Mapping[str, str],
    body: bytes,
) -> dict[str, Any]:
    payload = json.loads(body.decode("utf-8"))
    findings = harness._validate_response(  # noqa: SLF001 - schema gate
        "cancelTask",
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


def _claim_one(
    app: Any,
    *,
    queue_name: str,
    lease_seconds: int = 120,
) -> dict[str, Any]:
    body = json.dumps(
        {
            "queues": [queue_name],
            "max_tasks": 1,
            "lease_seconds": lease_seconds,
            "wait_seconds": 0,
            "worker_id": REPLICA_ID,
        },
        separators=(",", ":"),
    ).encode("utf-8")
    status, _hdrs, resp = _asgi_http_call(
        app,
        method="POST",
        path=CLAIM_PATH,
        headers=_worker_headers(),
        body=body,
    )
    assert status == 200, resp.decode("utf-8", errors="replace")
    payload = json.loads(resp.decode("utf-8"))
    assert len(payload["tasks"]) == 1
    return payload["tasks"][0]


def _heartbeat(
    app: Any,
    *,
    claim_id: str,
    claim_token: str,
    generation: int,
    lease_seconds: int = 120,
) -> dict[str, Any]:
    body = json.dumps(
        {"generation": generation, "lease_seconds": lease_seconds},
        separators=(",", ":"),
    ).encode("utf-8")
    status, _hdrs, resp = _asgi_http_call(
        app,
        method="POST",
        path=f"/v1/claims/{claim_id}:heartbeat",
        headers=_worker_headers(claim_token=claim_token),
        body=body,
    )
    assert status == 200, resp.decode("utf-8", errors="replace")
    return json.loads(resp.decode("utf-8"))


def _assert_no_side_effects(session: Session, *, task_id: str) -> None:
    tid = UUID(task_id)
    spawn_count = session.scalar(
        select(func.count()).select_from(TaskActive).where(TaskActive.source_task_id == tid)
    )
    assert int(spawn_count or 0) == 0
    event_count = session.scalar(
        select(func.count())
        .select_from(DeliveryEventActive)
        .where(DeliveryEventActive.source_task_id == tid)
    )
    assert int(event_count or 0) == 0


def test_openapi_cancel_fixtures_are_closed() -> None:
    harness = ConformanceHarness(openapi_path=OPENAPI_PATH, base_url="http://127.0.0.1:9")
    harness.validate_request_fixture("cancelTask", {})
    harness.validate_request_fixture("cancelTask", {"reason": "user revoked"})
    with pytest.raises(Exception):
        harness.validate_request_fixture("cancelTask", {"reason": "x", "extra": True})


def test_ready_cancel_becomes_terminal_cancelled(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    harness = ConformanceHarness(openapi_path=OPENAPI_PATH, base_url="http://127.0.0.1:9")
    with session_factory() as session:
        _seed_queue(session, name=queue_name)

    task_id = _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"secret": PAYLOAD_SENTINEL},
        idempotency_key=f"idem-{uuid.uuid4().hex}",
    )

    status, headers, resp = _asgi_http_call(
        app,
        method="POST",
        path=_cancel_path(task_id),
        headers=_producer_headers(),
        body=b"{}",
    )
    assert status == 200, resp.decode("utf-8", errors="replace")
    payload = _validate_cancel_response(
        harness, status=status, headers=headers, body=resp
    )
    task = payload["task"]
    assert task["task_id"] == task_id
    assert task["state"] == "cancelled"
    assert task["terminal_at"] is not None
    assert task["spawned_task_ids"] == []
    assert task["delivery_event_ids"] == []
    assert PAYLOAD_SENTINEL not in resp.decode("utf-8")

    with session_factory() as session:
        active = session.execute(
            select(TaskActive).where(TaskActive.task_id == UUID(task_id))
        ).scalar_one_or_none()
        assert active is None
        terminal = session.execute(
            select(TaskTerminal).where(TaskTerminal.task_id == UUID(task_id))
        ).scalar_one()
        assert int(terminal.state_code) == _TERMINAL_CANCELLED
        # Payload rows are FK-cascaded with tasks_active; this cancel must leave
        # no active row for the cancelled task. Do not assert global schema
        # emptiness — session-scoped migrated_schema is shared across
        # conformance modules that retain other active tasks.
        _assert_no_side_effects(session, task_id=task_id)
        terminals = session.scalar(
            select(func.count())
            .select_from(TaskTerminal)
            .where(TaskTerminal.task_id == UUID(task_id))
        )
        assert int(terminals or 0) == 1


def test_delayed_cancel_becomes_terminal_cancelled(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    harness = ConformanceHarness(openapi_path=OPENAPI_PATH, base_url="http://127.0.0.1:9")
    with session_factory() as session:
        _seed_queue(session, name=queue_name)

    future_at = datetime.now(tz=UTC) + timedelta(minutes=30)
    task_id = _enqueue_delayed(
        app,
        queue_name=queue_name,
        payload={"n": 1},
        idempotency_key=f"idem-{uuid.uuid4().hex}",
        available_at=future_at,
    )
    with session_factory() as session:
        delayed = session.execute(
            select(TaskActive).where(TaskActive.task_id == UUID(task_id))
        ).scalar_one()
        assert int(delayed.state_code) == _STATE_DELAYED
        queue_id = int(delayed.queue_id)
        counter_before = session.get(QueueCounter, queue_id)
        assert counter_before is not None
        assert int(counter_before.delayed_count) == 1
        assert int(counter_before.ready_count) == 0

    status, headers, resp = _asgi_http_call(
        app,
        method="POST",
        path=_cancel_path(task_id),
        headers=_producer_headers(),
        body=b'{"reason":"stale order"}',
    )
    assert status == 200, resp.decode("utf-8", errors="replace")
    payload = _validate_cancel_response(
        harness, status=status, headers=headers, body=resp
    )
    assert payload["task"]["state"] == "cancelled"

    with session_factory() as session:
        assert (
            session.execute(
                select(TaskActive).where(TaskActive.task_id == UUID(task_id))
            ).scalar_one_or_none()
            is None
        )
        terminal = session.execute(
            select(TaskTerminal).where(TaskTerminal.task_id == UUID(task_id))
        ).scalar_one()
        assert int(terminal.state_code) == _TERMINAL_CANCELLED
        counter_after = session.get(QueueCounter, queue_id)
        assert counter_after is not None
        assert int(counter_after.delayed_count) == 0
        assert int(counter_after.ready_count) == 0
        assert int(counter_after.leased_count) == 0
        attempt_count = session.scalar(
            select(func.count())
            .select_from(TaskAttempt)
            .where(TaskAttempt.task_id == UUID(task_id))
        )
        assert int(attempt_count or 0) == 0
        _assert_no_side_effects(session, task_id=task_id)


def test_leased_cancel_records_request_without_revoking_lease(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    harness = ConformanceHarness(openapi_path=OPENAPI_PATH, base_url="http://127.0.0.1:9")
    with session_factory() as session:
        _seed_queue(session, name=queue_name)

    task_id = _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"n": 2},
        idempotency_key=f"idem-{uuid.uuid4().hex}",
    )
    claim = _claim_one(app, queue_name=queue_name)
    assert claim["task"]["task_id"] == task_id
    claim_id = claim["claim"]["claim_id"]
    claim_token = claim["claim"]["claim_token"]
    generation = int(claim["claim"]["generation"])
    lease_before = claim["claim"]["lease_expires_at"]

    status, headers, resp = _asgi_http_call(
        app,
        method="POST",
        path=_cancel_path(task_id),
        headers=_producer_headers(),
        body=b"{}",
    )
    assert status == 200, resp.decode("utf-8", errors="replace")
    payload = _validate_cancel_response(
        harness, status=status, headers=headers, body=resp
    )
    task = payload["task"]
    assert task["state"] == "leased"
    assert task["current_claim"]["cancel_requested"] is True
    assert task["current_claim"]["claim_id"] == claim_id
    assert "claim_token" not in task.get("current_claim", {})

    with session_factory() as session:
        active = session.execute(
            select(TaskActive).where(TaskActive.task_id == UUID(task_id))
        ).scalar_one()
        assert int(active.state_code) == _STATE_LEASED
        assert active.cancel_requested_at is not None
        assert str(active.current_claim_id) == claim_id
        registry = session.execute(
            select(ClaimRegistry).where(ClaimRegistry.claim_id == UUID(claim_id))
        ).scalar_one()
        assert registry is not None
        attempt = session.execute(
            select(TaskAttempt).where(
                TaskAttempt.task_id == UUID(task_id),
                TaskAttempt.outcome_code == _OUTCOME_ACTIVE,
            )
        ).scalar_one()
        assert attempt.ended_at is None
        assert (
            session.execute(
                select(TaskTerminal).where(TaskTerminal.task_id == UUID(task_id))
            ).scalar_one_or_none()
            is None
        )
        _assert_no_side_effects(session, task_id=task_id)

    hb = _heartbeat(
        app,
        claim_id=claim_id,
        claim_token=claim_token,
        generation=generation,
    )
    assert hb["claim"]["cancel_requested"] is True
    assert hb["claim"]["claim_id"] == claim_id
    # Lease remains current; heartbeat may refresh expiry but claim authority stays.
    assert hb["claim"]["lease_expires_at"] is not None
    assert lease_before is not None


@pytest.mark.parametrize("state", [QueueState.ACTIVE, QueueState.PAUSED, QueueState.DRAINING])
def test_cancel_allowed_under_every_queue_runtime_state(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
    state: QueueState,
) -> None:
    harness = ConformanceHarness(openapi_path=OPENAPI_PATH, base_url="http://127.0.0.1:9")
    with session_factory() as session:
        queue = _seed_queue(session, name=queue_name)
        config_version = int(queue.config_version)

    task_id = _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"state": state.value},
        idempotency_key=f"idem-{uuid.uuid4().hex}",
    )
    if state is not QueueState.ACTIVE:
        with session_factory() as session:
            _set_state(
                session,
                queue_name=queue_name,
                state=state,
                expected_config_version=config_version,
            )

    status, headers, resp = _asgi_http_call(
        app,
        method="POST",
        path=_cancel_path(task_id),
        headers=_producer_headers(),
        body=b"{}",
    )
    assert status == 200, resp.decode("utf-8", errors="replace")
    payload = _validate_cancel_response(
        harness, status=status, headers=headers, body=resp
    )
    assert payload["task"]["state"] == "cancelled"


def test_repeated_cancel_is_idempotent_without_duplicates(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    harness = ConformanceHarness(openapi_path=OPENAPI_PATH, base_url="http://127.0.0.1:9")
    with session_factory() as session:
        _seed_queue(session, name=queue_name)

    task_id = _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"n": 3},
        idempotency_key=f"idem-{uuid.uuid4().hex}",
    )

    first_status, first_headers, first_resp = _asgi_http_call(
        app,
        method="POST",
        path=_cancel_path(task_id),
        headers=_producer_headers(),
        body=b"{}",
    )
    assert first_status == 200
    first = _validate_cancel_response(
        harness, status=first_status, headers=first_headers, body=first_resp
    )

    second_status, second_headers, second_resp = _asgi_http_call(
        app,
        method="POST",
        path=_cancel_path(task_id),
        headers=_producer_headers(),
        body=b'{"reason":"again"}',
    )
    assert second_status == 200
    second = _validate_cancel_response(
        harness, status=second_status, headers=second_headers, body=second_resp
    )
    assert second["task"]["state"] == "cancelled"
    assert second["task"]["task_id"] == first["task"]["task_id"]
    assert second["task"]["terminal_at"] == first["task"]["terminal_at"]

    with session_factory() as session:
        terminals = session.scalar(
            select(func.count())
            .select_from(TaskTerminal)
            .where(TaskTerminal.task_id == UUID(task_id))
        )
        assert int(terminals or 0) == 1
        _assert_no_side_effects(session, task_id=task_id)


def test_repeated_leased_cancel_keeps_single_request_marker(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    with session_factory() as session:
        _seed_queue(session, name=queue_name)

    task_id = _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"n": 4},
        idempotency_key=f"idem-{uuid.uuid4().hex}",
    )
    claim = _claim_one(app, queue_name=queue_name)

    for _ in range(2):
        status, _hdrs, resp = _asgi_http_call(
            app,
            method="POST",
            path=_cancel_path(task_id),
            headers=_producer_headers(),
            body=b"{}",
        )
        assert status == 200, resp.decode("utf-8", errors="replace")

    with session_factory() as session:
        active = session.execute(
            select(TaskActive).where(TaskActive.task_id == UUID(task_id))
        ).scalar_one()
        assert active.cancel_requested_at is not None
        assert str(active.current_claim_id) == claim["claim"]["claim_id"]
        assert int(active.state_code) == _STATE_LEASED
        _assert_no_side_effects(session, task_id=task_id)


def test_other_terminal_returns_task_already_terminal(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    with session_factory() as session:
        _seed_queue(session, name=queue_name)

    task_id = _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"n": 5},
        idempotency_key=f"idem-{uuid.uuid4().hex}",
    )
    claim = _claim_one(app, queue_name=queue_name)
    fail_body = json.dumps(
        {
            "generation": claim["claim"]["generation"],
            "retryable": False,
            "failure_code": "worker.fatal",
        },
        separators=(",", ":"),
    ).encode("utf-8")
    fail_status, _fh, fail_resp = _asgi_http_call(
        app,
        method="POST",
        path=f"/v1/claims/{claim['claim']['claim_id']}:fail",
        headers=_worker_headers(claim_token=claim["claim"]["claim_token"]),
        body=fail_body,
    )
    assert fail_status == 200, fail_resp.decode("utf-8", errors="replace")

    status, _hdrs, resp = _asgi_http_call(
        app,
        method="POST",
        path=_cancel_path(task_id),
        headers=_producer_headers(),
        body=b"{}",
    )
    assert status == 409
    _assert_error(resp, code="task_already_terminal", retryable=False)

    with session_factory() as session:
        terminal = session.execute(
            select(TaskTerminal).where(TaskTerminal.task_id == UUID(task_id))
        ).scalar_one()
        assert int(terminal.state_code) == _TERMINAL_DEAD


def test_auth_and_validation_error_projection(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    with session_factory() as session:
        _seed_queue(session, name=queue_name)

    task_id = _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"n": 6},
        idempotency_key=f"idem-{uuid.uuid4().hex}",
    )

    status, _hdrs, resp = _asgi_http_call(
        app,
        method="POST",
        path=_cancel_path(task_id),
        headers={"Content-Type": "application/json"},
        body=b"{}",
    )
    assert status == 401
    _assert_error(resp, code="unauthenticated", retryable=False)

    status, _hdrs, resp = _asgi_http_call(
        app,
        method="POST",
        path=_cancel_path(task_id),
        headers=_worker_headers(),
        body=b"{}",
    )
    assert status == 403
    _assert_error(resp, code="permission_denied", retryable=False)

    status, _hdrs, resp = _asgi_http_call(
        app,
        method="POST",
        path=_cancel_path("not-a-uuid"),
        headers=_producer_headers(),
        body=b"{}",
    )
    assert status == 400
    _assert_error(resp, code="validation_failed", retryable=False)

    status, _hdrs, resp = _asgi_http_call(
        app,
        method="POST",
        path=_cancel_path(task_id),
        headers=_producer_headers(),
        body=b'{"reason":"ok","extra":true}',
    )
    assert status == 400
    _assert_error(resp, code="validation_failed", retryable=False)

    missing = str(uuid.uuid4())
    status, _hdrs, resp = _asgi_http_call(
        app,
        method="POST",
        path=_cancel_path(missing),
        headers=_producer_headers(),
        body=b"{}",
    )
    assert status == 404
    _assert_error(resp, code="task_not_found", retryable=False)

    # Same queue scope, different producer → non-disclosing task_not_found.
    status, _hdrs, resp = _asgi_http_call(
        app,
        method="POST",
        path=_cancel_path(task_id),
        headers=_producer_headers(token=PRODUCER_OTHER_TOKEN),
        body=b"{}",
    )
    assert status == 404
    _assert_error(resp, code="task_not_found", retryable=False)

    # Claim-token header must not be required or confer authority.
    status, _hdrs, resp = _asgi_http_call(
        app,
        method="POST",
        path=_cancel_path(task_id),
        headers={
            **_producer_headers(),
            CLAIM_TOKEN_HEADER: str(uuid.uuid4()),
        },
        body=b"{}",
    )
    assert status == 200, resp.decode("utf-8", errors="replace")
