"""Black-box zero-spawn atomic Complete (Phase 03.7-01 / COMP-01).

Proves a current unexpired claim can succeed the source task exactly once,
all Queue-state writes commit or roll back together, and no delivery events
or business-result fields are stored.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
from sqlalchemy import create_engine, event, func, select, text, update
from sqlalchemy.orm import Session, sessionmaker

from workhold.api.application import create_application_app
from workhold.api.schemas.terminal import CompleteCommand, parse_complete_command
from workhold.api.security import ListenerBind
from workhold.application.claim_service import ClaimService
from workhold.application.completion import CompletionFaultHooks, CompletionService
from workhold.application.lease_service import LeaseService
from workhold.domain.queue_control import (
    AdminRequestMetadata,
    BackoffStrategy,
    CreateQueueMutation,
    RetryPolicyDraft,
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
from workhold.storage.models import (
    ClaimRegistry,
    CompleteReplay,
    DeliveryEventActive,
    Queue,
    QueueCounter,
    TaskActive,
    TaskAttempt,
    TaskPayloadActive,
    TaskTerminal,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
OPENAPI_PATH = REPO_ROOT / "openapi" / "queue.openapi.json"

PRODUCER_TOKEN = "tok-producer-complete-atomic"
WORKER_TOKEN = "tok-worker-complete-atomic"
WORKER_OTHER_TOKEN = "tok-worker-complete-other"
ADMIN_TOKEN = "tok-admin-complete-atomic"

PRODUCER_PRINCIPAL = "producer-complete-atomic"
WORKER_PRINCIPAL = "worker-complete-atomic"
WORKER_OTHER_PRINCIPAL = "worker-complete-other"
ADMIN_PRINCIPAL = "admin-complete-atomic"

BASE_QUEUE_NAME = "orders.complete"
OTHER_QUEUE = "billing.complete"
PAYLOAD_SENTINEL = "COMPLETE_SECRET_PAYLOAD_SHOULD_NEVER_LEAK"
CLAIM_PATH = "/v1/claims"
CLAIM_TOKEN_HEADER = "X-Queue-Claim-Token"
REPLICA_ID = "pool-complete/replica-1"

_OP_COMPLETE = 1
_OP_FAIL = 2
_OUTCOME_ACTIVE = 1
_OUTCOME_SUCCEEDED = 2
_STATE_LEASED = 3
_RESULT_SUCCEEDED = 10


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
        pytest.fail("TEST_DATABASE_URL is required for complete atomic integration")
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
        bind=ListenerBind(host="127.0.0.1", port=18098),
        session_factory=session_factory,
        enqueue_service=enqueue_service,
        claim_service=ClaimService(session_factory=session_factory),
        lease_service=LeaseService(session_factory=session_factory),
        completion_service=CompletionService(session_factory=session_factory),
    )


def _asgi_http_call(
    app: Any,
    *,
    method: str,
    path: str,
    headers: Mapping[str, str] | None = None,
    body: bytes = b"",
    query_string: bytes = b"",
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
        "query_string": query_string,
        "headers": header_list,
        "client": ("127.0.0.1", 50000),
        "server": ("127.0.0.1", 18098),
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


def _seed_queue(session: Session, *, name: str) -> Queue:
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
    status, _hdrs, resp = _asgi_http_call(
        app, method="POST", path=path, headers=headers, body=body
    )
    assert status == 201, resp.decode("utf-8", errors="replace")
    return json.loads(resp.decode("utf-8"))["task"]["task_id"]


def _claim_one(app: Any, *, queue_name: str) -> dict[str, Any]:
    body = json.dumps(
        {
            "queues": [queue_name],
            "max_tasks": 1,
            "lease_seconds": 30,
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
    tasks = json.loads(resp.decode("utf-8"))["tasks"]
    assert len(tasks) == 1
    claimed = tasks[0]
    claim = claimed["claim"]
    return {
        "task_id": claimed["task"]["task_id"],
        "claim_id": claim["claim_id"],
        "claim_token": claim["claim_token"],
        "generation": int(claim["generation"]),
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


def _complete_path(claim_id: str) -> str:
    return f"/v1/claims/{claim_id}:complete"


def _complete_body(*, generation: int, spawn: list[Any] | None = None) -> dict[str, Any]:
    return {"generation": generation, "spawn": [] if spawn is None else spawn}


def _assert_error(body: bytes, *, code: str, retryable: bool) -> dict[str, Any]:
    payload = json.loads(body.decode("utf-8"))
    assert isinstance(payload, dict)
    assert payload["code"] == code
    assert payload["retryable"] is retryable
    assert "request_id" in payload
    return payload


def _validate_complete_response(
    *,
    status: int,
    headers: Mapping[str, str],
    body: bytes,
) -> dict[str, Any]:
    assert status == 200
    assert "x-request-id" in {k.lower() for k in headers}
    payload = json.loads(body.decode("utf-8"))
    assert payload["state"] == "succeeded"
    assert isinstance(payload["task_id"], str)
    assert payload["spawned_task_ids"] == []
    assert isinstance(payload["replayed"], bool)
    assert "events" not in payload
    assert "result" not in payload
    assert "parse_result" not in payload
    raw = body.decode("utf-8")
    assert CLAIM_TOKEN_HEADER.lower() not in raw.lower()
    assert "claim_token" not in payload
    return payload


def _leased_count(session: Session, *, queue_id: int) -> int:
    counter = session.get(QueueCounter, queue_id)
    assert counter is not None
    return int(counter.leased_count)


def _delivery_event_count(session: Session, *, task_id: UUID) -> int:
    return int(
        session.scalar(
            select(func.count())
            .select_from(DeliveryEventActive)
            .where(DeliveryEventActive.source_task_id == task_id)
        )
        or 0
    )


def _snapshot_leased_world(
    session: Session,
    *,
    task_id: UUID,
    claim_id: UUID,
) -> dict[str, Any]:
    task = session.execute(
        select(TaskActive).where(TaskActive.task_id == task_id)
    ).scalar_one()
    attempt = session.execute(
        select(TaskAttempt).where(
            TaskAttempt.task_id == task_id,
            TaskAttempt.claim_id == claim_id,
        )
    ).scalar_one()
    registry = session.execute(
        select(ClaimRegistry).where(ClaimRegistry.claim_id == claim_id)
    ).scalar_one_or_none()
    terminal = session.execute(
        select(TaskTerminal).where(TaskTerminal.task_id == task_id)
    ).scalar_one_or_none()
    replay = session.execute(
        select(CompleteReplay).where(CompleteReplay.claim_id == claim_id)
    ).scalar_one_or_none()
    payload = session.get(TaskPayloadActive, task.id)
    return {
        "task_state": int(task.state_code),
        "generation": int(task.generation),
        "claim_id": task.current_claim_id,
        "attempt_outcome": int(attempt.outcome_code),
        "attempt_ended_at": attempt.ended_at,
        "registry_present": registry is not None,
        "terminal_present": terminal is not None,
        "replay_present": replay is not None,
        "payload_present": payload is not None,
        "leased_count": _leased_count(session, queue_id=int(task.queue_id)),
        "delivery_events": _delivery_event_count(session, task_id=task_id),
    }


def test_complete_request_fixture_is_closed() -> None:
    """Zero-spawn body rejects reserved events and unknown fields at parse."""
    with pytest.raises(Exception):
        parse_complete_command(
            {"generation": 1, "spawn": [], "events": []},
        )
    with pytest.raises(Exception):
        parse_complete_command(
            {"generation": 1, "spawn": [], "result": {"ok": True}},
        )
    cmd = parse_complete_command({"generation": 1, "spawn": []})
    assert isinstance(cmd, CompleteCommand)
    assert cmd.generation == 1
    assert cmd.spawn == ()


def test_current_claim_completes_source_exactly_once(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    with session_factory() as session:
        queue = _seed_queue(session, name=queue_name)

    task_id = _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"secret": PAYLOAD_SENTINEL},
        idempotency_key=f"idem-{uuid.uuid4().hex}",
    )
    claim = _claim_one(app, queue_name=queue_name)
    claim_id = claim["claim_id"]
    claim_token = claim["claim_token"]
    generation = int(claim["generation"])

    status, headers, body = _asgi_http_call(
        app,
        method="POST",
        path=_complete_path(claim_id),
        headers=_worker_headers(claim_token=claim_token),
        body=json.dumps(
            _complete_body(generation=generation), separators=(",", ":")
        ).encode("utf-8"),
    )
    result = _validate_complete_response(status=status, headers=headers, body=body)
    assert result["task_id"] == task_id
    assert result["replayed"] is False

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
        assert int(terminal.state_code) == _RESULT_SUCCEEDED
        assert terminal.failure_code is None
        assert terminal.failure_detail is None
        # Payload row is CASCADE-deleted with tasks_active (no active surrogate left).
        attempt = session.execute(
            select(TaskAttempt).where(
                TaskAttempt.task_id == UUID(task_id),
                TaskAttempt.claim_id == UUID(claim_id),
            )
        ).scalar_one()
        assert int(attempt.outcome_code) == _OUTCOME_SUCCEEDED
        assert attempt.ended_at is not None
        assert attempt.failure_code is None
        assert (
            session.execute(
                select(ClaimRegistry).where(ClaimRegistry.claim_id == UUID(claim_id))
            ).scalar_one_or_none()
            is None
        )
        replay = session.execute(
            select(CompleteReplay).where(
                CompleteReplay.claim_id == UUID(claim_id),
                CompleteReplay.operation_code == _OP_COMPLETE,
            )
        ).scalar_one()
        assert int(replay.result_state_code) == _RESULT_SUCCEEDED
        assert list(replay.spawned_task_ids) == []
        assert list(replay.event_ids) == []
        assert _leased_count(session, queue_id=int(queue.id)) == 0
        assert _delivery_event_count(session, task_id=UUID(task_id)) == 0

    # Second identical complete is replay (COMP-03 slice for this plan).
    status2, headers2, body2 = _asgi_http_call(
        app,
        method="POST",
        path=_complete_path(claim_id),
        headers=_worker_headers(claim_token=claim_token),
        body=json.dumps(
            _complete_body(generation=generation), separators=(",", ":")
        ).encode("utf-8"),
    )
    replayed = _validate_complete_response(
        status=status2, headers=headers2, body=body2
    )
    assert replayed["replayed"] is True
    assert replayed["task_id"] == task_id
    assert replayed["spawned_task_ids"] == []

    with session_factory() as session:
        assert (
            session.scalar(
                select(func.count())
                .select_from(CompleteReplay)
                .where(CompleteReplay.claim_id == UUID(claim_id))
            )
            == 1
        )
        assert (
            session.scalar(
                select(func.count())
                .select_from(TaskTerminal)
                .where(TaskTerminal.task_id == UUID(task_id))
            )
            == 1
        )


def test_invalid_claims_leave_zero_writes(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    with session_factory() as session:
        _seed_queue(session, name=queue_name)

    task_id = _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"secret": PAYLOAD_SENTINEL},
        idempotency_key=f"idem-{uuid.uuid4().hex}",
    )
    claim = _claim_one(app, queue_name=queue_name)
    claim_id = claim["claim_id"]
    claim_token = claim["claim_token"]
    generation = int(claim["generation"])

    cases: list[tuple[str, dict[str, str], dict[str, Any], str]] = [
        (
            "wrong_token",
            _worker_headers(claim_token=str(uuid.uuid4())),
            _complete_body(generation=generation),
            "lease_lost",
        ),
        (
            "wrong_generation",
            _worker_headers(claim_token=claim_token),
            _complete_body(generation=generation + 1),
            "lease_lost",
        ),
        (
            "unknown_claim",
            _worker_headers(claim_token=claim_token),
            _complete_body(generation=generation),
            "claim_not_found",
        ),
        (
            "out_of_scope_worker",
            _worker_headers(token=WORKER_OTHER_TOKEN, claim_token=claim_token),
            _complete_body(generation=generation),
            "permission_denied",
        ),
    ]

    for label, headers, body_obj, expected_code in cases:
        path_claim = claim_id if label != "unknown_claim" else str(uuid.uuid4())
        with session_factory() as before:
            snap = _snapshot_leased_world(
                before, task_id=UUID(task_id), claim_id=UUID(claim_id)
            )
        status, _hdrs, body = _asgi_http_call(
            app,
            method="POST",
            path=_complete_path(path_claim),
            headers=headers,
            body=json.dumps(body_obj, separators=(",", ":")).encode("utf-8"),
        )
        assert status in {400, 403, 404, 409}, (label, status, body)
        _assert_error(body, code=expected_code, retryable=False)
        with session_factory() as after:
            assert (
                _snapshot_leased_world(
                    after, task_id=UUID(task_id), claim_id=UUID(claim_id)
                )
                == snap
            ), label


def test_cancellation_requested_claim_rejects_complete(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    with session_factory() as session:
        _seed_queue(session, name=queue_name)

    task_id = _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"secret": PAYLOAD_SENTINEL},
        idempotency_key=f"idem-{uuid.uuid4().hex}",
    )
    claim = _claim_one(app, queue_name=queue_name)
    claim_id = claim["claim_id"]
    claim_token = claim["claim_token"]
    generation = int(claim["generation"])

    cancel_status, _h, cancel_body = _asgi_http_call(
        app,
        method="POST",
        path=f"/v1/tasks/{task_id}:cancel",
        headers={
            "Authorization": f"Bearer {PRODUCER_TOKEN}",
            "Content-Type": "application/json",
        },
        body=b"{}",
    )
    assert cancel_status == 200, cancel_body.decode("utf-8", errors="replace")

    with session_factory() as before:
        snap = _snapshot_leased_world(
            before, task_id=UUID(task_id), claim_id=UUID(claim_id)
        )

    status, _hdrs, body = _asgi_http_call(
        app,
        method="POST",
        path=_complete_path(claim_id),
        headers=_worker_headers(claim_token=claim_token),
        body=json.dumps(
            _complete_body(generation=generation), separators=(",", ":")
        ).encode("utf-8"),
    )
    _assert_error(body, code="cancel_race_lost", retryable=False)
    assert status in {409, 400}

    with session_factory() as after:
        assert (
            _snapshot_leased_world(
                after, task_id=UUID(task_id), claim_id=UUID(claim_id)
            )
            == snap
        )


def test_expired_claim_rejects_complete_without_write(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    with session_factory() as session:
        _seed_queue(session, name=queue_name)

    task_id = _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"secret": PAYLOAD_SENTINEL},
        idempotency_key=f"idem-{uuid.uuid4().hex}",
    )
    claim = _claim_one(app, queue_name=queue_name)
    claim_id = claim["claim_id"]
    claim_token = claim["claim_token"]
    generation = int(claim["generation"])

    with session_factory() as session:
        session.execute(
            update(TaskActive)
            .where(TaskActive.task_id == UUID(task_id))
            .values(
                claimed_at=func.transaction_timestamp() - text("interval '2 hours'"),
                lease_expires_at=func.transaction_timestamp()
                - text("interval '1 second'"),
            )
        )
        session.execute(
            update(ClaimRegistry)
            .where(ClaimRegistry.claim_id == UUID(claim_id))
            .values(
                claimed_at=func.transaction_timestamp() - text("interval '2 hours'"),
                lease_expires_at=func.transaction_timestamp()
                - text("interval '1 second'"),
            )
        )
        session.commit()
        snap = _snapshot_leased_world(
            session, task_id=UUID(task_id), claim_id=UUID(claim_id)
        )

    status, _hdrs, body = _asgi_http_call(
        app,
        method="POST",
        path=_complete_path(claim_id),
        headers=_worker_headers(claim_token=claim_token),
        body=json.dumps(
            _complete_body(generation=generation), separators=(",", ":")
        ).encode("utf-8"),
    )
    _assert_error(body, code="lease_lost", retryable=False)
    assert status in {409, 400}

    with session_factory() as after:
        assert (
            _snapshot_leased_world(
                after, task_id=UUID(task_id), claim_id=UUID(claim_id)
            )
            == snap
        )


def test_events_and_result_rejected(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    """Reserved events[] and business result fields reject without mutation."""
    with session_factory() as session:
        _seed_queue(session, name=queue_name)

    task_id = _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"secret": PAYLOAD_SENTINEL},
        idempotency_key=f"idem-{uuid.uuid4().hex}",
    )
    claim = _claim_one(app, queue_name=queue_name)
    claim_id = claim["claim_id"]
    claim_token = claim["claim_token"]
    generation = int(claim["generation"])

    with session_factory() as before:
        snap = _snapshot_leased_world(
            before, task_id=UUID(task_id), claim_id=UUID(claim_id)
        )

    for body_obj in (
        {"generation": generation, "spawn": [], "events": []},
        {"generation": generation, "spawn": [], "result": {"biz": True}},
    ):
        status, _hdrs, body = _asgi_http_call(
            app,
            method="POST",
            path=_complete_path(claim_id),
            headers=_worker_headers(claim_token=claim_token),
            body=json.dumps(body_obj, separators=(",", ":")).encode("utf-8"),
        )
        assert status == 400
        err = _assert_error(body, code="validation_failed", retryable=False)
        assert "result" not in err
        with session_factory() as after:
            assert (
                _snapshot_leased_world(
                    after, task_id=UUID(task_id), claim_id=UUID(claim_id)
                )
                == snap
            )


def test_injected_failure_after_each_write_boundary_rolls_back(
    session_factory: sessionmaker[Session],
    authorizer: Authorizer,
    queue_name: str,
) -> None:
    with session_factory() as session:
        queue = _seed_queue(session, name=queue_name)

    # Build a minimal app only to enqueue+claim, then call CompletionService directly.
    app = create_application_app(
        authenticator=BearerCredentialAuthenticator.from_bindings(_bindings()),
        authorizer=authorizer,
        bind=ListenerBind(host="127.0.0.1", port=18099),
        session_factory=session_factory,
        enqueue_service=EnqueueService(
            session_factory=session_factory,
            depth_ceilings=DepthCeilings(
                queue_active_depth=100,
                instance_active_depth=500,
                retry_after_ms=250,
            ),
        ),
        claim_service=ClaimService(session_factory=session_factory),
        lease_service=LeaseService(session_factory=session_factory),
        completion_service=CompletionService(session_factory=session_factory),
    )
    task_id = _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"secret": PAYLOAD_SENTINEL},
        idempotency_key=f"idem-{uuid.uuid4().hex}",
    )
    claim = _claim_one(app, queue_name=queue_name)
    claim_id = UUID(claim["claim_id"])
    claim_token = UUID(claim["claim_token"])
    generation = int(claim["generation"])
    command = parse_complete_command(_complete_body(generation=generation))

    stages = (
        "after_attempt_close",
        "after_claim_delete",
        "after_terminal_insert",
        "after_active_delete",
        "after_counter_adjust",
        "after_replay_flush",
    )
    for stage in stages:
        with session_factory() as before:
            # Re-seed leased world if a prior stage somehow committed (must not).
            active = before.execute(
                select(TaskActive).where(TaskActive.task_id == UUID(task_id))
            ).scalar_one_or_none()
            if active is None:
                pytest.fail(f"task left non-leased before stage {stage}")
            snap = _snapshot_leased_world(
                before, task_id=UUID(task_id), claim_id=claim_id
            )

        hooks = CompletionFaultHooks(
            **{
                stage: lambda s=stage: (_ for _ in ()).throw(
                    RuntimeError(f"injected failure at {s}")
                )
            }
        )
        service = CompletionService(
            session_factory=session_factory,
            fault_hooks=hooks,
        )

        def _authorize(name: str) -> bool:
            return name == queue_name

        with pytest.raises(RuntimeError, match="injected failure"):
            service.complete(
                claim_id=claim_id,
                claim_token=claim_token,
                command=command,
                authorize_queue=_authorize,
            )

        with session_factory() as after:
            assert (
                _snapshot_leased_world(
                    after, task_id=UUID(task_id), claim_id=claim_id
                )
                == snap
            ), stage
            assert _delivery_event_count(after, task_id=UUID(task_id)) == 0
            assert _leased_count(after, queue_id=int(queue.id)) == snap["leased_count"]


def test_claim_token_never_leaks_in_response_or_logs(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    with session_factory() as session:
        _seed_queue(session, name=queue_name)

    _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"secret": PAYLOAD_SENTINEL},
        idempotency_key=f"idem-{uuid.uuid4().hex}",
    )
    claim = _claim_one(app, queue_name=queue_name)
    claim_token = claim["claim_token"]

    with caplog.at_level(logging.DEBUG):
        status, headers, body = _asgi_http_call(
            app,
            method="POST",
            path=_complete_path(claim["claim_id"]),
            headers=_worker_headers(claim_token=claim_token),
            body=json.dumps(
                _complete_body(generation=int(claim["generation"])),
                separators=(",", ":"),
            ).encode("utf-8"),
        )
    _validate_complete_response(status=status, headers=headers, body=body)
    joined = " ".join(r.getMessage() for r in caplog.records)
    assert claim_token not in joined
    assert PAYLOAD_SENTINEL not in joined
    assert claim_token not in body.decode("utf-8")

    # Missing header → validation, token never in URL.
    status2, _h2, body2 = _asgi_http_call(
        app,
        method="POST",
        path=_complete_path(claim["claim_id"]),
        headers=_worker_headers(),
        body=json.dumps(
            _complete_body(generation=int(claim["generation"])),
            separators=(",", ":"),
        ).encode("utf-8"),
        query_string=b"claim_token=should-not-work",
    )
    assert status2 == 400
    _assert_error(body2, code="validation_failed", retryable=False)
