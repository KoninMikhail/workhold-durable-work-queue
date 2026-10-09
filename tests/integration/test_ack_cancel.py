"""Black-box worker HTTP ack_cancel integration (Phase 03.6-05).

Covers WORK-14, COMP-03, API-03: fenced cooperative cancellation acknowledgement
over real PostgreSQL with replay, fencing parity, and token redaction.
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

import pytest
from sqlalchemy import create_engine, event, func, select, text, update
from sqlalchemy.orm import Session, sessionmaker

from queue_service.api.application import create_application_app
from queue_service.api.security import ListenerBind
from queue_service.application.claim_service import ClaimService
from queue_service.application.lease_service import LeaseService
from queue_service.domain.queue_control import (
    AdminRequestMetadata,
    BackoffStrategy,
    CreateQueueMutation,
    RetryPolicyDraft,
)
from queue_service.infrastructure.postgres.lease_repository import (
    FenceDecision,
    LeaseRepository,
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
    CompleteReplay,
    DeliveryEventActive,
    Queue,
    QueueCounter,
    TaskActive,
    TaskAttempt,
    TaskTerminal,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
OPENAPI_PATH = REPO_ROOT / "openapi" / "queue.openapi.json"

PRODUCER_TOKEN = "tok-producer-ack-cancel"
WORKER_TOKEN = "tok-worker-ack-cancel"
WORKER_OTHER_TOKEN = "tok-worker-ack-cancel-other"
ADMIN_TOKEN = "tok-admin-ack-cancel"

PRODUCER_PRINCIPAL = "producer-ack-cancel"
WORKER_PRINCIPAL = "worker-ack-cancel"
WORKER_OTHER_PRINCIPAL = "worker-ack-cancel-other"
ADMIN_PRINCIPAL = "admin-ack-cancel"

BASE_QUEUE_NAME = "orders.ackcancel"
OTHER_QUEUE = "billing.ackcancel"
PAYLOAD_SENTINEL = "ACK_CANCEL_SECRET_PAYLOAD_SHOULD_NEVER_LEAK"
CLAIM_PATH = "/v1/claims"
CLAIM_TOKEN_HEADER = "X-Queue-Claim-Token"
REPLICA_ID = "pool-ack-cancel/replica-1"

_OP_FAIL = 2
_OP_ACK_CANCEL = 3
_OUTCOME_ACTIVE = 1
_OUTCOME_CANCELLED = 6
_STATE_LEASED = 3
_RESULT_CANCELLED = 12


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
        pytest.fail("TEST_DATABASE_URL is required for ack_cancel integration")
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


def _ack_path(claim_id: str) -> str:
    return f"/v1/claims/{claim_id}:ack-cancel"


def _ack_body(*, generation: int) -> dict[str, Any]:
    return {"generation": generation}


def _cancel_path(task_id: str) -> str:
    return f"/v1/tasks/{task_id}:cancel"


def _assert_error(body: bytes, *, code: str, retryable: bool) -> dict[str, Any]:
    payload = json.loads(body.decode("utf-8"))
    assert isinstance(payload, dict)
    assert payload["code"] == code
    assert payload["retryable"] is retryable
    assert "request_id" in payload
    return payload


def _validate_ack_response(
    *,
    status: int,
    headers: Mapping[str, str],
    body: bytes,
) -> dict[str, Any]:
    assert status == 200
    assert "x-request-id" in {k.lower() for k in headers}
    payload = json.loads(body.decode("utf-8"))
    assert isinstance(payload, dict)
    assert set(payload) >= {"task_id", "state", "terminal_at", "replayed"}
    assert payload["state"] == "cancelled"
    assert isinstance(payload["task_id"], str)
    uuid.UUID(payload["task_id"])
    assert isinstance(payload["terminal_at"], str)
    assert isinstance(payload["replayed"], bool)
    # Closed OpenAPI request fixture gate (generation-only body).
    assert OPENAPI_PATH.is_file()
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


def _request_cancel(app: Any, *, task_id: str) -> None:
    status, _hdrs, resp = _asgi_http_call(
        app,
        method="POST",
        path=_cancel_path(task_id),
        headers={
            "Authorization": f"Bearer {PRODUCER_TOKEN}",
            "Content-Type": "application/json",
        },
        body=b"{}",
    )
    assert status == 200, resp.decode("utf-8", errors="replace")


def _assert_no_side_effects(session: Session, *, task_id: uuid.UUID) -> None:
    spawned = session.scalar(
        select(func.count())
        .select_from(TaskActive)
        .where(TaskActive.source_task_id == task_id)
    )
    events = session.scalar(
        select(func.count())
        .select_from(DeliveryEventActive)
        .where(DeliveryEventActive.source_task_id == task_id)
    )
    assert int(spawned or 0) == 0
    assert int(events or 0) == 0


def _snapshot_lease_rows(
    session: Session,
    *,
    task_id: uuid.UUID,
) -> dict[str, Any]:
    task = session.execute(
        select(TaskActive).where(TaskActive.task_id == task_id)
    ).scalar_one_or_none()
    registry = session.execute(
        select(ClaimRegistry).where(ClaimRegistry.task_id == task_id)
    ).scalar_one_or_none()
    attempts = list(
        session.scalars(
            select(TaskAttempt)
            .where(TaskAttempt.task_id == task_id)
            .order_by(TaskAttempt.generation)
        )
    )
    return {
        "task": task,
        "registry": registry,
        "attempt_count": len(attempts),
        "attempt_outcomes": [int(a.outcome_code) for a in attempts],
        "cancel_requested_at": (
            None if task is None else task.cancel_requested_at
        ),
    }


def test_ack_cancel_request_fixture_is_closed() -> None:
    spec = json.loads(OPENAPI_PATH.read_text(encoding="utf-8"))
    schema = spec["components"]["schemas"]["AckCancelRequest"]
    assert schema["additionalProperties"] is False
    assert schema["required"] == ["generation"]
    assert set(schema["properties"]) == {"generation"}
    body = _ack_body(generation=1)
    assert set(body) == {"generation"}
    assert "extra" not in body


def test_ack_cancel_closes_attempt_and_task_atomically(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    session = session_factory()
    try:
        _seed_queue(session, name=queue_name)
    finally:
        session.close()

    task_id_str = _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"secret": PAYLOAD_SENTINEL, "n": 1},
        idempotency_key="idem-ack-cancel-ok",
    )
    claimed = _claim_one(app, queue_name=queue_name, lease_seconds=120)
    claim = claimed["claim"]
    task_id = uuid.UUID(claimed["task"]["task_id"])
    assert str(task_id) == task_id_str
    claim_id = claim["claim_id"]
    token = claim["claim_token"]
    generation = int(claim["generation"])

    _request_cancel(app, task_id=str(task_id))

    encoded = json.dumps(
        _ack_body(generation=generation), separators=(",", ":")
    ).encode("utf-8")

    with caplog.at_level(logging.INFO):
        status, headers, resp = _asgi_http_call(
            app,
            method="POST",
            path=_ack_path(claim_id),
            headers=_worker_headers(claim_token=token),
            body=encoded,
        )

    assert status == 200, resp.decode("utf-8", errors="replace")
    payload = _validate_ack_response(status=status, headers=headers, body=resp)
    assert payload["task_id"] == str(task_id)
    assert payload["state"] == "cancelled"
    assert payload["replayed"] is False
    assert "terminal_at" in payload
    assert token not in resp.decode("utf-8")
    assert token not in _ack_path(claim_id)
    assert PAYLOAD_SENTINEL not in resp.decode("utf-8")
    for record in caplog.records:
        assert token not in record.getMessage()
        assert PAYLOAD_SENTINEL not in record.getMessage()

    after = session_factory()
    try:
        assert (
            after.execute(
                select(TaskActive).where(TaskActive.task_id == task_id)
            ).scalar_one_or_none()
            is None
        )
        assert (
            after.execute(
                select(ClaimRegistry).where(
                    ClaimRegistry.claim_id == uuid.UUID(claim_id)
                )
            ).scalar_one_or_none()
            is None
        )
        terminal = after.execute(
            select(TaskTerminal).where(TaskTerminal.task_id == task_id)
        ).scalar_one()
        assert int(terminal.state_code) == _RESULT_CANCELLED
        attempt = after.execute(
            select(TaskAttempt).where(
                TaskAttempt.task_id == task_id,
                TaskAttempt.claim_id == uuid.UUID(claim_id),
            )
        ).scalar_one()
        assert int(attempt.outcome_code) == _OUTCOME_CANCELLED
        assert attempt.ended_at is not None
        replay = after.execute(
            select(CompleteReplay).where(
                CompleteReplay.claim_id == uuid.UUID(claim_id),
                CompleteReplay.operation_code == _OP_ACK_CANCEL,
            )
        ).scalar_one()
        assert int(replay.result_state_code) == _RESULT_CANCELLED
        assert replay.terminal_at is not None
        assert list(replay.spawned_task_ids) == []
        assert list(replay.event_ids) == []
        counter = after.get(QueueCounter, terminal.queue_id)
        assert counter is not None
        assert int(counter.leased_count) == 0
        _assert_no_side_effects(after, task_id=task_id)
    finally:
        after.close()


def test_same_body_replay_and_changed_body_conflict(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    session = session_factory()
    try:
        _seed_queue(session, name=queue_name)
    finally:
        session.close()

    _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"secret": PAYLOAD_SENTINEL},
        idempotency_key="idem-ack-cancel-replay",
    )
    claimed = _claim_one(app, queue_name=queue_name)
    claim = claimed["claim"]
    task_id = uuid.UUID(claimed["task"]["task_id"])
    claim_id = claim["claim_id"]
    token = claim["claim_token"]
    generation = int(claim["generation"])
    _request_cancel(app, task_id=str(task_id))

    encoded = json.dumps(
        _ack_body(generation=generation), separators=(",", ":")
    ).encode("utf-8")

    status1, headers1, resp1 = _asgi_http_call(
        app,
        method="POST",
        path=_ack_path(claim_id),
        headers=_worker_headers(claim_token=token),
        body=encoded,
    )
    assert status1 == 200
    first = _validate_ack_response(status=status1, headers=headers1, body=resp1)
    assert first["replayed"] is False

    status2, headers2, resp2 = _asgi_http_call(
        app,
        method="POST",
        path=_ack_path(claim_id),
        headers=_worker_headers(claim_token=token),
        body=encoded,
    )
    assert status2 == 200
    second = _validate_ack_response(status=status2, headers=headers2, body=resp2)
    assert second["replayed"] is True
    assert {k: v for k, v in second.items() if k != "replayed"} == {
        k: v for k, v in first.items() if k != "replayed"
    }

    after = session_factory()
    try:
        replays = list(
            after.scalars(
                select(CompleteReplay).where(
                    CompleteReplay.claim_id == uuid.UUID(claim_id),
                    CompleteReplay.operation_code == _OP_ACK_CANCEL,
                )
            )
        )
        assert len(replays) == 1
        terminals = list(
            after.scalars(select(TaskTerminal).where(TaskTerminal.task_id == task_id))
        )
        assert len(terminals) == 1
        _assert_no_side_effects(after, task_id=task_id)
    finally:
        after.close()

    status3, _h3, resp3 = _asgi_http_call(
        app,
        method="POST",
        path=_ack_path(claim_id),
        headers=_worker_headers(claim_token=token),
        body=json.dumps(
            _ack_body(generation=generation + 1), separators=(",", ":")
        ).encode("utf-8"),
    )
    assert status3 == 409
    _assert_error(resp3, code="idempotency_conflict", retryable=False)


@pytest.mark.parametrize(
    "mutate",
    ["wrong_token", "wrong_generation", "wrong_task", "expired", "superseded"],
)
def test_stale_credentials_return_lease_lost_without_mutation(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
    mutate: str,
) -> None:
    session = session_factory()
    try:
        _seed_queue(session, name=queue_name)
    finally:
        session.close()

    _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"secret": PAYLOAD_SENTINEL},
        idempotency_key=f"idem-ack-stale-{mutate}",
    )
    claimed = _claim_one(app, queue_name=queue_name, lease_seconds=90)
    claim = claimed["claim"]
    task_id = uuid.UUID(claimed["task"]["task_id"])
    claim_id = claim["claim_id"]
    token = claim["claim_token"]
    generation = int(claim["generation"])
    # Cancel only while the claim remains current. Supersede reclaim after a
    # cancellation request finalizes via expiry-cancel instead of reclaiming.
    if mutate != "superseded":
        _request_cancel(app, task_id=str(task_id))

    path_claim_id = claim_id
    if mutate == "wrong_token":
        token = str(uuid.uuid4())
    elif mutate == "wrong_generation":
        generation = generation + 1
    elif mutate == "wrong_task":
        path_claim_id = str(uuid.uuid4())
    elif mutate == "expired":
        expire = session_factory()
        try:
            expire.execute(
                update(TaskActive)
                .where(TaskActive.task_id == task_id)
                .values(
                    lease_expires_at=func.transaction_timestamp()
                    - text("interval '1 second'")
                )
            )
            expire.execute(
                update(ClaimRegistry)
                .where(ClaimRegistry.claim_id == uuid.UUID(claim_id))
                .values(
                    claimed_at=func.transaction_timestamp() - text("interval '2 hours'"),
                    lease_expires_at=func.transaction_timestamp()
                    - text("interval '1 second'"),
                )
            )
            expire.commit()
        finally:
            expire.close()
    elif mutate == "superseded":
        expire = session_factory()
        try:
            expire.execute(
                update(TaskActive)
                .where(TaskActive.task_id == task_id)
                .values(
                    lease_expires_at=func.transaction_timestamp()
                    - text("interval '1 second'")
                )
            )
            expire.commit()
        finally:
            expire.close()
        reclaim = ClaimService(session_factory=session_factory).claim(
            queue_name=queue_name,
            worker_id=f"{WORKER_PRINCIPAL}/reclaimer",
            lease_seconds=90,
        )
        assert reclaim.empty is False
        assert reclaim.generation == 2
        # Cancellation request is irrelevant for the stale prior claim; fence
        # must reject before the ack-specific predicate.

    before = session_factory()
    try:
        snap_before = _snapshot_lease_rows(before, task_id=task_id)
        outcomes_before = list(snap_before["attempt_outcomes"])
        cancel_before = snap_before["cancel_requested_at"]
    finally:
        before.close()

    status, _hdrs, resp = _asgi_http_call(
        app,
        method="POST",
        path=_ack_path(path_claim_id),
        headers=_worker_headers(claim_token=token),
        body=json.dumps(
            _ack_body(generation=generation), separators=(",", ":")
        ).encode("utf-8"),
    )
    if mutate == "wrong_task":
        assert status in {404, 409}
        payload = json.loads(resp.decode("utf-8"))
        assert payload["code"] in {"claim_not_found", "lease_lost"}
    else:
        assert status == 409
        _assert_error(resp, code="lease_lost", retryable=False)

    after = session_factory()
    try:
        snap_after = _snapshot_lease_rows(after, task_id=task_id)
        assert snap_after["attempt_outcomes"] == outcomes_before
        if mutate != "superseded":
            assert snap_after["cancel_requested_at"] == cancel_before
        assert (
            after.execute(
                select(CompleteReplay).where(
                    CompleteReplay.claim_id == uuid.UUID(claim_id),
                    CompleteReplay.operation_code == _OP_ACK_CANCEL,
                )
            ).scalar_one_or_none()
            is None
        )
        assert (
            after.execute(
                select(TaskTerminal).where(TaskTerminal.task_id == task_id)
            ).scalar_one_or_none()
            is None
        )
    finally:
        after.close()


def test_missing_cancellation_request_does_not_mutate(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    session = session_factory()
    try:
        _seed_queue(session, name=queue_name)
    finally:
        session.close()

    _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"secret": PAYLOAD_SENTINEL},
        idempotency_key="idem-ack-no-request",
    )
    claimed = _claim_one(app, queue_name=queue_name)
    claim = claimed["claim"]
    task_id = uuid.UUID(claimed["task"]["task_id"])
    claim_id = claim["claim_id"]
    token = claim["claim_token"]
    generation = int(claim["generation"])

    before = session_factory()
    try:
        snap_before = _snapshot_lease_rows(before, task_id=task_id)
        assert snap_before["cancel_requested_at"] is None
        outcomes_before = list(snap_before["attempt_outcomes"])
    finally:
        before.close()

    status, _hdrs, resp = _asgi_http_call(
        app,
        method="POST",
        path=_ack_path(claim_id),
        headers=_worker_headers(claim_token=token),
        body=json.dumps(
            _ack_body(generation=generation), separators=(",", ":")
        ).encode("utf-8"),
    )
    assert status == 409
    _assert_error(resp, code="cancel_race_lost", retryable=False)

    after = session_factory()
    try:
        snap_after = _snapshot_lease_rows(after, task_id=task_id)
        assert snap_after["attempt_outcomes"] == outcomes_before
        assert snap_after["cancel_requested_at"] is None
        assert int(snap_after["task"].state_code) == _STATE_LEASED
        assert (
            after.execute(
                select(CompleteReplay).where(
                    CompleteReplay.claim_id == uuid.UUID(claim_id)
                )
            ).scalar_one_or_none()
            is None
        )
        assert (
            after.execute(
                select(TaskTerminal).where(TaskTerminal.task_id == task_id)
            ).scalar_one_or_none()
            is None
        )
    finally:
        after.close()


def test_competing_fail_terminal_returns_task_already_terminal(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    session = session_factory()
    try:
        _seed_queue(session, name=queue_name)
    finally:
        session.close()

    _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"secret": PAYLOAD_SENTINEL},
        idempotency_key="idem-ack-vs-fail",
    )
    claimed = _claim_one(app, queue_name=queue_name)
    claim = claimed["claim"]
    task_id = uuid.UUID(claimed["task"]["task_id"])
    claim_id = claim["claim_id"]
    token = claim["claim_token"]
    generation = int(claim["generation"])

    fail_body = json.dumps(
        {
            "generation": generation,
            "retryable": False,
            "failure_code": "worker.boom",
            "failure_detail": "fail won",
        },
        separators=(",", ":"),
    ).encode("utf-8")
    fail_status, _fh, fail_resp = _asgi_http_call(
        app,
        method="POST",
        path=f"/v1/claims/{claim_id}:fail",
        headers=_worker_headers(claim_token=token),
        body=fail_body,
    )
    assert fail_status == 200, fail_resp.decode("utf-8", errors="replace")

    status, _hdrs, resp = _asgi_http_call(
        app,
        method="POST",
        path=_ack_path(claim_id),
        headers=_worker_headers(claim_token=token),
        body=json.dumps(
            _ack_body(generation=generation), separators=(",", ":")
        ).encode("utf-8"),
    )
    assert status == 409
    _assert_error(resp, code="task_already_terminal", retryable=False)

    after = session_factory()
    try:
        assert (
            after.execute(
                select(CompleteReplay).where(
                    CompleteReplay.claim_id == uuid.UUID(claim_id),
                    CompleteReplay.operation_code == _OP_ACK_CANCEL,
                )
            ).scalar_one_or_none()
            is None
        )
        terminal = after.execute(
            select(TaskTerminal).where(TaskTerminal.task_id == task_id)
        ).scalar_one()
        assert int(terminal.state_code) != _RESULT_CANCELLED
        fail_replays = list(
            after.scalars(
                select(CompleteReplay).where(
                    CompleteReplay.claim_id == uuid.UUID(claim_id),
                    CompleteReplay.operation_code == _OP_FAIL,
                )
            )
        )
        assert len(fail_replays) == 1
    finally:
        after.close()


def test_claim_token_header_required_and_never_in_url(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    session = session_factory()
    try:
        _seed_queue(session, name=queue_name)
    finally:
        session.close()

    _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"secret": PAYLOAD_SENTINEL},
        idempotency_key="idem-ack-token-header",
    )
    claimed = _claim_one(app, queue_name=queue_name)
    claim = claimed["claim"]
    task_id = claimed["task"]["task_id"]
    claim_id = claim["claim_id"]
    token = claim["claim_token"]
    generation = int(claim["generation"])
    _request_cancel(app, task_id=task_id)

    status_missing, _h1, resp_missing = _asgi_http_call(
        app,
        method="POST",
        path=_ack_path(claim_id),
        headers=_worker_headers(),
        body=json.dumps(
            _ack_body(generation=generation), separators=(",", ":")
        ).encode("utf-8"),
    )
    assert status_missing == 400
    _assert_error(resp_missing, code="validation_failed", retryable=False)

    status_query, _h2, resp_query = _asgi_http_call(
        app,
        method="POST",
        path=_ack_path(claim_id),
        headers=_worker_headers(),
        query_string=f"claim_token={token}".encode("utf-8"),
        body=json.dumps(
            _ack_body(generation=generation), separators=(",", ":")
        ).encode("utf-8"),
    )
    assert status_query == 400
    _assert_error(resp_query, code="validation_failed", retryable=False)

    status_ok, headers_ok, resp_ok = _asgi_http_call(
        app,
        method="POST",
        path=_ack_path(claim_id),
        headers=_worker_headers(claim_token=token),
        body=json.dumps(
            _ack_body(generation=generation), separators=(",", ":")
        ).encode("utf-8"),
    )
    assert status_ok == 200
    body_text = resp_ok.decode("utf-8")
    assert token not in body_text
    assert "claim_token" not in body_text
    assert CLAIM_TOKEN_HEADER.lower() not in {
        k.lower() for k in headers_ok
    } or token not in headers_ok.get(CLAIM_TOKEN_HEADER.lower(), "")


@pytest.mark.parametrize(
    "mutate",
    ["valid", "wrong_token", "wrong_generation", "expired", "superseded"],
)
def test_shared_fence_probe_matches_ack_cancel_decision(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
    mutate: str,
) -> None:
    session = session_factory()
    try:
        _seed_queue(session, name=queue_name)
    finally:
        session.close()

    _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"n": 1},
        idempotency_key=f"idem-ack-probe-{mutate}",
    )
    claimed = _claim_one(app, queue_name=queue_name, lease_seconds=60)
    claim = claimed["claim"]
    task_id = uuid.UUID(claimed["task"]["task_id"])
    claim_id = uuid.UUID(claim["claim_id"])
    token = uuid.UUID(claim["claim_token"])
    generation = int(claim["generation"])
    if mutate != "superseded":
        _request_cancel(app, task_id=str(task_id))

    probe_token = token
    probe_generation = generation
    if mutate == "wrong_token":
        probe_token = uuid.uuid4()
    elif mutate == "wrong_generation":
        probe_generation = generation + 1
    elif mutate == "expired":
        expire = session_factory()
        try:
            expire.execute(
                update(TaskActive)
                .where(TaskActive.task_id == task_id)
                .values(
                    lease_expires_at=func.transaction_timestamp()
                    - text("interval '1 second'")
                )
            )
            expire.execute(
                update(ClaimRegistry)
                .where(ClaimRegistry.claim_id == claim_id)
                .values(
                    claimed_at=func.transaction_timestamp() - text("interval '2 hours'"),
                    lease_expires_at=func.transaction_timestamp()
                    - text("interval '1 second'"),
                )
            )
            expire.commit()
        finally:
            expire.close()
    elif mutate == "superseded":
        expire = session_factory()
        try:
            expire.execute(
                update(TaskActive)
                .where(TaskActive.task_id == task_id)
                .values(
                    lease_expires_at=func.transaction_timestamp()
                    - text("interval '1 second'")
                )
            )
            expire.commit()
        finally:
            expire.close()
        reclaim = ClaimService(session_factory=session_factory).claim(
            queue_name=queue_name,
            worker_id=f"{WORKER_PRINCIPAL}/probe-reclaim",
            lease_seconds=60,
        )
        assert reclaim.empty is False
        assert reclaim.generation == 2

    repo = LeaseRepository()
    probe_session = session_factory()
    try:
        probe = repo.validate_current_lease(
            probe_session,
            claim_id=claim_id,
            claim_token=probe_token,
            generation=probe_generation,
            for_update=False,
        )
    finally:
        probe_session.rollback()
        probe_session.close()

    lease_probe = LeaseService(session_factory=session_factory).probe_current_lease(
        claim_id=claim_id,
        claim_token=probe_token,
        generation=probe_generation,
    )
    assert lease_probe.decision is probe.decision

    status, _hdrs, resp = _asgi_http_call(
        app,
        method="POST",
        path=_ack_path(str(claim_id)),
        headers=_worker_headers(claim_token=str(probe_token)),
        body=json.dumps(
            _ack_body(generation=probe_generation), separators=(",", ":")
        ).encode("utf-8"),
    )

    if mutate == "valid":
        assert probe.decision is FenceDecision.CURRENT
        assert status == 200
        payload = json.loads(resp.decode("utf-8"))
        assert payload["state"] == "cancelled"
        assert payload["replayed"] is False
    else:
        assert probe.decision is FenceDecision.STALE
        assert status == 409
        _assert_error(resp, code="lease_lost", retryable=False)


def test_out_of_scope_worker_denied(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    session = session_factory()
    try:
        _seed_queue(session, name=queue_name)
    finally:
        session.close()

    _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"n": 1},
        idempotency_key="idem-ack-scope",
    )
    claimed = _claim_one(app, queue_name=queue_name)
    claim = claimed["claim"]
    task_id = claimed["task"]["task_id"]
    _request_cancel(app, task_id=task_id)

    status, _hdrs, resp = _asgi_http_call(
        app,
        method="POST",
        path=_ack_path(claim["claim_id"]),
        headers=_worker_headers(
            token=WORKER_OTHER_TOKEN,
            claim_token=claim["claim_token"],
        ),
        body=json.dumps(
            _ack_body(generation=int(claim["generation"])), separators=(",", ":")
        ).encode("utf-8"),
    )
    assert status == 403
    _assert_error(resp, code="permission_denied", retryable=False)
