"""Complete uncertain-response replay (Phase 03.7-03 / COMP-03, API-03).

Same claim + normalized body recovers the committed protocol success after lost
HTTP delivery and process restart; changed fingerprints conflict with zero writes;
expired registry and other-terminal winners are not misreported as replay; replay
bypasses drain/depth admission because it writes nothing.

Phase 12 Wave 0 priority replay scaffolds are temporarily skipped; Plan 06 removes
the markers.
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

from queue_service.api.application import create_application_app
from queue_service.api.schemas.terminal import CompleteCommand, parse_complete_command
from queue_service.api.security import ListenerBind
from queue_service.application.claim_service import ClaimService
from queue_service.application.completion import CompletionFaultHooks, CompletionService
from queue_service.application.lease_service import LeaseService
from datetime import timedelta

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
    CompleteReplay,
    DeliveryEventActive,
    Queue,
    QueueCounter,
    TaskActive,
    TaskAttempt,
    TaskPayloadActive,
    TaskTerminal,
    CompletionEffect,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
OPENAPI_PATH = REPO_ROOT / "openapi" / "queue.openapi.json"

PRODUCER_TOKEN = "tok-producer-complete-replay"
WORKER_TOKEN = "tok-worker-complete-replay"
WORKER_OTHER_TOKEN = "tok-worker-complete-replay-other"
ADMIN_TOKEN = "tok-admin-complete-replay"

PRODUCER_PRINCIPAL = "producer-complete-replay"
WORKER_PRINCIPAL = "worker-complete-replay"
WORKER_OTHER_PRINCIPAL = "worker-complete-replay-other"
ADMIN_PRINCIPAL = "admin-complete-replay"

BASE_QUEUE_NAME = "orders.complete.replay"
OTHER_QUEUE = "billing.complete.replay"
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
def target_queue_name() -> str:
    return _unique_queue_name("billing.complete.replay")


@pytest.fixture
def authorizer(queue_name: str, target_queue_name: str) -> Authorizer:
    return Authorizer(
        queue_scopes={
            PRODUCER_PRINCIPAL: frozenset(
                {queue_name, target_queue_name, BASE_QUEUE_NAME, OTHER_QUEUE}
            ),
            WORKER_PRINCIPAL: frozenset(
                {queue_name, target_queue_name, BASE_QUEUE_NAME}
            ),
            WORKER_OTHER_PRINCIPAL: frozenset({OTHER_QUEUE}),
            ADMIN_PRINCIPAL: frozenset(
                {queue_name, target_queue_name, BASE_QUEUE_NAME}
            ),
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
        bind=ListenerBind(host="127.0.0.1", port=18103),
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
        "server": ("127.0.0.1", 18103),
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


def _build_app(
    *,
    session_factory: sessionmaker[Session],
    authorizer: Authorizer,
    depth_ceilings: DepthCeilings | None = None,
) -> Any:
    ceilings = depth_ceilings or DepthCeilings(
        queue_active_depth=100,
        instance_active_depth=500,
        retry_after_ms=250,
    )
    enqueue_service = EnqueueService(
        session_factory=session_factory,
        depth_ceilings=ceilings,
    )
    completion_service = CompletionService(session_factory=session_factory)
    app = create_application_app(
        authenticator=BearerCredentialAuthenticator.from_bindings(_bindings()),
        authorizer=authorizer,
        bind=ListenerBind(host="127.0.0.1", port=18103),
        session_factory=session_factory,
        enqueue_service=enqueue_service,
        claim_service=ClaimService(session_factory=session_factory),
        lease_service=LeaseService(session_factory=session_factory),
        completion_service=completion_service,
    )
    app._queue_completion_service = completion_service  # noqa: SLF001 — test seam
    return app


def _set_queue_state(session: Session, *, name: str, state: QueueState) -> None:
    queue = session.execute(select(Queue).where(Queue.name == name)).scalar_one()
    QueueControlRepository().set_queue_state(
        session,
        queue_name=name,
        mutation=SetQueueStateMutation(
            expected_config_version=ConfigVersion(value=int(queue.config_version)),
            state=state,
            metadata=_admin_meta(),
        ),
    )
    session.commit()


def _spawn_item(
    *,
    queue_name: str,
    payload: Any,
    idempotency_key: str | None = None,
) -> dict[str, Any]:
    return {
        "queue_name": queue_name,
        "idempotency_key": idempotency_key or f"spawn-{uuid.uuid4().hex}",
        "payload": payload,
        "priority": 0,
    }


def _world_counts(
    session: Session, *, claim_id: UUID, task_id: UUID, queue_id: int
) -> dict[str, int]:
    return {
        "replays": int(
            session.scalar(
                select(func.count())
                .select_from(CompleteReplay)
                .where(CompleteReplay.claim_id == claim_id)
            )
            or 0
        ),
        "terminals": int(
            session.scalar(
                select(func.count())
                .select_from(TaskTerminal)
                .where(TaskTerminal.task_id == task_id)
            )
            or 0
        ),
        "attempts": int(
            session.scalar(
                select(func.count())
                .select_from(TaskAttempt)
                .where(TaskAttempt.task_id == task_id)
            )
            or 0
        ),
        "effects": int(
            session.scalar(
                select(func.count())
                .select_from(CompletionEffect)
                .where(CompletionEffect.source_claim_id == claim_id)
            )
            or 0
        ),
        "active": int(
            session.scalar(
                select(func.count())
                .select_from(TaskActive)
                .where(TaskActive.task_id == task_id)
            )
            or 0
        ),
        "leased": _leased_count(session, queue_id=queue_id),
        "delivery_events": _delivery_event_count(session, task_id=task_id),
    }


def _complete(
    app: Any,
    *,
    claim_id: str,
    claim_token: str,
    generation: int,
    spawn: list[Any] | None = None,
    token: str = WORKER_TOKEN,
) -> tuple[int, dict[str, str], bytes]:
    return _asgi_http_call(
        app,
        method="POST",
        path=_complete_path(claim_id),
        headers=_worker_headers(token=token, claim_token=claim_token),
        body=json.dumps(
            _complete_body(generation=generation, spawn=spawn),
            separators=(",", ":"),
        ).encode("utf-8"),
    )


def _validate_any_complete(
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
    assert isinstance(payload["spawned_task_ids"], list)
    assert isinstance(payload["replayed"], bool)
    assert "events" not in payload
    assert "result" not in payload
    assert "claim_token" not in payload
    return payload


def test_uncertain_same_body_replays_after_process_restart(
    session_factory: sessionmaker[Session],
    authorizer: Authorizer,
    queue_name: str,
) -> None:
    with session_factory() as session:
        queue = _seed_queue(session, name=queue_name)

    app1 = _build_app(session_factory=session_factory, authorizer=authorizer)
    task_id = UUID(
        _enqueue_ready(
            app1,
            queue_name=queue_name,
            payload={"secret": "never-stored-in-replay"},
            idempotency_key=f"idem-{uuid.uuid4().hex}",
        )
    )
    claim = _claim_one(app1, queue_name=queue_name)
    claim_id = UUID(claim["claim_id"])
    status1, headers1, body1 = _complete(
        app1,
        claim_id=claim["claim_id"],
        claim_token=claim["claim_token"],
        generation=int(claim["generation"]),
    )
    first = _validate_complete_response(status=status1, headers=headers1, body=body1)
    assert first["replayed"] is False
    del body1
    del app1

    with session_factory() as session:
        before = _world_counts(
            session, claim_id=claim_id, task_id=task_id, queue_id=int(queue.id)
        )
        replay = session.execute(
            select(CompleteReplay).where(
                CompleteReplay.claim_id == claim_id,
                CompleteReplay.operation_code == _OP_COMPLETE,
            )
        ).scalar_one()
        assert list(replay.event_ids) == []
        assert int(replay.result_state_code) == _RESULT_SUCCEEDED
        assert len(bytes(replay.request_fingerprint)) == 32

    app2 = _build_app(session_factory=session_factory, authorizer=authorizer)
    status2, headers2, body2 = _complete(
        app2,
        claim_id=claim["claim_id"],
        claim_token=claim["claim_token"],
        generation=int(claim["generation"]),
    )
    second = _validate_complete_response(status=status2, headers=headers2, body=body2)
    assert second["replayed"] is True
    assert {k: second[k] for k in second if k != "replayed"} == {
        k: first[k] for k in first if k != "replayed"
    }

    with session_factory() as session:
        after = _world_counts(
            session, claim_id=claim_id, task_id=task_id, queue_id=int(queue.id)
        )
        assert after == before
        assert after["replays"] == 1
        assert after["terminals"] == 1
        assert after["active"] == 0
        assert after["delivery_events"] == 0


def test_changed_fingerprint_conflicts_with_zero_writes(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    with session_factory() as session:
        queue = _seed_queue(session, name=queue_name)
        child = _seed_queue(session, name=f"{queue_name}.child")

    task_id = UUID(
        _enqueue_ready(
            app,
            queue_name=queue_name,
            payload={"n": 1},
            idempotency_key=f"idem-{uuid.uuid4().hex}",
        )
    )
    claim = _claim_one(app, queue_name=queue_name)
    claim_id = UUID(claim["claim_id"])
    status1, headers1, body1 = _complete(
        app,
        claim_id=claim["claim_id"],
        claim_token=claim["claim_token"],
        generation=int(claim["generation"]),
        spawn=[],
    )
    first = _validate_complete_response(status=status1, headers=headers1, body=body1)
    assert first["replayed"] is False

    with session_factory() as session:
        before = _world_counts(
            session, claim_id=claim_id, task_id=task_id, queue_id=int(queue.id)
        )

    status2, _h2, body2 = _complete(
        app,
        claim_id=claim["claim_id"],
        claim_token=claim["claim_token"],
        generation=int(claim["generation"]),
        spawn=[_spawn_item(queue_name=child.name, payload={"different": True})],
    )
    assert status2 == 409
    _assert_error(body2, code="idempotency_conflict", retryable=False)

    with session_factory() as session:
        after = _world_counts(
            session, claim_id=claim_id, task_id=task_id, queue_id=int(queue.id)
        )
        assert after == before
        assert (
            int(
                session.scalar(
                    select(func.count())
                    .select_from(TaskActive)
                    .where(TaskActive.queue_id == int(child.id))
                )
                or 0
            )
            == 0
        )


def test_normalized_field_order_replays_same_fingerprint(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    with session_factory() as session:
        _seed_queue(session, name=queue_name)
        _seed_queue(session, name=f"{queue_name}.child")

    _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"n": 1},
        idempotency_key=f"idem-{uuid.uuid4().hex}",
    )
    claim = _claim_one(app, queue_name=queue_name)
    spawn_a = [
        {
            "queue_name": f"{queue_name}.child",
            "idempotency_key": "spawn-order-key",
            "payload": {"b": 2, "a": 1},
            "priority": 0,
        }
    ]
    spawn_b = [
        {
            "priority": 0,
            "payload": {"a": 1, "b": 2},
            "idempotency_key": "spawn-order-key",
            "queue_name": f"{queue_name}.child",
        }
    ]
    status1, headers1, body1 = _complete(
        app,
        claim_id=claim["claim_id"],
        claim_token=claim["claim_token"],
        generation=int(claim["generation"]),
        spawn=spawn_a,
    )
    first = _validate_any_complete(status=status1, headers=headers1, body=body1)
    assert first["replayed"] is False
    assert len(first["spawned_task_ids"]) == 1

    status2, headers2, body2 = _asgi_http_call(
        app,
        method="POST",
        path=_complete_path(claim["claim_id"]),
        headers=_worker_headers(claim_token=claim["claim_token"]),
        body=json.dumps(
            {"generation": int(claim["generation"]), "spawn": spawn_b},
            separators=(",", ":"),
        ).encode("utf-8"),
    )
    second = _validate_any_complete(status=status2, headers=headers2, body=body2)
    assert second["replayed"] is True
    assert second["spawned_task_ids"] == first["spawned_task_ids"]


def test_other_terminal_winner_not_misreported_as_replay(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    with session_factory() as session:
        _seed_queue(session, name=queue_name)

    task_id = UUID(
        _enqueue_ready(
            app,
            queue_name=queue_name,
            payload={"n": 1},
            idempotency_key=f"idem-{uuid.uuid4().hex}",
        )
    )
    claim = _claim_one(app, queue_name=queue_name)
    claim_id = UUID(claim["claim_id"])

    with session_factory() as session:
        now = session.scalar(select(func.transaction_timestamp()))
        assert now is not None
        session.add(
            CompleteReplay(
                claim_id=claim_id,
                operation_code=_OP_FAIL,
                request_fingerprint=b"\x11" * 32,
                task_id=task_id,
                result_state_code=11,
                available_at=None,
                terminal_at=now,
                spawned_task_ids=[],
                event_ids=[],
                created_at=now,
                expires_at=now + timedelta(days=7),
            )
        )
        session.commit()

    status, _hdrs, body = _complete(
        app,
        claim_id=claim["claim_id"],
        claim_token=claim["claim_token"],
        generation=int(claim["generation"]),
    )
    assert status == 409
    err = _assert_error(body, code="task_already_terminal", retryable=False)
    assert err["code"] == "task_already_terminal"
    assert "replayed" not in json.loads(body.decode("utf-8"))


def test_expired_replay_registry_returns_claim_not_found(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    with session_factory() as session:
        queue = _seed_queue(session, name=queue_name)

    task_id = UUID(
        _enqueue_ready(
            app,
            queue_name=queue_name,
            payload={"n": 1},
            idempotency_key=f"idem-{uuid.uuid4().hex}",
        )
    )
    claim = _claim_one(app, queue_name=queue_name)
    claim_id = UUID(claim["claim_id"])
    status1, headers1, body1 = _complete(
        app,
        claim_id=claim["claim_id"],
        claim_token=claim["claim_token"],
        generation=int(claim["generation"]),
    )
    _validate_complete_response(status=status1, headers=headers1, body=body1)

    with session_factory() as session:
        before = _world_counts(
            session, claim_id=claim_id, task_id=task_id, queue_id=int(queue.id)
        )
        now = session.scalar(select(func.transaction_timestamp()))
        assert now is not None
        session.execute(
            update(CompleteReplay)
            .where(CompleteReplay.claim_id == claim_id)
            .values(
                created_at=now - timedelta(days=8),
                expires_at=now - timedelta(days=1),
            )
        )
        session.commit()

    status2, _h2, body2 = _complete(
        app,
        claim_id=claim["claim_id"],
        claim_token=claim["claim_token"],
        generation=int(claim["generation"]),
    )
    assert status2 == 404
    _assert_error(body2, code="claim_not_found", retryable=False)

    with session_factory() as session:
        after = _world_counts(
            session, claim_id=claim_id, task_id=task_id, queue_id=int(queue.id)
        )
        assert after["terminals"] == before["terminals"]
        assert after["attempts"] == before["attempts"]
        assert after["active"] == 0


def test_replay_bypasses_drain_and_depth_gates(
    session_factory: sessionmaker[Session],
    authorizer: Authorizer,
    queue_name: str,
) -> None:
    with session_factory() as session:
        queue = _seed_queue(session, name=queue_name)

    # Use normal ceilings for intake/complete; saturation is applied after commit so
    # only the replay path is asked to bypass drain/depth admission.
    app = _build_app(session_factory=session_factory, authorizer=authorizer)
    task_id = UUID(
        _enqueue_ready(
            app,
            queue_name=queue_name,
            payload={"n": 1},
            idempotency_key=f"idem-{uuid.uuid4().hex}",
        )
    )
    claim = _claim_one(app, queue_name=queue_name)
    claim_id = UUID(claim["claim_id"])
    status1, headers1, body1 = _complete(
        app,
        claim_id=claim["claim_id"],
        claim_token=claim["claim_token"],
        generation=int(claim["generation"]),
    )
    first = _validate_complete_response(status=status1, headers=headers1, body=body1)
    assert first["replayed"] is False

    original_ready = 0
    original_leased = 0
    with session_factory() as session:
        _set_queue_state(session, name=queue_name, state=QueueState.DRAINING)
        counter = session.get(QueueCounter, int(queue.id))
        assert counter is not None
        # Saturate queue counters so a write-path admission check would fail; restore
        # after the replay assertion so sibling tests in the shared schema stay green.
        original_ready = int(counter.ready_count)
        original_leased = int(counter.leased_count)
        counter.ready_count = 10_000
        counter.leased_count = 10_000
        session.commit()
        before = _world_counts(
            session, claim_id=claim_id, task_id=task_id, queue_id=int(queue.id)
        )

    try:
        status2, headers2, body2 = _complete(
            app,
            claim_id=claim["claim_id"],
            claim_token=claim["claim_token"],
            generation=int(claim["generation"]),
        )
        second = _validate_complete_response(status=status2, headers=headers2, body=body2)
        assert second["replayed"] is True
        assert {k: second[k] for k in second if k != "replayed"} == {
            k: first[k] for k in first if k != "replayed"
        }

        with session_factory() as session:
            after = _world_counts(
                session, claim_id=claim_id, task_id=task_id, queue_id=int(queue.id)
            )
            assert after["replays"] == before["replays"] == 1
            assert after["terminals"] == before["terminals"]
    finally:
        with session_factory() as session:
            counter = session.get(QueueCounter, int(queue.id))
            assert counter is not None
            counter.ready_count = original_ready
            counter.leased_count = original_leased
            session.commit()


def test_replay_enforces_worker_auth_and_claim_token_header(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    with session_factory() as session:
        _seed_queue(session, name=queue_name)

    _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"n": 1},
        idempotency_key=f"idem-{uuid.uuid4().hex}",
    )
    claim = _claim_one(app, queue_name=queue_name)
    status1, headers1, body1 = _complete(
        app,
        claim_id=claim["claim_id"],
        claim_token=claim["claim_token"],
        generation=int(claim["generation"]),
    )
    _validate_complete_response(status=status1, headers=headers1, body=body1)

    status2, _h2, body2 = _asgi_http_call(
        app,
        method="POST",
        path=_complete_path(claim["claim_id"]),
        headers={
            "Authorization": f"Bearer {WORKER_TOKEN}",
            "Content-Type": "application/json",
        },
        body=json.dumps(
            _complete_body(generation=int(claim["generation"])),
            separators=(",", ":"),
        ).encode("utf-8"),
    )
    assert status2 in {400, 401, 403, 422}
    assert json.loads(body2.decode("utf-8")).get("replayed") is not True

    status3, _h3, body3 = _complete(
        app,
        claim_id=claim["claim_id"],
        claim_token=claim["claim_token"],
        generation=int(claim["generation"]),
        token=WORKER_OTHER_TOKEN,
    )
    assert status3 == 403
    _assert_error(body3, code="permission_denied", retryable=False)


def test_committed_complete_replay_bypasses_tightened_horizon(
    session_factory: sessionmaker[Session],
    authorizer: Authorizer,
    queue_name: str,
    target_queue_name: str,
) -> None:
    """Same Complete body replays after deployment horizon tightens; zero new writes."""
    from datetime import timedelta

    with session_factory() as session:
        store_now = session.scalar(select(func.transaction_timestamp()))
        assert store_now is not None
        future_at = (store_now + timedelta(hours=2)).isoformat().replace("+00:00", "Z")
    spawn_idempotency_key = "spawn-horizon-replay-scaffold"
    spawn_body = [
        {
            "queue_name": target_queue_name,
            "idempotency_key": spawn_idempotency_key,
            "payload": {"delayed": True},
            "priority": 0,
            "available_at": future_at,
        }
    ]

    with session_factory() as session:
        _seed_queue(session, name=queue_name)
        _seed_queue(session, name=target_queue_name)

    app = _build_app(session_factory=session_factory, authorizer=authorizer)
    source_id = UUID(
        _enqueue_ready(
            app,
            queue_name=queue_name,
            payload={"n": 1},
            idempotency_key=f"idem-{uuid.uuid4().hex}",
        )
    )
    claim = _claim_one(app, queue_name=queue_name)
    claim_id = UUID(claim["claim_id"])
    status1, headers1, body1 = _complete(
        app,
        claim_id=claim["claim_id"],
        claim_token=claim["claim_token"],
        generation=int(claim["generation"]),
        spawn=spawn_body,
    )
    first = _validate_any_complete(status=status1, headers=headers1, body=body1)
    assert first["replayed"] is False
    assert len(first["spawned_task_ids"]) == 1

    with session_factory() as session:
        before = _world_counts(
            session, claim_id=claim_id, task_id=source_id, queue_id=int(
                session.execute(
                    select(Queue).where(Queue.name == queue_name)
                ).scalar_one().id
            )
        )

    def _apply_tightened_horizon_stub(application: Any) -> None:
        from queue_service.scheduling import SchedulingPolicy

        service = getattr(application, "_queue_completion_service", None)
        assert service is not None
        service._repository._scheduling_policy = SchedulingPolicy(horizon_seconds=0)

    _apply_tightened_horizon_stub(app)

    status2, headers2, body2 = _complete(
        app,
        claim_id=claim["claim_id"],
        claim_token=claim["claim_token"],
        generation=int(claim["generation"]),
        spawn=spawn_body,
    )
    second = _validate_any_complete(status=status2, headers=headers2, body=body2)
    assert second["replayed"] is True
    assert {k: second[k] for k in second if k != "replayed"} == {
        k: first[k] for k in first if k != "replayed"
    }

    with session_factory() as session:
        after = _world_counts(
            session, claim_id=claim_id, task_id=source_id, queue_id=int(
                session.execute(
                    select(Queue).where(Queue.name == queue_name)
                ).scalar_one().id
            )
        )
        assert after["replays"] == before["replays"] == 1
        assert after["terminals"] == before["terminals"]
        assert after["attempts"] == before["attempts"]


def test_changed_spawn_available_at_conflicts_with_zero_writes(
    session_factory: sessionmaker[Session],
    authorizer: Authorizer,
    queue_name: str,
    target_queue_name: str,
) -> None:
    """Committed delayed spawn replay with only available_at changed conflicts."""
    with session_factory() as session:
        store_now = session.scalar(select(func.transaction_timestamp()))
        assert store_now is not None
        future_at = (store_now + timedelta(hours=2)).isoformat().replace("+00:00", "Z")
        shifted_at = (store_now + timedelta(hours=3)).isoformat().replace("+00:00", "Z")

    spawn_idempotency_key = f"spawn-delay-conflict-{uuid.uuid4().hex[:8]}"
    spawn_first = [
        {
            "queue_name": target_queue_name,
            "idempotency_key": spawn_idempotency_key,
            "payload": {"delayed": True},
            "priority": 0,
            "available_at": future_at,
        }
    ]
    spawn_second = [
        {
            "queue_name": target_queue_name,
            "idempotency_key": spawn_idempotency_key,
            "payload": {"delayed": True},
            "priority": 0,
            "available_at": shifted_at,
        }
    ]

    with session_factory() as session:
        queue = _seed_queue(session, name=queue_name)
        _seed_queue(session, name=target_queue_name)

    app = _build_app(session_factory=session_factory, authorizer=authorizer)
    source_id = UUID(
        _enqueue_ready(
            app,
            queue_name=queue_name,
            payload={"n": 1},
            idempotency_key=f"idem-{uuid.uuid4().hex}",
        )
    )
    claim = _claim_one(app, queue_name=queue_name)
    claim_id = UUID(claim["claim_id"])
    status1, headers1, body1 = _complete(
        app,
        claim_id=claim["claim_id"],
        claim_token=claim["claim_token"],
        generation=int(claim["generation"]),
        spawn=spawn_first,
    )
    first = _validate_any_complete(status=status1, headers=headers1, body=body1)
    assert first["replayed"] is False
    assert len(first["spawned_task_ids"]) == 1

    with session_factory() as session:
        before = _world_counts(
            session,
            claim_id=claim_id,
            task_id=source_id,
            queue_id=int(queue.id),
        )

    status2, _h2, body2 = _complete(
        app,
        claim_id=claim["claim_id"],
        claim_token=claim["claim_token"],
        generation=int(claim["generation"]),
        spawn=spawn_second,
    )
    assert status2 == 409
    _assert_error(body2, code="idempotency_conflict", retryable=False)

    with session_factory() as session:
        after = _world_counts(
            session,
            claim_id=claim_id,
            task_id=source_id,
            queue_id=int(queue.id),
        )
        assert after == before


# ---------------------------------------------------------------------------
# Phase 12 Wave 0 scaffolds (WORK-16 spawn priority replay identity)
# ---------------------------------------------------------------------------

_SPAWN_PRIORITY = 500
_SPAWN_PRIORITY_CHANGED = 750


def _spawn_with_priority(*, queue_name: str, priority: int) -> list[dict[str, Any]]:
    return [
        {
            "queue_name": queue_name,
            "idempotency_key": "spawn-priority-replay-key",
            "payload": {"branch": "priority-replay"},
            "priority": priority,
        }
    ]


def test_complete_spawn_same_priority_replays_without_new_writes(
    session_factory: sessionmaker[Session],
    authorizer: Authorizer,
    queue_name: str,
    target_queue_name: str,
) -> None:
    with session_factory() as session:
        queue = _seed_queue(session, name=queue_name)
        _seed_queue(session, name=target_queue_name)

    app = _build_app(session_factory=session_factory, authorizer=authorizer)
    source_id = UUID(
        _enqueue_ready(
            app,
            queue_name=queue_name,
            payload={"n": 1},
            idempotency_key=f"idem-{uuid.uuid4().hex}",
        )
    )
    claim = _claim_one(app, queue_name=queue_name)
    claim_id = UUID(claim["claim_id"])
    spawn_body = _spawn_with_priority(
        queue_name=target_queue_name,
        priority=_SPAWN_PRIORITY,
    )
    status1, headers1, body1 = _complete(
        app,
        claim_id=claim["claim_id"],
        claim_token=claim["claim_token"],
        generation=int(claim["generation"]),
        spawn=spawn_body,
    )
    first = _validate_any_complete(status=status1, headers=headers1, body=body1)
    assert first["replayed"] is False
    assert len(first["spawned_task_ids"]) == 1

    with session_factory() as session:
        before = _world_counts(
            session,
            claim_id=claim_id,
            task_id=source_id,
            queue_id=int(queue.id),
        )
        child = session.execute(
            select(TaskActive).where(TaskActive.task_id == UUID(first["spawned_task_ids"][0]))
        ).scalar_one()
        assert int(child.priority) == _SPAWN_PRIORITY

    status2, headers2, body2 = _complete(
        app,
        claim_id=claim["claim_id"],
        claim_token=claim["claim_token"],
        generation=int(claim["generation"]),
        spawn=spawn_body,
    )
    second = _validate_any_complete(status=status2, headers=headers2, body=body2)
    assert second["replayed"] is True
    assert second["spawned_task_ids"] == first["spawned_task_ids"]

    with session_factory() as session:
        after = _world_counts(
            session,
            claim_id=claim_id,
            task_id=source_id,
            queue_id=int(queue.id),
        )
        assert after == before


def test_complete_spawn_changed_priority_conflicts_with_zero_writes(
    session_factory: sessionmaker[Session],
    authorizer: Authorizer,
    queue_name: str,
    target_queue_name: str,
) -> None:
    with session_factory() as session:
        queue = _seed_queue(session, name=queue_name)
        _seed_queue(session, name=target_queue_name)

    app = _build_app(session_factory=session_factory, authorizer=authorizer)
    source_id = UUID(
        _enqueue_ready(
            app,
            queue_name=queue_name,
            payload={"n": 1},
            idempotency_key=f"idem-{uuid.uuid4().hex}",
        )
    )
    claim = _claim_one(app, queue_name=queue_name)
    claim_id = UUID(claim["claim_id"])
    spawn_first = _spawn_with_priority(
        queue_name=target_queue_name,
        priority=_SPAWN_PRIORITY,
    )
    status1, headers1, body1 = _complete(
        app,
        claim_id=claim["claim_id"],
        claim_token=claim["claim_token"],
        generation=int(claim["generation"]),
        spawn=spawn_first,
    )
    first = _validate_any_complete(status=status1, headers=headers1, body=body1)
    assert first["replayed"] is False

    with session_factory() as session:
        before = _world_counts(
            session,
            claim_id=claim_id,
            task_id=source_id,
            queue_id=int(queue.id),
        )

    spawn_changed = _spawn_with_priority(
        queue_name=target_queue_name,
        priority=_SPAWN_PRIORITY_CHANGED,
    )
    status2, _h2, body2 = _complete(
        app,
        claim_id=claim["claim_id"],
        claim_token=claim["claim_token"],
        generation=int(claim["generation"]),
        spawn=spawn_changed,
    )
    assert status2 == 409
    _assert_error(body2, code="idempotency_conflict", retryable=False)

    with session_factory() as session:
        after = _world_counts(
            session,
            claim_id=claim_id,
            task_id=source_id,
            queue_id=int(queue.id),
        )
        assert after == before
        child = session.execute(
            select(TaskActive).where(TaskActive.task_id == UUID(first["spawned_task_ids"][0]))
        ).scalar_one()
        assert int(child.priority) == _SPAWN_PRIORITY
