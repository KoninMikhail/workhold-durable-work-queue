"""Black-box Complete-with-spawn (Phase 03.7-02 / COMP-01 + API-04 lineage).

Proves ordered spawn[] Work Queue descendants commit with the source Complete
transaction, respect active/paused/draining internal-spawn gates, and roll back
with the source when admission or registry uniqueness fails. No delivery events.

Phase 12 Wave 0 priority scaffolds are temporarily skipped; Plans 04 and 06
remove the markers.
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from collections.abc import Iterator, Mapping
from datetime import datetime
from typing import Any
from uuid import UUID

import pytest
from sqlalchemy import create_engine, event, func, select
from sqlalchemy.orm import Session, sessionmaker

from workhold.api.application import create_application_app
from workhold.api.schemas.terminal import parse_complete_command
from workhold.api.security import ListenerBind
from workhold.application.claim_service import ClaimService
from workhold.application.completion import CompletionFaultHooks, CompletionService
from workhold.application.lease_service import LeaseService
from workhold.domain.queue_control import (
    ActivatePolicyMutation,
    AdminRequestMetadata,
    BackoffStrategy,
    ConfigVersion,
    CreatePolicyMutation,
    CreateQueueMutation,
    PolicyVersion,
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
    CompletionEffect,
    DeliveryEventActive,
    Queue,
    QueueCounter,
    QueuePolicyVersion,
    TaskActive,
    TaskAttempt,
    TaskPayloadActive,
    TaskTerminal,
)

PRODUCER_TOKEN = "tok-producer-complete-spawn"
WORKER_TOKEN = "tok-worker-complete-spawn"
ADMIN_TOKEN = "tok-admin-complete-spawn"

PRODUCER_PRINCIPAL = "producer-complete-spawn"
WORKER_PRINCIPAL = "worker-complete-spawn"
ADMIN_PRINCIPAL = "admin-complete-spawn"

BASE_QUEUE_NAME = "orders.spawn"
TARGET_QUEUE = "billing.spawn"
CLAIM_PATH = "/v1/claims"
CLAIM_TOKEN_HEADER = "X-Queue-Claim-Token"
REPLICA_ID = "pool-spawn/replica-1"

_EFFECT_KIND_SPAWN = 1
_OUTCOME_ACTIVE = 1
_OUTCOME_SUCCEEDED = 2
_STATE_DELAYED = 1
_STATE_READY = 2
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
    return _unique_queue_name(TARGET_QUEUE)


@pytest.fixture
def authorizer(queue_name: str, target_queue_name: str) -> Authorizer:
    return Authorizer(
        queue_scopes={
            PRODUCER_PRINCIPAL: frozenset(
                {queue_name, target_queue_name, BASE_QUEUE_NAME, TARGET_QUEUE}
            ),
            WORKER_PRINCIPAL: frozenset({queue_name, target_queue_name, BASE_QUEUE_NAME}),
            ADMIN_PRINCIPAL: frozenset({queue_name, target_queue_name, BASE_QUEUE_NAME}),
        }
    )


@pytest.fixture
def session_factory(migrated_schema) -> Iterator[sessionmaker[Session]]:
    _conn, schema = migrated_schema
    database_url = os.environ.get("TEST_DATABASE_URL", "").strip()
    if not database_url:
        pytest.fail("TEST_DATABASE_URL is required for complete spawn integration")
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
        bind=ListenerBind(host="127.0.0.1", port=18108),
        session_factory=session_factory,
        enqueue_service=enqueue_service,
        claim_service=ClaimService(session_factory=session_factory),
        lease_service=LeaseService(session_factory=session_factory),
        completion_service=CompletionService(
            session_factory=session_factory,
            depth_ceilings=DepthCeilings(
                queue_active_depth=100,
                instance_active_depth=500,
                retry_after_ms=250,
            ),
        ),
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
        "server": ("127.0.0.1", 18108),
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


def _enqueue_ready(
    app: Any,
    *,
    queue_name: str,
    payload: Any,
    idempotency_key: str,
    priority: int = 0,
) -> str:
    path = f"/v1/queues/{queue_name}/tasks"
    body = json.dumps(
        {"payload": payload, "priority": priority},
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


def _worker_headers(*, claim_token: str | None = None) -> dict[str, str]:
    headers = {
        "Authorization": f"Bearer {WORKER_TOKEN}",
        "Content-Type": "application/json",
    }
    if claim_token is not None:
        headers[CLAIM_TOKEN_HEADER] = claim_token
    return headers


def _complete_path(claim_id: str) -> str:
    return f"/v1/claims/{claim_id}:complete"


def _spawn_item(
    *,
    queue_name: str,
    payload: Any,
    idempotency_key: str | None = None,
    priority: int = 0,
    available_at: Any = None,
    include_available_at: bool = False,
) -> dict[str, Any]:
    item: dict[str, Any] = {
        "queue_name": queue_name,
        "idempotency_key": idempotency_key or f"spawn-{uuid.uuid4().hex}",
        "payload": payload,
        "priority": priority,
    }
    if include_available_at or available_at is not None:
        item["available_at"] = available_at
    return item


def _assert_error(body: bytes, *, code: str, retryable: bool) -> dict[str, Any]:
    payload = json.loads(body.decode("utf-8"))
    assert payload["code"] == code
    assert payload["retryable"] is retryable
    return payload


def _parse_rfc3339(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)


def _effects_for_claim(session: Session, *, claim_id: UUID) -> list[CompletionEffect]:
    rows = session.execute(
        select(CompletionEffect)
        .where(CompletionEffect.source_claim_id == claim_id)
        .order_by(CompletionEffect.ordinal)
    ).scalars().all()
    return list(rows)


def _assert_spawn_lineage(
    session: Session,
    *,
    source_task_id: UUID,
    claim_id: UUID,
    spawned_task_ids: list[str],
) -> None:
    effects = _effects_for_claim(session, claim_id=claim_id)
    assert len(effects) == len(spawned_task_ids)
    replay = session.execute(
        select(CompleteReplay).where(CompleteReplay.claim_id == claim_id)
    ).scalar_one()
    assert list(replay.spawned_task_ids) == [UUID(x) for x in spawned_task_ids]
    assert list(replay.event_ids or []) == []

    for ordinal, task_id_str in enumerate(spawned_task_ids):
        effect = effects[ordinal]
        assert int(effect.effect_kind_code) == _EFFECT_KIND_SPAWN
        assert int(effect.ordinal) == ordinal
        assert effect.resource_id == UUID(task_id_str)
        assert effect.source_claim_id == claim_id

        child = session.execute(
            select(TaskActive).where(TaskActive.task_id == UUID(task_id_str))
        ).scalar_one()
        assert child.source_task_id == source_task_id
        assert int(child.spawn_ordinal) == ordinal
        assert int(child.priority) == 0
        payload = session.get(TaskPayloadActive, child.id)
        assert payload is not None


@pytest.mark.parametrize("count", [0, 1, 64])
def test_complete_commits_ordered_spawns_with_registry_and_replay(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
    target_queue_name: str,
    count: int,
) -> None:
    with session_factory() as session:
        _seed_queue(session, name=queue_name)
        _seed_queue(session, name=target_queue_name)

    source_id = _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"kind": "source"},
        idempotency_key=f"idem-{uuid.uuid4().hex}",
    )
    claim = _claim_one(app, queue_name=queue_name)
    spawn = [
        _spawn_item(
            queue_name=target_queue_name,
            payload={"i": i},
            idempotency_key=f"spawn-{i}",
        )
        for i in range(count)
    ]
    # Duplicate target queue is valid when count > 1.
    if count >= 2:
        spawn[1]["queue_name"] = target_queue_name

    status, headers, body = _asgi_http_call(
        app,
        method="POST",
        path=_complete_path(claim["claim_id"]),
        headers=_worker_headers(claim_token=claim["claim_token"]),
        body=json.dumps(
            {"generation": claim["generation"], "spawn": spawn},
            separators=(",", ":"),
        ).encode("utf-8"),
    )
    assert status == 200, body.decode("utf-8", errors="replace")
    assert "x-request-id" in {k.lower() for k in headers}
    result = json.loads(body.decode("utf-8"))
    assert result["state"] == "succeeded"
    assert result["task_id"] == source_id
    assert result["replayed"] is False
    assert len(result["spawned_task_ids"]) == count
    assert "events" not in result

    with session_factory() as session:
        _assert_spawn_lineage(
            session,
            source_task_id=UUID(source_id),
            claim_id=UUID(claim["claim_id"]),
            spawned_task_ids=result["spawned_task_ids"],
        )
        assert (
            session.scalar(
                select(func.count())
                .select_from(DeliveryEventActive)
                .where(DeliveryEventActive.source_task_id == UUID(source_id))
            )
            or 0
        ) == 0
        # No event-kind completion_effects rows.
        event_kind = session.scalar(
            select(func.count())
            .select_from(CompletionEffect)
            .where(
                CompletionEffect.source_claim_id == UUID(claim["claim_id"]),
                CompletionEffect.effect_kind_code == 2,
            )
        )
        assert int(event_kind or 0) == 0


@pytest.mark.parametrize("state", [QueueState.ACTIVE, QueueState.PAUSED, QueueState.DRAINING])
def test_internal_spawn_accepted_for_target_queue_states(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
    target_queue_name: str,
    state: QueueState,
) -> None:
    with session_factory() as session:
        _seed_queue(session, name=queue_name)
        _seed_queue(session, name=target_queue_name)
        if state is not QueueState.ACTIVE:
            _set_queue_state(session, name=target_queue_name, state=state)

    source_id = _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"kind": "source"},
        idempotency_key=f"idem-{uuid.uuid4().hex}",
    )
    claim = _claim_one(app, queue_name=queue_name)
    status, _hdrs, body = _asgi_http_call(
        app,
        method="POST",
        path=_complete_path(claim["claim_id"]),
        headers=_worker_headers(claim_token=claim["claim_token"]),
        body=json.dumps(
            {
                "generation": claim["generation"],
                "spawn": [
                    _spawn_item(queue_name=target_queue_name, payload={"state": state.value})
                ],
            },
            separators=(",", ":"),
        ).encode("utf-8"),
    )
    assert status == 200, body.decode("utf-8", errors="replace")
    result = json.loads(body.decode("utf-8"))
    assert len(result["spawned_task_ids"]) == 1

    with session_factory() as session:
        _assert_spawn_lineage(
            session,
            source_task_id=UUID(source_id),
            claim_id=UUID(claim["claim_id"]),
            spawned_task_ids=result["spawned_task_ids"],
        )
        if state is QueueState.DRAINING:
            # External enqueue still rejected while spawn succeeded.
            path = f"/v1/queues/{target_queue_name}/tasks"
            enqueue_status, _eh, ebody = _asgi_http_call(
                app,
                method="POST",
                path=path,
                headers={
                    "Authorization": f"Bearer {PRODUCER_TOKEN}",
                    "Content-Type": "application/json",
                    "Idempotency-Key": f"ext-{uuid.uuid4().hex}",
                },
                body=json.dumps(
                    {"payload": {"x": 1}, "priority": 0}, separators=(",", ":")
                ).encode("utf-8"),
            )
            assert enqueue_status in (409, 503, 429, 400)
            err = json.loads(ebody.decode("utf-8"))
            assert err["code"] == "queue_draining"


def test_rejected_spawn_leaves_source_leased_with_zero_descendants(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    with session_factory() as session:
        _seed_queue(session, name=queue_name)

    source_id = _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"kind": "source"},
        idempotency_key=f"idem-{uuid.uuid4().hex}",
    )
    claim = _claim_one(app, queue_name=queue_name)
    claim_id = UUID(claim["claim_id"])

    with session_factory() as before:
        leased_before = before.execute(
            select(TaskActive).where(TaskActive.task_id == UUID(source_id))
        ).scalar_one()
        assert int(leased_before.state_code) == _STATE_LEASED
        assert leased_before.current_claim_id == claim_id

    # Unknown target queue.
    status, _hdrs, body = _asgi_http_call(
        app,
        method="POST",
        path=_complete_path(claim["claim_id"]),
        headers=_worker_headers(claim_token=claim["claim_token"]),
        body=json.dumps(
            {
                "generation": claim["generation"],
                "spawn": [
                    _spawn_item(
                        queue_name=f"missing.{uuid.uuid4().hex[:8]}",
                        payload={"x": 1},
                    )
                ],
            },
            separators=(",", ":"),
        ).encode("utf-8"),
    )
    assert status == 404
    _assert_error(body, code="queue_not_found", retryable=False)

    with session_factory() as after:
        still = after.execute(
            select(TaskActive).where(TaskActive.task_id == UUID(source_id))
        ).scalar_one()
        assert int(still.state_code) == _STATE_LEASED
        assert still.current_claim_id == claim_id
        assert (
            after.execute(
                select(TaskTerminal).where(TaskTerminal.task_id == UUID(source_id))
            ).scalar_one_or_none()
            is None
        )
        assert (
            after.execute(
                select(CompleteReplay).where(CompleteReplay.claim_id == claim_id)
            ).scalar_one_or_none()
            is None
        )
        assert _effects_for_claim(after, claim_id=claim_id) == []
        attempt = after.execute(
            select(TaskAttempt).where(
                TaskAttempt.task_id == UUID(source_id),
                TaskAttempt.claim_id == claim_id,
            )
        ).scalar_one()
        assert int(attempt.outcome_code) == _OUTCOME_ACTIVE
        assert (
            after.execute(
                select(ClaimRegistry).where(ClaimRegistry.claim_id == claim_id)
            ).scalar_one_or_none()
            is not None
        )


def test_hard_admission_rejects_without_partial_effects(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
    target_queue_name: str,
) -> None:
    with session_factory() as session:
        _seed_queue(session, name=queue_name)
        _seed_queue(session, name=target_queue_name)

    source_id = _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"kind": "source"},
        idempotency_key=f"idem-{uuid.uuid4().hex}",
    )
    claim = _claim_one(app, queue_name=queue_name)

    # 65 items exceeds maxItems=64.
    spawn_65 = [
        _spawn_item(queue_name=target_queue_name, payload={"i": i}, idempotency_key=f"k{i}")
        for i in range(65)
    ]
    status, _hdrs, body = _asgi_http_call(
        app,
        method="POST",
        path=_complete_path(claim["claim_id"]),
        headers=_worker_headers(claim_token=claim["claim_token"]),
        body=json.dumps(
            {"generation": claim["generation"], "spawn": spawn_65},
            separators=(",", ":"),
        ).encode("utf-8"),
    )
    assert status == 400
    err = _assert_error(body, code="validation_failed", retryable=False)
    assert "exceeds hard item ceiling" in err["message"]
    # Parse-level details carry the ceiling before HTTP diagnostic allowlisting.
    with pytest.raises(Exception) as excinfo:
        parse_complete_command({"generation": 1, "spawn": spawn_65})
    assert excinfo.value.details["limit"] == 64  # type: ignore[attr-defined]

    # Out-of-range priority (in-range non-zero is valid at request boundary).
    status, _hdrs, body = _asgi_http_call(
        app,
        method="POST",
        path=_complete_path(claim["claim_id"]),
        headers=_worker_headers(claim_token=claim["claim_token"]),
        body=json.dumps(
            {
                "generation": claim["generation"],
                "spawn": [
                    _spawn_item(
                        queue_name=target_queue_name,
                        payload={"x": 1},
                        priority=PRIORITY_MAX + 1,
                    )
                ],
            },
            separators=(",", ":"),
        ).encode("utf-8"),
    )
    assert status == 400
    _assert_error(body, code="validation_failed", retryable=False)

    # Unsupported future available_at.
    status, _hdrs, body = _asgi_http_call(
        app,
        method="POST",
        path=_complete_path(claim["claim_id"]),
        headers=_worker_headers(claim_token=claim["claim_token"]),
        body=json.dumps(
            {
                "generation": claim["generation"],
                "spawn": [
                    _spawn_item(
                        queue_name=target_queue_name,
                        payload={"x": 1},
                        available_at="2099-01-01T00:00:00Z",
                        include_available_at=True,
                    )
                ],
            },
            separators=(",", ":"),
        ).encode("utf-8"),
    )
    assert status == 400
    _assert_error(body, code="validation_failed", retryable=False)

    with session_factory() as session:
        still = session.execute(
            select(TaskActive).where(TaskActive.task_id == UUID(source_id))
        ).scalar_one()
        assert int(still.state_code) == _STATE_LEASED
        assert _effects_for_claim(session, claim_id=UUID(claim["claim_id"])) == []


def test_depth_overflow_rolls_back_source_and_spawns(
    session_factory: sessionmaker[Session],
    authorizer: Authorizer,
    queue_name: str,
    target_queue_name: str,
) -> None:
    with session_factory() as session:
        _seed_queue(session, name=queue_name)
        _seed_queue(session, name=target_queue_name)

    tight = DepthCeilings(
        queue_active_depth=1,
        instance_active_depth=500,
        retry_after_ms=100,
    )
    app = create_application_app(
        authenticator=BearerCredentialAuthenticator.from_bindings(_bindings()),
        authorizer=authorizer,
        bind=ListenerBind(host="127.0.0.1", port=18109),
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
        completion_service=CompletionService(
            session_factory=session_factory,
            depth_ceilings=tight,
        ),
    )
    # Fill target queue depth to ceiling before complete.
    _enqueue_ready(
        app,
        queue_name=target_queue_name,
        payload={"fill": True},
        idempotency_key=f"fill-{uuid.uuid4().hex}",
    )
    source_id = _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"kind": "source"},
        idempotency_key=f"idem-{uuid.uuid4().hex}",
    )
    claim = _claim_one(app, queue_name=queue_name)
    status, _hdrs, body = _asgi_http_call(
        app,
        method="POST",
        path=_complete_path(claim["claim_id"]),
        headers=_worker_headers(claim_token=claim["claim_token"]),
        body=json.dumps(
            {
                "generation": claim["generation"],
                "spawn": [_spawn_item(queue_name=target_queue_name, payload={"n": 1})],
            },
            separators=(",", ":"),
        ).encode("utf-8"),
    )
    assert status in (429, 503, 400)
    err = json.loads(body.decode("utf-8"))
    assert err["code"] == "resource_exhausted"
    assert err["retryable"] is True

    with session_factory() as session:
        still = session.execute(
            select(TaskActive).where(TaskActive.task_id == UUID(source_id))
        ).scalar_one()
        assert int(still.state_code) == _STATE_LEASED
        assert _effects_for_claim(session, claim_id=UUID(claim["claim_id"])) == []
        assert (
            session.execute(
                select(CompleteReplay).where(
                    CompleteReplay.claim_id == UUID(claim["claim_id"])
                )
            ).scalar_one_or_none()
            is None
        )


def test_policy_snapshot_uses_target_active_version(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
    target_queue_name: str,
) -> None:
    with session_factory() as session:
        _seed_queue(session, name=queue_name)
        target = _seed_queue(session, name=target_queue_name)
        repo = QueueControlRepository()
        created = repo.create_policy_version(
            session,
            queue_name=target_queue_name,
            mutation=CreatePolicyMutation(
                policy=RetryPolicyDraft(
                    enabled=True,
                    max_attempts=7,
                    backoff_strategy=BackoffStrategy.FIXED,
                    retry_delay_seconds=0,
                ),
                metadata=_admin_meta(),
            ),
        )
        assert created.active_policy.version.value == 1
        session.flush()
        fresh = session.execute(
            select(Queue).where(Queue.name == target_queue_name)
        ).scalar_one()
        repo.activate_policy_version(
            session,
            queue_name=target_queue_name,
            mutation=ActivatePolicyMutation(
                expected_config_version=ConfigVersion(value=int(fresh.config_version)),
                policy_version=PolicyVersion(value=2),
                metadata=_admin_meta(),
            ),
        )
        session.commit()
        _ = target

    source_id = _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"kind": "source"},
        idempotency_key=f"idem-{uuid.uuid4().hex}",
    )
    claim = _claim_one(app, queue_name=queue_name)
    status, _hdrs, body = _asgi_http_call(
        app,
        method="POST",
        path=_complete_path(claim["claim_id"]),
        headers=_worker_headers(claim_token=claim["claim_token"]),
        body=json.dumps(
            {
                "generation": claim["generation"],
                "spawn": [_spawn_item(queue_name=target_queue_name, payload={"p": 1})],
            },
            separators=(",", ":"),
        ).encode("utf-8"),
    )
    assert status == 200, body.decode("utf-8", errors="replace")
    spawned = json.loads(body.decode("utf-8"))["spawned_task_ids"][0]

    with session_factory() as session:
        child = session.execute(
            select(TaskActive).where(TaskActive.task_id == UUID(spawned))
        ).scalar_one()
        queue = session.execute(
            select(Queue).where(Queue.name == target_queue_name)
        ).scalar_one()
        assert int(child.retry_policy_version_id) == int(queue.active_policy_version_id)
        policy = session.get(QueuePolicyVersion, child.retry_policy_version_id)
        assert policy is not None
        assert int(policy.max_attempts) == 7
        _assert_spawn_lineage(
            session,
            source_task_id=UUID(source_id),
            claim_id=UUID(claim["claim_id"]),
            spawned_task_ids=[spawned],
        )


def test_registry_conflict_and_injected_failure_roll_back(
    session_factory: sessionmaker[Session],
    authorizer: Authorizer,
    queue_name: str,
    target_queue_name: str,
) -> None:
    with session_factory() as session:
        _seed_queue(session, name=queue_name)
        _seed_queue(session, name=target_queue_name)

    app = create_application_app(
        authenticator=BearerCredentialAuthenticator.from_bindings(_bindings()),
        authorizer=authorizer,
        bind=ListenerBind(host="127.0.0.1", port=18110),
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
    source_id = _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"kind": "source"},
        idempotency_key=f"idem-{uuid.uuid4().hex}",
    )
    claim = _claim_one(app, queue_name=queue_name)
    claim_id = UUID(claim["claim_id"])
    claim_token = UUID(claim["claim_token"])
    generation = int(claim["generation"])
    command = parse_complete_command(
        {
            "generation": generation,
            "spawn": [_spawn_item(queue_name=target_queue_name, payload={"x": 1})],
        }
    )

    # Pre-insert conflicting completion_effects row for this claim ordinal 0.
    with session_factory() as session:
        session.add(
            CompletionEffect(
                source_claim_id=claim_id,
                effect_kind_code=_EFFECT_KIND_SPAWN,
                ordinal=0,
                resource_id=uuid.uuid4(),
            )
        )
        session.commit()

    service = CompletionService(session_factory=session_factory)
    with pytest.raises(Exception):
        service.complete(
            claim_id=claim_id,
            claim_token=claim_token,
            command=command,
            authorize_queue=lambda _q: True,
        )

    with session_factory() as session:
        still = session.execute(
            select(TaskActive).where(TaskActive.task_id == UUID(source_id))
        ).scalar_one()
        assert int(still.state_code) == _STATE_LEASED
        assert (
            session.execute(
                select(CompleteReplay).where(CompleteReplay.claim_id == claim_id)
            ).scalar_one_or_none()
            is None
        )
        # Only the pre-seeded conflicting effect remains (resource_id not a real task).
        effects = _effects_for_claim(session, claim_id=claim_id)
        assert len(effects) == 1
        assert (
            session.execute(
                select(TaskActive).where(TaskActive.source_task_id == UUID(source_id))
            ).scalars().all()
            == []
        )

    # Injected failure after spawn effect rolls back everything including the conflict
    # seed for a fresh claim.
    source2 = _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"kind": "source2"},
        idempotency_key=f"idem-{uuid.uuid4().hex}",
    )
    claim2 = _claim_one(app, queue_name=queue_name)
    command2 = parse_complete_command(
        {
            "generation": int(claim2["generation"]),
            "spawn": [_spawn_item(queue_name=target_queue_name, payload={"y": 2})],
        }
    )

    def _boom() -> None:
        raise RuntimeError("injected after spawn effect")

    failing = CompletionService(
        session_factory=session_factory,
        fault_hooks=CompletionFaultHooks(after_spawn_effect=_boom),
    )
    with pytest.raises(RuntimeError, match="injected after spawn effect"):
        failing.complete(
            claim_id=UUID(claim2["claim_id"]),
            claim_token=UUID(claim2["claim_token"]),
            command=command2,
            authorize_queue=lambda _q: True,
        )

    with session_factory() as session:
        still2 = session.execute(
            select(TaskActive).where(TaskActive.task_id == UUID(source2))
        ).scalar_one()
        assert int(still2.state_code) == _STATE_LEASED
        assert _effects_for_claim(session, claim_id=UUID(claim2["claim_id"])) == []
        assert (
            session.execute(
                select(TaskActive).where(TaskActive.source_task_id == UUID(source2))
            ).scalars().all()
            == []
        )


def test_mixed_immediate_and_delayed_spawn_commits_atomically_with_exact_counters(
    session_factory: sessionmaker[Session],
    authorizer: Authorizer,
    queue_name: str,
    target_queue_name: str,
) -> None:
    """Omitted/past children ready; in-horizon future child delayed; counters match."""
    from datetime import timedelta

    with session_factory() as session:
        source_queue = _seed_queue(session, name=queue_name)
        target_queue = _seed_queue(session, name=target_queue_name)
        store_now = session.scalar(select(func.transaction_timestamp()))
        assert store_now is not None
        future_at = (store_now + timedelta(hours=2)).isoformat().replace("+00:00", "Z")
        past_at = (store_now - timedelta(minutes=5)).isoformat().replace("+00:00", "Z")

    app = create_application_app(
        authenticator=BearerCredentialAuthenticator.from_bindings(_bindings()),
        authorizer=authorizer,
        bind=ListenerBind(host="127.0.0.1", port=18110),
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
    source_id = _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"kind": "source"},
        idempotency_key=f"idem-{uuid.uuid4().hex}",
    )
    claim = _claim_one(app, queue_name=queue_name)
    spawn = [
        _spawn_item(queue_name=target_queue_name, payload={"branch": "immediate"}),
        _spawn_item(
            queue_name=target_queue_name,
            payload={"branch": "past"},
            available_at=past_at,
            include_available_at=True,
        ),
        _spawn_item(
            queue_name=target_queue_name,
            payload={"branch": "delayed"},
            available_at=future_at,
            include_available_at=True,
        ),
    ]
    status, _hdrs, body = _asgi_http_call(
        app,
        method="POST",
        path=_complete_path(claim["claim_id"]),
        headers=_worker_headers(claim_token=claim["claim_token"]),
        body=json.dumps(
            {"generation": claim["generation"], "spawn": spawn},
            separators=(",", ":"),
        ).encode("utf-8"),
    )
    assert status == 200, body.decode("utf-8", errors="replace")
    payload = json.loads(body.decode("utf-8"))
    spawned_ids = payload["spawned_task_ids"]
    assert len(spawned_ids) == 3

    with session_factory() as session:
        children = list(
            session.scalars(
                select(TaskActive).where(
                    TaskActive.source_task_id == UUID(source_id)
                )
            )
        )
        assert len(children) == 3
        states = {int(c.state_code) for c in children}
        assert _STATE_READY in states
        assert _STATE_DELAYED in states
        counter = session.get(QueueCounter, int(target_queue.id))
        assert counter is not None
        assert int(counter.ready_count) == 2
        assert int(counter.delayed_count) == 1
        assert int(counter.leased_count) == 0
        replay = session.execute(
            select(CompleteReplay).where(
                CompleteReplay.claim_id == UUID(claim["claim_id"])
            )
        ).scalar_one()
        complete_tx_at = replay.terminal_at
        assert complete_tx_at is not None
        expected_past = _parse_rfc3339(past_at)
        expected_future = _parse_rfc3339(future_at)
        by_ordinal = sorted(children, key=lambda c: int(c.spawn_ordinal))
        immediate = by_ordinal[0]
        past_child = by_ordinal[1]
        delayed_child = by_ordinal[2]
        assert int(immediate.state_code) == _STATE_READY
        assert abs((immediate.available_at - complete_tx_at).total_seconds()) < 1.0
        assert int(past_child.state_code) == _STATE_READY
        assert abs((past_child.available_at - expected_past).total_seconds()) < 1.0
        assert int(delayed_child.state_code) == _STATE_DELAYED
        assert abs((delayed_child.available_at - expected_future).total_seconds()) < 1.0
        _assert_spawn_lineage(
            session,
            source_task_id=UUID(source_id),
            claim_id=UUID(claim["claim_id"]),
            spawned_task_ids=spawned_ids,
        )


@pytest.mark.parametrize(
    "bad_available_at",
    [
        pytest.param("over_horizon", id="over_horizon"),
        pytest.param("naive", id="naive"),
    ],
)
def test_mixed_spawn_batch_invalid_horizon_child_rolls_back_all(
    session_factory: sessionmaker[Session],
    authorizer: Authorizer,
    queue_name: str,
    target_queue_name: str,
    bad_available_at: str,
) -> None:
    """One over-horizon or naive child aborts Complete with zero counter/descendant delta."""
    from datetime import timedelta

    with session_factory() as session:
        _seed_queue(session, name=queue_name)
        target = _seed_queue(session, name=target_queue_name)
        store_now = session.scalar(select(func.transaction_timestamp()))
        assert store_now is not None
        if bad_available_at == "over_horizon":
            bad_at = (store_now + timedelta(seconds=86401)).isoformat().replace(
                "+00:00",
                "Z",
            )
        else:
            bad_at = "2026-09-18T12:00:00"
        before_counter = session.get(QueueCounter, int(target.id))
        assert before_counter is not None
        ready_before = int(before_counter.ready_count)
        delayed_before = int(before_counter.delayed_count)
        leased_before = int(before_counter.leased_count)

    app = create_application_app(
        authenticator=BearerCredentialAuthenticator.from_bindings(_bindings()),
        authorizer=authorizer,
        bind=ListenerBind(host="127.0.0.1", port=18111),
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
    source_id = _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"kind": "source"},
        idempotency_key=f"idem-{uuid.uuid4().hex}",
    )
    claim = _claim_one(app, queue_name=queue_name)
    status, _hdrs, body = _asgi_http_call(
        app,
        method="POST",
        path=_complete_path(claim["claim_id"]),
        headers=_worker_headers(claim_token=claim["claim_token"]),
        body=json.dumps(
            {
                "generation": claim["generation"],
                "spawn": [
                    _spawn_item(
                        queue_name=target_queue_name,
                        payload={"ok": True},
                    ),
                    _spawn_item(
                        queue_name=target_queue_name,
                        payload={"bad": True},
                        available_at=bad_at,
                        include_available_at=True,
                    ),
                ],
            },
            separators=(",", ":"),
        ).encode("utf-8"),
    )
    assert status == 400
    _assert_error(body, code="validation_failed", retryable=False)

    with session_factory() as session:
        still = session.execute(
            select(TaskActive).where(TaskActive.task_id == UUID(source_id))
        ).scalar_one()
        assert int(still.state_code) == _STATE_LEASED
        assert (
            session.execute(
                select(TaskActive).where(TaskActive.source_task_id == UUID(source_id))
            ).scalars().all()
            == []
        )
        assert _effects_for_claim(session, claim_id=UUID(claim["claim_id"])) == []
        counter = session.get(QueueCounter, int(target.id))
        assert counter is not None
        assert int(counter.ready_count) == ready_before
        assert int(counter.delayed_count) == delayed_before
        assert int(counter.leased_count) == leased_before


def test_parse_rejects_oversized_spawn_fanout_bytes() -> None:
    # Combined spawn payloads above 512 KiB fail closed before persistence.
    big = "x" * (256 * 1024)
    spawn = [
        {
            "queue_name": "a.q",
            "idempotency_key": "k1",
            "payload": {"b": big},
            "priority": 0,
        },
        {
            "queue_name": "a.q",
            "idempotency_key": "k2",
            "payload": {"b": big},
            "priority": 0,
        },
        {
            "queue_name": "a.q",
            "idempotency_key": "k3",
            "payload": {"b": big},
            "priority": 0,
        },
    ]
    with pytest.raises(Exception) as excinfo:
        parse_complete_command({"generation": 1, "spawn": spawn})
    assert excinfo.value.code == "payload_too_large"  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Phase 12 Wave 0 scaffolds (WORK-16 bounded priority)
# ---------------------------------------------------------------------------

_PRIORITY_MIN = -32768
_PRIORITY_MAX = 32767


def _spawn_parse_item(*, priority: int) -> dict[str, Any]:
    return {
        "queue_name": "orders.spawn.priority",
        "idempotency_key": "spawn-priority-parse",
        "payload": {"branch": "priority"},
        "priority": priority,
    }


@pytest.mark.parametrize("priority", [_PRIORITY_MIN, _PRIORITY_MAX, 100, -50])
def test_parse_spawn_accepts_bounded_priority_at_index(priority: int) -> None:
    command = parse_complete_command(
        {"generation": 1, "spawn": [_spawn_parse_item(priority=priority)]}
    )
    assert len(command.spawn) == 1
    item = command.spawn[0]
    assert item.priority == priority
    assert item.canonical["priority"] == priority


@pytest.mark.parametrize(
    "priority",
    [True, "1", _PRIORITY_MAX + 1, _PRIORITY_MIN - 1],
)
def test_parse_spawn_rejects_invalid_priority_with_index_metadata(
    priority: object,
) -> None:
    with pytest.raises(Exception) as exc_info:
        parse_complete_command(
            {
                "generation": 1,
                "spawn": [
                    {
                        "queue_name": "orders.spawn.priority",
                        "idempotency_key": "spawn-invalid",
                        "payload": {"x": 1},
                        "priority": priority,
                    }
                ],
            }
        )
    err = exc_info.value
    assert err.code == "validation_failed"  # type: ignore[attr-defined]
    assert err.details.get("index") == 0  # type: ignore[attr-defined]


def test_spawn_persists_exact_non_zero_priority_at_endpoints(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
    target_queue_name: str,
) -> None:
    with session_factory() as session:
        _seed_queue(session, name=queue_name)
        _seed_queue(session, name=target_queue_name)

    source_id = _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"kind": "source"},
        idempotency_key=f"idem-{uuid.uuid4().hex}",
    )
    claim = _claim_one(app, queue_name=queue_name)
    spawn = [
        _spawn_item(
            queue_name=target_queue_name,
            payload={"endpoint": "min"},
            priority=_PRIORITY_MIN,
            idempotency_key=f"spawn-min-{uuid.uuid4().hex[:8]}",
        ),
        _spawn_item(
            queue_name=target_queue_name,
            payload={"endpoint": "max"},
            priority=_PRIORITY_MAX,
            idempotency_key=f"spawn-max-{uuid.uuid4().hex[:8]}",
        ),
    ]
    status, _hdrs, body = _asgi_http_call(
        app,
        method="POST",
        path=_complete_path(claim["claim_id"]),
        headers=_worker_headers(claim_token=claim["claim_token"]),
        body=json.dumps(
            {"generation": claim["generation"], "spawn": spawn},
            separators=(",", ":"),
        ).encode("utf-8"),
    )
    assert status == 200, body.decode("utf-8", errors="replace")
    spawned_ids = json.loads(body.decode("utf-8"))["spawned_task_ids"]
    assert len(spawned_ids) == 2

    with session_factory() as session:
        children = list(
            session.scalars(
                select(TaskActive).where(
                    TaskActive.source_task_id == UUID(source_id)
                )
            )
        )
        assert len(children) == 2
        priorities = sorted(int(child.priority) for child in children)
        assert priorities == [_PRIORITY_MIN, _PRIORITY_MAX]
        terminal = session.execute(
            select(TaskTerminal).where(TaskTerminal.task_id == UUID(source_id))
        ).scalar_one()
        assert int(terminal.priority) == 0


def test_mixed_spawn_invalid_priority_rolls_back_source_and_descendants(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
    target_queue_name: str,
) -> None:
    with session_factory() as session:
        _seed_queue(session, name=queue_name)
        target = _seed_queue(session, name=target_queue_name)
        before_counter = session.get(QueueCounter, int(target.id))
        assert before_counter is not None
        ready_before = int(before_counter.ready_count)
        delayed_before = int(before_counter.delayed_count)
        leased_before = int(before_counter.leased_count)

    source_id = _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"kind": "source"},
        idempotency_key=f"idem-{uuid.uuid4().hex}",
    )
    claim = _claim_one(app, queue_name=queue_name)
    claim_id = UUID(claim["claim_id"])
    status, _hdrs, body = _asgi_http_call(
        app,
        method="POST",
        path=_complete_path(claim["claim_id"]),
        headers=_worker_headers(claim_token=claim["claim_token"]),
        body=json.dumps(
            {
                "generation": claim["generation"],
                "spawn": [
                    _spawn_item(
                        queue_name=target_queue_name,
                        payload={"ok": True},
                        priority=100,
                        idempotency_key=f"ok-{uuid.uuid4().hex[:8]}",
                    ),
                    _spawn_item(
                        queue_name=target_queue_name,
                        payload={"bad": True},
                        priority=_PRIORITY_MAX + 1,
                        idempotency_key=f"bad-{uuid.uuid4().hex[:8]}",
                    ),
                ],
            },
            separators=(",", ":"),
        ).encode("utf-8"),
    )
    assert status == 400
    _assert_error(body, code="validation_failed", retryable=False)

    with session_factory() as session:
        still = session.execute(
            select(TaskActive).where(TaskActive.task_id == UUID(source_id))
        ).scalar_one()
        assert int(still.state_code) == _STATE_LEASED
        assert still.current_claim_id == claim_id
        assert (
            session.execute(
                select(TaskActive).where(TaskActive.source_task_id == UUID(source_id))
            ).scalars().all()
            == []
        )
        assert _effects_for_claim(session, claim_id=claim_id) == []
        assert (
            session.execute(
                select(CompleteReplay).where(CompleteReplay.claim_id == claim_id)
            ).scalar_one_or_none()
            is None
        )
        counter = session.get(QueueCounter, int(target.id))
        assert counter is not None
        assert int(counter.ready_count) == ready_before
        assert int(counter.delayed_count) == delayed_before
        assert int(counter.leased_count) == leased_before


_LIFECYCLE_PRIORITY = 500


def _complete_success(
    app: Any,
    *,
    claim: Mapping[str, Any],
) -> None:
    status, _hdrs, body = _asgi_http_call(
        app,
        method="POST",
        path=_complete_path(claim["claim_id"]),
        headers=_worker_headers(claim_token=claim["claim_token"]),
        body=json.dumps(
            {"generation": claim["generation"], "spawn": []},
            separators=(",", ":"),
        ).encode("utf-8"),
    )
    assert status == 200, body.decode("utf-8", errors="replace")


def test_complete_success_active_to_terminal_preserves_non_zero_priority(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    with session_factory() as session:
        _seed_queue(session, name=queue_name)

    source_id = _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"kind": "priority-lifecycle"},
        idempotency_key=f"idem-{uuid.uuid4().hex}",
        priority=_LIFECYCLE_PRIORITY,
    )
    claim = _claim_one(app, queue_name=queue_name)

    with session_factory() as session:
        active = session.execute(
            select(TaskActive).where(TaskActive.task_id == UUID(source_id))
        ).scalar_one()
        assert int(active.priority) == _LIFECYCLE_PRIORITY

    _complete_success(app, claim=claim)

    with session_factory() as session:
        assert (
            session.execute(
                select(TaskActive).where(TaskActive.task_id == UUID(source_id))
            ).scalar_one_or_none()
            is None
        )
        terminal = session.execute(
            select(TaskTerminal).where(TaskTerminal.task_id == UUID(source_id))
        ).scalar_one()
        assert int(terminal.priority) == _LIFECYCLE_PRIORITY


def test_completed_task_inspection_returns_exact_non_zero_priority(
    app: Any,
    session_factory: sessionmaker[Session],
    queue_name: str,
) -> None:
    with session_factory() as session:
        _seed_queue(session, name=queue_name)

    source_id = _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"kind": "priority-inspection"},
        idempotency_key=f"idem-{uuid.uuid4().hex}",
        priority=_LIFECYCLE_PRIORITY,
    )
    claim = _claim_one(app, queue_name=queue_name)
    _complete_success(app, claim=claim)

    status, _hdrs, resp = _asgi_http_call(
        app,
        method="GET",
        path=f"/v1/tasks/{source_id}",
        headers={"Authorization": f"Bearer {PRODUCER_TOKEN}"},
    )
    assert status == 200, resp.decode("utf-8", errors="replace")
    task = json.loads(resp.decode("utf-8"))
    assert task["task_id"] == source_id
    assert task["state"] == "succeeded"
    assert int(task["priority"]) == _LIFECYCLE_PRIORITY
