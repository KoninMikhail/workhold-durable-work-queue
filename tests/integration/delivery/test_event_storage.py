"""Real-PostgreSQL Delivery Outbox persistence in Complete (05-02 / COMP-02).

Proves atomic complete+spawn+events, idempotent replay, partition layout, and
state constraints without any network publication path.
"""

from __future__ import annotations

import json
import os
import socket
import uuid
from collections.abc import Iterator, Mapping
from datetime import timedelta
from typing import Any
from uuid import UUID

import pytest
from sqlalchemy import create_engine, event, func, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from workhold.api.application import create_application_app
from workhold.api.schemas.terminal import (
    CompleteCommand,
    SpawnCommand,
    compute_complete_fingerprint,
    parse_complete_command,
)
from workhold.api.security import ListenerBind
from workhold.application.claim_service import ClaimService
from workhold.application.completion import CompletionFaultHooks, CompletionService
from workhold.application.lease_service import LeaseService
from workhold.delivery.cloudevents import CloudEventInput
from workhold.delivery.models import (
    EFFECT_KIND_EVENT,
    STATE_DEAD_LETTERED,
    STATE_PENDING,
    STATE_PUBLISHED,
    STATE_PUBLISHING,
    EventCommand,
    TERMINAL_OUTCOME_DEAD_LETTERED,
    TERMINAL_OUTCOME_PUBLISHED,
)
from workhold.delivery.repository import DeliveryEventRepository
from workhold.domain.queue_control import (
    AdminRequestMetadata,
    BackoffStrategy,
    CreateQueueMutation,
    DomainValidationError,
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
    CompletionEffect,
    DeliveryEventActive,
    DeliveryEventTerminal,
    Queue,
    TaskActive,
    TaskTerminal,
)

PRODUCER_TOKEN = "tok-producer-delivery-storage"
WORKER_TOKEN = "tok-worker-delivery-storage"
ADMIN_TOKEN = "tok-admin-delivery-storage"

PRODUCER_PRINCIPAL = "producer-delivery-storage"
WORKER_PRINCIPAL = "worker-delivery-storage"
ADMIN_PRINCIPAL = "admin-delivery-storage"

BASE_QUEUE_NAME = "orders.delivery"
TARGET_QUEUE_PREFIX = "billing.delivery"
CLAIM_PATH = "/v1/claims"
CLAIM_TOKEN_HEADER = "X-Queue-Claim-Token"
REPLICA_ID = "pool-delivery/replica-1"
PAYLOAD_SENTINEL = "DELIVERY_SECRET_PAYLOAD_SHOULD_NEVER_LEAK"

_OUTCOME_SUCCEEDED = 2
_RESULT_SUCCEEDED = 10
_EFFECT_KIND_SPAWN = 1


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
def authorizer(queue_name: str, target_queue_name: str) -> Authorizer:
    return Authorizer(
        queue_scopes={
            PRODUCER_PRINCIPAL: frozenset(
                {queue_name, BASE_QUEUE_NAME, target_queue_name, TARGET_QUEUE_PREFIX}
            ),
            WORKER_PRINCIPAL: frozenset(
                {queue_name, BASE_QUEUE_NAME, target_queue_name, TARGET_QUEUE_PREFIX}
            ),
            ADMIN_PRINCIPAL: frozenset(
                {queue_name, BASE_QUEUE_NAME, target_queue_name, TARGET_QUEUE_PREFIX}
            ),
        }
    )


@pytest.fixture
def target_queue_name() -> str:
    return _unique_queue_name(TARGET_QUEUE_PREFIX)


@pytest.fixture
def session_factory(migrated_schema) -> Iterator[sessionmaker[Session]]:
    _conn, schema = migrated_schema
    database_url = os.environ.get("TEST_DATABASE_URL", "").strip()
    if not database_url:
        pytest.fail("TEST_DATABASE_URL is required for delivery storage integration")
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
    return create_application_app(
        authenticator=BearerCredentialAuthenticator.from_bindings(_bindings()),
        authorizer=authorizer,
        bind=ListenerBind(host="127.0.0.1", port=18105),
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


def _asgi_http_call(
    app: Any,
    *,
    method: str,
    path: str,
    headers: Mapping[str, str],
    body: bytes = b"",
) -> tuple[int, dict[str, str], bytes]:
    status_holder: dict[str, int] = {}
    header_holder: dict[str, str] = {}
    body_chunks: list[bytes] = []

    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": body, "more_body": False}

    async def send(message: dict[str, Any]) -> None:
        if message["type"] == "http.response.start":
            status_holder["status"] = int(message["status"])
            header_holder.clear()
            header_holder.update(
                {
                    k.decode("latin-1").lower(): v.decode("latin-1")
                    for k, v in message.get("headers", [])
                }
            )
        elif message["type"] == "http.response.body":
            body_chunks.append(message.get("body", b"") or b"")

    import asyncio

    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("ascii"),
        "query_string": b"",
        "headers": [
            (k.lower().encode("latin-1"), v.encode("latin-1"))
            for k, v in headers.items()
        ],
        "client": ("127.0.0.1", 9),
        "server": ("127.0.0.1", 18105),
    }
    asyncio.run(app(scope, receive, send))
    return status_holder["status"], header_holder, b"".join(body_chunks)


def _producer_headers(*, idempotency_key: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {PRODUCER_TOKEN}",
        "Content-Type": "application/json",
        "Idempotency-Key": idempotency_key,
    }


def _worker_headers(*, claim_token: str | None = None) -> dict[str, str]:
    headers = {
        "Authorization": f"Bearer {WORKER_TOKEN}",
        "Content-Type": "application/json",
    }
    if claim_token is not None:
        headers[CLAIM_TOKEN_HEADER] = claim_token
    return headers


def _enqueue_ready(
    app: Any,
    *,
    queue_name: str,
    payload: Mapping[str, Any],
    idempotency_key: str,
) -> str:
    status, _hdrs, body = _asgi_http_call(
        app,
        method="POST",
        path=f"/v1/queues/{queue_name}/tasks",
        headers=_producer_headers(idempotency_key=idempotency_key),
        body=json.dumps(
            {"payload": dict(payload), "priority": 0},
            separators=(",", ":"),
        ).encode("utf-8"),
    )
    assert status == 201, body.decode("utf-8", errors="replace")
    return json.loads(body.decode("utf-8"))["task"]["task_id"]


def _claim_one(app: Any, *, queue_name: str) -> dict[str, Any]:
    status, _hdrs, body = _asgi_http_call(
        app,
        method="POST",
        path=CLAIM_PATH,
        headers=_worker_headers(),
        body=json.dumps(
            {
                "queues": [queue_name],
                "max_tasks": 1,
                "lease_seconds": 30,
                "wait_seconds": 0,
                "worker_id": REPLICA_ID,
            },
            separators=(",", ":"),
        ).encode("utf-8"),
    )
    assert status == 200, body.decode("utf-8", errors="replace")
    tasks = json.loads(body.decode("utf-8"))["tasks"]
    assert len(tasks) == 1
    claimed = tasks[0]
    claim = claimed["claim"]
    return {
        "task_id": claimed["task"]["task_id"],
        "claim_id": claim["claim_id"],
        "claim_token": claim["claim_token"],
        "generation": int(claim["generation"]),
    }


def _event_canonical(
    *,
    source: str = "https://app.example/orders",
    type_: str = "com.example.order.completed",
    data: Any | None = None,
) -> dict[str, Any]:
    return {
        "source": source,
        "type": type_,
        "subject": "order-1",
        "datacontenttype": "application/json",
        "data": {"secret": PAYLOAD_SENTINEL} if data is None else data,
        "extensions": {},
    }


def _event_command(**kwargs: Any) -> EventCommand:
    canonical = _event_canonical(**kwargs)
    return EventCommand(
        source=str(canonical["source"]),
        type=str(canonical["type"]),
        subject=canonical.get("subject"),
        datacontenttype=canonical.get("datacontenttype"),
        data=canonical.get("data"),
        extensions=dict(canonical.get("extensions") or {}),
        canonical=canonical,
    )


def _complete_command(
    *,
    generation: int,
    spawn: tuple[SpawnCommand, ...] = (),
    events: tuple[EventCommand, ...] = (),
) -> CompleteCommand:
    fingerprint = compute_complete_fingerprint(
        spawn=[dict(item.canonical) for item in spawn],
        events=[dict(item.canonical) for item in events],
    )
    return CompleteCommand(
        generation=generation,
        spawn=spawn,
        events=events,
        fingerprint=fingerprint,
    )


def _spawn_command(*, queue_name: str, key: str) -> SpawnCommand:
    canonical = {
        "queue_name": queue_name,
        "idempotency_key": key,
        "payload": {"spawn": True},
        "priority": 0,
    }
    return SpawnCommand(
        queue_name=queue_name,
        idempotency_key=key,
        payload=canonical["payload"],
        priority=0,
        available_at=None,
        canonical=canonical,
    )


def _authorize(*allowed: str):
    allowed_set = frozenset(allowed)

    def _inner(name: str) -> bool:
        return name in allowed_set

    return _inner


def _active_events(session: Session, *, source_task_id: UUID) -> list[DeliveryEventActive]:
    return list(
        session.execute(
            select(DeliveryEventActive)
            .where(DeliveryEventActive.source_task_id == source_task_id)
            .order_by(DeliveryEventActive.ordinal)
        ).scalars()
    )


def _terminal_events(
    session: Session, *, source_task_id: UUID
) -> list[DeliveryEventTerminal]:
    return list(
        session.execute(
            select(DeliveryEventTerminal)
            .where(DeliveryEventTerminal.source_task_id == source_task_id)
            .order_by(DeliveryEventTerminal.ordinal)
        ).scalars()
    )


def test_complete_zero_events_remains_valid(
    session_factory: sessionmaker[Session],
    authorizer: Authorizer,
    queue_name: str,
    app: Any,
) -> None:
    with session_factory() as session:
        _seed_queue(session, name=queue_name)
    task_id = _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"n": 1},
        idempotency_key=f"idem-{uuid.uuid4().hex}",
    )
    claim = _claim_one(app, queue_name=queue_name)
    command = _complete_command(generation=int(claim["generation"]), events=())
    service = CompletionService(session_factory=session_factory)
    result = service.complete(
        claim_id=UUID(claim["claim_id"]),
        claim_token=UUID(claim["claim_token"]),
        command=command,
        authorize_queue=_authorize(queue_name),
    )
    assert result.state == "succeeded"
    assert result.event_ids == ()
    with session_factory() as session:
        assert _active_events(session, source_task_id=UUID(task_id)) == []
        replay = session.execute(
            select(CompleteReplay).where(
                CompleteReplay.claim_id == UUID(claim["claim_id"])
            )
        ).scalar_one()
        assert list(replay.event_ids or []) == []


def test_complete_with_ordered_events_and_spawns_atomic(
    session_factory: sessionmaker[Session],
    queue_name: str,
    target_queue_name: str,
    app: Any,
) -> None:
    with session_factory() as session:
        _seed_queue(session, name=queue_name)
        _seed_queue(session, name=target_queue_name)
    task_id = _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"n": 1},
        idempotency_key=f"idem-{uuid.uuid4().hex}",
    )
    claim = _claim_one(app, queue_name=queue_name)
    events = (
        _event_command(type_="com.example.a"),
        _event_command(type_="com.example.b"),
    )
    spawn = (
        _spawn_command(queue_name=target_queue_name, key=f"s-{uuid.uuid4().hex}"),
    )
    command = _complete_command(
        generation=int(claim["generation"]),
        spawn=spawn,
        events=events,
    )
    service = CompletionService(session_factory=session_factory)
    result = service.complete(
        claim_id=UUID(claim["claim_id"]),
        claim_token=UUID(claim["claim_token"]),
        command=command,
        authorize_queue=_authorize(queue_name, target_queue_name),
    )
    assert len(result.spawned_task_ids) == 1
    assert len(result.event_ids) == 2
    assert result.event_ids[0] != result.event_ids[1]

    with session_factory() as session:
        rows = _active_events(session, source_task_id=UUID(task_id))
        assert len(rows) == 2
        assert [int(r.ordinal) for r in rows] == [0, 1]
        assert [r.event_id for r in rows] == list(result.event_ids)
        for row in rows:
            assert int(row.state_code) == STATE_PENDING
            assert row.current_claim_id is None
            assert row.claimed_at is None
            assert row.lease_expires_at is None
            assert int(row.generation) == 0
            assert row.relay_principal_id is None
            assert int(row.delivery_attempt) == 0
            assert row.last_failure_code is None
            assert row.available_at is not None
            assert isinstance(row.envelope, dict)
            assert row.envelope.get("id") == str(row.event_id)
            assert PAYLOAD_SENTINEL in json.dumps(row.envelope)
        effects = list(
            session.execute(
                select(CompletionEffect)
                .where(
                    CompletionEffect.source_claim_id == UUID(claim["claim_id"]),
                    CompletionEffect.effect_kind_code == EFFECT_KIND_EVENT,
                )
                .order_by(CompletionEffect.ordinal)
            ).scalars()
        )
        assert [e.resource_id for e in effects] == list(result.event_ids)
        replay = session.execute(
            select(CompleteReplay).where(
                CompleteReplay.claim_id == UUID(claim["claim_id"])
            )
        ).scalar_one()
        assert list(replay.event_ids) == list(result.event_ids)
        assert (
            session.execute(
                select(func.count())
                .select_from(TaskTerminal)
                .where(TaskTerminal.task_id == UUID(task_id))
            ).scalar_one()
            == 1
        )
        assert (
            session.execute(
                select(TaskActive).where(TaskActive.task_id == UUID(task_id))
            ).scalar_one_or_none()
            is None
        )


def test_same_claim_same_body_replay_stable_event_ids(
    session_factory: sessionmaker[Session],
    queue_name: str,
    app: Any,
) -> None:
    with session_factory() as session:
        _seed_queue(session, name=queue_name)
    task_id = _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"n": 1},
        idempotency_key=f"idem-{uuid.uuid4().hex}",
    )
    claim = _claim_one(app, queue_name=queue_name)
    events = (_event_command(), _event_command(type_="com.example.other"))
    command = _complete_command(generation=int(claim["generation"]), events=events)
    service = CompletionService(session_factory=session_factory)
    first = service.complete(
        claim_id=UUID(claim["claim_id"]),
        claim_token=UUID(claim["claim_token"]),
        command=command,
        authorize_queue=_authorize(queue_name),
    )
    second = service.complete(
        claim_id=UUID(claim["claim_id"]),
        claim_token=UUID(claim["claim_token"]),
        command=command,
        authorize_queue=_authorize(queue_name),
    )
    assert first.replayed is False
    assert second.replayed is True
    assert second.event_ids == first.event_ids
    with session_factory() as session:
        assert len(_active_events(session, source_task_id=UUID(task_id))) == 2


def test_changed_body_conflicts_without_new_event_rows(
    session_factory: sessionmaker[Session],
    queue_name: str,
    app: Any,
) -> None:
    with session_factory() as session:
        _seed_queue(session, name=queue_name)
    task_id = _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"n": 1},
        idempotency_key=f"idem-{uuid.uuid4().hex}",
    )
    claim = _claim_one(app, queue_name=queue_name)
    first_cmd = _complete_command(
        generation=int(claim["generation"]),
        events=(_event_command(),),
    )
    service = CompletionService(session_factory=session_factory)
    first = service.complete(
        claim_id=UUID(claim["claim_id"]),
        claim_token=UUID(claim["claim_token"]),
        command=first_cmd,
        authorize_queue=_authorize(queue_name),
    )
    changed = _complete_command(
        generation=int(claim["generation"]),
        events=(_event_command(type_="com.example.changed"),),
    )
    with pytest.raises(DomainValidationError) as exc:
        service.complete(
            claim_id=UUID(claim["claim_id"]),
            claim_token=UUID(claim["claim_token"]),
            command=changed,
            authorize_queue=_authorize(queue_name),
        )
    assert exc.value.code == "idempotency_conflict"
    with session_factory() as session:
        rows = _active_events(session, source_task_id=UUID(task_id))
        assert len(rows) == 1
        assert rows[0].event_id == first.event_ids[0]


def test_injected_failure_at_event_boundaries_rolls_back_all(
    session_factory: sessionmaker[Session],
    queue_name: str,
    target_queue_name: str,
    app: Any,
) -> None:
    with session_factory() as session:
        queue = _seed_queue(session, name=queue_name)
        _seed_queue(session, name=target_queue_name)
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
    claim_token = UUID(claim["claim_token"])
    command = _complete_command(
        generation=int(claim["generation"]),
        spawn=(
            _spawn_command(queue_name=target_queue_name, key=f"s-{uuid.uuid4().hex}"),
        ),
        events=(_event_command(), _event_command(type_="com.example.second")),
    )

    stages = (
        "after_attempt_close",
        "after_spawns",
        "after_event_effect",
        "after_events",
        "after_replay_flush",
    )
    for stage in stages:
        with session_factory() as before:
            active = before.execute(
                select(TaskActive).where(TaskActive.task_id == task_id)
            ).scalar_one_or_none()
            if active is None:
                pytest.fail(f"task left non-leased before stage {stage}")
            leased = before.execute(
                select(func.count())
                .select_from(ClaimRegistry)
                .where(ClaimRegistry.claim_id == claim_id)
            ).scalar_one()
            assert leased == 1

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
        with pytest.raises(RuntimeError, match="injected failure"):
            service.complete(
                claim_id=claim_id,
                claim_token=claim_token,
                command=command,
                authorize_queue=_authorize(queue_name, target_queue_name),
            )
        with session_factory() as after:
            assert (
                after.execute(
                    select(TaskActive).where(TaskActive.task_id == task_id)
                ).scalar_one_or_none()
                is not None
            )
            assert _active_events(after, source_task_id=task_id) == []
            assert (
                after.execute(
                    select(func.count())
                    .select_from(CompleteReplay)
                    .where(CompleteReplay.claim_id == claim_id)
                ).scalar_one()
                == 0
            )
            assert (
                after.execute(
                    select(func.count())
                    .select_from(CompletionEffect)
                    .where(CompletionEffect.source_claim_id == claim_id)
                ).scalar_one()
                == 0
            )
            assert (
                after.execute(
                    select(func.count())
                    .select_from(TaskTerminal)
                    .where(TaskTerminal.task_id == task_id)
                ).scalar_one()
                == 0
            )

    # Successful complete after rollback injections.
    service = CompletionService(session_factory=session_factory)
    result = service.complete(
        claim_id=claim_id,
        claim_token=claim_token,
        command=command,
        authorize_queue=_authorize(queue_name, target_queue_name),
    )
    assert len(result.event_ids) == 2
    assert len(result.spawned_task_ids) == 1
    assert queue.id is not None


def test_active_unpartitioned_terminal_daily_utc_partitioned(
    session_factory: sessionmaker[Session],
    queue_name: str,
    app: Any,
) -> None:
    with session_factory() as session:
        _seed_queue(session, name=queue_name)
    task_id = _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"n": 1},
        idempotency_key=f"idem-{uuid.uuid4().hex}",
    )
    claim = _claim_one(app, queue_name=queue_name)
    command = _complete_command(
        generation=int(claim["generation"]),
        events=(_event_command(),),
    )
    service = CompletionService(session_factory=session_factory)
    result = service.complete(
        claim_id=UUID(claim["claim_id"]),
        claim_token=UUID(claim["claim_token"]),
        command=command,
        authorize_queue=_authorize(queue_name),
    )
    event_id = result.event_ids[0]

    with session_factory() as session:
        # Active parent is ordinary (relkind r), not partitioned.
        active_kind = session.execute(
            text(
                """
                SELECT c.relkind
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = current_schema()
                  AND c.relname = 'delivery_events_active'
                """
            )
        ).scalar_one()
        assert active_kind == "r"

        term_kind = session.execute(
            text(
                """
                SELECT c.relkind
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = current_schema()
                  AND c.relname = 'delivery_events_terminal'
                """
            )
        ).scalar_one()
        assert term_kind == "p"

        children = session.execute(
            text(
                """
                SELECT child.relname
                FROM pg_inherits i
                JOIN pg_class child ON child.oid = i.inhrelid
                JOIN pg_class parent ON parent.oid = i.inhparent
                JOIN pg_namespace n ON n.oid = parent.relnamespace
                WHERE n.nspname = current_schema()
                  AND parent.relname = 'delivery_events_terminal'
                ORDER BY child.relname
                """
            )
        ).scalars().all()
        assert any(name.startswith("delivery_events_terminal_") for name in children)

        repo = DeliveryEventRepository()
        now = session.scalar(select(func.transaction_timestamp()))
        assert now is not None
        repo.transition_active_to_terminal(
            session,
            event_id=event_id,
            terminal_outcome=TERMINAL_OUTCOME_PUBLISHED,
            terminal_at=now,
            final_delivery_attempt=1,
            final_failure_code=None,
        )
        session.commit()

        terminal_rows = _terminal_events(session, source_task_id=UUID(task_id))
        assert len(terminal_rows) == 1
        term = terminal_rows[0]
        assert term.event_id == event_id
        assert int(term.state_code) == STATE_PUBLISHED
        assert term.terminal_at is not None
        assert int(term.delivery_attempt) == 1
        assert _active_events(session, source_task_id=UUID(task_id)) == []

        child = session.execute(
            text(
                """
                SELECT tableoid::regclass::text
                FROM delivery_events_terminal
                WHERE event_id = :eid
                """
            ),
            {"eid": event_id},
        ).scalar_one()
        assert "delivery_events_terminal_" in child


def test_state_constraints_pending_and_publishing(
    session_factory: sessionmaker[Session],
) -> None:
    with session_factory() as session:
        now = session.scalar(select(func.transaction_timestamp()))
        assert now is not None
        event_id = uuid.uuid4()
        # Pending with claim fields must fail.
        with pytest.raises(IntegrityError):
            with session.begin_nested():
                session.execute(
                    text(
                        """
                        INSERT INTO delivery_events_active (
                            event_id, source_task_id, ordinal, state_code,
                            envelope, envelope_bytes, available_at, generation,
                            current_claim_id, claimed_at, lease_expires_at,
                            delivery_attempt, relay_principal_id
                        ) VALUES (
                            :eid, :sid, 0, :pending,
                            '{}'::jsonb, 2, :now, 0,
                            :claim, :now, :exp,
                            0, NULL
                        )
                        """
                    ),
                    {
                        "eid": event_id,
                        "sid": uuid.uuid4(),
                        "pending": STATE_PENDING,
                        "now": now,
                        "claim": uuid.uuid4(),
                        "exp": now + timedelta(seconds=30),
                    },
                )
        session.rollback()

    with session_factory() as session:
        now = session.scalar(select(func.transaction_timestamp()))
        assert now is not None
        # Publishing without complete fence must fail.
        with pytest.raises(IntegrityError):
            with session.begin_nested():
                session.execute(
                    text(
                        """
                        INSERT INTO delivery_events_active (
                            event_id, source_task_id, ordinal, state_code,
                            envelope, envelope_bytes, available_at, generation,
                            current_claim_id, claimed_at, lease_expires_at,
                            delivery_attempt, relay_principal_id
                        ) VALUES (
                            :eid, :sid, 0, :publishing,
                            '{}'::jsonb, 2, :now, 0,
                            NULL, NULL, NULL,
                            0, NULL
                        )
                        """
                    ),
                    {
                        "eid": uuid.uuid4(),
                        "sid": uuid.uuid4(),
                        "publishing": STATE_PUBLISHING,
                        "now": now,
                    },
                )
        session.rollback()

    with session_factory() as session:
        now = session.scalar(select(func.transaction_timestamp()))
        assert now is not None
        # Valid publishing fence inserts.
        session.execute(
            text(
                """
                INSERT INTO delivery_events_active (
                    event_id, source_task_id, ordinal, state_code,
                    envelope, envelope_bytes, available_at, generation,
                    current_claim_id, claimed_at, lease_expires_at,
                    delivery_attempt, relay_principal_id, last_failure_code
                ) VALUES (
                    :eid, :sid, 0, :publishing,
                    '{"id":"x"}'::jsonb, 10, :now, 1,
                    :claim, :now, :exp,
                    1, 'relay-principal-1', NULL
                )
                """
            ),
            {
                "eid": uuid.uuid4(),
                "sid": uuid.uuid4(),
                "publishing": STATE_PUBLISHING,
                "now": now,
                "claim": uuid.uuid4(),
                "exp": now + timedelta(seconds=30),
            },
        )
        session.commit()


def test_complete_performs_no_network_io(
    session_factory: sessionmaker[Session],
    queue_name: str,
    app: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with session_factory() as session:
        _seed_queue(session, name=queue_name)
    task_id = _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"n": 1},
        idempotency_key=f"idem-{uuid.uuid4().hex}",
    )
    claim = _claim_one(app, queue_name=queue_name)

    def _blocked(*_a: Any, **_k: Any) -> Any:
        raise AssertionError("network I/O attempted during complete")

    monkeypatch.setattr(socket, "create_connection", _blocked)
    monkeypatch.setattr(socket.socket, "connect", _blocked)

    command = _complete_command(
        generation=int(claim["generation"]),
        events=(_event_command(),),
    )
    service = CompletionService(session_factory=session_factory)
    result = service.complete(
        claim_id=UUID(claim["claim_id"]),
        claim_token=UUID(claim["claim_token"]),
        command=command,
        authorize_queue=_authorize(queue_name),
    )
    assert len(result.event_ids) == 1
    assert task_id


def test_http_complete_still_rejects_reserved_events_key(
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
    status, _hdrs, body = _asgi_http_call(
        app,
        method="POST",
        path=f"/v1/claims/{claim['claim_id']}:complete",
        headers=_worker_headers(claim_token=claim["claim_token"]),
        body=json.dumps(
            {"generation": int(claim["generation"]), "spawn": [], "events": []},
            separators=(",", ":"),
        ).encode("utf-8"),
    )
    assert status == 400
    err = json.loads(body.decode("utf-8"))
    assert err["code"] == "validation_failed"


def test_dead_letter_terminal_preserves_lineage(
    session_factory: sessionmaker[Session],
    queue_name: str,
    app: Any,
) -> None:
    with session_factory() as session:
        _seed_queue(session, name=queue_name)
    task_id = _enqueue_ready(
        app,
        queue_name=queue_name,
        payload={"n": 1},
        idempotency_key=f"idem-{uuid.uuid4().hex}",
    )
    claim = _claim_one(app, queue_name=queue_name)
    command = _complete_command(
        generation=int(claim["generation"]),
        events=(_event_command(),),
    )
    service = CompletionService(session_factory=session_factory)
    result = service.complete(
        claim_id=UUID(claim["claim_id"]),
        claim_token=UUID(claim["claim_token"]),
        command=command,
        authorize_queue=_authorize(queue_name),
    )
    with session_factory() as session:
        repo = DeliveryEventRepository()
        now = session.scalar(select(func.transaction_timestamp()))
        assert now is not None
        repo.transition_active_to_terminal(
            session,
            event_id=result.event_ids[0],
            terminal_outcome=TERMINAL_OUTCOME_DEAD_LETTERED,
            terminal_at=now,
            final_delivery_attempt=3,
            final_failure_code="delivery_exhausted",
        )
        session.commit()
        term = _terminal_events(session, source_task_id=UUID(task_id))[0]
        assert int(term.state_code) == STATE_DEAD_LETTERED
        assert term.failure_code == "delivery_exhausted"
        assert int(term.delivery_attempt) == 3
        assert term.ordinal == 0
        assert term.source_task_id == UUID(task_id)
        assert term.event_id == result.event_ids[0]
        # Constants used by relay plans.
        assert TERMINAL_OUTCOME_PUBLISHED == "published"
        assert TERMINAL_OUTCOME_DEAD_LETTERED == "dead_lettered"
        assert CloudEventInput  # import retained for envelope contract linkage
        assert parse_complete_command  # HTTP path still closed for events
