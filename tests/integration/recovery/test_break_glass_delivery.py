"""Delivery break-glass reclaim / dead-letter (D-04..D-06 / REC-03 / CTRL-06).

Domain coverage lands in 14-03-01; HTTP/OpenAPI unskip in 14-03-02.
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from collections.abc import Iterator, Mapping
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

import pytest
from sqlalchemy import create_engine, event, func, select, text
from sqlalchemy.orm import Session, sessionmaker

from workhold.api.admin import create_admin_app
from workhold.api.security import ListenerBind
from workhold.delivery.models import (
    STATE_DEAD_LETTERED,
    STATE_PENDING,
    STATE_PUBLISHING,
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
from workhold.operations.break_glass import (
    BreakGlassAck,
    force_delivery_dead_letter,
    force_delivery_reclaim,
    parse_break_glass_ack,
)
from workhold.security.authorization import Authorizer, Operation
from workhold.security.credentials import (
    BearerCredentialAuthenticator,
    CredentialBinding,
)
from workhold.security.principals import ServiceRole
from workhold.settings import Secret
from workhold.storage.models import (
    AdminAuditLog,
    DeliveryEventActive,
    DeliveryEventTerminal,
)

pytest_plugins = ["tests.integration.conftest"]

ADMIN_PRINCIPAL = "admin-bg-delivery"
ADMIN_TOKEN = "tok-admin-bg-dlv"
BREAK_GLASS_TOKEN = "tok-break-glass-bg-dlv"
BREAK_GLASS_PRINCIPAL = "break-glass-bg-dlv"


def _unique(prefix: str) -> str:
    return f"{prefix}.{uuid.uuid4().hex[:8]}"


def _bindings() -> tuple[CredentialBinding, ...]:
    now = datetime.now(timezone.utc)
    all_bg_ops = frozenset(
        {
            "forceLeaseExpiry",
            "reconcileCounters",
            "raiseReplayLimit",
            "dropExpiredPartition",
            "repairRegistryEntry",
            "forceDeliveryReclaim",
            "forceDeliveryDeadLetter",
        }
    )
    return (
        CredentialBinding(
            principal_id=ADMIN_PRINCIPAL,
            role=ServiceRole.ADMIN,
            generation_id="g1",
            secret=Secret(ADMIN_TOKEN),
        ),
        CredentialBinding(
            principal_id=BREAK_GLASS_PRINCIPAL,
            role=ServiceRole.BREAK_GLASS,
            generation_id="g1",
            secret=Secret(BREAK_GLASS_TOKEN),
            expires_at=now + timedelta(hours=1),
            allowed_operations=all_bg_ops,
        ),
    )


def _make_admin_app(
    session_factory: sessionmaker[Session],
    sa_engine,
    *,
    queue_name: str,
) -> Any:
    return create_admin_app(
        authenticator=BearerCredentialAuthenticator.from_bindings(_bindings()),
        authorizer=Authorizer(
            queue_scopes={
                ADMIN_PRINCIPAL: frozenset({queue_name}),
                BREAK_GLASS_PRINCIPAL: frozenset({queue_name}),
            }
        ),
        bind=ListenerBind(host="127.0.0.1", port=18199),
        session_factory=session_factory,
        repository=QueueControlRepository(),
        engine=sa_engine,
        cursor_secret=Secret("break-glass-delivery-test-secret"),
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
    status_holder: dict[str, Any] = {}
    response_headers: dict[str, str] = {}
    body_chunks: list[bytes] = []

    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": body, "more_body": False}

    async def send(message: dict[str, Any]) -> None:
        if message["type"] == "http.response.start":
            status_holder["status"] = int(message["status"])
            for raw_k, raw_v in message.get("headers", []):
                response_headers[raw_k.decode("latin-1").lower()] = raw_v.decode(
                    "latin-1"
                )
        elif message["type"] == "http.response.body":
            body_chunks.append(message.get("body", b""))

    asyncio.run(
        app(
            {
                "type": "http",
                "asgi": {"version": "3.0"},
                "http_version": "1.1",
                "method": method,
                "path": path,
                "raw_path": path.encode("utf-8"),
                "query_string": b"",
                "headers": header_list,
                "client": ("127.0.0.1", 9),
                "server": ("127.0.0.1", 18199),
                "scheme": "http",
            },
            receive,
            send,
        )
    )
    return (
        int(status_holder["status"]),
        response_headers,
        b"".join(body_chunks),
    )


def _ack_body(**extra: Any) -> bytes:
    payload = {
        "reason": "stuck publishing reclaim",
        "incident_reference": "INC-2026-0921-DELIVERY",
        "risk_acknowledged": True,
        **extra,
    }
    return json.dumps(payload).encode("utf-8")


@pytest.fixture
def sa_engine(migrated_schema):
    _conn, schema = migrated_schema
    database_url = os.environ.get("TEST_DATABASE_URL", "").strip()
    if not database_url:
        pytest.fail("TEST_DATABASE_URL is required for tests/integration/recovery")
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

    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture
def sa_session(sa_engine) -> Iterator[Session]:
    factory = sessionmaker(bind=sa_engine, expire_on_commit=False)
    session = factory()
    try:
        yield session
    finally:
        session.close()


@pytest.fixture
def session_factory(sa_engine) -> sessionmaker[Session]:
    return sessionmaker(bind=sa_engine, expire_on_commit=False)


@pytest.fixture(autouse=True)
def _clean_delivery_tables(session_factory: sessionmaker[Session]) -> Iterator[None]:
    """Isolate leftover delivery rows so alembic downgrade teardown can succeed."""
    with session_factory() as session:
        with session.begin():
            session.execute(text("DELETE FROM delivery_events_terminal"))
            session.execute(text("DELETE FROM delivery_events_active"))
    yield
    with session_factory() as session:
        with session.begin():
            session.execute(text("DELETE FROM delivery_events_terminal"))
            session.execute(text("DELETE FROM delivery_events_active"))


def _seed_queue(session: Session, *, name: str) -> tuple[int, int]:
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
            metadata=AdminRequestMetadata(
                actor_id=ADMIN_PRINCIPAL,
                request_id=str(uuid.uuid4()),
                idempotency_key=f"seed-{uuid.uuid4().hex}",
            ),
        ),
    )
    session.commit()
    row = session.execute(
        text(
            "SELECT id, active_policy_version_id FROM queues WHERE name = :name"
        ),
        {"name": name},
    ).one()
    return int(row[0]), int(row[1])


def _seed_task(session: Session, *, queue_id: int, policy_id: int) -> UUID:
    task_id = uuid.uuid4()
    now = session.scalar(select(func.transaction_timestamp()))
    assert now is not None
    session.execute(
        text(
            """
            INSERT INTO tasks_active (
                task_id, queue_id, producer_id, state_code, priority,
                available_at, retry_policy_version_id, generation,
                created_at, updated_at
            ) VALUES (
                :tid, :qid, 'producer-bg-dlv', 2, 0,
                :now, :policy_id, 0, :now, :now
            )
            """
        ),
        {"tid": task_id, "qid": queue_id, "now": now, "policy_id": policy_id},
    )
    internal_id = session.execute(
        text("SELECT id FROM tasks_active WHERE task_id = :tid"),
        {"tid": task_id},
    ).scalar_one()
    session.execute(
        text(
            """
            INSERT INTO task_payloads_active (task_id, payload, payload_bytes)
            VALUES (:task_pk, '{}'::jsonb, 2)
            """
        ),
        {"task_pk": int(internal_id)},
    )
    session.commit()
    return task_id


def _seed_publishing_event(
    session: Session,
    *,
    source_task_id: UUID,
    generation: int = 3,
    delivery_attempt: int = 2,
) -> UUID:
    event_id = uuid.uuid4()
    claim_token = uuid.uuid4()
    now = session.scalar(select(func.transaction_timestamp()))
    assert now is not None
    envelope = {
        "specversion": "1.0",
        "id": str(event_id),
        "source": "urn:test:bg-delivery",
        "type": "com.example.bg.v1",
    }
    envelope_json = json.dumps(envelope, separators=(",", ":"), sort_keys=True)
    session.execute(
        text(
            """
            INSERT INTO delivery_events_active (
                event_id, source_task_id, ordinal, state_code,
                envelope, envelope_bytes, available_at, generation,
                current_claim_id, claimed_at, lease_expires_at,
                relay_principal_id, delivery_attempt, last_failure_code,
                created_at, updated_at
            ) VALUES (
                :eid, :sid, 0, :publishing,
                CAST(:envelope AS jsonb), :ebytes,
                :now, :generation,
                :claim, :now, :now + interval '120 seconds',
                'relay-stuck', :attempt, NULL,
                :now, :now
            )
            """
        ),
        {
            "eid": event_id,
            "sid": source_task_id,
            "publishing": STATE_PUBLISHING,
            "envelope": envelope_json,
            "ebytes": len(envelope_json.encode("utf-8")),
            "now": now,
            "generation": generation,
            "claim": claim_token,
            "attempt": delivery_attempt,
        },
    )
    session.commit()
    return event_id


def _ack() -> BreakGlassAck:
    return BreakGlassAck(
        reason="stuck publishing reclaim",
        incident_reference="INC-2026-0921-DELIVERY",
        risk_acknowledged=True,
    )


def test_force_delivery_reclaim_requires_ack_triad() -> None:
    """reason + incident_reference + risk_acknowledged=true required (D-05)."""
    with pytest.raises(DomainValidationError, match="risk_acknowledged"):
        parse_break_glass_ack(
            {
                "reason": "x",
                "incident_reference": "INC-1",
                "risk_acknowledged": False,
            }
        )
    with pytest.raises(DomainValidationError):
        parse_break_glass_ack({"reason": "x", "incident_reference": "INC-1"})


def test_force_delivery_dead_letter_requires_ack_triad() -> None:
    with pytest.raises(DomainValidationError, match="risk_acknowledged"):
        parse_break_glass_ack(
            {
                "reason": "dead letter stuck event",
                "incident_reference": "INC-2",
                "risk_acknowledged": False,
            }
        )


def test_force_delivery_reclaim_returns_pending_preserves_generation(
    sa_session: Session,
) -> None:
    """Stuck publishing → pending; generation and delivery_attempt preserved; no claim_token."""
    queue_name = _unique("orders.bg.dlv")
    queue_id, policy_id = _seed_queue(sa_session, name=queue_name)
    task_id = _seed_task(sa_session, queue_id=queue_id, policy_id=policy_id)
    generation = 5
    attempt = 4
    event_id = _seed_publishing_event(
        sa_session,
        source_task_id=task_id,
        generation=generation,
        delivery_attempt=attempt,
    )
    request_id = str(uuid.uuid4())

    result = force_delivery_reclaim(
        sa_session,
        queue_name=queue_name,
        event_id=event_id,
        actor_id="break-glass-dlv",
        request_id=request_id,
        ack=_ack(),
    )
    sa_session.commit()

    assert result.operation == "forceDeliveryReclaim"
    assert result.outcome == "reclaimed"
    assert result.generation == generation
    assert result.target_id == str(event_id)

    sa_session.expire_all()
    row = sa_session.execute(
        select(DeliveryEventActive).where(DeliveryEventActive.event_id == event_id)
    ).scalar_one()
    assert int(row.state_code) == STATE_PENDING
    assert int(row.generation) == generation
    assert int(row.delivery_attempt) == attempt
    assert row.current_claim_id is None
    assert row.claimed_at is None
    assert row.lease_expires_at is None
    assert row.relay_principal_id is None

    audits = list(
        sa_session.execute(
            select(AdminAuditLog).where(AdminAuditLog.operation_code == 14)
        ).scalars()
    )
    assert len(audits) == 1
    details = dict(audits[0].details)
    assert details["claim_token_issued"] is False
    assert details["generation"] == generation
    assert "claim_token" not in details


def test_force_delivery_dead_letter_terminals_without_claim_token(
    sa_session: Session,
) -> None:
    """Stuck active delivery becomes terminal dead-lettered without minting claim_token."""
    queue_name = _unique("orders.bg.dl")
    queue_id, policy_id = _seed_queue(sa_session, name=queue_name)
    task_id = _seed_task(sa_session, queue_id=queue_id, policy_id=policy_id)
    generation = 7
    attempt = 3
    event_id = _seed_publishing_event(
        sa_session,
        source_task_id=task_id,
        generation=generation,
        delivery_attempt=attempt,
    )
    request_id = str(uuid.uuid4())

    result = force_delivery_dead_letter(
        sa_session,
        queue_name=queue_name,
        event_id=event_id,
        actor_id="break-glass-dlv",
        request_id=request_id,
        ack=_ack(),
        failure_code="break_glass_force_dead_letter",
    )
    sa_session.commit()

    assert result.operation == "forceDeliveryDeadLetter"
    assert result.outcome == "dead_lettered"
    assert result.generation == generation

    sa_session.expire_all()
    active = sa_session.execute(
        select(DeliveryEventActive).where(DeliveryEventActive.event_id == event_id)
    ).scalar_one_or_none()
    assert active is None
    terminal = sa_session.execute(
        select(DeliveryEventTerminal).where(DeliveryEventTerminal.event_id == event_id)
    ).scalar_one()
    assert int(terminal.state_code) == STATE_DEAD_LETTERED
    assert int(terminal.delivery_attempt) == attempt
    assert terminal.failure_code == "break_glass_force_dead_letter"

    audits = list(
        sa_session.execute(
            select(AdminAuditLog).where(AdminAuditLog.operation_code == 15)
        ).scalars()
    )
    assert len(audits) == 1
    details = dict(audits[0].details)
    assert details["claim_token_issued"] is False
    assert "claim_token" not in details


def test_concurrent_delivery_reclaim_fencing_stub(sa_session: Session) -> None:
    """Concurrent fencing case: reclaim must not skip generation checks (D-06)."""
    queue_name = _unique("orders.bg.fence")
    queue_id, policy_id = _seed_queue(sa_session, name=queue_name)
    task_id = _seed_task(sa_session, queue_id=queue_id, policy_id=policy_id)
    generation = 2
    event_id = _seed_publishing_event(
        sa_session,
        source_task_id=task_id,
        generation=generation,
        delivery_attempt=1,
    )
    force_delivery_reclaim(
        sa_session,
        queue_name=queue_name,
        event_id=event_id,
        actor_id="break-glass-dlv",
        request_id=str(uuid.uuid4()),
        ack=_ack(),
    )
    sa_session.commit()
    with pytest.raises(DomainValidationError, match="not publishing"):
        force_delivery_reclaim(
            sa_session,
            queue_name=queue_name,
            event_id=event_id,
            actor_id="break-glass-dlv",
            request_id=str(uuid.uuid4()),
            ack=_ack(),
        )
    sa_session.rollback()
    row = sa_session.execute(
        select(DeliveryEventActive).where(DeliveryEventActive.event_id == event_id)
    ).scalar_one()
    assert int(row.generation) == generation
    assert int(row.state_code) == STATE_PENDING


def test_admin_denied_for_force_delivery_reclaim(
    sa_session: Session,
    session_factory: sessionmaker[Session],
    sa_engine,
) -> None:
    """Ordinary ADMIN cannot call forceDeliveryReclaim (D-03 / D-04)."""
    reclaim = Operation.FORCE_DELIVERY_RECLAIM
    dead = Operation.FORCE_DELIVERY_DEAD_LETTER
    assert reclaim.value == "forceDeliveryReclaim"
    assert dead.value == "forceDeliveryDeadLetter"

    queue_name = _unique("orders.bg.http")
    queue_id, policy_id = _seed_queue(sa_session, name=queue_name)
    task_id = _seed_task(sa_session, queue_id=queue_id, policy_id=policy_id)
    event_id = _seed_publishing_event(sa_session, source_task_id=task_id)
    app = _make_admin_app(session_factory, sa_engine, queue_name=queue_name)
    path = (
        f"/admin/v1/queues/{queue_name}/delivery-events/{event_id}:force-reclaim"
    )

    status, _h, raw = _asgi_http_call(
        app,
        method="POST",
        path=path,
        headers={
            "authorization": f"Bearer {ADMIN_TOKEN}",
            "content-type": "application/json",
        },
        body=_ack_body(),
    )
    assert status == 403
    assert json.loads(raw.decode("utf-8"))["code"] == "permission_denied"

    # Missing ack triad rejected for BREAK_GLASS.
    status, _h, raw = _asgi_http_call(
        app,
        method="POST",
        path=path,
        headers={
            "authorization": f"Bearer {BREAK_GLASS_TOKEN}",
            "content-type": "application/json",
        },
        body=json.dumps(
            {
                "reason": "missing ack",
                "incident_reference": "INC-X",
                "risk_acknowledged": False,
            }
        ).encode("utf-8"),
    )
    assert status == 400

    # Happy path: reclaim without claim_token.
    status, headers, raw = _asgi_http_call(
        app,
        method="POST",
        path=path,
        headers={
            "authorization": f"Bearer {BREAK_GLASS_TOKEN}",
            "content-type": "application/json",
        },
        body=_ack_body(),
    )
    assert status == 200, raw.decode("utf-8")
    payload = json.loads(raw.decode("utf-8"))
    assert payload["operation"] == "forceDeliveryReclaim"
    assert payload["target_id"] == str(event_id)
    assert "claim_token" not in payload
    assert "claim_token" not in json.dumps(payload)
    assert "x-queue-claim-token" not in headers


def test_force_delivery_reclaim_repo_missing_fails_closed(
    sa_session: Session,
) -> None:
    repo = DeliveryEventRepository()
    with pytest.raises(DomainValidationError, match="not found"):
        repo.force_reclaim_to_pending(sa_session, event_id=uuid.uuid4())
