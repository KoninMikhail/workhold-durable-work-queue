"""Live BreakGlassClient conformance (Phase 19 / SDK-08 / REC-03).

Proves ordinary ADMIN/OBSERVER/producer/worker fail every break-glass call,
JIT expiry/audience fail closed, and successful emergency calls preserve fencing
without minting claim tokens.
"""

from __future__ import annotations

import json
import os
import socket
import threading
import uuid
from collections.abc import Iterator
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
from sqlalchemy import create_engine, event, func, select, text
from sqlalchemy.engine import Connection
from sqlalchemy.orm import Session, sessionmaker

from workhold.api.admin import create_admin_app
from workhold.api.security import ListenerBind
from workhold.application.claim_service import ClaimService
from workhold.delivery.models import STATE_PENDING, STATE_PUBLISHING
from workhold.domain.queue_control import (
    AdminRequestMetadata,
    BackoffStrategy,
    ConfigVersion,
    CreateQueueMutation,
    QueueState,
    RetryPolicyDraft,
    SetQueueStateMutation,
)
from workhold.intake.contracts import normalize_enqueue_command
from workhold.intake.repository import EnqueueRepository
from workhold.infrastructure.postgres import partition_catalog
from workhold.infrastructure.postgres.queue_control_repository import (
    QueueControlRepository,
)
from workhold.lifecycle import Lifecycle
from workhold.roles.api import AsgiRequestHandler, InFlightGate, QuietThreadingHTTPServer
from workhold.security.authorization import Authorizer
from workhold.security.credentials import (
    BearerCredentialAuthenticator,
    CredentialBinding,
)
from workhold.security.payload_policy import PayloadRetentionPolicy
from workhold.security.principals import ServiceRole
from workhold.settings import Secret
from workhold.storage.models import (
    AdminAuditLog,
    DeliveryEventActive,
    EnqueueDedup,
    QueueCounter,
    TaskActive,
)
from tests.conformance.clients import (
    PHASE_19_BREAK_GLASS_OPS,
    BreakGlassClientAdapter,
)

pytest_plugins = ["tests.integration.conftest"]

REPO_ROOT = Path(__file__).resolve().parents[2]
OWNERSHIP_PATH = REPO_ROOT / "packages" / "client-operation-ownership.json"

ADMIN_TOKEN = "tok-admin-bg-live"
OBSERVER_TOKEN = "tok-observer-bg-live"
PRODUCER_TOKEN = "tok-producer-bg-live"
WORKER_TOKEN = "tok-worker-bg-live"
BREAK_GLASS_TOKEN = "tok-break-glass-bg-live"
EXPIRED_BG_TOKEN = "tok-break-glass-expired-live"
NARROW_BG_TOKEN = "tok-break-glass-narrow-live"

ADMIN_PRINCIPAL = "admin-bg-live"
OBSERVER_PRINCIPAL = "observer-bg-live"
PRODUCER_PRINCIPAL = "producer-bg-live"
WORKER_PRINCIPAL = "worker-bg-live"
BREAK_GLASS_PRINCIPAL = "break-glass-bg-live"
EXPIRED_BG_PRINCIPAL = "break-glass-expired-live"
NARROW_BG_PRINCIPAL = "break-glass-narrow-live"

ACK = {
    "reason": "incident remediation for stuck lease",
    "incident_reference": "INC-2026-0922-LIVE",
    "risk_acknowledged": True,
}

DENIED_TOKENS = (
    ("admin", ADMIN_TOKEN),
    ("observer", OBSERVER_TOKEN),
    ("producer", PRODUCER_TOKEN),
    ("worker", WORKER_TOKEN),
)


def _unique(prefix: str) -> str:
    return f"{prefix}.{uuid.uuid4().hex[:8]}"


def _all_bg_ops() -> frozenset[str]:
    return frozenset(PHASE_19_BREAK_GLASS_OPS)


def _bindings() -> tuple[CredentialBinding, ...]:
    now = datetime.now(timezone.utc)
    return (
        CredentialBinding(
            principal_id=ADMIN_PRINCIPAL,
            role=ServiceRole.ADMIN,
            generation_id="g1",
            secret=Secret(ADMIN_TOKEN),
        ),
        CredentialBinding(
            principal_id=OBSERVER_PRINCIPAL,
            role=ServiceRole.OBSERVER,
            generation_id="g1",
            secret=Secret(OBSERVER_TOKEN),
        ),
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
            principal_id=BREAK_GLASS_PRINCIPAL,
            role=ServiceRole.BREAK_GLASS,
            generation_id="g1",
            secret=Secret(BREAK_GLASS_TOKEN),
            expires_at=now + timedelta(hours=1),
            allowed_operations=_all_bg_ops(),
        ),
        CredentialBinding(
            principal_id=EXPIRED_BG_PRINCIPAL,
            role=ServiceRole.BREAK_GLASS,
            generation_id="g1",
            secret=Secret(EXPIRED_BG_TOKEN),
            expires_at=now - timedelta(minutes=1),
            allowed_operations=_all_bg_ops(),
        ),
        CredentialBinding(
            principal_id=NARROW_BG_PRINCIPAL,
            role=ServiceRole.BREAK_GLASS,
            generation_id="g1",
            secret=Secret(NARROW_BG_TOKEN),
            expires_at=now + timedelta(hours=1),
            allowed_operations=frozenset({"reconcileCounters"}),
        ),
    )


def _ownership_break_glass_ops() -> set[str]:
    payload = json.loads(OWNERSHIP_PATH.read_text(encoding="utf-8"))
    return {
        row["operationId"]
        for row in payload["operations"]
        if "BreakGlassClient" in row.get("clients", [])
    }


@pytest.fixture
def sa_engine(migrated_schema):
    _conn, schema = migrated_schema
    database_url = os.environ.get("TEST_DATABASE_URL", "").strip()
    if not database_url:
        pytest.fail("TEST_DATABASE_URL is required for break-glass conformance")
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
def session_factory(sa_engine) -> sessionmaker[Session]:
    return sessionmaker(bind=sa_engine, expire_on_commit=False)


@pytest.fixture
def sa_session(session_factory: sessionmaker[Session]) -> Iterator[Session]:
    session = session_factory()
    try:
        yield session
    finally:
        session.close()


@pytest.fixture(autouse=True)
def _clean_delivery_tables(session_factory: sessionmaker[Session]) -> Iterator[None]:
    """Keep alembic teardown free of leftover delivery claim fence rows."""

    with session_factory() as session:
        with session.begin():
            session.execute(text("DELETE FROM delivery_events_terminal"))
            session.execute(text("DELETE FROM delivery_events_active"))
    yield
    with session_factory() as session:
        with session.begin():
            session.execute(text("DELETE FROM delivery_events_terminal"))
            session.execute(text("DELETE FROM delivery_events_active"))


def _start_admin_server(
    session_factory: sessionmaker[Session],
    sa_engine,
    *,
    queue_name: str,
) -> tuple[str, QuietThreadingHTTPServer, threading.Thread]:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        port = int(probe.getsockname()[1])
    scopes = {
        ADMIN_PRINCIPAL: frozenset({queue_name}),
        OBSERVER_PRINCIPAL: frozenset({queue_name}),
        PRODUCER_PRINCIPAL: frozenset({queue_name}),
        WORKER_PRINCIPAL: frozenset({queue_name}),
        BREAK_GLASS_PRINCIPAL: frozenset({queue_name}),
        EXPIRED_BG_PRINCIPAL: frozenset({queue_name}),
        NARROW_BG_PRINCIPAL: frozenset({queue_name}),
    }
    app = create_admin_app(
        authenticator=BearerCredentialAuthenticator.from_bindings(_bindings()),
        authorizer=Authorizer(queue_scopes=scopes),
        bind=ListenerBind(host="127.0.0.1", port=port),
        session_factory=session_factory,
        repository=QueueControlRepository(),
        engine=sa_engine,
        cursor_secret=Secret("break-glass-live-confirm"),
        payload_retention_policy=PayloadRetentionPolicy(retention_days=30),
    )
    lifecycle = Lifecycle()
    gate = InFlightGate()
    server = QuietThreadingHTTPServer(
        ("127.0.0.1", port),
        AsgiRequestHandler,
        app=app,
        lifecycle=lifecycle,
        gate=gate,
        api_engine=None,
        schema=None,
        premake_days=0,
    )
    lifecycle.mark_running()
    thread = threading.Thread(
        target=server.serve_forever,
        kwargs={"poll_interval": 0.05},
        name="break-glass-live",
        daemon=True,
    )
    thread.start()
    return f"http://127.0.0.1:{port}", server, thread


def _stop_server(server: QuietThreadingHTTPServer, thread: threading.Thread) -> None:
    try:
        server.shutdown()
    except Exception:  # noqa: BLE001
        pass
    try:
        server.server_close()
    except Exception:  # noqa: BLE001
        pass
    thread.join(timeout=2.0)


@pytest.fixture
def bg_world(
    sa_session: Session,
    session_factory: sessionmaker[Session],
    sa_engine,
) -> Iterator[tuple[BreakGlassClientAdapter, str, int]]:
    queue_name = _unique("orders.bg.live")
    queue_pk = _seed_queue(sa_session, name=queue_name)
    url, server, thread = _start_admin_server(
        session_factory, sa_engine, queue_name=queue_name
    )
    adapter = BreakGlassClientAdapter(url)
    try:
        yield adapter, queue_name, queue_pk
    finally:
        _stop_server(server, thread)


def _seed_queue(session: Session, *, name: str) -> int:
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
    return int(
        session.execute(
            text("SELECT id FROM queues WHERE name = :name"),
            {"name": name},
        ).scalar_one()
    )


def _enqueue_and_claim(
    session_factory: sessionmaker[Session],
    *,
    queue_name: str,
) -> tuple[uuid.UUID, uuid.UUID, int]:
    session = session_factory()
    try:
        cmd = normalize_enqueue_command(
            producer_id="producer-bg-live",
            queue_name=queue_name,
            idempotency_key=f"idem-{uuid.uuid4().hex}",
            payload={"secret": "must-not-leak"},
            priority=0,
            available_at=None,
        )
        staged = EnqueueRepository().stage_enqueue(session, cmd)
        session.commit()
        task_id = staged.task_id
    finally:
        session.close()

    claim = ClaimService(session_factory=session_factory).claim(
        queue_name=queue_name,
        worker_id="worker-bg-live-1",
        lease_seconds=120,
    )
    assert claim.task_id == task_id
    assert claim.claim_id is not None
    assert claim.generation is not None
    return task_id, claim.claim_id, int(claim.generation)


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
                :tid, :qid, 'producer-bg-live', 2, 0,
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
) -> UUID:
    event_id = uuid.uuid4()
    now = session.scalar(select(func.transaction_timestamp()))
    assert now is not None
    envelope = {
        "specversion": "1.0",
        "id": str(event_id),
        "source": "urn:test:bg-live",
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
                :now, 3,
                :claim, :now, :now + interval '120 seconds',
                'relay-stuck', 2, NULL,
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
            "claim": uuid.uuid4(),
        },
    )
    session.commit()
    return event_id


def _create_past_child(conn: Connection, parent: str, day: date) -> str:
    spec = partition_catalog.day_spec_for(parent, day)
    ddl = conn.execute(
        text(
            """
            SELECT format(
                'CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
                CAST(:child_name AS text),
                CAST(:parent_name AS text),
                CAST(:bound_from AS timestamptz),
                CAST(:bound_to AS timestamptz)
            )
            """
        ),
        {
            "child_name": spec.child_name,
            "parent_name": parent,
            "bound_from": spec.bound_from,
            "bound_to": spec.bound_to,
        },
    ).scalar_one()
    conn.execute(text(str(ddl)))
    return spec.child_name


def test_manifest_lists_phase_19_break_glass_ops() -> None:
    assert _ownership_break_glass_ops() == set(PHASE_19_BREAK_GLASS_OPS)


def test_force_lease_expiry_success_preserves_fencing(
    bg_world: tuple[BreakGlassClientAdapter, str, int],
    session_factory: sessionmaker[Session],
    sa_session: Session,
) -> None:
    adapter, queue_name, _queue_pk = bg_world
    task_id, _claim_id, generation = _enqueue_and_claim(
        session_factory, queue_name=queue_name
    )
    result = adapter.force_lease_expiry(
        queue_name=queue_name,
        task_id=str(task_id),
        bearer_token=BREAK_GLASS_TOKEN,
        **ACK,
    )
    assert result.ok, result.error_code
    assert result.data["operation"] == "forceLeaseExpiry"
    assert result.data["generation"] == generation
    assert "claim_token" not in result.data

    sa_session.expire_all()
    task = sa_session.execute(
        select(TaskActive).where(TaskActive.task_id == task_id)
    ).scalar_one_or_none()
    if task is not None:
        assert task.current_claim_id is None
        assert int(task.generation) == generation

    audits = list(
        sa_session.execute(
            select(AdminAuditLog).where(AdminAuditLog.operation_code == 9)
        ).scalars()
    )
    assert any(
        dict(a.details).get("incident_reference") == ACK["incident_reference"]
        for a in audits
    )


def test_reconcile_raise_repair_and_delivery_ops(
    bg_world: tuple[BreakGlassClientAdapter, str, int],
    sa_session: Session,
    sa_engine,
) -> None:
    adapter, queue_name, queue_pk = bg_world

    sa_session.execute(
        text(
            """
            UPDATE queue_counters
            SET delayed_count = 9, ready_count = 9, leased_count = 9
            WHERE queue_id = :qid
            """
        ),
        {"qid": queue_pk},
    )
    sa_session.commit()

    reconciled = adapter.reconcile_counters(
        queue_name=queue_name,
        bearer_token=BREAK_GLASS_TOKEN,
        **ACK,
    )
    assert reconciled.ok, reconciled.error_code
    assert reconciled.data["outcome"] == "reconciled"

    raised = adapter.raise_replay_limit(
        queue_name=queue_name,
        bearer_token=BREAK_GLASS_TOKEN,
        factor=2.0,
        ttl_seconds=120,
        **ACK,
    )
    assert raised.ok, raised.error_code
    assert raised.data["outcome"] == "raised"

    policy_id = int(
        sa_session.execute(
            text("SELECT active_policy_version_id FROM queues WHERE id = :id"),
            {"id": queue_pk},
        ).scalar_one()
    )
    source_task = _seed_task(sa_session, queue_id=queue_pk, policy_id=policy_id)
    event_id = _seed_publishing_event(sa_session, source_task_id=source_task)
    reclaimed = adapter.force_delivery_reclaim(
        queue_name=queue_name,
        event_id=str(event_id),
        bearer_token=BREAK_GLASS_TOKEN,
        **ACK,
    )
    assert reclaimed.ok, reclaimed.error_code
    assert "claim_token" not in reclaimed.data
    sa_session.expire_all()
    event = sa_session.execute(
        select(DeliveryEventActive).where(DeliveryEventActive.event_id == event_id)
    ).scalar_one()
    assert int(event.state_code) == STATE_PENDING

    source_task2 = _seed_task(sa_session, queue_id=queue_pk, policy_id=policy_id)
    event_id2 = _seed_publishing_event(sa_session, source_task_id=source_task2)
    dead = adapter.force_delivery_dead_letter(
        queue_name=queue_name,
        event_id=str(event_id2),
        bearer_token=BREAK_GLASS_TOKEN,
        **ACK,
    )
    assert dead.ok, dead.error_code
    assert "claim_token" not in dead.data

    now = datetime.now(timezone.utc)
    sa_session.execute(
        text(
            """
            INSERT INTO enqueue_dedup (
                producer_id, queue_id, key_hash, request_fingerprint, task_id,
                created_at, expires_at
            ) VALUES (
                'producer-bg-live', :qid, :kh, :fp, :task_id,
                :created, :expires
            )
            """
        ),
        {
            "qid": queue_pk,
            "kh": bytes.fromhex("33" * 32),
            "fp": bytes.fromhex("44" * 32),
            "task_id": str(uuid.uuid4()),
            "created": now - timedelta(days=40),
            "expires": now + timedelta(days=10),
        },
    )
    sa_session.commit()
    entry_id = int(
        sa_session.execute(
            text(
                "SELECT id FROM enqueue_dedup WHERE queue_id = :qid ORDER BY id DESC LIMIT 1"
            ),
            {"qid": queue_pk},
        ).scalar_one()
    )
    QueueControlRepository().set_queue_state(
        sa_session,
        queue_name=queue_name,
        mutation=SetQueueStateMutation(
            state=QueueState.PAUSED,
            expected_config_version=ConfigVersion(value=1),
            metadata=AdminRequestMetadata(
                actor_id=ADMIN_PRINCIPAL,
                request_id=str(uuid.uuid4()),
                idempotency_key=f"pause-{uuid.uuid4().hex}",
            ),
        ),
    )
    sa_session.commit()
    repaired = adapter.repair_registry_entry(
        queue_name=queue_name,
        bearer_token=BREAK_GLASS_TOKEN,
        entry_id=entry_id,
        acknowledge_duplicate_window=True,
        **ACK,
    )
    assert repaired.ok, repaired.error_code
    assert repaired.data["outcome"] == "repaired"

    # Expired history partition drop (far past day vs 30d retention).
    day = date.today() - timedelta(days=200)
    with sa_engine.connect() as conn:
        if conn.in_transaction():
            conn.commit()
        child = _create_past_child(conn, "admin_audit_log", day)
        conn.commit()
    dropped = adapter.drop_expired_partition(
        partition_name=child,
        bearer_token=BREAK_GLASS_TOKEN,
        **ACK,
    )
    assert dropped.ok, dropped.error_code
    assert dropped.data["outcome"] == "dropped"


@pytest.mark.parametrize("op_id", PHASE_19_BREAK_GLASS_OPS)
@pytest.mark.parametrize("role_name,token", DENIED_TOKENS, ids=[r for r, _ in DENIED_TOKENS])
def test_ordinary_roles_fail_every_break_glass_op(
    bg_world: tuple[BreakGlassClientAdapter, str, int],
    session_factory: sessionmaker[Session],
    sa_session: Session,
    op_id: str,
    role_name: str,
    token: str,
) -> None:
    adapter, queue_name, queue_pk = bg_world
    result = _invoke_break_glass(
        adapter,
        op_id=op_id,
        queue_name=queue_name,
        queue_pk=queue_pk,
        session_factory=session_factory,
        sa_session=sa_session,
        bearer_token=token,
    )
    assert not result.ok, f"{role_name} must fail {op_id}"
    assert result.error_code in {
        "permission_denied",
        "unauthenticated",
        "forbidden",
    }


def test_expired_and_wrong_audience_fail_closed(
    bg_world: tuple[BreakGlassClientAdapter, str, int],
    session_factory: sessionmaker[Session],
    sa_session: Session,
) -> None:
    adapter, queue_name, queue_pk = bg_world
    expired = _invoke_break_glass(
        adapter,
        op_id="forceLeaseExpiry",
        queue_name=queue_name,
        queue_pk=queue_pk,
        session_factory=session_factory,
        sa_session=sa_session,
        bearer_token=EXPIRED_BG_TOKEN,
    )
    assert not expired.ok
    assert expired.error_code in {"unauthenticated", "permission_denied"}

    wrong_audience = _invoke_break_glass(
        adapter,
        op_id="forceLeaseExpiry",
        queue_name=queue_name,
        queue_pk=queue_pk,
        session_factory=session_factory,
        sa_session=sa_session,
        bearer_token=NARROW_BG_TOKEN,
    )
    assert not wrong_audience.ok
    assert wrong_audience.error_code in {"permission_denied", "forbidden"}


def _invoke_break_glass(
    adapter: BreakGlassClientAdapter,
    *,
    op_id: str,
    queue_name: str,
    queue_pk: int,
    session_factory: sessionmaker[Session],
    sa_session: Session,
    bearer_token: str,
) -> Any:
    if op_id == "forceLeaseExpiry":
        task_id, _c, _g = _enqueue_and_claim(session_factory, queue_name=queue_name)
        return adapter.force_lease_expiry(
            queue_name=queue_name,
            task_id=str(task_id),
            bearer_token=bearer_token,
            **ACK,
        )
    if op_id in {"forceDeliveryReclaim", "forceDeliveryDeadLetter"}:
        policy_id = int(
            sa_session.execute(
                text("SELECT active_policy_version_id FROM queues WHERE id = :id"),
                {"id": queue_pk},
            ).scalar_one()
        )
        source = _seed_task(sa_session, queue_id=queue_pk, policy_id=policy_id)
        event_id = _seed_publishing_event(sa_session, source_task_id=source)
        if op_id == "forceDeliveryReclaim":
            return adapter.force_delivery_reclaim(
                queue_name=queue_name,
                event_id=str(event_id),
                bearer_token=bearer_token,
                **ACK,
            )
        return adapter.force_delivery_dead_letter(
            queue_name=queue_name,
            event_id=str(event_id),
            bearer_token=bearer_token,
            **ACK,
        )
    if op_id == "reconcileCounters":
        return adapter.reconcile_counters(
            queue_name=queue_name, bearer_token=bearer_token, **ACK
        )
    if op_id == "raiseReplayLimit":
        return adapter.raise_replay_limit(
            queue_name=queue_name, bearer_token=bearer_token, **ACK
        )
    if op_id == "dropExpiredPartition":
        return adapter.drop_expired_partition(
            partition_name="admin_audit_log_20000101",
            bearer_token=bearer_token,
            **ACK,
        )
    # repairRegistryEntry
    return adapter.repair_registry_entry(
        queue_name=queue_name,
        bearer_token=bearer_token,
        entry_id=1,
        acknowledge_duplicate_window=True,
        **ACK,
    )
