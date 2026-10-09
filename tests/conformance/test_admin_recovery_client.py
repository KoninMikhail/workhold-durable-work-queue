"""Live AdminClient recovery conformance (Phase 19 / SDK-07 / REC-01 / REC-02).

Manifest-driven success + negative authorization for replay and bulk preview/execute.
Uses ``AdminClientAdapter`` against a real admin-plane HTTP server and PostgreSQL.
"""

from __future__ import annotations

import json
import os
import socket
import threading
import uuid
from collections.abc import Iterator, Mapping
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import create_engine, event, select, text
from sqlalchemy.orm import Session, sessionmaker

from queue_service.api.admin import create_admin_app
from queue_service.api.security import ListenerBind
from queue_service.domain.queue_control import (
    AdminRequestMetadata,
    BackoffStrategy,
    CreateQueueMutation,
    RetryPolicyDraft,
)
from queue_service.infrastructure.postgres.queue_control_repository import (
    QueueControlRepository,
)
from queue_service.lifecycle import Lifecycle
from queue_service.roles.api import AsgiRequestHandler, InFlightGate, QuietThreadingHTTPServer
from queue_service.security.authorization import Authorizer
from queue_service.security.credentials import (
    BearerCredentialAuthenticator,
    CredentialBinding,
)
from queue_service.security.principals import ServiceRole
from queue_service.settings import Secret
from queue_service.storage.models import TaskActive, TaskAttempt, TaskTerminal
from queue_service_admin.models import BulkPreviewResult
from tests.conformance.clients import (
    PHASE_19_ADMIN_RECOVERY_OPS,
    AdminClientAdapter,
)

pytest_plugins = ["tests.integration.conftest"]

REPO_ROOT = Path(__file__).resolve().parents[2]
OWNERSHIP_PATH = REPO_ROOT / "packages" / "client-operation-ownership.json"

ADMIN_TOKEN = "tok-admin-recovery-live"
OBSERVER_TOKEN = "tok-observer-recovery-live"
PRODUCER_TOKEN = "tok-producer-recovery-live"
WORKER_TOKEN = "tok-worker-recovery-live"

ADMIN_PRINCIPAL = "admin-recovery-live"
OBSERVER_PRINCIPAL = "observer-recovery-live"
PRODUCER_PRINCIPAL = "producer-recovery-live"
WORKER_PRINCIPAL = "worker-recovery-live"

DENIED_TOKENS = (
    ("observer", OBSERVER_TOKEN),
    ("producer", PRODUCER_TOKEN),
    ("worker", WORKER_TOKEN),
)


def _unique(prefix: str) -> str:
    return f"{prefix}.{uuid.uuid4().hex[:8]}"


def _bindings() -> tuple[CredentialBinding, ...]:
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
    )


def _ownership_admin_recovery_ops() -> set[str]:
    payload = json.loads(OWNERSHIP_PATH.read_text(encoding="utf-8"))
    ops = {
        row["operationId"]
        for row in payload["operations"]
        if "AdminClient" in row.get("clients", [])
        and row["operationId"] in PHASE_19_ADMIN_RECOVERY_OPS
    }
    return ops


@pytest.fixture
def sa_engine(migrated_schema):
    _conn, schema = migrated_schema
    database_url = os.environ.get("TEST_DATABASE_URL", "").strip()
    if not database_url:
        pytest.fail("TEST_DATABASE_URL is required for admin recovery conformance")
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


def _start_admin_server(
    session_factory: sessionmaker[Session],
    sa_engine,
    *,
    queue_name: str,
) -> tuple[str, QuietThreadingHTTPServer, threading.Thread]:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        port = int(probe.getsockname()[1])
    app = create_admin_app(
        authenticator=BearerCredentialAuthenticator.from_bindings(_bindings()),
        authorizer=Authorizer(
            queue_scopes={
                ADMIN_PRINCIPAL: frozenset({queue_name}),
                OBSERVER_PRINCIPAL: frozenset({queue_name}),
                PRODUCER_PRINCIPAL: frozenset({queue_name}),
                WORKER_PRINCIPAL: frozenset({queue_name}),
            }
        ),
        bind=ListenerBind(host="127.0.0.1", port=port),
        session_factory=session_factory,
        repository=QueueControlRepository(),
        engine=sa_engine,
        cursor_secret=Secret("admin-recovery-live-confirm"),
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
        name="admin-recovery-live",
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
def recovery_world(
    sa_session: Session,
    session_factory: sessionmaker[Session],
    sa_engine,
) -> Iterator[tuple[str, AdminClientAdapter, str, int]]:
    queue_name = _unique("orders.recovery")
    queue_pk = _seed_queue(sa_session, name=queue_name)
    url, server, thread = _start_admin_server(
        session_factory, sa_engine, queue_name=queue_name
    )
    adapter = AdminClientAdapter(url)
    try:
        yield url, adapter, queue_name, queue_pk
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


def _insert_dead_letter(session: Session, *, queue_pk: int) -> uuid.UUID:
    task_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    body = {"secret": "must-not-leak", "n": 1}
    payload_bytes = len(json.dumps(body, separators=(",", ":")).encode("utf-8"))
    session.execute(
        text(
            """
            INSERT INTO tasks_terminal (
                task_id, queue_id, producer_id, state_code, priority,
                available_at, retry_policy_version, payload, payload_bytes,
                created_at, terminal_at, failure_code, failure_detail
            ) VALUES (
                :task_id, :queue_id, :producer_id, 11, 0,
                :now, 1, CAST(:payload AS jsonb), :payload_bytes,
                :now, :now, 'exhausted', 'retries exhausted'
            )
            """
        ),
        {
            "task_id": str(task_id),
            "queue_id": queue_pk,
            "producer_id": PRODUCER_PRINCIPAL,
            "now": now,
            "payload": json.dumps(body),
            "payload_bytes": payload_bytes,
        },
    )
    session.execute(
        text(
            """
            INSERT INTO task_attempts (
                task_id, claim_id, generation, claimed_at, worker_id,
                lease_expires_at, ended_at, outcome_code, failure_code, failure_detail
            ) VALUES (
                :task_id, :claim_id, 1, :now, 'worker-recovery',
                :lease_expires, :now, 4, 'exhausted', 'retries exhausted'
            )
            """
        ),
        {
            "task_id": str(task_id),
            "claim_id": str(uuid.uuid4()),
            "now": now,
            "lease_expires": now + timedelta(seconds=30),
        },
    )
    session.commit()
    return task_id


def _insert_ready(session: Session, *, queue_pk: int, policy_id: int) -> uuid.UUID:
    task_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    session.execute(
        text(
            """
            INSERT INTO tasks_active (
                task_id, queue_id, producer_id, state_code, priority,
                available_at, retry_policy_version_id, generation,
                created_at, updated_at
            ) VALUES (
                :task_id, :queue_id, :producer_id, 2, 0,
                :now, :policy_id, 0, :now, :now
            )
            """
        ),
        {
            "task_id": str(task_id),
            "queue_id": queue_pk,
            "producer_id": PRODUCER_PRINCIPAL,
            "policy_id": policy_id,
            "now": now,
        },
    )
    internal_id = session.execute(
        text("SELECT id FROM tasks_active WHERE task_id = :task_id"),
        {"task_id": str(task_id)},
    ).scalar_one()
    body = {"secret": "active"}
    session.execute(
        text(
            """
            INSERT INTO task_payloads_active (task_id, payload, payload_bytes)
            VALUES (:task_id, CAST(:payload AS jsonb), :payload_bytes)
            """
        ),
        {
            "task_id": int(internal_id),
            "payload": json.dumps(body),
            "payload_bytes": len(json.dumps(body).encode("utf-8")),
        },
    )
    session.commit()
    return task_id


def _snapshot_source(
    session: Session, task_id: uuid.UUID
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    terminal = session.execute(
        select(TaskTerminal)
        .where(TaskTerminal.task_id == task_id)
        .order_by(TaskTerminal.terminal_at.desc())
        .limit(1)
    ).scalar_one()
    terminal_snap = {
        "task_id": str(terminal.task_id),
        "state_code": int(terminal.state_code),
        "payload": dict(terminal.payload),
        "payload_bytes": int(terminal.payload_bytes),
        "failure_code": terminal.failure_code,
        "failure_detail": terminal.failure_detail,
        "terminal_at": terminal.terminal_at,
    }
    attempts = list(
        session.execute(
            select(TaskAttempt)
            .where(TaskAttempt.task_id == task_id)
            .order_by(TaskAttempt.claimed_at, TaskAttempt.id)
        ).scalars()
    )
    attempt_snaps = [
        {
            "id": int(a.id),
            "outcome_code": int(a.outcome_code),
            "failure_code": a.failure_code,
            "failure_detail": a.failure_detail,
            "claimed_at": a.claimed_at,
        }
        for a in attempts
    ]
    return terminal_snap, attempt_snaps


def _time_window() -> dict[str, str]:
    now = datetime.now(timezone.utc)
    return {
        "from": (now - timedelta(hours=1)).isoformat().replace("+00:00", "Z"),
        "to": (now + timedelta(hours=1)).isoformat().replace("+00:00", "Z"),
    }


def test_manifest_lists_phase_19_admin_recovery_ops() -> None:
    owned = _ownership_admin_recovery_ops()
    assert owned == set(PHASE_19_ADMIN_RECOVERY_OPS)


def test_replay_creates_linked_task_without_mutating_source(
    recovery_world: tuple[str, AdminClientAdapter, str, int],
    sa_session: Session,
) -> None:
    _url, adapter, queue_name, queue_pk = recovery_world
    source_id = _insert_dead_letter(sa_session, queue_pk=queue_pk)
    before_terminal, before_attempts = _snapshot_source(sa_session, source_id)

    result = adapter.replay_dead_letter(
        queue_name=queue_name,
        task_id=str(source_id),
        bearer_token=ADMIN_TOKEN,
        idempotency_key=f"replay-{uuid.uuid4().hex}",
        reason="fixed poison handler",
    )
    assert result.ok, result.error_code
    assert result.data["source_task_id"] == str(source_id)
    assert result.data["task_id"] != str(source_id)
    assert "at-least-once" in str(result.data.get("warning", "")).lower()
    assert "claim_token" not in result.data

    sa_session.expire_all()
    new_task = sa_session.execute(
        select(TaskActive).where(TaskActive.task_id == uuid.UUID(result.data["task_id"]))
    ).scalar_one()
    assert new_task.source_task_id == source_id

    after_terminal, after_attempts = _snapshot_source(sa_session, source_id)
    assert after_terminal == before_terminal
    assert after_attempts == before_attempts


def test_bulk_execute_requires_valid_preview_confirmation(
    recovery_world: tuple[str, AdminClientAdapter, str, int],
    sa_session: Session,
) -> None:
    _url, adapter, queue_name, queue_pk = recovery_world
    _insert_dead_letter(sa_session, queue_pk=queue_pk)
    filters = {**_time_window(), "failure_code": "exhausted"}

    denied = adapter.execute_bulk_replay(
        queue_name=queue_name,
        bearer_token=ADMIN_TOKEN,
        preview=BulkPreviewResult(
            operation=__import__(
                "queue_service_admin.models", fromlist=["BulkOperation"]
            ).BulkOperation.parse("bulk_replay"),
            queue=queue_name,
            candidate_count=1,
            truncated=False,
            sample_task_ids=(str(uuid.uuid4()),),
            confirmation_token="not-a-real-confirmation-token",
            confirmation_expires_at=datetime.now(timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
            max_batch=25,
        ),
        idempotency_key=f"bulk-{uuid.uuid4().hex}",
        reason="attempt without preview",
        filters=filters,
    )
    assert not denied.ok
    assert denied.error_code in {
        "validation_failed",
        "confirmation_invalid",
        "confirmation_expired",
        "permission_denied",
    }

    preview = adapter.preview_bulk_replay(
        queue_name=queue_name,
        bearer_token=ADMIN_TOKEN,
        filters=filters,
    )
    assert preview.ok, preview.error_code
    assert isinstance(preview.typed, BulkPreviewResult)
    assert preview.typed.confirmation_token
    assert preview.typed.candidate_count >= 1

    executed = adapter.execute_bulk_replay(
        queue_name=queue_name,
        bearer_token=ADMIN_TOKEN,
        preview=preview.typed,
        idempotency_key=f"bulk-{uuid.uuid4().hex}",
        reason="bounded bulk replay",
        filters=filters,
        batch_limit=25,
    )
    assert executed.ok, executed.error_code
    assert executed.data.get("processed", executed.data.get("succeeded", 0)) >= 1


def test_bulk_cancel_preview_and_execute(
    recovery_world: tuple[str, AdminClientAdapter, str, int],
    sa_session: Session,
) -> None:
    _url, adapter, queue_name, queue_pk = recovery_world
    policy_id = int(
        sa_session.execute(
            text("SELECT active_policy_version_id FROM queues WHERE id = :id"),
            {"id": queue_pk},
        ).scalar_one()
    )
    _insert_ready(sa_session, queue_pk=queue_pk, policy_id=policy_id)
    filters = {**_time_window(), "state": "ready"}

    preview = adapter.preview_bulk_cancel(
        queue_name=queue_name,
        bearer_token=ADMIN_TOKEN,
        filters=filters,
    )
    assert preview.ok, preview.error_code
    assert isinstance(preview.typed, BulkPreviewResult)

    executed = adapter.execute_bulk_cancel(
        queue_name=queue_name,
        bearer_token=ADMIN_TOKEN,
        preview=preview.typed,
        reason="bounded bulk cancel",
        filters=filters,
        batch_limit=25,
    )
    assert executed.ok, executed.error_code


@pytest.mark.parametrize("op_id", PHASE_19_ADMIN_RECOVERY_OPS)
@pytest.mark.parametrize("role_name,token", DENIED_TOKENS, ids=[r for r, _ in DENIED_TOKENS])
def test_non_admin_denied_for_every_recovery_op(
    recovery_world: tuple[str, AdminClientAdapter, str, int],
    sa_session: Session,
    op_id: str,
    role_name: str,
    token: str,
) -> None:
    _url, adapter, queue_name, queue_pk = recovery_world
    source_id = _insert_dead_letter(sa_session, queue_pk=queue_pk)
    filters = {**_time_window(), "failure_code": "exhausted"}
    fake_preview = BulkPreviewResult(
        operation=__import__(
            "queue_service_admin.models", fromlist=["BulkOperation"]
        ).BulkOperation.parse(
            "bulk_replay" if "Replay" in op_id else "bulk_cancel"
        ),
        queue=queue_name,
        candidate_count=1,
        truncated=False,
        sample_task_ids=(str(source_id),),
        confirmation_token="denied-token",
        confirmation_expires_at=datetime.now(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z"),
        max_batch=25,
    )

    if op_id == "replayDeadLetter":
        result = adapter.replay_dead_letter(
            queue_name=queue_name,
            task_id=str(source_id),
            bearer_token=token,
            idempotency_key=f"deny-{uuid.uuid4().hex}",
            reason="should fail",
        )
    elif op_id == "previewBulkReplay":
        result = adapter.preview_bulk_replay(
            queue_name=queue_name, bearer_token=token, filters=filters
        )
    elif op_id == "executeBulkReplay":
        result = adapter.execute_bulk_replay(
            queue_name=queue_name,
            bearer_token=token,
            preview=fake_preview,
            idempotency_key=f"deny-{uuid.uuid4().hex}",
            reason="should fail",
            filters=filters,
        )
    elif op_id == "previewBulkCancel":
        result = adapter.preview_bulk_cancel(
            queue_name=queue_name, bearer_token=token, filters=filters
        )
    else:
        result = adapter.execute_bulk_cancel(
            queue_name=queue_name,
            bearer_token=token,
            preview=fake_preview,
            reason="should fail",
            filters=filters,
        )

    assert not result.ok, f"{role_name} must fail {op_id}"
    assert result.error_code in {
        "permission_denied",
        "unauthenticated",
        "forbidden",
    }
