"""PostgreSQL coverage for single-task dead-letter replay (CTRL-06 / REC-01).

Phase 12 Wave 0 priority-preservation scaffolds are temporarily skipped; Plan 06
removes the markers.
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from collections.abc import Iterator, Mapping
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from sqlalchemy import create_engine, event, select, text
from sqlalchemy.orm import Session, sessionmaker

from workhold.api.admin import create_admin_app
from workhold.api.security import ListenerBind
from workhold.domain.queue_control import (
    AdminRequestMetadata,
    BackoffStrategy,
    CreateQueueMutation,
    QueueState,
    RetryPolicyDraft,
    SetQueueStateMutation,
)
from workhold.infrastructure.postgres.queue_control_repository import (
    QueueControlRepository,
)
from workhold.security.authorization import Authorizer
from workhold.security.credentials import (
    BearerCredentialAuthenticator,
    CredentialBinding,
)
from workhold.security.principals import ServiceRole
from workhold.settings import Secret
from workhold.storage.models import AdminAuditLog, TaskActive, TaskAttempt, TaskTerminal

pytest_plugins = ["tests.integration.conftest"]

OBSERVER_TOKEN = "tok-observer-dlq"
ADMIN_TOKEN = "tok-admin-dlq"
PRODUCER_TOKEN = "tok-producer-dlq"
WORKER_TOKEN = "tok-worker-dlq"

OBSERVER_PRINCIPAL = "observer-dlq"
ADMIN_PRINCIPAL = "admin-dlq"
PRODUCER_PRINCIPAL = "producer-dlq"
WORKER_PRINCIPAL = "worker-dlq"


def _unique(prefix: str) -> str:
    return f"{prefix}.{uuid.uuid4().hex[:8]}"


def _bindings() -> tuple[CredentialBinding, ...]:
    return (
        CredentialBinding(
            principal_id=OBSERVER_PRINCIPAL,
            role=ServiceRole.OBSERVER,
            generation_id="g1",
            secret=Secret(OBSERVER_TOKEN),
        ),
        CredentialBinding(
            principal_id=ADMIN_PRINCIPAL,
            role=ServiceRole.ADMIN,
            generation_id="g1",
            secret=Secret(ADMIN_TOKEN),
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
                OBSERVER_PRINCIPAL: frozenset({queue_name}),
                ADMIN_PRINCIPAL: frozenset({queue_name}),
                PRODUCER_PRINCIPAL: frozenset({queue_name}),
                WORKER_PRINCIPAL: frozenset({queue_name}),
            }
        ),
        bind=ListenerBind(host="127.0.0.1", port=18097),
        session_factory=session_factory,
        repository=QueueControlRepository(),
        engine=sa_engine,
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
        "client": ("127.0.0.1", 9),
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
            for key, value in message.get("headers", []):
                header_box[key.decode("latin-1").lower()] = value.decode("latin-1")
        elif message["type"] == "http.response.body":
            body_chunks.append(message.get("body", b"") or b"")

    asyncio.run(app(scope, receive, send))
    return status_box["status"], header_box, b"".join(body_chunks)


def _seed_queue(session: Session, *, name: str) -> int:
    repo = QueueControlRepository()
    repo.create_named_queue(
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


def _insert_dead_letter(
    session: Session,
    *,
    queue_pk: int,
    payload: dict[str, Any] | None = None,
    priority: int = 0,
) -> uuid.UUID:
    task_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    body = payload or {"secret": "should-not-leak", "n": 1}
    payload_bytes = len(json.dumps(body, separators=(",", ":")).encode("utf-8"))
    session.execute(
        text(
            """
            INSERT INTO tasks_terminal (
                task_id, queue_id, producer_id, state_code, priority,
                available_at, retry_policy_version, payload, payload_bytes,
                created_at, terminal_at, failure_code, failure_detail
            ) VALUES (
                :task_id, :queue_id, :producer_id, 11, :priority,
                :now, 1, CAST(:payload AS jsonb), :payload_bytes,
                :now, :now, 'exhausted', 'retries exhausted'
            )
            """
        ),
        {
            "task_id": str(task_id),
            "queue_id": queue_pk,
            "producer_id": "producer-dlq",
            "priority": priority,
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
                :task_id, :claim_id, 1, :now, 'worker-dlq',
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


def _snapshot_source(session: Session, task_id: uuid.UUID) -> tuple[dict[str, Any], list[dict[str, Any]]]:
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


def test_replay_creates_linked_task_preserves_source_and_is_idempotent(
    sa_session: Session,
    session_factory: sessionmaker[Session],
    sa_engine,
) -> None:
    queue_name = _unique("orders.dlq")
    admin_app = _make_admin_app(session_factory, sa_engine, queue_name=queue_name)
    queue_pk = _seed_queue(sa_session, name=queue_name)
    source_id = _insert_dead_letter(sa_session, queue_pk=queue_pk)
    before_terminal, before_attempts = _snapshot_source(sa_session, source_id)

    path = f"/admin/v1/queues/{queue_name}/dead-letters/{source_id}:replay"
    body = json.dumps({"reason": "fixed poison handler"}).encode("utf-8")
    headers = {
        "authorization": f"Bearer {ADMIN_TOKEN}",
        "idempotency-key": "replay-key-1",
        "content-type": "application/json",
    }

    status, _hdrs, raw = _asgi_http_call(
        admin_app, method="POST", path=path, headers=headers, body=body
    )
    assert status == 200, raw
    first = json.loads(raw.decode("utf-8"))
    assert first["source_task_id"] == str(source_id)
    assert first["task_id"] != str(source_id)
    assert first["queue"] == queue_name
    assert first["replayed"] is False
    assert first["policy_version"] >= 1
    assert "at-least-once" in first["warning"].lower()
    assert "claim_token" not in first
    assert "payload" not in first

    new_task = sa_session.execute(
        select(TaskActive).where(TaskActive.task_id == uuid.UUID(first["task_id"]))
    ).scalar_one()
    assert new_task.source_task_id == source_id
    assert new_task.spawn_ordinal == 0
    assert int(new_task.state_code) == 2  # ready
    assert "claim_token" not in dir(new_task) or getattr(new_task, "current_claim_id", None) is None

    after_terminal, after_attempts = _snapshot_source(sa_session, source_id)
    assert after_terminal == before_terminal
    assert after_attempts == before_attempts

    status2, _hdrs2, raw2 = _asgi_http_call(
        admin_app, method="POST", path=path, headers=headers, body=body
    )
    assert status2 == 200, raw2
    second = json.loads(raw2.decode("utf-8"))
    assert second["task_id"] == first["task_id"]
    assert second["replayed"] is True

    # Same key + different fingerprint conflicts.
    conflict_body = json.dumps({"reason": "different reason"}).encode("utf-8")
    status3, _hdrs3, raw3 = _asgi_http_call(
        admin_app, method="POST", path=path, headers=headers, body=conflict_body
    )
    assert status3 == 409, raw3
    err = json.loads(raw3.decode("utf-8"))
    assert err["code"] == "idempotency_conflict"

    # Source still unchanged after conflict.
    final_terminal, final_attempts = _snapshot_source(sa_session, source_id)
    assert final_terminal == before_terminal
    assert final_attempts == before_attempts

    # Session-scoped migrated_schema retains prior bulk-replay audits (op 6
    # per item). Scope to this source_task_id rather than audits[0].
    audits = [
        row
        for row in sa_session.execute(
            select(AdminAuditLog).where(AdminAuditLog.operation_code == 6)
        ).scalars()
        if row.details.get("source_task_id") == str(source_id)
    ]
    assert len(audits) >= 1
    assert audits[0].details.get("task_id") == first["task_id"]


def test_replay_rejects_non_dead_letter_drain_and_non_admin(
    sa_session: Session,
    session_factory: sessionmaker[Session],
    sa_engine,
) -> None:
    queue_name = _unique("orders.dlq")
    admin_app = _make_admin_app(session_factory, sa_engine, queue_name=queue_name)
    queue_pk = _seed_queue(sa_session, name=queue_name)
    # Succeeded terminal (state 10) is not replayable as dead letter.
    task_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    sa_session.execute(
        text(
            """
            INSERT INTO tasks_terminal (
                task_id, queue_id, producer_id, state_code, priority,
                available_at, retry_policy_version, payload, payload_bytes,
                created_at, terminal_at
            ) VALUES (
                :task_id, :queue_id, 'producer-dlq', 10, 0,
                :now, 1, CAST(:payload AS jsonb), :payload_bytes,
                :now, :now
            )
            """
        ),
        {
            "task_id": str(task_id),
            "queue_id": queue_pk,
            "now": now,
            "payload": '{"ok":true}',
            "payload_bytes": 10,
        },
    )
    sa_session.commit()

    path = f"/admin/v1/queues/{queue_name}/dead-letters/{task_id}:replay"
    body = json.dumps({"reason": "nope"}).encode("utf-8")
    status, _h, raw = _asgi_http_call(
        admin_app,
        method="POST",
        path=path,
        headers={
            "authorization": f"Bearer {ADMIN_TOKEN}",
            "idempotency-key": "k-missing",
            "content-type": "application/json",
        },
        body=body,
    )
    assert status == 404, raw
    assert json.loads(raw.decode("utf-8"))["code"] == "task_not_found"

    # Drain gate.
    source_id = _insert_dead_letter(sa_session, queue_pk=queue_pk)
    repo = QueueControlRepository()
    cfg = repo.get_queue_configuration(sa_session, name=queue_name)
    assert cfg is not None
    repo.set_queue_state(
        sa_session,
        queue_name=queue_name,
        mutation=SetQueueStateMutation(
            state=QueueState.DRAINING,
            expected_config_version=cfg.config_version,
            metadata=AdminRequestMetadata(
                actor_id=ADMIN_PRINCIPAL,
                request_id=str(uuid.uuid4()),
                idempotency_key=f"drain-{uuid.uuid4().hex}",
            ),
        ),
    )
    sa_session.commit()

    drain_path = f"/admin/v1/queues/{queue_name}/dead-letters/{source_id}:replay"
    status_d, _hd, raw_d = _asgi_http_call(
        admin_app,
        method="POST",
        path=drain_path,
        headers={
            "authorization": f"Bearer {ADMIN_TOKEN}",
            "idempotency-key": "k-drain",
            "content-type": "application/json",
        },
        body=body,
    )
    assert status_d == 409, raw_d
    assert json.loads(raw_d.decode("utf-8"))["code"] == "queue_draining"

    # Non-admin denied.
    status_p, _hp, raw_p = _asgi_http_call(
        admin_app,
        method="POST",
        path=drain_path,
        headers={
            "authorization": f"Bearer {PRODUCER_TOKEN}",
            "idempotency-key": "k-prod",
            "content-type": "application/json",
        },
        body=body,
    )
    assert status_p == 403, raw_p
    assert json.loads(raw_p.decode("utf-8"))["code"] == "permission_denied"


# ---------------------------------------------------------------------------
# Phase 12 Wave 0 scaffolds (WORK-16 dead-letter replay priority preservation)
# ---------------------------------------------------------------------------

_SOURCE_PRIORITY_MIN = -32768
_SOURCE_PRIORITY_MAX = 32767


def test_single_replay_preserves_source_priority_without_override(
    sa_session: Session,
    session_factory: sessionmaker[Session],
    sa_engine,
) -> None:
    queue_name = _unique("orders.dlq.priority")
    admin_app = _make_admin_app(session_factory, sa_engine, queue_name=queue_name)
    queue_pk = _seed_queue(sa_session, name=queue_name)
    source_id = _insert_dead_letter(
        sa_session,
        queue_pk=queue_pk,
        priority=_SOURCE_PRIORITY_MAX,
    )

    path = f"/admin/v1/queues/{queue_name}/dead-letters/{source_id}:replay"
    body = json.dumps({"reason": "preserve priority on replay"}).encode("utf-8")
    headers = {
        "authorization": f"Bearer {ADMIN_TOKEN}",
        "idempotency-key": f"replay-priority-{uuid.uuid4().hex[:8]}",
        "content-type": "application/json",
    }
    status, _hdrs, raw = _asgi_http_call(
        admin_app, method="POST", path=path, headers=headers, body=body
    )
    assert status == 200, raw
    result = json.loads(raw.decode("utf-8"))
    assert result["replayed"] is False
    assert "priority" not in result

    new_task = sa_session.execute(
        select(TaskActive).where(TaskActive.task_id == uuid.UUID(result["task_id"]))
    ).scalar_one()
    assert int(new_task.priority) == _SOURCE_PRIORITY_MAX
    assert new_task.source_task_id == source_id


def test_bulk_replay_preserves_each_source_priority_without_override(
    sa_session: Session,
    session_factory: sessionmaker[Session],
    sa_engine,
) -> None:
    queue_name = _unique("orders.dlq.bulk.priority")
    admin_app = _make_admin_app(session_factory, sa_engine, queue_name=queue_name)
    queue_pk = _seed_queue(sa_session, name=queue_name)
    source_low = _insert_dead_letter(
        sa_session,
        queue_pk=queue_pk,
        priority=_SOURCE_PRIORITY_MIN,
        payload={"tier": "low"},
    )
    source_high = _insert_dead_letter(
        sa_session,
        queue_pk=queue_pk,
        priority=_SOURCE_PRIORITY_MAX,
        payload={"tier": "high"},
    )

    now = datetime.now(timezone.utc)
    frm = (now - timedelta(hours=1)).isoformat().replace("+00:00", "Z")
    to = (now + timedelta(hours=1)).isoformat().replace("+00:00", "Z")
    filters = {"from": frm, "to": to, "failure_code": "exhausted"}

    preview_path = f"/admin/v1/queues/{queue_name}/bulk:preview-replay"
    status, _h, raw = _asgi_http_call(
        admin_app,
        method="POST",
        path=preview_path,
        headers={
            "authorization": f"Bearer {ADMIN_TOKEN}",
            "content-type": "application/json",
        },
        body=json.dumps({"filters": filters}).encode("utf-8"),
    )
    assert status == 200, raw
    preview = json.loads(raw.decode("utf-8"))
    token = preview["confirmation_token"]
    assert token

    execute_path = f"/admin/v1/queues/{queue_name}/bulk:execute-replay"
    exec_body = {
        "confirmation_token": token,
        "filters": filters,
        "reason": "bulk preserve source priority",
        "start_index": 0,
        "batch_limit": 2,
    }
    status2, _h2, raw2 = _asgi_http_call(
        admin_app,
        method="POST",
        path=execute_path,
        headers={
            "authorization": f"Bearer {ADMIN_TOKEN}",
            "idempotency-key": f"bulk-priority-{uuid.uuid4().hex[:8]}",
            "content-type": "application/json",
        },
        body=json.dumps(exec_body).encode("utf-8"),
    )
    assert status2 == 200, raw2
    batch = json.loads(raw2.decode("utf-8"))
    assert batch["succeeded"] == 2
    assert "priority" not in batch

    replayed = {
        row.source_task_id: int(row.priority)
        for row in sa_session.execute(
            select(TaskActive).where(TaskActive.queue_id == queue_pk)
        ).scalars()
    }
    assert replayed[source_low] == _SOURCE_PRIORITY_MIN
    assert replayed[source_high] == _SOURCE_PRIORITY_MAX
