"""PostgreSQL coverage for guarded bulk replay/cancel (REC-02)."""

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
from queue_service.operations.bulk import BULK_EXECUTE_BATCH_MAX, BulkConfirmationCodec
from queue_service.security.authorization import Authorizer
from queue_service.security.credentials import (
    BearerCredentialAuthenticator,
    CredentialBinding,
)
from queue_service.security.principals import ServiceRole
from queue_service.settings import Secret
from queue_service.storage.models import AdminAuditLog, TaskActive, TaskTerminal

pytest_plugins = ["tests.integration.conftest"]

OBSERVER_TOKEN = "tok-observer-bulk"
ADMIN_TOKEN = "tok-admin-bulk"
PRODUCER_TOKEN = "tok-producer-bulk"

OBSERVER_PRINCIPAL = "observer-bulk"
ADMIN_PRINCIPAL = "admin-bulk"
PRODUCER_PRINCIPAL = "producer-bulk"


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
            }
        ),
        bind=ListenerBind(host="127.0.0.1", port=18098),
        session_factory=session_factory,
        repository=QueueControlRepository(),
        engine=sa_engine,
        cursor_secret=Secret("bulk-confirm-test-secret"),
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
    failure_code: str = "exhausted",
    terminal_at: datetime | None = None,
) -> uuid.UUID:
    task_id = uuid.uuid4()
    now = terminal_at or datetime.now(timezone.utc)
    body = {"secret": "should-not-leak", "n": 1}
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
                :now, :now, :failure_code, 'retries exhausted'
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
            "failure_code": failure_code,
        },
    )
    session.commit()
    return task_id


def _insert_active(
    session: Session,
    *,
    queue_pk: int,
    state_code: int,
    policy_version_id: int,
) -> uuid.UUID:
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
                :task_id, :queue_id, :producer_id, :state_code, 0,
                :now, :policy_id, 0, :now, :now
            )
            """
        ),
        {
            "task_id": str(task_id),
            "queue_id": queue_pk,
            "producer_id": PRODUCER_PRINCIPAL,
            "state_code": state_code,
            "policy_id": policy_version_id,
            "now": now,
        },
    )
    body = {"secret": "active-payload"}
    payload_bytes = len(json.dumps(body, separators=(",", ":")).encode("utf-8"))
    internal_id = session.execute(
        text("SELECT id FROM tasks_active WHERE task_id = :task_id"),
        {"task_id": str(task_id)},
    ).scalar_one()
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
            "payload_bytes": payload_bytes,
        },
    )
    if state_code == 3:  # leased
        session.execute(
            text(
                """
                UPDATE tasks_active
                SET current_claim_id = :claim_id,
                    claimed_at = :now,
                    lease_expires_at = :lease_expires,
                    worker_id = 'worker-bulk',
                    generation = 1
                WHERE task_id = :task_id
                """
            ),
            {
                "task_id": str(task_id),
                "claim_id": str(uuid.uuid4()),
                "now": now,
                "lease_expires": now + timedelta(seconds=60),
            },
        )
    session.commit()
    return task_id


def _policy_id(session: Session, queue_pk: int) -> int:
    return int(
        session.execute(
            text(
                """
                SELECT active_policy_version_id FROM queues WHERE id = :id
                """
            ),
            {"id": queue_pk},
        ).scalar_one()
    )


def _time_window() -> tuple[str, str]:
    now = datetime.now(timezone.utc)
    frm = (now - timedelta(hours=1)).isoformat().replace("+00:00", "Z")
    to = (now + timedelta(hours=1)).isoformat().replace("+00:00", "Z")
    return frm, to


def test_bulk_replay_preview_execute_partial_and_idempotent(
    sa_session: Session,
    session_factory: sessionmaker[Session],
    sa_engine,
) -> None:
    queue_name = _unique("orders.bulk")
    app = _make_admin_app(session_factory, sa_engine, queue_name=queue_name)
    queue_pk = _seed_queue(sa_session, name=queue_name)
    sources = [_insert_dead_letter(sa_session, queue_pk=queue_pk) for _ in range(3)]
    frm, to = _time_window()
    filters = {"from": frm, "to": to, "failure_code": "exhausted"}

    preview_path = f"/admin/v1/queues/{queue_name}/bulk:preview-replay"
    status, _h, raw = _asgi_http_call(
        app,
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
    assert preview["candidate_count"] == 3
    assert preview["truncated"] is False
    assert preview["max_batch"] == BULK_EXECUTE_BATCH_MAX
    token = preview["confirmation_token"]
    assert token
    assert set(preview["sample_task_ids"]) <= {str(s) for s in sources}

    # Forbidden search filter.
    bad_status, _bh, bad_raw = _asgi_http_call(
        app,
        method="POST",
        path=preview_path,
        headers={
            "authorization": f"Bearer {ADMIN_TOKEN}",
            "content-type": "application/json",
        },
        body=json.dumps({"filters": {**filters, "search": "poison"}}).encode("utf-8"),
    )
    assert bad_status == 400, bad_raw
    assert json.loads(bad_raw.decode("utf-8"))["code"] == "validation_failed"

    execute_path = f"/admin/v1/queues/{queue_name}/bulk:execute-replay"
    exec_body = {
        "confirmation_token": token,
        "filters": filters,
        "reason": "bulk recover poison",
        "start_index": 0,
        "batch_limit": 2,
    }
    status2, _h2, raw2 = _asgi_http_call(
        app,
        method="POST",
        path=execute_path,
        headers={
            "authorization": f"Bearer {ADMIN_TOKEN}",
            "idempotency-key": "bulk-replay-1",
            "content-type": "application/json",
        },
        body=json.dumps(exec_body).encode("utf-8"),
    )
    assert status2 == 200, raw2
    first = json.loads(raw2.decode("utf-8"))
    assert first["processed"] == 2
    assert first["succeeded"] == 2
    assert first["partial"] is True
    assert first["next_start_index"] == 2
    assert "at-least-once" in (first.get("warning") or "").lower()

    # Retry same batch → per-item idempotent replayed/skipped, no duplicate tasks.
    status3, _h3, raw3 = _asgi_http_call(
        app,
        method="POST",
        path=execute_path,
        headers={
            "authorization": f"Bearer {ADMIN_TOKEN}",
            "idempotency-key": "bulk-replay-1",
            "content-type": "application/json",
        },
        body=json.dumps(exec_body).encode("utf-8"),
    )
    assert status3 == 200, raw3
    retry = json.loads(raw3.decode("utf-8"))
    assert retry["succeeded"] == 0
    assert retry["skipped"] == 2
    assert all(item["outcome"] == "replayed" for item in retry["outcomes"])

    active_count = sa_session.execute(
        select(TaskActive).where(TaskActive.queue_id == queue_pk)
    ).scalars().all()
    assert len(active_count) == 2

    # Finish remaining batch.
    exec_body2 = {**exec_body, "start_index": 2, "batch_limit": 2}
    status4, _h4, raw4 = _asgi_http_call(
        app,
        method="POST",
        path=execute_path,
        headers={
            "authorization": f"Bearer {ADMIN_TOKEN}",
            "idempotency-key": "bulk-replay-2",
            "content-type": "application/json",
        },
        body=json.dumps(exec_body2).encode("utf-8"),
    )
    assert status4 == 200, raw4
    second = json.loads(raw4.decode("utf-8"))
    assert second["partial"] is False
    assert second["succeeded"] == 1

    # Filter-changed execute rejected.
    changed = {**exec_body, "filters": {**filters, "failure_code": "other"}}
    status5, _h5, raw5 = _asgi_http_call(
        app,
        method="POST",
        path=execute_path,
        headers={
            "authorization": f"Bearer {ADMIN_TOKEN}",
            "idempotency-key": "bulk-replay-3",
            "content-type": "application/json",
        },
        body=json.dumps(changed).encode("utf-8"),
    )
    assert status5 == 400, raw5
    assert json.loads(raw5.decode("utf-8"))["code"] == "validation_failed"

    # Tampered token rejected.
    tampered = {**exec_body, "confirmation_token": token[:-4] + "aaaa"}
    status6, _h6, raw6 = _asgi_http_call(
        app,
        method="POST",
        path=execute_path,
        headers={
            "authorization": f"Bearer {ADMIN_TOKEN}",
            "idempotency-key": "bulk-replay-4",
            "content-type": "application/json",
        },
        body=json.dumps(tampered).encode("utf-8"),
    )
    assert status6 == 400, raw6
    assert json.loads(raw6.decode("utf-8"))["code"] == "validation_failed"

    audits = list(
        sa_session.execute(
            select(AdminAuditLog).where(AdminAuditLog.operation_code == 7)
        ).scalars()
    )
    assert len(audits) >= 2
    assert "payload" not in json.dumps(audits[0].details)


def test_bulk_cancel_ready_leased_and_toctou_skip(
    sa_session: Session,
    session_factory: sessionmaker[Session],
    sa_engine,
) -> None:
    queue_name = _unique("orders.cancel")
    app = _make_admin_app(session_factory, sa_engine, queue_name=queue_name)
    queue_pk = _seed_queue(sa_session, name=queue_name)
    policy_id = _policy_id(sa_session, queue_pk)
    ready_id = _insert_active(
        sa_session, queue_pk=queue_pk, state_code=2, policy_version_id=policy_id
    )
    leased_id = _insert_active(
        sa_session, queue_pk=queue_pk, state_code=3, policy_version_id=policy_id
    )
    disappearing = _insert_active(
        sa_session, queue_pk=queue_pk, state_code=2, policy_version_id=policy_id
    )

    preview_path = f"/admin/v1/queues/{queue_name}/bulk:preview-cancel"
    status, _h, raw = _asgi_http_call(
        app,
        method="POST",
        path=preview_path,
        headers={
            "authorization": f"Bearer {ADMIN_TOKEN}",
            "content-type": "application/json",
        },
        body=json.dumps({"filters": {"states": "ready,leased"}}).encode("utf-8"),
    )
    assert status == 200, raw
    preview = json.loads(raw.decode("utf-8"))
    assert preview["candidate_count"] == 3
    token = preview["confirmation_token"]

    # TOCTOU: remove one candidate between preview and execute.
    sa_session.execute(
        text("DELETE FROM task_payloads_active WHERE task_id = ("
             "SELECT id FROM tasks_active WHERE task_id = :tid)"),
        {"tid": str(disappearing)},
    )
    sa_session.execute(
        text("DELETE FROM tasks_active WHERE task_id = :tid"),
        {"tid": str(disappearing)},
    )
    sa_session.commit()

    execute_path = f"/admin/v1/queues/{queue_name}/bulk:execute-cancel"
    status2, _h2, raw2 = _asgi_http_call(
        app,
        method="POST",
        path=execute_path,
        headers={
            "authorization": f"Bearer {ADMIN_TOKEN}",
            "content-type": "application/json",
        },
        body=json.dumps(
            {
                "confirmation_token": token,
                "filters": {"states": "ready,leased"},
                "reason": "incident drain",
                "batch_limit": 25,
            }
        ).encode("utf-8"),
    )
    assert status2 == 200, raw2
    result = json.loads(raw2.decode("utf-8"))
    assert result["processed"] == 3
    by_id = {item["task_id"]: item for item in result["outcomes"]}
    assert by_id[str(ready_id)]["outcome"] == "cancelled"
    assert by_id[str(leased_id)]["outcome"] == "cancel_requested"
    assert by_id[str(disappearing)]["outcome"] == "skipped"
    assert by_id[str(disappearing)]["code"] == "task_not_found"

    terminal = sa_session.execute(
        select(TaskTerminal).where(TaskTerminal.task_id == ready_id)
    ).scalar_one()
    assert int(terminal.state_code) == 12  # cancelled
    leased = sa_session.execute(
        select(TaskActive).where(TaskActive.task_id == leased_id)
    ).scalar_one()
    assert leased.cancel_requested_at is not None

    # Observer denied.
    status3, _h3, raw3 = _asgi_http_call(
        app,
        method="POST",
        path=preview_path,
        headers={
            "authorization": f"Bearer {OBSERVER_TOKEN}",
            "content-type": "application/json",
        },
        body=json.dumps({"filters": {"states": "ready"}}).encode("utf-8"),
    )
    assert status3 == 403, raw3

    audits = list(
        sa_session.execute(
            select(AdminAuditLog).where(AdminAuditLog.operation_code == 8)
        ).scalars()
    )
    assert len(audits) >= 1
    detail_blob = json.dumps(audits[0].details)
    assert "active-payload" not in detail_blob
    assert "secret" not in detail_blob or "should-not" not in detail_blob


def test_expired_confirmation_rejected(
    sa_session: Session,
    session_factory: sessionmaker[Session],
    sa_engine,
) -> None:
    queue_name = _unique("orders.exp")
    app = _make_admin_app(session_factory, sa_engine, queue_name=queue_name)
    queue_pk = _seed_queue(sa_session, name=queue_name)
    _insert_dead_letter(sa_session, queue_pk=queue_pk)
    frm, to = _time_window()
    filters = {"from": frm, "to": to}

    # Build an already-expired token directly.
    codec = BulkConfirmationCodec(Secret("bulk-confirm-test-secret"))
    expired = codec.encode(
        {
            "op": "bulk_replay",
            "principal_id": ADMIN_PRINCIPAL,
            "queue": queue_name,
            "filters_hash": __import__("hashlib")
            .sha256(
                json.dumps(filters, separators=(",", ":"), sort_keys=True).encode()
            )
            .hexdigest(),
            "filters": filters,
            "candidate_ids": [],
            "candidate_count": 0,
            "max_batch": 25,
            "exp": int((datetime.now(timezone.utc) - timedelta(seconds=10)).timestamp()),
        }
    )
    status, _h, raw = _asgi_http_call(
        app,
        method="POST",
        path=f"/admin/v1/queues/{queue_name}/bulk:execute-replay",
        headers={
            "authorization": f"Bearer {ADMIN_TOKEN}",
            "idempotency-key": "expired-1",
            "content-type": "application/json",
        },
        body=json.dumps(
            {
                "confirmation_token": expired,
                "filters": filters,
                "reason": "too late",
            }
        ).encode("utf-8"),
    )
    assert status == 400, raw
    assert json.loads(raw.decode("utf-8"))["code"] == "confirmation_expired"
