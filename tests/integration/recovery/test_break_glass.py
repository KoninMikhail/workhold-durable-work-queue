"""PostgreSQL coverage for short-lived audited break-glass ops (REC-03)."""

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
from queue_service.application.claim_service import ClaimService
from queue_service.domain.queue_control import (
    AdminRequestMetadata,
    BackoffStrategy,
    CreateQueueMutation,
    QueueState,
    RetryPolicyDraft,
    SetQueueStateMutation,
    ConfigVersion,
)
from queue_service.infrastructure.postgres.queue_control_repository import (
    QueueControlRepository,
)
from queue_service.intake.contracts import normalize_enqueue_command
from queue_service.intake.repository import EnqueueRepository
from queue_service.security.authorization import Authorizer
from queue_service.security.credentials import (
    BearerCredentialAuthenticator,
    CredentialBinding,
)
from queue_service.security.principals import ServiceRole
from queue_service.settings import Secret
from queue_service.storage.models import (
    AdminAuditLog,
    EnqueueDedup,
    QueueCounter,
    TaskActive,
)

pytest_plugins = ["tests.integration.conftest"]

ADMIN_TOKEN = "tok-admin-bg"
BREAK_GLASS_TOKEN = "tok-break-glass-bg"
EXPIRED_BG_TOKEN = "tok-break-glass-expired"
NARROW_BG_TOKEN = "tok-break-glass-narrow"

ADMIN_PRINCIPAL = "admin-bg"
BREAK_GLASS_PRINCIPAL = "break-glass-bg"
EXPIRED_BG_PRINCIPAL = "break-glass-expired"
NARROW_BG_PRINCIPAL = "break-glass-narrow"


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
        CredentialBinding(
            principal_id=EXPIRED_BG_PRINCIPAL,
            role=ServiceRole.BREAK_GLASS,
            generation_id="g1",
            secret=Secret(EXPIRED_BG_TOKEN),
            expires_at=now - timedelta(minutes=1),
            allowed_operations=all_bg_ops,
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
                ADMIN_PRINCIPAL: frozenset({queue_name}),
                BREAK_GLASS_PRINCIPAL: frozenset({queue_name}),
                EXPIRED_BG_PRINCIPAL: frozenset({queue_name}),
                NARROW_BG_PRINCIPAL: frozenset({queue_name}),
            }
        ),
        bind=ListenerBind(host="127.0.0.1", port=18099),
        session_factory=session_factory,
        repository=QueueControlRepository(),
        engine=sa_engine,
        cursor_secret=Secret("break-glass-test-secret"),
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
                "server": ("127.0.0.1", 18099),
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


def _ack_body(**extra: Any) -> bytes:
    payload = {
        "reason": "incident remediation for stuck lease",
        "incident_reference": "INC-2026-0919-001",
        "risk_acknowledged": True,
        **extra,
    }
    return json.dumps(payload).encode("utf-8")


def _enqueue_and_claim(
    session_factory: sessionmaker[Session],
    *,
    queue_name: str,
) -> tuple[uuid.UUID, uuid.UUID, int]:
    session = session_factory()
    try:
        cmd = normalize_enqueue_command(
            producer_id="producer-bg",
            queue_name=queue_name,
            idempotency_key=f"idem-{uuid.uuid4().hex}",
            payload={"secret": "must-not-leak"},
            priority=0,
            available_at=None
        )
        staged = EnqueueRepository().stage_enqueue(session, cmd)
        session.commit()
        task_id = staged.task_id
    finally:
        session.close()

    claim = ClaimService(session_factory=session_factory).claim(
        queue_name=queue_name,
        worker_id="worker-bg-1",
        lease_seconds=120,
    )
    assert claim.task_id == task_id
    assert claim.claim_id is not None
    assert claim.generation is not None
    return task_id, claim.claim_id, int(claim.generation)


def test_admin_denied_break_glass_requires_role_ack_and_preserves_fencing(
    sa_session: Session,
    session_factory: sessionmaker[Session],
    sa_engine,
) -> None:
    queue_name = _unique("orders.bg")
    app = _make_admin_app(session_factory, sa_engine, queue_name=queue_name)
    _seed_queue(sa_session, name=queue_name)
    task_id, _claim_id, generation = _enqueue_and_claim(
        session_factory, queue_name=queue_name
    )

    path = f"/admin/v1/queues/{queue_name}/tasks/{task_id}:force-lease-expiry"
    body = _ack_body()

    # Normal admin credentials are denied.
    status, _h, raw = _asgi_http_call(
        app,
        method="POST",
        path=path,
        headers={
            "authorization": f"Bearer {ADMIN_TOKEN}",
            "content-type": "application/json",
        },
        body=body,
    )
    assert status == 403
    assert json.loads(raw.decode("utf-8"))["code"] == "permission_denied"

    # Expired break-glass credentials are unauthenticated.
    status, _h, raw = _asgi_http_call(
        app,
        method="POST",
        path=path,
        headers={
            "authorization": f"Bearer {EXPIRED_BG_TOKEN}",
            "content-type": "application/json",
        },
        body=body,
    )
    assert status == 401
    assert json.loads(raw.decode("utf-8"))["code"] == "unauthenticated"

    # Operation-scoped audience rejects out-of-scope ops.
    status, _h, raw = _asgi_http_call(
        app,
        method="POST",
        path=path,
        headers={
            "authorization": f"Bearer {NARROW_BG_TOKEN}",
            "content-type": "application/json",
        },
        body=body,
    )
    assert status == 403

    # Missing risk acknowledgement is rejected.
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

    # Valid break-glass force expiry succeeds without issuing a claim token.
    status, headers, raw = _asgi_http_call(
        app,
        method="POST",
        path=path,
        headers={
            "authorization": f"Bearer {BREAK_GLASS_TOKEN}",
            "content-type": "application/json",
        },
        body=body,
    )
    assert status == 200, raw.decode("utf-8")
    payload = json.loads(raw.decode("utf-8"))
    assert payload["operation"] == "forceLeaseExpiry"
    assert payload["target_id"] == str(task_id)
    assert payload["generation"] == generation
    assert "claim_token" not in payload
    assert "claim_token" not in json.dumps(payload)
    assert "x-queue-claim-token" not in headers

    sa_session.expire_all()
    task = sa_session.execute(
        select(TaskActive).where(TaskActive.task_id == task_id)
    ).scalar_one_or_none()
    # Lease released via fencing path: either retry-scheduled ready/delayed or gone.
    if task is not None:
        assert task.current_claim_id is None
        assert int(task.state_code) in {1, 2}  # delayed or ready
        assert int(task.generation) == generation

    audits = list(
        sa_session.execute(
            select(AdminAuditLog).where(AdminAuditLog.operation_code == 9)
        ).scalars()
    )
    assert len(audits) == 1
    details = dict(audits[0].details)
    assert details["claim_token_issued"] is False
    assert details["generation"] == generation
    assert details["incident_reference"] == "INC-2026-0919-001"
    assert "reason" in details


def test_reconcile_raise_limit_and_registry_repair_preconditions(
    sa_session: Session,
    session_factory: sessionmaker[Session],
    sa_engine,
) -> None:
    queue_name = _unique("orders.bg2")
    app = _make_admin_app(session_factory, sa_engine, queue_name=queue_name)
    queue_pk = _seed_queue(sa_session, name=queue_name)

    # Skew counters intentionally.
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

    status, _h, raw = _asgi_http_call(
        app,
        method="POST",
        path=f"/admin/v1/queues/{queue_name}:reconcile-counters",
        headers={
            "authorization": f"Bearer {BREAK_GLASS_TOKEN}",
            "content-type": "application/json",
        },
        body=_ack_body(),
    )
    assert status == 200, raw.decode("utf-8")
    body = json.loads(raw.decode("utf-8"))
    assert body["outcome"] == "reconciled"
    assert body["delayed_count"] == 0
    assert body["ready_count"] == 0
    assert body["leased_count"] == 0

    sa_session.expire_all()
    counter = sa_session.get(QueueCounter, queue_pk)
    assert counter is not None
    assert int(counter.delayed_count) == 0
    assert int(counter.ready_count) == 0
    assert int(counter.leased_count) == 0

    status, _h, raw = _asgi_http_call(
        app,
        method="POST",
        path=f"/admin/v1/queues/{queue_name}:raise-replay-limit",
        headers={
            "authorization": f"Bearer {BREAK_GLASS_TOKEN}",
            "content-type": "application/json",
        },
        body=_ack_body(factor=2.0, ttl_seconds=120),
    )
    assert status == 200, raw.decode("utf-8")
    raised = json.loads(raw.decode("utf-8"))
    assert raised["outcome"] == "raised"
    assert float(raised["effective_rps"]) >= 2.0

    # Registry repair requires pause + duplicate-window acknowledgement.
    now = datetime.now(timezone.utc)
    sa_session.execute(
        text(
            """
            INSERT INTO enqueue_dedup (
                producer_id, queue_id, key_hash, request_fingerprint, task_id,
                created_at, expires_at
            ) VALUES (
                'producer-bg', :qid, :kh, :fp, :task_id,
                :created, :expires
            )
            """
        ),
        {
            "qid": queue_pk,
            "kh": bytes.fromhex("11" * 32),
            "fp": bytes.fromhex("22" * 32),
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

    status, _h, raw = _asgi_http_call(
        app,
        method="POST",
        path=f"/admin/v1/queues/{queue_name}/registry:repair",
        headers={
            "authorization": f"Bearer {BREAK_GLASS_TOKEN}",
            "content-type": "application/json",
        },
        body=_ack_body(
            entry_id=entry_id,
            acknowledge_duplicate_window=True,
            extend_seconds=86400,
        ),
    )
    assert status == 400
    assert "paused" in json.loads(raw.decode("utf-8"))["message"].lower()

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

    status, _h, raw = _asgi_http_call(
        app,
        method="POST",
        path=f"/admin/v1/queues/{queue_name}/registry:repair",
        headers={
            "authorization": f"Bearer {BREAK_GLASS_TOKEN}",
            "content-type": "application/json",
        },
        body=_ack_body(
            entry_id=entry_id,
            acknowledge_duplicate_window=True,
            extend_seconds=86400,
        ),
    )
    assert status == 200, raw.decode("utf-8")
    repaired = json.loads(raw.decode("utf-8"))
    assert repaired["outcome"] == "repaired"
    assert "key_hash" not in json.dumps(repaired)

    sa_session.expire_all()
    row = sa_session.get(EnqueueDedup, entry_id)
    assert row is not None
    assert row.expires_at > now + timedelta(days=10)

    # Forbidden: drop with invalid partition name (no arbitrary SQL).
    status, _h, raw = _asgi_http_call(
        app,
        method="POST",
        path="/admin/v1/partitions/not-a-partition:force-drop",
        headers={
            "authorization": f"Bearer {BREAK_GLASS_TOKEN}",
            "content-type": "application/json",
        },
        body=_ack_body(),
    )
    assert status in {400, 404}
