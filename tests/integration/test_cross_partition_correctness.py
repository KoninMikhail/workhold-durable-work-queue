"""Black-box cross-partition correctness windows (STOR-05 / Phase 03.8-05).

Proves global enqueue dedup, claim fencing, and terminal replay survive UTC
daily history partitions and history detach/drop, while registry TTL expiry is
enforced only via the wired ``queue maintain`` role (not retention primitives).
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import uuid
from collections.abc import Iterator, Mapping
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
from sqlalchemy import create_engine, event, func, select, text, update
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.orm import Session, sessionmaker

from queue_service.api.application import create_application_app
from queue_service.api.security import ListenerBind
from queue_service.application.claim_service import ClaimService
from queue_service.application.completion import CompletionService
from queue_service.application.lease_service import LeaseService
from queue_service.application.worker_terminal import WorkerTerminalService
from queue_service.domain.queue_control import (
    AdminRequestMetadata,
    BackoffStrategy,
    CreateQueueMutation,
    RetryPolicyDraft,
)
from queue_service.health import DAILY_RANGE_PARENTS, DEFAULT_PARTITION_PREMAKE_DAYS
from queue_service.infrastructure.postgres import partition_catalog
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
    AdminReplay,
    ClaimRegistry,
    CompleteReplay,
    CompletionEffect,
    EnqueueDedup,
    Queue,
    TaskAttempt,
    TaskTerminal,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
UTC = timezone.utc

PRODUCER_TOKEN = "tok-producer-xpart"
WORKER_TOKEN = "tok-worker-xpart"
ADMIN_TOKEN = "tok-admin-xpart"

PRODUCER_PRINCIPAL = "producer-xpart"
WORKER_PRINCIPAL = "worker-xpart"
ADMIN_PRINCIPAL = "admin-xpart"

BASE_QUEUE = "orders.xpart"
TARGET_QUEUE = "billing.xpart"
CLAIM_PATH = "/v1/claims"
CLAIM_TOKEN_HEADER = "X-Queue-Claim-Token"
REPLICA_ID = "pool-xpart/replica-1"

HISTORY_PARENTS = (
    "admin_audit_log",
    "task_attempts",
    "tasks_terminal",
    "delivery_events_terminal",
)

# Phase 5: delivery_events_terminal detaches on the 90d dead-letter window;
# other history parents use the 30d payload retention default.
_PAYLOAD_EXPIRED_DAYS = 32
_DELIVERY_EXPIRED_DAYS = 91


def _expired_day_for(parent: str, today: date) -> date:
    days = (
        _DELIVERY_EXPIRED_DAYS
        if parent == "delivery_events_terminal"
        else _PAYLOAD_EXPIRED_DAYS
    )
    return today - timedelta(days=days)

_OP_FAIL = 2
_OP_ACK_CANCEL = 3


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
    return f"{BASE_QUEUE}.{uuid.uuid4().hex[:12]}"


@pytest.fixture
def target_queue_name() -> str:
    return f"{TARGET_QUEUE}.{uuid.uuid4().hex[:12]}"


@pytest.fixture
def authorizer(queue_name: str, target_queue_name: str) -> Authorizer:
    return Authorizer(
        queue_scopes={
            PRODUCER_PRINCIPAL: frozenset({queue_name, target_queue_name, BASE_QUEUE}),
            WORKER_PRINCIPAL: frozenset({queue_name, target_queue_name, BASE_QUEUE}),
            ADMIN_PRINCIPAL: frozenset({queue_name, target_queue_name}),
        }
    )


@pytest.fixture
def xpart_schema(test_database_url: str) -> Iterator[tuple[str, str, Engine]]:
    """Fresh migrated schema; yield (schema, url, engine). Always DROP CASCADE."""
    from tests.integration.conftest import run_alembic, to_psycopg_conninfo

    import psycopg

    schema = f"qit_{uuid.uuid4().hex}"
    admin = psycopg.connect(to_psycopg_conninfo(test_database_url))
    admin.autocommit = True
    try:
        admin.execute(f'CREATE SCHEMA "{schema}"')
    finally:
        admin.close()

    run_alembic("upgrade", "head", schema=schema, database_url=test_database_url)
    engine = create_engine(test_database_url, pool_pre_ping=True)
    try:
        yield schema, test_database_url, engine
    finally:
        engine.dispose()
        drop = psycopg.connect(to_psycopg_conninfo(test_database_url))
        drop.autocommit = True
        try:
            drop.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            drop.close()


@pytest.fixture
def session_factory(xpart_schema: tuple[str, str, Engine]) -> Iterator[sessionmaker[Session]]:
    schema, _url, engine = xpart_schema

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
        pass


@pytest.fixture
def app(
    session_factory: sessionmaker[Session],
    authorizer: Authorizer,
) -> Any:
    depth = DepthCeilings(
        queue_active_depth=100,
        instance_active_depth=500,
        retry_after_ms=250,
    )
    return create_application_app(
        authenticator=BearerCredentialAuthenticator.from_bindings(_bindings()),
        authorizer=authorizer,
        bind=ListenerBind(host="127.0.0.1", port=18185),
        session_factory=session_factory,
        enqueue_service=EnqueueService(
            session_factory=session_factory,
            depth_ceilings=depth,
        ),
        claim_service=ClaimService(session_factory=session_factory),
        lease_service=LeaseService(session_factory=session_factory),
        completion_service=CompletionService(
            session_factory=session_factory,
            depth_ceilings=depth,
        ),
        worker_terminal_service=WorkerTerminalService(session_factory=session_factory),
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
        "client": ("127.0.0.1", 50000),
        "server": ("127.0.0.1", 18185),
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


def _seed_queue(session: Session, *, name: str, max_attempts: int = 5) -> Queue:
    QueueControlRepository().create_named_queue(
        session,
        CreateQueueMutation(
            name=name,
            initial_policy=RetryPolicyDraft(
                enabled=True,
                max_attempts=max_attempts,
                backoff_strategy=BackoffStrategy.FIXED,
                retry_delay_seconds=0,
            ),
            metadata=_admin_meta(),
        ),
    )
    session.commit()
    return session.execute(select(Queue).where(Queue.name == name)).scalar_one()


def _worker_headers(*, claim_token: str | None = None) -> dict[str, str]:
    headers = {
        "Authorization": f"Bearer {WORKER_TOKEN}",
        "Content-Type": "application/json",
    }
    if claim_token is not None:
        headers[CLAIM_TOKEN_HEADER] = claim_token
    return headers


def _enqueue(
    app: Any,
    *,
    queue_name: str,
    payload: Any,
    idempotency_key: str,
) -> tuple[int, dict[str, Any]]:
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
    return status, json.loads(resp.decode("utf-8"))


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


def _heartbeat(
    app: Any,
    *,
    claim_id: str,
    claim_token: str,
    generation: int,
    lease_seconds: int = 30,
) -> tuple[int, bytes]:
    status, _hdrs, resp = _asgi_http_call(
        app,
        method="POST",
        path=f"/v1/claims/{claim_id}:heartbeat",
        headers=_worker_headers(claim_token=claim_token),
        body=json.dumps(
            {"generation": generation, "lease_seconds": lease_seconds},
            separators=(",", ":"),
        ).encode("utf-8"),
    )
    return status, resp


def _fail(
    app: Any,
    *,
    claim_id: str,
    claim_token: str,
    generation: int,
    retryable: bool = True,
    failure_code: str = "worker.timeout",
    failure_detail: str = "xpart-retry",
) -> tuple[int, bytes]:
    status, _hdrs, resp = _asgi_http_call(
        app,
        method="POST",
        path=f"/v1/claims/{claim_id}:fail",
        headers=_worker_headers(claim_token=claim_token),
        body=json.dumps(
            {
                "generation": generation,
                "retryable": retryable,
                "failure_code": failure_code,
                "failure_detail": failure_detail,
            },
            separators=(",", ":"),
        ).encode("utf-8"),
    )
    return status, resp


def _request_cancel(app: Any, *, task_id: str) -> None:
    status, _hdrs, resp = _asgi_http_call(
        app,
        method="POST",
        path=f"/v1/tasks/{task_id}:cancel",
        headers={
            "Authorization": f"Bearer {PRODUCER_TOKEN}",
            "Content-Type": "application/json",
        },
        body=b"{}",
    )
    assert status == 200, resp.decode("utf-8", errors="replace")


def _ack_cancel(
    app: Any,
    *,
    claim_id: str,
    claim_token: str,
    generation: int,
) -> tuple[int, bytes]:
    status, _hdrs, resp = _asgi_http_call(
        app,
        method="POST",
        path=f"/v1/claims/{claim_id}:ack-cancel",
        headers=_worker_headers(claim_token=claim_token),
        body=json.dumps(
            {"generation": generation},
            separators=(",", ":"),
        ).encode("utf-8"),
    )
    return status, resp


def _complete(
    app: Any,
    *,
    claim_id: str,
    claim_token: str,
    generation: int,
    spawn: list[dict[str, Any]] | None = None,
) -> tuple[int, dict[str, str], bytes]:
    return _asgi_http_call(
        app,
        method="POST",
        path=f"/v1/claims/{claim_id}:complete",
        headers=_worker_headers(claim_token=claim_token),
        body=json.dumps(
            {
                "generation": generation,
                "spawn": [] if spawn is None else spawn,
            },
            separators=(",", ":"),
        ).encode("utf-8"),
    )


def _spawn_item(*, queue_name: str, payload: Any) -> dict[str, Any]:
    return {
        "queue_name": queue_name,
        "payload": payload,
        "priority": 0,
        "idempotency_key": f"spawn-{uuid.uuid4().hex}",
    }


def _maintain_env(database_url: str, schema: str, **extra: str) -> dict[str, str]:
    env = os.environ.copy()
    env["DATABASE_URL"] = database_url
    env["QUEUE_SCHEMA"] = schema
    env["ALEMBIC_VERSION_TABLE_SCHEMA"] = schema
    env["QUEUE_ENVIRONMENT"] = "development"
    env["QUEUE_LISTENER_TLS_MODE"] = "plaintext_public"
    env["QUEUE_POSTGRES_MAX_CONNECTIONS"] = "100"
    env["QUEUE_POSTGRES_RESERVED_CONNECTIONS"] = "10"
    for role in ("API", "ADMIN", "MIGRATE", "MAINTAIN", "RELAY"):
        env.setdefault(f"QUEUE_{role}_REPLICA_CEILING", "1")
        env.setdefault(f"QUEUE_{role}_POOL_CEILING", "2")
    src = str(REPO_ROOT / "src")
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = src if not existing else f"{src}{os.pathsep}{existing}"
    env.update(extra)
    return env


def _run_maintain(
    *,
    database_url: str,
    schema: str,
    premake_days: int = DEFAULT_PARTITION_PREMAKE_DAYS,
    payload_retention_days: int = 30,
    extra_env: Mapping[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Invoke wired ``queue maintain`` as a subprocess (black-box only)."""
    env = _maintain_env(
        database_url,
        schema,
        QUEUE_PAYLOAD_RETENTION_DAYS=str(payload_retention_days),
        **dict(extra_env or {}),
    )
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "queue_service",
            "maintain",
            "--schema",
            schema,
            "--premake-days",
            str(premake_days),
            "--lock-timeout-seconds",
            "10",
        ],
        cwd=str(REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


def _assert_maintain_ok(completed: subprocess.CompletedProcess[str]) -> None:
    assert completed.returncode == 0, (
        f"maintain exit={completed.returncode}\n"
        f"stdout={completed.stdout}\nstderr={completed.stderr}"
    )
    assert "maintain status=ok" in completed.stdout
    assert "status=failed" not in completed.stdout


def _utc_today(session: Session) -> date:
    value = session.execute(
        text("SELECT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date")
    ).scalar_one()
    assert isinstance(value, date)
    return value


def _store_now(session: Session) -> datetime:
    value = session.scalar(select(func.transaction_timestamp()))
    assert value is not None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value


def _create_past_child(conn: Connection, parent: str, day: date) -> str:
    spec = partition_catalog.day_spec_for(parent, day)
    ddl = conn.execute(
        text(
            """
            SELECT format(
                'CREATE TABLE IF NOT EXISTS %I PARTITION OF %I '
                'FOR VALUES FROM (%L) TO (%L)',
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


def _relation_exists(session: Session, relname: str) -> bool:
    return bool(
        session.execute(
            text(
                """
                SELECT EXISTS (
                  SELECT 1 FROM pg_class c
                  JOIN pg_namespace n ON n.oid = c.relnamespace
                  WHERE n.nspname = current_schema() AND c.relname = :rel
                )
                """
            ),
            {"rel": relname},
        ).scalar_one()
    )


def _attempt_partition(session: Session, *, claim_id: UUID) -> str:
    row = session.execute(
        text(
            """
            SELECT c.relname
            FROM task_attempts a
            JOIN pg_class c ON c.oid = a.tableoid
            WHERE a.claim_id = :claim_id
            """
        ),
        {"claim_id": claim_id},
    ).scalar_one()
    return str(row)


def _terminal_partition(session: Session, *, task_id: UUID) -> str:
    row = session.execute(
        text(
            """
            SELECT c.relname
            FROM tasks_terminal t
            JOIN pg_class c ON c.oid = t.tableoid
            WHERE t.task_id = :task_id
            """
        ),
        {"task_id": task_id},
    ).scalar_one()
    return str(row)


def _move_attempt_claimed_at(
    session: Session,
    *,
    claim_id: UUID,
    day: date,
) -> None:
    """Land an attempt on ``day``'s child via Queue-store partition-key rewrite."""
    mid = datetime(day.year, day.month, day.day, 12, 0, 0, tzinfo=UTC)
    session.execute(
        update(TaskAttempt)
        .where(TaskAttempt.claim_id == claim_id)
        .values(
            claimed_at=mid,
            lease_expires_at=mid + timedelta(seconds=30),
            ended_at=func.coalesce(TaskAttempt.ended_at, mid + timedelta(seconds=1)),
        )
    )
    session.commit()


def _move_terminal_at(session: Session, *, task_id: UUID, day: date) -> None:
    mid = datetime(day.year, day.month, day.day, 12, 0, 0, tzinfo=UTC)
    session.execute(
        update(TaskTerminal)
        .where(TaskTerminal.task_id == task_id)
        .values(terminal_at=mid)
    )
    session.commit()


def test_maintain_subprocess_premakes_and_reports_ok(
    xpart_schema: tuple[str, str, Engine],
) -> None:
    schema, url, _engine = xpart_schema
    completed = _run_maintain(database_url=url, schema=schema, premake_days=14)
    _assert_maintain_ok(completed)

    with create_engine(url).connect() as conn:
        conn.execute(text(f'SET search_path TO "{schema}"'))
        today = conn.execute(
            text("SELECT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date")
        ).scalar_one()
        for parent in DAILY_RANGE_PARENTS:
            child = partition_catalog.child_name_for(parent, today)
            exists = conn.execute(
                text(
                    """
                    SELECT EXISTS (
                      SELECT 1 FROM pg_class c
                      JOIN pg_namespace n ON n.oid = c.relnamespace
                      WHERE n.nspname = current_schema() AND c.relname = :rel
                    )
                    """
                ),
                {"rel": child},
            ).scalar_one()
            assert exists, child
        status = conn.execute(
            text(
                """
                SELECT last_succeeded_at IS NOT NULL, premade_through IS NOT NULL
                FROM partition_maintenance_status WHERE singleton_id = 1
                """
            )
        ).one()
        assert status[0] is True
        assert status[1] is True


def test_enqueue_uniqueness_survives_utc_children_and_history_detach(
    app: Any,
    session_factory: sessionmaker[Session],
    xpart_schema: tuple[str, str, Engine],
    queue_name: str,
) -> None:
    schema, url, _engine = xpart_schema
    completed = _run_maintain(database_url=url, schema=schema, premake_days=14)
    _assert_maintain_ok(completed)

    with session_factory() as session:
        _seed_queue(session, name=queue_name)
        today = _utc_today(session)
        yesterday = today - timedelta(days=1)
        for parent in HISTORY_PARENTS:
            _create_past_child(session.connection(), parent, yesterday)
        session.commit()

    idem = f"idem-xpart-{uuid.uuid4().hex}"
    status1, body1 = _enqueue(
        app, queue_name=queue_name, payload={"n": 1}, idempotency_key=idem
    )
    assert status1 == 201
    task_id = body1["task"]["task_id"]

    status2, body2 = _enqueue(
        app, queue_name=queue_name, payload={"n": 1}, idempotency_key=idem
    )
    assert status2 == 200
    assert body2["task"]["task_id"] == task_id
    assert body2["replayed"] is True

    status3, body3 = _enqueue(
        app, queue_name=queue_name, payload={"n": 2}, idempotency_key=idem
    )
    assert status3 == 409
    assert body3["code"] == "idempotency_conflict"
    assert body3["retryable"] is False

    # Seed an expired history child and drop it via maintain; dedup must stay live.
    with session_factory() as session:
        today = _utc_today(session)
        expired_children = [
            _create_past_child(
                session.connection(),
                parent,
                _expired_day_for(parent, today),
            )
            for parent in HISTORY_PARENTS
        ]
        session.commit()
        dedup_before = session.scalar(select(func.count()).select_from(EnqueueDedup))
        assert int(dedup_before or 0) >= 1

    completed2 = _run_maintain(
        database_url=url,
        schema=schema,
        premake_days=14,
        payload_retention_days=30,
    )
    _assert_maintain_ok(completed2)

    with session_factory() as session:
        for child in expired_children:
            assert not _relation_exists(session, child), child
        dedup_after = session.scalar(select(func.count()).select_from(EnqueueDedup))
        assert int(dedup_after or 0) == int(dedup_before or 0)

    status4, body4 = _enqueue(
        app, queue_name=queue_name, payload={"n": 1}, idempotency_key=idem
    )
    assert status4 == 200, body4
    assert body4["task"]["task_id"] == task_id
    assert body4["replayed"] is True


def test_claim_fencing_global_across_attempt_partitions(
    app: Any,
    session_factory: sessionmaker[Session],
    xpart_schema: tuple[str, str, Engine],
    queue_name: str,
) -> None:
    schema, url, _engine = xpart_schema
    _assert_maintain_ok(_run_maintain(database_url=url, schema=schema, premake_days=14))

    with session_factory() as session:
        _seed_queue(session, name=queue_name)
        today = _utc_today(session)
        yesterday = today - timedelta(days=1)
        for parent in HISTORY_PARENTS:
            _create_past_child(session.connection(), parent, yesterday)
        session.commit()

    status, body = _enqueue(
        app,
        queue_name=queue_name,
        payload={"kind": "fence"},
        idempotency_key=f"idem-fence-{uuid.uuid4().hex}",
    )
    assert status == 201
    task_id = UUID(body["task"]["task_id"])

    claim1 = _claim_one(app, queue_name=queue_name)
    assert UUID(claim1["task_id"]) == task_id
    fail_status, _ = _fail(
        app,
        claim_id=claim1["claim_id"],
        claim_token=claim1["claim_token"],
        generation=claim1["generation"],
    )
    assert fail_status == 200

    with session_factory() as session:
        yesterday = _utc_today(session) - timedelta(days=1)
        _move_attempt_claimed_at(
            session, claim_id=UUID(claim1["claim_id"]), day=yesterday
        )
        part1 = _attempt_partition(session, claim_id=UUID(claim1["claim_id"]))
        assert part1.endswith(yesterday.strftime("%Y%m%d"))

    claim2 = _claim_one(app, queue_name=queue_name)
    assert claim2["claim_id"] != claim1["claim_id"]
    assert claim2["generation"] == claim1["generation"] + 1

    with session_factory() as session:
        today = _utc_today(session)
        part2 = _attempt_partition(session, claim_id=UUID(claim2["claim_id"]))
        assert part2.endswith(today.strftime("%Y%m%d"))
        assert part1 != part2
        # Registry remains unpartitioned and points at current authority.
        registry = session.execute(
            select(ClaimRegistry).where(
                ClaimRegistry.claim_id == UUID(claim2["claim_id"])
            )
        ).scalar_one()
        assert int(registry.generation) == claim2["generation"]

    hb_ok, _ = _heartbeat(
        app,
        claim_id=claim2["claim_id"],
        claim_token=claim2["claim_token"],
        generation=claim2["generation"],
    )
    assert hb_ok == 200

    # Stale claim from yesterday's attempt partition cannot mutate.
    hb_stale, stale_body = _heartbeat(
        app,
        claim_id=claim1["claim_id"],
        claim_token=claim1["claim_token"],
        generation=claim1["generation"],
    )
    assert hb_stale in {404, 409}
    stale = json.loads(stale_body.decode("utf-8"))
    assert stale["retryable"] is False
    assert stale["code"] in {"claim_not_found", "lease_lost", "generation_mismatch"}


def test_terminal_replay_and_spawn_uniqueness_across_partitions(
    app: Any,
    session_factory: sessionmaker[Session],
    xpart_schema: tuple[str, str, Engine],
    queue_name: str,
    target_queue_name: str,
) -> None:
    schema, url, _engine = xpart_schema
    _assert_maintain_ok(_run_maintain(database_url=url, schema=schema, premake_days=14))

    with session_factory() as session:
        _seed_queue(session, name=queue_name)
        _seed_queue(session, name=target_queue_name)
        today = _utc_today(session)
        yesterday = today - timedelta(days=1)
        for parent in HISTORY_PARENTS:
            _create_past_child(session.connection(), parent, yesterday)
        session.commit()

    status, body = _enqueue(
        app,
        queue_name=queue_name,
        payload={"kind": "complete"},
        idempotency_key=f"idem-complete-{uuid.uuid4().hex}",
    )
    assert status == 201
    source_id = body["task"]["task_id"]
    claim = _claim_one(app, queue_name=queue_name)
    spawn = [
        _spawn_item(queue_name=target_queue_name, payload={"ord": 0}),
        _spawn_item(queue_name=target_queue_name, payload={"ord": 1}),
    ]
    status1, headers1, body1 = _complete(
        app,
        claim_id=claim["claim_id"],
        claim_token=claim["claim_token"],
        generation=claim["generation"],
        spawn=spawn,
    )
    assert status1 == 200, body1.decode("utf-8", errors="replace")
    first = json.loads(body1.decode("utf-8"))
    assert first["replayed"] is False
    assert len(first["spawned_task_ids"]) == 2
    spawned = list(first["spawned_task_ids"])

    with session_factory() as session:
        yesterday = _utc_today(session) - timedelta(days=1)
        today = _utc_today(session)
        _move_attempt_claimed_at(
            session, claim_id=UUID(claim["claim_id"]), day=yesterday
        )
        _move_terminal_at(session, task_id=UUID(source_id), day=today)
        # Force attempt/terminal onto distinct UTC children when possible.
        if yesterday != today:
            att_part = _attempt_partition(session, claim_id=UUID(claim["claim_id"]))
            term_part = _terminal_partition(session, task_id=UUID(source_id))
            assert att_part != term_part
        effects_before = session.scalar(
            select(func.count())
            .select_from(CompletionEffect)
            .where(CompletionEffect.source_claim_id == UUID(claim["claim_id"]))
        )
        assert int(effects_before or 0) == 2

    status2, _h2, body2 = _complete(
        app,
        claim_id=claim["claim_id"],
        claim_token=claim["claim_token"],
        generation=claim["generation"],
        spawn=spawn,
    )
    assert status2 == 200
    second = json.loads(body2.decode("utf-8"))
    assert second["replayed"] is True
    assert second["spawned_task_ids"] == spawned
    assert second["task_id"] == source_id

    # Changed body conflicts with zero duplicate spawn effects.
    bad_spawn = [
        _spawn_item(queue_name=target_queue_name, payload={"ord": "changed"}),
    ]
    status3, _h3, body3 = _complete(
        app,
        claim_id=claim["claim_id"],
        claim_token=claim["claim_token"],
        generation=claim["generation"],
        spawn=bad_spawn,
    )
    assert status3 == 409
    err = json.loads(body3.decode("utf-8"))
    assert err["code"] == "idempotency_conflict"
    assert err["retryable"] is False

    with session_factory() as session:
        effects_after = session.scalar(
            select(func.count())
            .select_from(CompletionEffect)
            .where(CompletionEffect.source_claim_id == UUID(claim["claim_id"]))
        )
        assert int(effects_after or 0) == 2
        ordinals = list(
            session.scalars(
                select(CompletionEffect.ordinal)
                .where(CompletionEffect.source_claim_id == UUID(claim["claim_id"]))
                .order_by(CompletionEffect.ordinal)
            )
        )
        assert ordinals == [0, 1]


def test_fail_and_ack_cancel_replay_across_attempt_partition(
    app: Any,
    session_factory: sessionmaker[Session],
    xpart_schema: tuple[str, str, Engine],
    queue_name: str,
) -> None:
    """Fail and ack_cancel same-body replay stay global across attempt partitions."""
    schema, url, _engine = xpart_schema
    _assert_maintain_ok(_run_maintain(database_url=url, schema=schema, premake_days=14))

    with session_factory() as session:
        _seed_queue(session, name=queue_name, max_attempts=3)
        yesterday = _utc_today(session) - timedelta(days=1)
        for parent in HISTORY_PARENTS:
            _create_past_child(session.connection(), parent, yesterday)
        session.commit()

    # --- Fail: same-body replay + conflict after attempt lands on yesterday child ---
    status, body = _enqueue(
        app,
        queue_name=queue_name,
        payload={"kind": "fail"},
        idempotency_key=f"idem-fail-{uuid.uuid4().hex}",
    )
    assert status == 201
    fail_task_id = body["task"]["task_id"]
    claim_fail = _claim_one(app, queue_name=queue_name)
    fail_kwargs = {
        "claim_id": claim_fail["claim_id"],
        "claim_token": claim_fail["claim_token"],
        "generation": claim_fail["generation"],
        "retryable": False,
        "failure_code": "worker.fatal",
        "failure_detail": "xpart-fail-same",
    }
    st_fail, body_fail = _fail(app, **fail_kwargs)
    assert st_fail == 200, body_fail.decode("utf-8", errors="replace")
    first_fail = json.loads(body_fail.decode("utf-8"))
    assert first_fail["replayed"] is False
    assert first_fail["task_id"] == fail_task_id

    with session_factory() as session:
        yesterday = _utc_today(session) - timedelta(days=1)
        today = _utc_today(session)
        _move_attempt_claimed_at(
            session, claim_id=UUID(claim_fail["claim_id"]), day=yesterday
        )
        _move_terminal_at(session, task_id=UUID(fail_task_id), day=today)
        if yesterday != today:
            att_part = _attempt_partition(
                session, claim_id=UUID(claim_fail["claim_id"])
            )
            term_part = _terminal_partition(session, task_id=UUID(fail_task_id))
            assert att_part != term_part
            assert yesterday.strftime("%Y%m%d") in att_part

    st_fail2, body_fail2 = _fail(app, **fail_kwargs)
    assert st_fail2 == 200, body_fail2.decode("utf-8", errors="replace")
    second_fail = json.loads(body_fail2.decode("utf-8"))
    assert second_fail["replayed"] is True
    assert {k: v for k, v in second_fail.items() if k != "replayed"} == {
        k: v for k, v in first_fail.items() if k != "replayed"
    }

    st_fail_conflict, body_fail_conflict = _fail(
        app,
        **{**fail_kwargs, "failure_detail": "xpart-fail-changed"},
    )
    assert st_fail_conflict == 409, body_fail_conflict.decode(
        "utf-8", errors="replace"
    )
    fail_err = json.loads(body_fail_conflict.decode("utf-8"))
    assert fail_err["code"] == "idempotency_conflict"
    assert fail_err["retryable"] is False

    with session_factory() as session:
        fail_replays = list(
            session.scalars(
                select(CompleteReplay).where(
                    CompleteReplay.claim_id == UUID(claim_fail["claim_id"]),
                    CompleteReplay.operation_code == _OP_FAIL,
                )
            )
        )
        assert len(fail_replays) == 1

    # --- Ack-cancel: same-body replay + conflict after partition-key rewrite ---
    status_c, body_c = _enqueue(
        app,
        queue_name=queue_name,
        payload={"kind": "ack-cancel"},
        idempotency_key=f"idem-ack-cancel-{uuid.uuid4().hex}",
    )
    assert status_c == 201
    cancel_task_id = body_c["task"]["task_id"]
    claim_cancel = _claim_one(app, queue_name=queue_name)
    _request_cancel(app, task_id=cancel_task_id)

    st_ack, body_ack = _ack_cancel(
        app,
        claim_id=claim_cancel["claim_id"],
        claim_token=claim_cancel["claim_token"],
        generation=claim_cancel["generation"],
    )
    assert st_ack == 200, body_ack.decode("utf-8", errors="replace")
    first_ack = json.loads(body_ack.decode("utf-8"))
    assert first_ack["replayed"] is False
    assert first_ack["state"] == "cancelled"
    assert first_ack["task_id"] == cancel_task_id

    with session_factory() as session:
        yesterday = _utc_today(session) - timedelta(days=1)
        today = _utc_today(session)
        _move_attempt_claimed_at(
            session, claim_id=UUID(claim_cancel["claim_id"]), day=yesterday
        )
        _move_terminal_at(session, task_id=UUID(cancel_task_id), day=today)
        if yesterday != today:
            att_part = _attempt_partition(
                session, claim_id=UUID(claim_cancel["claim_id"])
            )
            term_part = _terminal_partition(session, task_id=UUID(cancel_task_id))
            assert att_part != term_part
            assert yesterday.strftime("%Y%m%d") in att_part
        terminal = session.execute(
            select(TaskTerminal).where(TaskTerminal.task_id == UUID(cancel_task_id))
        ).scalar_one()
        # cancelled state_code matches storage contract (12).
        assert int(terminal.state_code) == 12

    st_ack2, body_ack2 = _ack_cancel(
        app,
        claim_id=claim_cancel["claim_id"],
        claim_token=claim_cancel["claim_token"],
        generation=claim_cancel["generation"],
    )
    assert st_ack2 == 200, body_ack2.decode("utf-8", errors="replace")
    second_ack = json.loads(body_ack2.decode("utf-8"))
    assert second_ack["replayed"] is True
    assert second_ack["state"] == "cancelled"
    assert {k: v for k, v in second_ack.items() if k != "replayed"} == {
        k: v for k, v in first_ack.items() if k != "replayed"
    }

    st_ack_conflict, body_ack_conflict = _ack_cancel(
        app,
        claim_id=claim_cancel["claim_id"],
        claim_token=claim_cancel["claim_token"],
        generation=claim_cancel["generation"] + 1,
    )
    assert st_ack_conflict == 409, body_ack_conflict.decode(
        "utf-8", errors="replace"
    )
    ack_err = json.loads(body_ack_conflict.decode("utf-8"))
    assert ack_err["code"] == "idempotency_conflict"
    assert ack_err["retryable"] is False

    with session_factory() as session:
        ack_replays = list(
            session.scalars(
                select(CompleteReplay).where(
                    CompleteReplay.claim_id == UUID(claim_cancel["claim_id"]),
                    CompleteReplay.operation_code == _OP_ACK_CANCEL,
                )
            )
        )
        assert len(ack_replays) == 1
        assert int(ack_replays[0].result_state_code) == 12


def test_history_drop_preserves_live_registries_then_ttl_purge_via_maintain(
    app: Any,
    session_factory: sessionmaker[Session],
    xpart_schema: tuple[str, str, Engine],
    queue_name: str,
    target_queue_name: str,
) -> None:
    schema, url, _engine = xpart_schema
    _assert_maintain_ok(
        _run_maintain(
            database_url=url,
            schema=schema,
            premake_days=14,
            payload_retention_days=30,
        )
    )

    with session_factory() as session:
        _seed_queue(session, name=queue_name)
        _seed_queue(session, name=target_queue_name)
        today = _utc_today(session)
        yesterday = today - timedelta(days=1)
        for parent in HISTORY_PARENTS:
            _create_past_child(session.connection(), parent, yesterday)
        session.commit()

    idem = f"idem-ttl-{uuid.uuid4().hex}"
    status, body = _enqueue(
        app, queue_name=queue_name, payload={"n": 7}, idempotency_key=idem
    )
    assert status == 201
    task_id = body["task"]["task_id"]
    claim = _claim_one(app, queue_name=queue_name)
    spawn = [_spawn_item(queue_name=target_queue_name, payload={"x": 1})]
    st1, _h1, b1 = _complete(
        app,
        claim_id=claim["claim_id"],
        claim_token=claim["claim_token"],
        generation=claim["generation"],
        spawn=spawn,
    )
    assert st1 == 200
    first = json.loads(b1.decode("utf-8"))
    spawned = list(first["spawned_task_ids"])

    with session_factory() as session:
        yesterday = _utc_today(session) - timedelta(days=1)
        _move_attempt_claimed_at(
            session, claim_id=UUID(claim["claim_id"]), day=yesterday
        )
        # Seed expired history + unexpired-looking admin_replay for later purge.
        today = _utc_today(session)
        expired_children = [
            _create_past_child(
                session.connection(),
                parent,
                _expired_day_for(parent, today),
            )
            for parent in HISTORY_PARENTS
        ]
        now = _store_now(session)
        # Admin row that will become purgeable after we force expires_at.
        session.add(
            AdminReplay(
                admin_principal_id=ADMIN_PRINCIPAL,
                operation_code=1,
                key_hash=bytes.fromhex("aa" * 32),
                request_fingerprint=bytes.fromhex("bb" * 32),
                http_status=200,
                response_body={"op": "seed"},
                created_at=now - timedelta(days=40),
                expires_at=now - timedelta(days=10),
            )
        )
        session.commit()
        dedup_count = int(
            session.scalar(select(func.count()).select_from(EnqueueDedup)) or 0
        )
        replay_count = int(
            session.scalar(select(func.count()).select_from(CompleteReplay)) or 0
        )
        admin_count = int(
            session.scalar(select(func.count()).select_from(AdminReplay)) or 0
        )
        assert dedup_count >= 1
        assert replay_count >= 1
        assert admin_count >= 1

    # Drop expired history; live registries must still authorize replay/dedup.
    completed = _run_maintain(
        database_url=url,
        schema=schema,
        premake_days=14,
        payload_retention_days=30,
    )
    _assert_maintain_ok(completed)

    with session_factory() as session:
        for child in expired_children:
            assert not _relation_exists(session, child), child
        assert (
            int(session.scalar(select(func.count()).select_from(EnqueueDedup)) or 0)
            == dedup_count
        )
        assert (
            int(session.scalar(select(func.count()).select_from(CompleteReplay)) or 0)
            == replay_count
        )

    st2, _h2, b2 = _complete(
        app,
        claim_id=claim["claim_id"],
        claim_token=claim["claim_token"],
        generation=claim["generation"],
        spawn=spawn,
    )
    assert st2 == 200
    replayed = json.loads(b2.decode("utf-8"))
    assert replayed["replayed"] is True
    assert replayed["spawned_task_ids"] == spawned

    # Live enqueue_dedup row still enforces fingerprint conflict after history drop
    # (task is terminal, so matching replay of the active projection is not required).
    status_conflict, body_conflict = _enqueue(
        app, queue_name=queue_name, payload={"n": 8}, idempotency_key=idem
    )
    assert status_conflict == 409, body_conflict
    assert body_conflict["code"] == "idempotency_conflict"

    # Keep a second still-active task to prove matching enqueue replay across detach.
    idem_live = f"idem-live-{uuid.uuid4().hex}"
    status_live, body_live = _enqueue(
        app, queue_name=queue_name, payload={"live": 1}, idempotency_key=idem_live
    )
    assert status_live == 201
    live_task = body_live["task"]["task_id"]
    status_live2, body_live2 = _enqueue(
        app, queue_name=queue_name, payload={"live": 1}, idempotency_key=idem_live
    )
    assert status_live2 == 200, body_live2
    assert body_live2["task"]["task_id"] == live_task
    assert body_live2["replayed"] is True

    # Advance through configured registry expiry (Queue-store timestamp rewrite).
    # Exact Phase 3.1 post-TTL complete semantics are claim_not_found while the
    # expired row still exists; maintain then purges storage/singleton evidence.
    with session_factory() as session:
        now = _store_now(session)
        session.execute(
            update(EnqueueDedup).values(
                created_at=now - timedelta(days=100),
                expires_at=now - timedelta(days=1),
            )
        )
        session.execute(
            update(CompleteReplay).values(
                created_at=now - timedelta(days=10),
                expires_at=now - timedelta(days=1),
            )
        )
        session.execute(
            update(AdminReplay).values(
                created_at=now - timedelta(days=40),
                expires_at=now - timedelta(days=1),
            )
        )
        session.commit()

    st_nf, _hnf, body_nf = _complete(
        app,
        claim_id=claim["claim_id"],
        claim_token=claim["claim_token"],
        generation=claim["generation"],
        spawn=spawn,
    )
    assert st_nf == 404, body_nf.decode("utf-8", errors="replace")
    nf = json.loads(body_nf.decode("utf-8"))
    assert nf["code"] == "claim_not_found"
    assert nf["retryable"] is False

    # Phase 3.1 post-TTL: expired CompleteReplay row still present until maintain purge.
    with session_factory() as session:
        assert (
            int(session.scalar(select(func.count()).select_from(CompleteReplay)) or 0)
            >= 1
        )

    completed_purge = _run_maintain(
        database_url=url,
        schema=schema,
        premake_days=14,
        payload_retention_days=30,
        extra_env={"QUEUE_REGISTRY_PURGE_BATCH_SIZE": "1000"},
    )
    _assert_maintain_ok(completed_purge)

    with session_factory() as session:
        assert (
            int(session.scalar(select(func.count()).select_from(EnqueueDedup)) or 0)
            == 0
        )
        assert (
            int(session.scalar(select(func.count()).select_from(CompleteReplay)) or 0)
            == 0
        )
        assert (
            int(session.scalar(select(func.count()).select_from(AdminReplay)) or 0) == 0
        )
        status_row = session.execute(
            text(
                """
                SELECT last_succeeded_at IS NOT NULL
                FROM partition_maintenance_status WHERE singleton_id = 1
                """
            )
        ).scalar_one()
        assert status_row is True

    # Post-purge: same producer key may create a new operation.
    status_new, body_new = _enqueue(
        app, queue_name=queue_name, payload={"n": 7}, idempotency_key=idem
    )
    assert status_new == 201
    assert body_new["task"]["task_id"] != task_id


def test_tests_never_call_maintenance_primitives_directly() -> None:
    """STOR-05 black-box gate: retention must go through ``queue maintain`` CLI."""
    tree = Path(__file__).read_text(encoding="utf-8")
    # Strip this function's own documentation/asserts so the gate is real.
    marker = "def test_tests_never_call_maintenance_primitives_directly"
    body = tree.split(marker, 1)[0]
    assert "history_retention" not in body
    assert "registry_retention" not in body
    assert "run_storage_maintenance" not in body
    assert "from queue_service.roles import maintain" not in body
    assert "maintain.run_cycle" not in body
    assert '"maintain"' in body
    assert "queue_service" in body

