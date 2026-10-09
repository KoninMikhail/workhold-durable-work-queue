"""PostgreSQL coverage for bounded operational inspection lists (API-05)."""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from collections.abc import Iterator, Mapping
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlencode

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import Session, sessionmaker

from workhold.api.admin import create_admin_app
from workhold.api.security import ListenerBind
from workhold.domain.queue_control import (
    AdminRequestMetadata,
    BackoffStrategy,
    CreateQueueMutation,
    RetryPolicyDraft,
)
from workhold.infrastructure.postgres.queue_control_repository import (
    QueueControlRepository,
)
from workhold.operations.inspection import OperationalInspectionService
from workhold.security.authorization import Authorizer
from workhold.security.credentials import (
    BearerCredentialAuthenticator,
    CredentialBinding,
)
from workhold.security.cursors import InspectionCursorCodec
from workhold.security.principals import ServiceRole
from workhold.settings import Secret

pytest_plugins = ["tests.integration.conftest"]

OBSERVER_TOKEN = "tok-observer-inspect"
ADMIN_TOKEN = "tok-admin-inspect"
PRODUCER_TOKEN = "tok-producer-inspect"
WORKER_TOKEN = "tok-worker-inspect"

OBSERVER_PRINCIPAL = "observer-inspect"
ADMIN_PRINCIPAL = "admin-inspect"
PRODUCER_PRINCIPAL = "producer-inspect"
WORKER_PRINCIPAL = "worker-inspect"

QUEUE_NAME = "orders.inspect"
CURSOR_SECRET = Secret("inspection-cursor-test-secret")


def _unique_queue_name(prefix: str = "orders.inspect") -> str:
    return f"{prefix}.{uuid.uuid4().hex[:8]}"


def _bindings(_queue_name: str) -> tuple[CredentialBinding, ...]:
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
        pytest.fail("TEST_DATABASE_URL is required for tests/integration/operations")
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
    *,
    queue_name: str,
) -> Any:
    return create_admin_app(
        authenticator=BearerCredentialAuthenticator.from_bindings(_bindings(queue_name)),
        authorizer=Authorizer(
            queue_scopes={
                OBSERVER_PRINCIPAL: frozenset({queue_name}),
                ADMIN_PRINCIPAL: frozenset({queue_name}),
                PRODUCER_PRINCIPAL: frozenset({queue_name}),
                WORKER_PRINCIPAL: frozenset({queue_name}),
            }
        ),
        bind=ListenerBind(host="127.0.0.1", port=18095),
        session_factory=session_factory,
        repository=QueueControlRepository(),
        cursor_secret=CURSOR_SECRET,
    )


def _asgi_http_call(
    app: Any,
    *,
    method: str,
    path: str,
    headers: Mapping[str, str] | None = None,
    body: bytes = b"",
    query: Mapping[str, str] | None = None,
) -> tuple[int, dict[str, str], bytes]:
    header_list = [
        (k.lower().encode("latin-1"), v.encode("latin-1"))
        for k, v in (headers or {}).items()
    ]
    if body and not any(k == b"content-length" for k, _ in header_list):
        header_list.append((b"content-length", str(len(body)).encode("latin-1")))
    query_string = urlencode(dict(query or {})).encode("utf-8")

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
        "client": ("127.0.0.1", 9),
        "server": ("127.0.0.1", 18095),
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


def _admin_meta(*, actor_id: str = ADMIN_PRINCIPAL) -> AdminRequestMetadata:
    return AdminRequestMetadata(
        actor_id=actor_id,
        request_id=str(uuid.uuid4()),
        idempotency_key=f"admin-idem-{uuid.uuid4().hex}",
    )


def _seed_queue(session: Session, *, name: str) -> int:
    control = QueueControlRepository()
    control.create_named_queue(
        session,
        CreateQueueMutation(
            name=name,
            initial_policy=RetryPolicyDraft(
                enabled=True,
                max_attempts=3,
                backoff_strategy=BackoffStrategy.FIXED,
                retry_delay_seconds=5,
            ),
            metadata=_admin_meta(),
        ),
    )
    session.commit()
    return int(
        session.execute(
            text("SELECT id FROM queues WHERE name = :name"),
            {"name": name},
        ).scalar_one()
    )


def _policy_id(session: Session, queue_pk: int) -> int:
    return int(
        session.execute(
            text(
                "SELECT active_policy_version_id FROM queues WHERE id = :queue_id"
            ),
            {"queue_id": queue_pk},
        ).scalar_one()
    )


def _insert_active_tasks(session: Session, *, queue_pk: int, count: int) -> list[uuid.UUID]:
    policy = _policy_id(session, queue_pk)
    ids: list[uuid.UUID] = []
    for i in range(count):
        task_id = uuid.uuid4()
        ids.append(task_id)
        session.execute(
            text(
                """
                INSERT INTO tasks_active (
                    task_id, queue_id, producer_id, state_code, priority,
                    available_at, retry_policy_version_id, generation, created_at, updated_at
                ) VALUES (
                    :task_id, :queue_id, :producer_id, 2, 0,
                    statement_timestamp() - make_interval(mins => :mins),
                    :policy_id, 0,
                    statement_timestamp() - make_interval(mins => :mins),
                    statement_timestamp()
                )
                """
            ),
            {
                "task_id": str(task_id),
                "queue_id": queue_pk,
                "producer_id": "producer-inspect",
                "policy_id": policy,
                "mins": count - i,
            },
        )
    session.commit()
    return ids


def _insert_attempts(
    session: Session,
    *,
    task_id: uuid.UUID,
    count: int,
    base: datetime,
) -> None:
    for i in range(count):
        claimed = base + timedelta(seconds=i)
        session.execute(
            text(
                """
                INSERT INTO task_attempts (
                    task_id, claim_id, generation, claimed_at, worker_id,
                    lease_expires_at, ended_at, outcome_code
                ) VALUES (
                    :task_id, :claim_id, 1, :claimed_at, :worker_id,
                    :claimed_at + interval '30 seconds', :claimed_at + interval '1 second', 2
                )
                """
            ),
            {
                "task_id": str(task_id),
                "claim_id": str(uuid.uuid4()),
                "claimed_at": claimed,
                "worker_id": "worker-a",
            },
        )
    session.commit()


def _insert_dead_letters(
    session: Session,
    *,
    queue_pk: int,
    count: int,
    base: datetime,
) -> list[uuid.UUID]:
    ids: list[uuid.UUID] = []
    for i in range(count):
        task_id = uuid.uuid4()
        ids.append(task_id)
        terminal_at = base + timedelta(seconds=i)
        session.execute(
            text(
                """
                INSERT INTO tasks_terminal (
                    task_id, queue_id, producer_id, state_code, priority,
                    available_at, retry_policy_version, payload, payload_bytes,
                    created_at, terminal_at, failure_code, failure_detail
                ) VALUES (
                    :task_id, :queue_id, :producer_id, 11, 0,
                    :terminal_at, 1, '{"secret":"should-not-leak"}'::jsonb, 28,
                    :terminal_at, :terminal_at, 'exhausted', 'retries exhausted'
                )
                """
            ),
            {
                "task_id": str(task_id),
                "queue_id": queue_pk,
                "producer_id": "producer-inspect",
                "terminal_at": terminal_at,
            },
        )
    session.commit()
    return ids


def test_task_list_paginates_stably_and_redacts(
    sa_session: Session,
    session_factory: sessionmaker[Session],
) -> None:
    queue_name = _unique_queue_name()
    admin_app = _make_admin_app(session_factory, queue_name=queue_name)
    queue_pk = _seed_queue(sa_session, name=queue_name)
    _insert_active_tasks(sa_session, queue_pk=queue_pk, count=5)

    status, _headers, body = _asgi_http_call(
        admin_app,
        method="GET",
        path="/admin/v1/tasks",
        headers={"authorization": f"Bearer {OBSERVER_TOKEN}"},
        query={"queue_name": queue_name, "limit": "2"},
    )
    assert status == 200, body
    page1 = json.loads(body.decode("utf-8"))
    assert len(page1["items"]) == 2
    assert page1["next_cursor"]
    for item in page1["items"]:
        assert item.get("payload") is None
        assert "claim_token" not in item
        if item.get("current_claim") is not None:
            assert "claim_token" not in item["current_claim"]

    status, _headers, body = _asgi_http_call(
        admin_app,
        method="GET",
        path="/admin/v1/tasks",
        headers={"authorization": f"Bearer {OBSERVER_TOKEN}"},
        query={
            "queue_name": queue_name,
            "limit": "2",
            "cursor": page1["next_cursor"],
        },
    )
    assert status == 200, body
    page2 = json.loads(body.decode("utf-8"))
    ids1 = {item["task_id"] for item in page1["items"]}
    ids2 = {item["task_id"] for item in page2["items"]}
    assert ids1.isdisjoint(ids2)


def test_attempt_pagination_requires_bounds_and_rejects_tampered_cursor(
    sa_session: Session,
    session_factory: sessionmaker[Session],
) -> None:
    queue_name = _unique_queue_name()
    admin_app = _make_admin_app(session_factory, queue_name=queue_name)
    queue_pk = _seed_queue(sa_session, name=queue_name)
    task_ids = _insert_active_tasks(sa_session, queue_pk=queue_pk, count=1)
    task_id = task_ids[0]
    base = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(minutes=10)
    _insert_attempts(sa_session, task_id=task_id, count=4, base=base)
    window = {
        "task_id": str(task_id),
        "from": (base - timedelta(seconds=1)).isoformat().replace("+00:00", "Z"),
        "to": (base + timedelta(hours=1)).isoformat().replace("+00:00", "Z"),
        "limit": "2",
    }

    status, _headers, body = _asgi_http_call(
        admin_app,
        method="GET",
        path="/admin/v1/attempts",
        headers={"authorization": f"Bearer {ADMIN_TOKEN}"},
        query={"task_id": str(task_id), "limit": "2"},
    )
    assert status == 400
    err = json.loads(body.decode("utf-8"))
    assert err["code"] == "validation_failed"
    assert err["retryable"] is False

    status, _headers, body = _asgi_http_call(
        admin_app,
        method="GET",
        path="/admin/v1/attempts",
        headers={"authorization": f"Bearer {ADMIN_TOKEN}"},
        query=window,
    )
    assert status == 200, body
    page1 = json.loads(body.decode("utf-8"))
    assert len(page1["items"]) == 2
    assert page1["next_cursor"]

    tampered = page1["next_cursor"][:-4] + "dead"
    status, _headers, body = _asgi_http_call(
        admin_app,
        method="GET",
        path="/admin/v1/attempts",
        headers={"authorization": f"Bearer {ADMIN_TOKEN}"},
        query={**window, "cursor": tampered},
    )
    assert status == 400
    err = json.loads(body.decode("utf-8"))
    assert err["code"] == "validation_failed"
    assert err["retryable"] is False

    status, _headers, body = _asgi_http_call(
        admin_app,
        method="GET",
        path="/admin/v1/attempts",
        headers={"authorization": f"Bearer {ADMIN_TOKEN}"},
        query={**window, "cursor": page1["next_cursor"]},
    )
    assert status == 200, body
    page2 = json.loads(body.decode("utf-8"))
    assert {i["attempt_id"] for i in page1["items"]}.isdisjoint(
        {i["attempt_id"] for i in page2["items"]}
    )


def test_dead_letter_and_audit_lists_reject_search_and_authorize(
    sa_session: Session,
    session_factory: sessionmaker[Session],
) -> None:
    queue_name = _unique_queue_name()
    admin_app = _make_admin_app(session_factory, queue_name=queue_name)
    queue_pk = _seed_queue(sa_session, name=queue_name)
    base = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(minutes=5)
    _insert_dead_letters(sa_session, queue_pk=queue_pk, count=3, base=base)
    bounds = {
        "queue_name": queue_name,
        "from": (base - timedelta(seconds=1)).isoformat().replace("+00:00", "Z"),
        "to": (base + timedelta(hours=1)).isoformat().replace("+00:00", "Z"),
        "limit": "10",
    }

    status, _headers, body = _asgi_http_call(
        admin_app,
        method="GET",
        path="/admin/v1/dead-letters",
        headers={"authorization": f"Bearer {PRODUCER_TOKEN}"},
        query=bounds,
    )
    assert status == 403

    status, _headers, body = _asgi_http_call(
        admin_app,
        method="GET",
        path="/admin/v1/dead-letters",
        headers={"authorization": f"Bearer {OBSERVER_TOKEN}"},
        query={**bounds, "search": "secret"},
    )
    assert status == 400
    err = json.loads(body.decode("utf-8"))
    assert err["code"] == "validation_failed"

    status, _headers, body = _asgi_http_call(
        admin_app,
        method="GET",
        path="/admin/v1/dead-letters",
        headers={"authorization": f"Bearer {OBSERVER_TOKEN}"},
        query=bounds,
    )
    assert status == 200, body
    page = json.loads(body.decode("utf-8"))
    assert len(page["items"]) == 3
    for item in page["items"]:
        assert item["state"] == "dead_lettered"
        assert item.get("payload") is None
        assert "claim_token" not in item
        assert "should-not-leak" not in json.dumps(item)

    status, _headers, body = _asgi_http_call(
        admin_app,
        method="GET",
        path="/admin/v1/audit",
        headers={"authorization": f"Bearer {OBSERVER_TOKEN}"},
        query={
            "from": bounds["from"],
            "to": bounds["to"],
        },
    )
    assert status == 403

    status, _headers, body = _asgi_http_call(
        admin_app,
        method="GET",
        path="/admin/v1/audit",
        headers={"authorization": f"Bearer {ADMIN_TOKEN}"},
        query={
            "queue_name": queue_name,
            "from": bounds["from"],
            "to": bounds["to"],
            "limit": "10",
        },
    )
    assert status == 200, body
    audit = json.loads(body.decode("utf-8"))
    assert len(audit["items"]) >= 1
    for item in audit["items"]:
        assert "claim_token" not in item
        assert "payload" not in item or item["payload"] is None


def test_query_plans_use_partition_keys_and_indexes(
    sa_session: Session,
) -> None:
    queue_name = _unique_queue_name()
    queue_pk = _seed_queue(sa_session, name=queue_name)
    task_ids = _insert_active_tasks(sa_session, queue_pk=queue_pk, count=1)
    task_id = task_ids[0]
    base = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(minutes=30)
    _insert_attempts(sa_session, task_id=task_id, count=20, base=base)
    _insert_dead_letters(sa_session, queue_pk=queue_pk, count=20, base=base)

    service = OperationalInspectionService(
        cursor_codec=InspectionCursorCodec(CURSOR_SECRET)
    )
    time_from = base - timedelta(seconds=1)
    time_to = base + timedelta(hours=2)

    attempts_plan = service.explain_attempts_plan(
        sa_session,
        task_id=task_id,
        time_from=time_from,
        time_to=time_to,
    ).lower()
    assert "task_attempts" in attempts_plan
    assert "seq scan" not in attempts_plan or "index" in attempts_plan
    assert "claimed_at" in attempts_plan or "task_attempts_task_claimed" in attempts_plan

    dlq_plan = service.explain_dead_letters_plan(
        sa_session,
        queue_id=queue_pk,
        time_from=time_from,
        time_to=time_to,
    ).lower()
    assert "tasks_terminal" in dlq_plan
    assert "terminal_at" in dlq_plan

    audit_plan = service.explain_audit_plan(
        sa_session,
        time_from=time_from,
        time_to=time_to,
        queue_id=queue_pk,
    ).lower()
    assert "admin_audit_log" in audit_plan
    assert "audit_at" in audit_plan
    assert "seq scan on admin_audit_log " not in audit_plan or "index" in audit_plan


def test_observer_out_of_scope_queue_denied(
    sa_session: Session,
    session_factory: sessionmaker[Session],
) -> None:
    queue_name = _unique_queue_name()
    admin_app = _make_admin_app(session_factory, queue_name=queue_name)
    _seed_queue(sa_session, name=queue_name)
    other = _unique_queue_name("other.queue")
    _seed_queue(sa_session, name=other)
    status, _headers, body = _asgi_http_call(
        admin_app,
        method="GET",
        path="/admin/v1/tasks",
        headers={"authorization": f"Bearer {OBSERVER_TOKEN}"},
        query={"queue_name": other, "limit": "10"},
    )
    assert status == 403
    err = json.loads(body.decode("utf-8"))
    assert err["code"] == "permission_denied"
