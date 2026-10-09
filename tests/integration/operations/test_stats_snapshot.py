"""PostgreSQL integration coverage for bounded observer statistics (OPS-03)."""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from collections.abc import Iterator, Mapping
from datetime import datetime, timedelta, timezone
from typing import Any

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
from workhold.observability.metrics import KernelMetrics
from workhold.operations.stats import STATS_MAX_AGE_SECONDS, build_stats_snapshot
from workhold.security.authorization import Authorizer
from workhold.security.credentials import (
    BearerCredentialAuthenticator,
    CredentialBinding,
)
from workhold.security.principals import ServiceRole
from workhold.settings import Secret

pytest_plugins = ["tests.integration.conftest"]

OBSERVER_TOKEN = "tok-observer-stats"
ADMIN_TOKEN = "tok-admin-stats"
PRODUCER_TOKEN = "tok-producer-stats"
WORKER_TOKEN = "tok-worker-stats"

OBSERVER_PRINCIPAL = "observer-stats"
ADMIN_PRINCIPAL = "admin-stats"
PRODUCER_PRINCIPAL = "producer-stats"
WORKER_PRINCIPAL = "worker-stats"


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


@pytest.fixture
def metrics() -> KernelMetrics:
    return KernelMetrics(process_role="admin")


@pytest.fixture
def admin_app(session_factory: sessionmaker[Session], metrics: KernelMetrics) -> Any:
    return create_admin_app(
        authenticator=BearerCredentialAuthenticator.from_bindings(_bindings()),
        authorizer=Authorizer(
            queue_scopes={
                OBSERVER_PRINCIPAL: frozenset({"orders.stats"}),
                ADMIN_PRINCIPAL: frozenset({"orders.stats"}),
                PRODUCER_PRINCIPAL: frozenset({"orders.stats"}),
                WORKER_PRINCIPAL: frozenset({"orders.stats"}),
            }
        ),
        bind=ListenerBind(host="127.0.0.1", port=18092),
        session_factory=session_factory,
        repository=QueueControlRepository(),
        metrics=metrics,
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
        "server": ("127.0.0.1", 18092),
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


def _admin_meta(*, actor_id: str = "admin-stats") -> AdminRequestMetadata:
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
    queue_id = session.execute(
        text("SELECT id FROM queues WHERE name = :name"),
        {"name": name},
    ).scalar_one()
    return int(queue_id)


def _seed_ready_task(session: Session, *, queue_id: int, age_seconds: float) -> None:
    available_at = datetime.now(timezone.utc) - timedelta(seconds=age_seconds)
    session.execute(
        text(
            """
            INSERT INTO tasks_active (
                task_id, queue_id, producer_id, state_code, priority,
                available_at, retry_policy_version_id, generation
            )
            VALUES (
                gen_random_uuid(),
                :queue_id,
                'producer-stats',
                2,
                0,
                :available_at,
                (SELECT active_policy_version_id FROM queues WHERE id = :queue_id),
                0
            )
            """
        ),
        {"queue_id": queue_id, "available_at": available_at},
    )
    session.execute(
        text(
            """
            UPDATE queue_counters
            SET ready_count = ready_count + 1,
                as_of = statement_timestamp()
            WHERE queue_id = :queue_id
            """
        ),
        {"queue_id": queue_id},
    )
    session.commit()


def _seed_poison_terminal_history(session: Session, *, queue_id: int, rows: int) -> None:
    """Insert many terminal rows to prove stats ignore retained history volume."""
    session.execute(
        text(
            """
            INSERT INTO tasks_terminal (
                task_id, queue_id, producer_id, state_code, priority,
                available_at, retry_policy_version, payload, payload_bytes,
                created_at, terminal_at, failure_detail
            )
            SELECT
                gen_random_uuid(),
                :queue_id,
                'poison',
                10,
                0,
                statement_timestamp(),
                1,
                CAST(:payload AS jsonb),
                :payload_bytes,
                statement_timestamp(),
                statement_timestamp(),
                :failure_detail
            FROM generate_series(1, :rows)
            """
        ),
        {
            "queue_id": queue_id,
            "rows": rows,
            "payload": '{"poison": true}',
            "payload_bytes": 16,
            "failure_detail": "poison-payload-should-never-be-scanned",
        },
    )
    session.execute(
        text(
            """
            INSERT INTO task_attempts (
                task_id, claim_id, generation, claimed_at, worker_id,
                lease_expires_at, ended_at, outcome_code, failure_detail
            )
            SELECT
                gen_random_uuid(),
                gen_random_uuid(),
                1,
                statement_timestamp(),
                'poison-worker',
                statement_timestamp() + interval '30 seconds',
                statement_timestamp(),
                1,
                :failure_detail
            FROM generate_series(1, :rows)
            """
        ),
        {
            "queue_id": queue_id,
            "rows": rows,
            "failure_detail": "poison-payload-should-never-be-scanned",
        },
    )
    session.commit()


def _instrument_statements(session: Session) -> list[str]:
    statements: list[str] = []

    def _before_cursor_execute(  # noqa: ANN001
        _conn,
        _cursor,
        statement,
        _parameters,
        _context,
        _executemany,
    ) -> None:
        statements.append(str(statement))

    event.listen(session.get_bind(), "before_cursor_execute", _before_cursor_execute)
    return statements


def test_observer_stats_snapshot_declares_freshness_and_depths(
    sa_session: Session,
    admin_app: Any,
    metrics: KernelMetrics,
) -> None:
    queue_name = f"orders.stats.{uuid.uuid4().hex[:8]}"
    queue_id = _seed_queue(sa_session, name=queue_name)
    _seed_ready_task(sa_session, queue_id=queue_id, age_seconds=12.0)

    status, headers, body = _asgi_http_call(
        admin_app,
        method="GET",
        path="/admin/v1/stats",
        headers={"authorization": f"Bearer {OBSERVER_TOKEN}"},
    )
    assert status == 200, body.decode()
    assert "x-request-id" in headers
    payload = json.loads(body.decode())

    assert "as_of" in payload
    assert "generated_at" in payload
    assert "age_seconds" in payload
    assert payload["freshness"] in {"fresh", "stale", "unavailable"}
    assert isinstance(payload["queues"], list)
    match = next(q for q in payload["queues"] if q["name"] == queue_name)
    assert match["ready_depth"] == 1
    assert match["delayed_depth"] == 0
    assert match["leased_depth"] == 0
    assert match["oldest_ready_age_seconds"] is not None
    assert match["oldest_ready_age_seconds"] >= 10.0
    assert "retry" in payload
    assert "dead_letter" in payload
    assert "maintenance" in payload
    # Threat T-04-02-I: no payloads / claim tokens / worker ids.
    dumped = json.dumps(payload)
    assert "poison-payload" not in dumped
    assert "claim_token" not in dumped
    assert "worker_id" not in dumped

    samples = metrics.snapshot()
    age_samples = [
        s for s in samples if s.name == "queue_stats_snapshot_age_seconds"
    ]
    assert age_samples, "Plan 01 telemetry must record stats snapshot age"
    assert age_samples[0].value >= 0.0


def test_producer_and_worker_denied_for_stats(admin_app: Any) -> None:
    for token in (PRODUCER_TOKEN, WORKER_TOKEN):
        status, _headers, body = _asgi_http_call(
            admin_app,
            method="GET",
            path="/admin/v1/stats",
            headers={"authorization": f"Bearer {token}"},
        )
        assert status == 403, body.decode()
        payload = json.loads(body.decode())
        assert payload["code"] == "permission_denied"


def test_admin_allowed_for_stats(sa_session: Session, admin_app: Any) -> None:
    _seed_queue(sa_session, name=f"orders.stats.admin.{uuid.uuid4().hex[:8]}")
    status, _headers, body = _asgi_http_call(
        admin_app,
        method="GET",
        path="/admin/v1/stats",
        headers={"authorization": f"Bearer {ADMIN_TOKEN}"},
    )
    assert status == 200, body.decode()
    payload = json.loads(body.decode())
    assert payload["freshness"] in {"fresh", "stale", "unavailable"}


def test_stats_query_budget_independent_of_terminal_history(
    sa_session: Session,
) -> None:
    queue_name = f"orders.stats.hist.{uuid.uuid4().hex[:8]}"
    queue_id = _seed_queue(sa_session, name=queue_name)
    _seed_ready_task(sa_session, queue_id=queue_id, age_seconds=3.0)
    _seed_poison_terminal_history(sa_session, queue_id=queue_id, rows=200)

    statements = _instrument_statements(sa_session)
    snapshot = build_stats_snapshot(sa_session, metrics=KernelMetrics(process_role="admin"))
    sa_session.rollback()

    joined = "\n".join(statements).lower()
    # Phase 4 invariant: no terminal/attempt/payload history scans; queue depth
    # comes from queue_counters. Phase 5 may COUNT(*) FILTER on
    # delivery_events_active only (bounded active projection, not history).
    assert "tasks_terminal" not in joined
    assert "task_attempts" not in joined
    assert "task_payloads" not in joined
    assert "queue_counters" in joined
    for stmt in statements:
        low = stmt.lower()
        if "count(*)" in low:
            assert "delivery_events_active" in low
            assert "tasks_active" not in low
            assert "tasks_terminal" not in low

    match = next(q for q in snapshot["queues"] if q["name"] == queue_name)
    assert match["ready_depth"] == 1
    assert snapshot["freshness"] in {"fresh", "stale", "unavailable"}


def test_stale_counter_material_is_explicit(sa_session: Session) -> None:
    queue_name = f"orders.stats.stale.{uuid.uuid4().hex[:8]}"
    queue_id = _seed_queue(sa_session, name=queue_name)
    stale_at = datetime.now(timezone.utc) - timedelta(
        seconds=STATS_MAX_AGE_SECONDS + 120
    )
    sa_session.execute(
        text(
            """
            UPDATE queue_counters
            SET as_of = :stale_at
            WHERE queue_id = :queue_id
            """
        ),
        {"queue_id": queue_id, "stale_at": stale_at},
    )
    sa_session.commit()

    snapshot = build_stats_snapshot(sa_session, metrics=None)
    match = next(q for q in snapshot["queues"] if q["name"] == queue_name)
    assert match["freshness"] == "stale"
    assert snapshot["freshness"] == "stale"
    assert snapshot["retry"]["availability"] == "unavailable"
    assert snapshot["dead_letter"]["availability"] == "unavailable"
