"""Real-PostgreSQL proof of transactional queue/instance depth admission."""

from __future__ import annotations

import os
import threading
import uuid
from collections.abc import Iterator
from typing import Any

import pytest
from sqlalchemy import create_engine, event, select, text
from sqlalchemy.orm import Session, sessionmaker

from workhold.domain.queue_control import (
    AdminRequestMetadata,
    BackoffStrategy,
    CreateQueueMutation,
    RetryPolicyDraft,
)
from workhold.infrastructure.postgres.queue_control_repository import (
    QueueControlRepository,
)
from workhold.intake.contracts import IntakeValidationError
from workhold.intake.depth import (
    DEFAULT_INSTANCE_ACTIVE_DEPTH,
    DEFAULT_QUEUE_ACTIVE_DEPTH,
    DepthCeilings,
    reserve_active_depth,
)
from workhold.storage.models import Queue, QueueCounter, TaskActive

_JOIN_TIMEOUT_S = 30.0


@pytest.fixture
def sa_engine(migrated_schema):
    """Engine bound to the isolated Alembic-migrated schema."""
    _conn, schema = migrated_schema
    database_url = os.environ.get("TEST_DATABASE_URL", "").strip()
    if not database_url:
        pytest.fail("TEST_DATABASE_URL is required for tests/integration")
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


def _admin_meta(*, actor_id: str = "admin-depth") -> AdminRequestMetadata:
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
    queue = session.execute(select(Queue).where(Queue.name == name)).scalar_one()
    return int(queue.id)


def _counter_depth(session: Session, queue_id: int) -> int:
    row = session.execute(
        select(QueueCounter).where(QueueCounter.queue_id == queue_id)
    ).scalar_one_or_none()
    if row is None:
        return 0
    return int(row.delayed_count + row.ready_count + row.leased_count)


def _instance_depth(session: Session) -> int:
    rows = session.execute(select(QueueCounter)).scalars().all()
    return sum(int(r.delayed_count + r.ready_count + r.leased_count) for r in rows)


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


def _assert_no_tasks_active_scan(statements: list[str]) -> None:
    joined = "\n".join(statements).lower()
    assert "tasks_active" not in joined
    assert "from tasks_active" not in joined
    assert "join tasks_active" not in joined


def test_exact_queue_ceiling_passes_and_one_over_is_retryable(
    sa_session: Session,
) -> None:
    queue_id = _seed_queue(sa_session, name=f"depth.q.{uuid.uuid4().hex[:8]}")
    # Shared session schema may already hold counters; keep instance headroom high.
    ceilings = DepthCeilings(
        queue_active_depth=2,
        instance_active_depth=DEFAULT_INSTANCE_ACTIVE_DEPTH,
    )

    first = reserve_active_depth(
        sa_session, queue_id=queue_id, units=1, ceilings=ceilings
    )
    second = reserve_active_depth(
        sa_session, queue_id=queue_id, units=1, ceilings=ceilings
    )
    sa_session.flush()
    assert first.queue_depth_after == 1
    assert second.queue_depth_after == 2
    assert _counter_depth(sa_session, queue_id) == 2

    with pytest.raises(IntakeValidationError) as exc_info:
        reserve_active_depth(sa_session, queue_id=queue_id, units=1, ceilings=ceilings)
    err = exc_info.value
    assert err.code == "resource_exhausted"
    assert err.retryable is True
    assert isinstance(err.retry_after_ms, int)
    assert err.retry_after_ms >= 0
    assert _counter_depth(sa_session, queue_id) == 2
    sa_session.rollback()


def test_instance_ceiling_independent_of_queue_ceiling(
    sa_session: Session,
) -> None:
    left = _seed_queue(sa_session, name=f"depth.i.l.{uuid.uuid4().hex[:8]}")
    right = _seed_queue(sa_session, name=f"depth.i.r.{uuid.uuid4().hex[:8]}")
    baseline = _instance_depth(sa_session)
    ceilings = DepthCeilings(
        queue_active_depth=DEFAULT_QUEUE_ACTIVE_DEPTH,
        instance_active_depth=baseline + 2,
    )

    reserve_active_depth(sa_session, queue_id=left, units=1, ceilings=ceilings)
    reserve_active_depth(sa_session, queue_id=right, units=1, ceilings=ceilings)
    sa_session.flush()
    assert _instance_depth(sa_session) == baseline + 2

    with pytest.raises(IntakeValidationError) as exc_info:
        reserve_active_depth(sa_session, queue_id=left, units=1, ceilings=ceilings)
    assert exc_info.value.code == "resource_exhausted"
    assert exc_info.value.retryable is True
    assert _instance_depth(sa_session) == baseline + 2
    sa_session.rollback()


def test_rollback_restores_queue_and_instance_counters(
    sa_session: Session,
) -> None:
    queue_id = _seed_queue(sa_session, name=f"depth.rb.{uuid.uuid4().hex[:8]}")
    baseline_instance = _instance_depth(sa_session)
    ceilings = DepthCeilings(
        queue_active_depth=10,
        instance_active_depth=baseline_instance + 10,
    )

    reserve_active_depth(sa_session, queue_id=queue_id, units=1, ceilings=ceilings)
    sa_session.commit()
    assert _counter_depth(sa_session, queue_id) == 1
    assert _instance_depth(sa_session) == baseline_instance + 1

    reserve_active_depth(sa_session, queue_id=queue_id, units=2, ceilings=ceilings)
    sa_session.flush()
    assert _counter_depth(sa_session, queue_id) == 3
    assert _instance_depth(sa_session) == baseline_instance + 3

    sa_session.rollback()
    sa_session.expire_all()
    assert _counter_depth(sa_session, queue_id) == 1
    assert _instance_depth(sa_session) == baseline_instance + 1


def test_concurrent_reservations_never_exceed_ceilings(
    session_factory: sessionmaker[Session],
) -> None:
    with session_factory() as setup:
        queue_id = _seed_queue(setup, name=f"depth.race.{uuid.uuid4().hex[:8]}")
        baseline = _instance_depth(setup)
    ceilings = DepthCeilings(
        queue_active_depth=5,
        instance_active_depth=baseline + 5,
    )
    barrier = threading.Barrier(8, timeout=_JOIN_TIMEOUT_S)
    successes: list[bool] = []
    lock = threading.Lock()

    def _worker() -> None:
        session = session_factory()
        try:
            barrier.wait()
            try:
                reserve_active_depth(
                    session, queue_id=queue_id, units=1, ceilings=ceilings
                )
                session.commit()
                with lock:
                    successes.append(True)
            except IntakeValidationError as exc:
                session.rollback()
                assert exc.code == "resource_exhausted"
                with lock:
                    successes.append(False)
        finally:
            session.close()

    threads = [threading.Thread(target=_worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=_JOIN_TIMEOUT_S)
        assert not t.is_alive()

    assert sum(1 for ok in successes if ok) == 5
    assert sum(1 for ok in successes if not ok) == 3

    with session_factory() as check:
        assert _counter_depth(check, queue_id) == 5
        assert _instance_depth(check) == baseline + 5


def test_depth_reservation_never_scans_tasks_active(
    sa_session: Session,
) -> None:
    queue_id = _seed_queue(sa_session, name=f"depth.scan.{uuid.uuid4().hex[:8]}")
    # Poison the hot table so any accidental scan would be expensive/visible.
    sa_session.execute(
        text(
            """
            INSERT INTO tasks_active (
                task_id, queue_id, producer_id, state_code, priority,
                available_at, retry_policy_version_id, generation
            )
            SELECT
                gen_random_uuid(),
                :queue_id,
                'poison',
                2,
                0,
                statement_timestamp(),
                (SELECT active_policy_version_id FROM queues WHERE id = :queue_id),
                0
            FROM generate_series(1, 3)
            """
        ),
        {"queue_id": queue_id},
    )
    sa_session.commit()
    assert sa_session.scalar(select(TaskActive).where(TaskActive.queue_id == queue_id))

    statements = _instrument_statements(sa_session)
    ceilings = DepthCeilings(queue_active_depth=10, instance_active_depth=10)
    reserve_active_depth(sa_session, queue_id=queue_id, units=1, ceilings=ceilings)
    sa_session.flush()
    _assert_no_tasks_active_scan(statements)

    explain = sa_session.execute(
        text(
            """
            EXPLAIN
            SELECT delayed_count, ready_count, leased_count
            FROM queue_counters
            WHERE queue_id = :queue_id
            FOR UPDATE
            """
        ),
        {"queue_id": queue_id},
    ).fetchall()
    plan = "\n".join(row[0] for row in explain).lower()
    assert "tasks_active" not in plan
    sa_session.rollback()


def test_depth_helpers_never_commit_or_allocate_session(
    sa_session: Session,
) -> None:
    queue_id = _seed_queue(sa_session, name=f"depth.uow.{uuid.uuid4().hex[:8]}")
    baseline = _instance_depth(sa_session)
    commits = {"n": 0}
    rollbacks = {"n": 0}
    real_commit = sa_session.commit
    real_rollback = sa_session.rollback

    def counting_commit(*args: Any, **kwargs: Any) -> None:
        commits["n"] += 1
        return real_commit(*args, **kwargs)

    def counting_rollback(*args: Any, **kwargs: Any) -> None:
        rollbacks["n"] += 1
        return real_rollback(*args, **kwargs)

    sa_session.commit = counting_commit  # type: ignore[method-assign]
    sa_session.rollback = counting_rollback  # type: ignore[method-assign]

    ceilings = DepthCeilings(
        queue_active_depth=3,
        instance_active_depth=baseline + 3,
    )
    reserve_active_depth(sa_session, queue_id=queue_id, units=1, ceilings=ceilings)
    assert commits["n"] == 0
    assert rollbacks["n"] == 0
    sa_session.rollback = real_rollback  # type: ignore[method-assign]
    sa_session.commit = real_commit  # type: ignore[method-assign]
    sa_session.rollback()


def test_runtime_queue_ceiling_may_only_tighten_deployment(
    sa_session: Session,
) -> None:
    queue_id = _seed_queue(sa_session, name=f"depth.tight.{uuid.uuid4().hex[:8]}")
    deployment = DepthCeilings(
        queue_active_depth=DEFAULT_QUEUE_ACTIVE_DEPTH,
        instance_active_depth=DEFAULT_INSTANCE_ACTIVE_DEPTH,
    )
    tight = deployment.with_queue_ceiling(1)
    reserve_active_depth(sa_session, queue_id=queue_id, units=1, ceilings=tight)
    with pytest.raises(IntakeValidationError) as exc_info:
        reserve_active_depth(sa_session, queue_id=queue_id, units=1, ceilings=tight)
    assert exc_info.value.code == "resource_exhausted"
    # Raising above deployment hard ceiling is rejected.
    with pytest.raises(ValueError):
        deployment.with_queue_ceiling(DEFAULT_QUEUE_ACTIVE_DEPTH + 1)
    sa_session.rollback()
