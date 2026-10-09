"""Real-PostgreSQL proof of flush-only enqueue repository UoW (Phase 03.4-02)."""

from __future__ import annotations

import hashlib
import os
import threading
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import pytest
from sqlalchemy import create_engine, event, func, select
from sqlalchemy.orm import Session, sessionmaker

from queue_service.domain.queue_control import (
    ActivatePolicyMutation,
    AdminRequestMetadata,
    BackoffStrategy,
    ConfigVersion,
    CreatePolicyMutation,
    CreateQueueMutation,
    PolicyVersion,
    RetryPolicyDraft,
)
from queue_service.infrastructure.postgres.queue_control_repository import (
    QueueControlRepository,
)
from queue_service.intake.contracts import IntakeValidationError, normalize_enqueue_command
from queue_service.intake.repository import (
    EnqueuePersistenceResult,
    EnqueueRepository,
    _probe_dedup,
)
from queue_service.storage.models import (
    EnqueueDedup,
    Queue,
    QueuePolicyVersion,
    TaskActive,
    TaskPayloadActive,
)

_STATE_DELAYED = 1

_JOIN_TIMEOUT_S = 30.0
_STATE_READY = 2
_DEDUP_TTL_DAYS = 90


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


def _admin_meta(*, actor_id: str = "admin-enqueue") -> AdminRequestMetadata:
    return AdminRequestMetadata(
        actor_id=actor_id,
        request_id=str(uuid.uuid4()),
        idempotency_key=f"admin-idem-{uuid.uuid4().hex}",
    )


def _seed_queue(
    session: Session,
    *,
    name: str,
    max_attempts: int = 3,
    retry_delay_seconds: int = 5,
) -> None:
    control = QueueControlRepository()
    control.create_named_queue(
        session,
        CreateQueueMutation(
            name=name,
            initial_policy=RetryPolicyDraft(
                enabled=True,
                max_attempts=max_attempts,
                backoff_strategy=BackoffStrategy.FIXED,
                retry_delay_seconds=retry_delay_seconds,
            ),
            metadata=_admin_meta(),
        ),
    )
    session.commit()


def _command(
    *,
    producer_id: str = "producer-a",
    queue_name: str,
    idempotency_key: str,
    payload: Any | None = None,
    fingerprint_payload: Any | None = None,
    available_at: datetime | None = None,
    priority: int = 0,
) -> Any:
    return normalize_enqueue_command(
        producer_id=producer_id,
        queue_name=queue_name,
        idempotency_key=idempotency_key,
        payload=fingerprint_payload if fingerprint_payload is not None else (payload or {"n": 1}),
        priority=priority,
        available_at=available_at,
    )


def _counts_for_queue(session: Session, queue_name: str) -> tuple[int, int, int]:
    queue = session.execute(select(Queue).where(Queue.name == queue_name)).scalar_one()
    tasks = int(
        session.scalar(
            select(func.count())
            .select_from(TaskActive)
            .where(TaskActive.queue_id == queue.id)
        )
        or 0
    )
    payloads = int(
        session.scalar(
            select(func.count())
            .select_from(TaskPayloadActive)
            .join(TaskActive, TaskActive.id == TaskPayloadActive.task_id)
            .where(TaskActive.queue_id == queue.id)
        )
        or 0
    )
    dedups = int(
        session.scalar(
            select(func.count())
            .select_from(EnqueueDedup)
            .where(EnqueueDedup.queue_id == queue.id)
        )
        or 0
    )
    return tasks, payloads, dedups


def _instrument_session(session: Session) -> dict[str, int]:
    counters = {"commit": 0, "rollback": 0, "flush": 0}
    real_commit = session.commit
    real_rollback = session.rollback
    real_flush = session.flush

    def counting_commit(*args: Any, **kwargs: Any) -> None:
        counters["commit"] += 1
        return real_commit(*args, **kwargs)

    def counting_rollback(*args: Any, **kwargs: Any) -> None:
        counters["rollback"] += 1
        return real_rollback(*args, **kwargs)

    def counting_flush(*args: Any, **kwargs: Any) -> None:
        counters["flush"] += 1
        return real_flush(*args, **kwargs)

    session.commit = counting_commit  # type: ignore[method-assign]
    session.rollback = counting_rollback  # type: ignore[method-assign]
    session.flush = counting_flush  # type: ignore[method-assign]
    return counters


def test_flush_only_stages_task_payload_dedup_and_policy_snapshot(
    sa_session: Session,
) -> None:
    queue_name = f"enq.flush.{uuid.uuid4().hex[:8]}"
    _seed_queue(sa_session, name=queue_name, max_attempts=7, retry_delay_seconds=11)

    queue = sa_session.execute(select(Queue).where(Queue.name == queue_name)).scalar_one()
    policy_id = queue.active_policy_version_id
    assert policy_id is not None

    repo = EnqueueRepository()
    counters = _instrument_session(sa_session)
    created_engines: list[Any] = []
    real_create_engine = create_engine

    def tracking_create_engine(*args: Any, **kwargs: Any) -> Any:
        engine = real_create_engine(*args, **kwargs)
        created_engines.append(engine)
        return engine

    import queue_service.intake.repository as repository_mod

    original_create = getattr(repository_mod, "create_engine", None)
    # Repository must not import/create engines; patch create_engine globally if used.
    import sqlalchemy as sa_mod

    sa_mod.create_engine = tracking_create_engine  # type: ignore[assignment]
    try:
        cmd = _command(queue_name=queue_name, idempotency_key="key-flush-1")
        result = repo.stage_enqueue(sa_session, cmd)
    finally:
        sa_mod.create_engine = real_create_engine  # type: ignore[assignment]
        if original_create is not None:
            repository_mod.create_engine = original_create

    assert isinstance(result, EnqueuePersistenceResult)
    assert result.replayed is False
    assert isinstance(result.task_id, UUID)
    assert counters["commit"] == 0
    assert counters["rollback"] == 0
    assert counters["flush"] >= 1
    assert created_engines == []

    # Identifiers visible inside the open caller transaction after flush.
    task = sa_session.execute(
        select(TaskActive).where(TaskActive.task_id == result.task_id)
    ).scalar_one()
    assert task.priority == 0
    assert task.state_code == _STATE_READY
    assert task.retry_policy_version_id == policy_id
    assert task.producer_id == "producer-a"
    assert task.available_at is not None

    payload = sa_session.get(TaskPayloadActive, task.id)
    assert payload is not None
    assert payload.payload == {"n": 1}
    assert payload.payload_bytes >= 1

    key_hash = hashlib.sha256(b"key-flush-1").digest()
    dedup = sa_session.execute(
        select(EnqueueDedup).where(
            EnqueueDedup.producer_id == "producer-a",
            EnqueueDedup.queue_id == queue.id,
            EnqueueDedup.key_hash == key_hash,
        )
    ).scalar_one()
    assert dedup.task_id == result.task_id
    assert dedup.request_fingerprint == cmd.fingerprint
    assert dedup.expires_at >= dedup.created_at + timedelta(days=30)
    assert dedup.expires_at <= dedup.created_at + timedelta(days=365)
    assert (dedup.expires_at - dedup.created_at) == timedelta(days=_DEDUP_TTL_DAYS)

    sa_session.commit()
    assert _counts_for_queue(sa_session, queue_name) == (1, 1, 1)


def test_caller_rollback_removes_all_staged_rows(sa_session: Session) -> None:
    queue_name = f"enq.rollback.{uuid.uuid4().hex[:8]}"
    _seed_queue(sa_session, name=queue_name)
    repo = EnqueueRepository()
    cmd = _command(queue_name=queue_name, idempotency_key="key-rb-1")

    result = repo.stage_enqueue(sa_session, cmd)
    assert result.replayed is False
    assert _counts_for_queue(sa_session, queue_name) == (1, 1, 1)

    sa_session.rollback()
    # New query after rollback must see empty set for this queue.
    sa_session.expire_all()
    assert _counts_for_queue(sa_session, queue_name) == (0, 0, 0)


def test_matching_replay_returns_original_without_mutation(
    sa_session: Session,
) -> None:
    queue_name = f"enq.replay.{uuid.uuid4().hex[:8]}"
    _seed_queue(sa_session, name=queue_name)
    repo = EnqueueRepository()
    cmd = _command(queue_name=queue_name, idempotency_key="key-replay")

    first = repo.stage_enqueue(sa_session, cmd)
    sa_session.commit()
    before = _counts_for_queue(sa_session, queue_name)

    counters = _instrument_session(sa_session)
    second = repo.stage_enqueue(sa_session, cmd)

    assert second.replayed is True
    assert second.task_id == first.task_id
    assert counters["commit"] == 0
    assert counters["rollback"] == 0
    assert _counts_for_queue(sa_session, queue_name) == before


def test_changed_fingerprint_conflicts_without_mutation_or_finalization(
    sa_session: Session,
) -> None:
    queue_name = f"enq.conflict.{uuid.uuid4().hex[:8]}"
    _seed_queue(sa_session, name=queue_name)
    repo = EnqueueRepository()
    first_cmd = _command(
        queue_name=queue_name,
        idempotency_key="key-conflict",
        payload={"v": 1},
    )
    first = repo.stage_enqueue(sa_session, first_cmd)
    sa_session.commit()
    before = _counts_for_queue(sa_session, queue_name)

    conflict_cmd = _command(
        queue_name=queue_name,
        idempotency_key="key-conflict",
        payload={"v": 2},
    )
    assert conflict_cmd.fingerprint != first_cmd.fingerprint

    counters = _instrument_session(sa_session)
    with pytest.raises(IntakeValidationError) as exc_info:
        repo.stage_enqueue(sa_session, conflict_cmd)

    assert exc_info.value.code == "idempotency_conflict"
    assert exc_info.value.retryable is False
    assert counters["commit"] == 0
    assert counters["rollback"] == 0
    assert _counts_for_queue(sa_session, queue_name) == before

    # Original rows unchanged.
    task = sa_session.execute(
        select(TaskActive).where(TaskActive.task_id == first.task_id)
    ).scalar_one()
    payload = sa_session.get(TaskPayloadActive, task.id)
    assert payload is not None
    assert payload.payload == {"v": 1}


def test_policy_snapshot_frozen_at_enqueue_serialization(
    sa_session: Session,
) -> None:
    queue_name = f"enq.policy.{uuid.uuid4().hex[:8]}"
    control = QueueControlRepository()
    control.create_named_queue(
        sa_session,
        CreateQueueMutation(
            name=queue_name,
            initial_policy=RetryPolicyDraft(
                enabled=True,
                max_attempts=2,
                backoff_strategy=BackoffStrategy.FIXED,
                retry_delay_seconds=1,
            ),
            metadata=_admin_meta(actor_id="policy-seed"),
        ),
    )
    sa_session.commit()

    cfg = control.get_queue_configuration(sa_session, name=queue_name)
    assert cfg is not None
    v1_policy_id = cfg.active_policy.policy_version_id

    created = control.create_policy_version(
        sa_session,
        queue_name=queue_name,
        mutation=CreatePolicyMutation(
            policy=RetryPolicyDraft(
                enabled=True,
                max_attempts=9,
                backoff_strategy=BackoffStrategy.FIXED,
                retry_delay_seconds=42,
            ),
            metadata=_admin_meta(actor_id="policy-v2"),
        ),
    )
    sa_session.commit()
    assert created.active_policy.version.value == 1

    control.activate_policy_version(
        sa_session,
        queue_name=queue_name,
        mutation=ActivatePolicyMutation(
            policy_version=PolicyVersion(value=2),
            expected_config_version=ConfigVersion(value=1),
            metadata=_admin_meta(actor_id="activate-v2"),
        ),
    )
    sa_session.commit()

    cfg2 = control.get_queue_configuration(sa_session, name=queue_name)
    assert cfg2 is not None
    v2_policy_id = cfg2.active_policy.policy_version_id
    assert v2_policy_id != v1_policy_id

    repo = EnqueueRepository()
    result = repo.stage_enqueue(
        sa_session,
        _command(queue_name=queue_name, idempotency_key="key-policy-snap"),
    )
    # Still inside the same open transaction: snapshot must be v2.
    task = sa_session.execute(
        select(TaskActive).where(TaskActive.task_id == result.task_id)
    ).scalar_one()
    assert task.retry_policy_version_id == v2_policy_id

    # Activate v1 again after staging but before commit — must not alter snapshot.
    control.activate_policy_version(
        sa_session,
        queue_name=queue_name,
        mutation=ActivatePolicyMutation(
            policy_version=PolicyVersion(value=1),
            expected_config_version=ConfigVersion(value=2),
            metadata=_admin_meta(actor_id="activate-v1-late"),
        ),
    )
    sa_session.flush()
    sa_session.refresh(task)
    assert task.retry_policy_version_id == v2_policy_id
    sa_session.commit()

    # Later activation on a new transaction still leaves the committed snapshot.
    control.activate_policy_version(
        sa_session,
        queue_name=queue_name,
        mutation=ActivatePolicyMutation(
            policy_version=PolicyVersion(value=2),
            expected_config_version=ConfigVersion(value=3),
            metadata=_admin_meta(actor_id="activate-again"),
        ),
    )
    sa_session.commit()

    frozen = sa_session.execute(
        select(TaskActive).where(TaskActive.task_id == result.task_id)
    ).scalar_one()
    assert frozen.retry_policy_version_id == v2_policy_id
    # Policy row v2 still has the activated-at-enqueue parameters.
    policy_row = sa_session.get(QueuePolicyVersion, v2_policy_id)
    assert policy_row is not None
    assert policy_row.max_attempts == 9


def test_two_sessions_converge_via_post_probe_pre_lock_barrier(
    session_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    setup = session_factory()
    try:
        queue_name = f"enq.race.{uuid.uuid4().hex[:8]}"
        _seed_queue(setup, name=queue_name)
    finally:
        setup.close()

    barrier = threading.Barrier(2, timeout=_JOIN_TIMEOUT_S)
    barrier_hits: list[int] = []
    reprobe_sessions: list[int] = []
    first_none_seen: dict[int, bool] = {}
    lock = threading.Lock()

    real_probe = _probe_dedup

    def wrapped_probe(session: Session, scope: Any) -> Any:
        result = real_probe(session, scope)
        session_key = id(session)
        wait_on_barrier = False
        with lock:
            if result is None and session_key not in first_none_seen:
                first_none_seen[session_key] = True
                barrier_hits.append(session_key)
                wait_on_barrier = True
            elif session_key in first_none_seen:
                # Second probe for this session (re-probe under queue lock).
                reprobe_sessions.append(session_key)
        if wait_on_barrier:
            barrier.wait()
        return result

    monkeypatch.setattr(
        "queue_service.intake.repository._probe_dedup",
        wrapped_probe,
    )

    cmd = _command(
        queue_name=queue_name,
        idempotency_key="race-key",
        payload={"race": True},
    )
    outcomes: list[EnqueuePersistenceResult | None] = [None, None]
    errors: list[BaseException | None] = [None, None]

    def worker(index: int) -> None:
        session = session_factory()
        try:
            try:
                repo = EnqueueRepository()
                outcomes[index] = repo.stage_enqueue(session, cmd)
                session.commit()
            except BaseException as exc:  # noqa: BLE001 — propagate via join
                session.rollback()
                errors[index] = exc
        finally:
            session.close()

    threads = [
        threading.Thread(target=worker, args=(0,), daemon=True),
        threading.Thread(target=worker, args=(1,), daemon=True),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=_JOIN_TIMEOUT_S)
        assert not thread.is_alive(), "race worker did not finish within timeout"

    for index, err in enumerate(errors):
        if err is not None:
            raise AssertionError(f"worker {index} failed") from err

    assert len(barrier_hits) == 2
    assert len(reprobe_sessions) >= 1
    assert outcomes[0] is not None and outcomes[1] is not None

    creators = [r for r in outcomes if r is not None and not r.replayed]
    replays = [r for r in outcomes if r is not None and r.replayed]
    assert len(creators) == 1
    assert len(replays) == 1
    assert creators[0].task_id == replays[0].task_id

    verify = session_factory()
    try:
        assert _counts_for_queue(verify, queue_name) == (1, 1, 1)
        tasks = list(
            verify.scalars(
                select(TaskActive).where(
                    TaskActive.task_id == creators[0].task_id
                )
            )
        )
        assert len(tasks) == 1
        assert tasks[0].task_id == creators[0].task_id
    finally:
        verify.close()


def test_future_available_at_staged_as_delayed_with_unchanged_timestamp(
    sa_session: Session,
) -> None:
    queue_name = f"enq.delayed.{uuid.uuid4().hex[:8]}"
    _seed_queue(sa_session, name=queue_name)
    future_at = datetime.now(tz=UTC) + timedelta(minutes=15)
    repo = EnqueueRepository()
    cmd = _command(
        queue_name=queue_name,
        idempotency_key="key-delayed",
        available_at=future_at,
    )
    result = repo.stage_enqueue(sa_session, cmd)
    sa_session.commit()

    task = sa_session.execute(
        select(TaskActive).where(TaskActive.task_id == result.task_id)
    ).scalar_one()
    assert int(task.state_code) == _STATE_DELAYED
    assert abs((task.available_at - future_at).total_seconds()) < 1.0

def test_past_available_at_staged_ready_with_supplied_timestamp(
    sa_session: Session,
) -> None:
    queue_name = f"enq.past.{uuid.uuid4().hex[:8]}"
    _seed_queue(sa_session, name=queue_name)
    past_at = datetime.now(tz=UTC) - timedelta(minutes=5)
    repo = EnqueueRepository()
    cmd = _command(
        queue_name=queue_name,
        idempotency_key="key-past",
        available_at=past_at,
    )
    result = repo.stage_enqueue(sa_session, cmd)
    sa_session.commit()

    task = sa_session.execute(
        select(TaskActive).where(TaskActive.task_id == result.task_id)
    ).scalar_one()
    assert int(task.state_code) == _STATE_READY
    assert abs((task.available_at - past_at).total_seconds()) < 1.0


def test_over_horizon_available_at_rejected_without_writes(
    sa_session: Session,
) -> None:
    queue_name = f"enq.horizon.{uuid.uuid4().hex[:8]}"
    _seed_queue(sa_session, name=queue_name)
    over_horizon = datetime.now(tz=UTC) + timedelta(days=2)
    repo = EnqueueRepository()
    cmd = _command(
        queue_name=queue_name,
        idempotency_key="key-horizon",
        available_at=over_horizon,
    )
    with pytest.raises(IntakeValidationError) as exc_info:
        repo.stage_enqueue(sa_session, cmd)
    assert exc_info.value.code == "validation_failed"
    assert exc_info.value.retryable is False
    assert exc_info.value.details.get("field") == "available_at"
    sa_session.rollback()
    sa_session.expire_all()
    assert _counts_for_queue(sa_session, queue_name) == (0, 0, 0)


_PRIORITY_MIN = -32768
_PRIORITY_MAX = 32767


@pytest.mark.parametrize("priority", [_PRIORITY_MIN, _PRIORITY_MAX, 100, -50])
def test_stage_enqueue_persists_exact_non_zero_priority(
    sa_session: Session,
    priority: int,
) -> None:
    queue_name = f"enq.priority.{uuid.uuid4().hex[:8]}"
    _seed_queue(sa_session, name=queue_name)
    repo = EnqueueRepository()
    cmd = _command(
        queue_name=queue_name,
        idempotency_key=f"key-priority-{priority}",
        priority=priority,
    )
    result = repo.stage_enqueue(sa_session, cmd)
    sa_session.commit()

    task = sa_session.execute(
        select(TaskActive).where(TaskActive.task_id == result.task_id)
    ).scalar_one()
    assert int(task.priority) == priority
