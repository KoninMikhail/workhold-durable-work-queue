"""Real-PostgreSQL proof of service-owned enqueue unit of work (Phase 03.4-04)."""

from __future__ import annotations

import json
import os
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, event, func, select
from sqlalchemy.orm import Session, sessionmaker

from queue_service.domain.queue_control import (
    AdminRequestMetadata,
    BackoffStrategy,
    ConfigVersion,
    CreateQueueMutation,
    QueueState,
    RetryPolicyDraft,
    SetQueueStateMutation,
)
from queue_service.infrastructure.postgres.queue_control_repository import (
    QueueControlRepository,
)
from queue_service.intake.contracts import IntakeValidationError
from queue_service.intake.depth import DepthCeilings, reserve_active_depth
from queue_service.intake.repository import EnqueueRepository
from queue_service.intake.service import EnqueueFaultHooks, EnqueueService
from queue_service.scheduling import SchedulingPolicy
from queue_service.storage.models import (
    EnqueueDedup,
    Queue,
    QueueCounter,
    TaskActive,
    TaskPayloadActive,
)


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
def session_factory(sa_engine) -> sessionmaker[Session]:
    return sessionmaker(bind=sa_engine, expire_on_commit=False)


@pytest.fixture
def sa_session(session_factory: sessionmaker[Session]) -> Iterator[Session]:
    session = session_factory()
    try:
        yield session
    finally:
        session.close()


def _admin_meta(*, actor_id: str = "admin-uow") -> AdminRequestMetadata:
    return AdminRequestMetadata(
        actor_id=actor_id,
        request_id=str(uuid.uuid4()),
        idempotency_key=f"admin-idem-{uuid.uuid4().hex}",
    )


def _seed_queue(session: Session, *, name: str) -> Queue:
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
    return session.execute(select(Queue).where(Queue.name == name)).scalar_one()


def _set_state(
    session: Session,
    *,
    queue_name: str,
    state: QueueState,
    expected_config_version: int,
) -> None:
    QueueControlRepository().set_queue_state(
        session,
        queue_name=queue_name,
        mutation=SetQueueStateMutation(
            expected_config_version=ConfigVersion(value=expected_config_version),
            state=state,
            metadata=_admin_meta(actor_id=f"state-{state.value}"),
        ),
    )
    session.commit()


def _body(payload: Any | None = None) -> tuple[dict[str, Any], bytes]:
    body: dict[str, Any] = {"payload": payload if payload is not None else {"n": 1}}
    raw = json.dumps(body, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return body, raw


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


def _instrument_session_factory(
    session_factory: sessionmaker[Session],
) -> tuple[sessionmaker[Session], dict[str, Any]]:
    """Wrap factory so tests observe sessions and commit/rollback ownership."""
    stats: dict[str, Any] = {
        "sessions_created": 0,
        "sessions": [],
        "commit_by_session": [],
        "rollback_by_session": [],
    }

    def factory() -> Session:
        session = session_factory()
        stats["sessions_created"] += 1
        stats["sessions"].append(session)
        real_commit = session.commit
        real_rollback = session.rollback

        def counting_commit(*args: Any, **kwargs: Any) -> None:
            stats["commit_by_session"].append(id(session))
            return real_commit(*args, **kwargs)

        def counting_rollback(*args: Any, **kwargs: Any) -> None:
            stats["rollback_by_session"].append(id(session))
            return real_rollback(*args, **kwargs)

        session.commit = counting_commit  # type: ignore[method-assign]
        session.rollback = counting_rollback  # type: ignore[method-assign]
        return session

    return factory, stats  # type: ignore[return-value]


def test_matching_replay_while_draining_and_depth_full(
    session_factory: sessionmaker[Session],
    sa_session: Session,
) -> None:
    queue_name = f"uow.replay.drain.{uuid.uuid4().hex[:8]}"
    queue = _seed_queue(sa_session, name=queue_name)
    body, body_bytes = _body({"k": "same"})
    key = f"idem-{uuid.uuid4().hex}"
    baseline = _instance_depth(sa_session)
    # First enqueue fills both ceilings so replay must skip depth reservation.
    ceilings = DepthCeilings(
        queue_active_depth=1,
        instance_active_depth=baseline + 1,
    )
    service = EnqueueService(
        session_factory=session_factory,
        depth_ceilings=ceilings,
    )
    first = service.enqueue(
        producer_id="producer-a",
        queue_name=queue_name,
        idempotency_key=key,
        body=body,
        body_bytes=body_bytes
    )
    assert first.replayed is False
    original_task_id = first.task_id
    assert _counter_depth(sa_session, int(queue.id)) == 1
    assert _instance_depth(sa_session) == baseline + 1

    _set_state(
        sa_session,
        queue_name=queue_name,
        state=QueueState.DRAINING,
        expected_config_version=1,
    )

    replay = service.enqueue(
        producer_id="producer-a",
        queue_name=queue_name,
        idempotency_key=key,
        body=body,
        body_bytes=body_bytes
    )
    assert replay.replayed is True
    assert replay.task_id == original_task_id
    assert _counts_for_queue(sa_session, queue_name) == (1, 1, 1)
    assert _counter_depth(sa_session, int(queue.id)) == 1
    assert _instance_depth(sa_session) == baseline + 1


def test_changed_fingerprint_conflicts_before_state_or_depth(
    session_factory: sessionmaker[Session],
    sa_session: Session,
) -> None:
    queue_name = f"uow.conflict.{uuid.uuid4().hex[:8]}"
    queue = _seed_queue(sa_session, name=queue_name)
    body, body_bytes = _body({"v": 1})
    key = f"idem-{uuid.uuid4().hex}"
    baseline = _instance_depth(sa_session)
    ceilings = DepthCeilings(
        queue_active_depth=1,
        instance_active_depth=baseline + 1,
    )
    service = EnqueueService(
        session_factory=session_factory,
        depth_ceilings=ceilings,
    )

    first = service.enqueue(
        producer_id="producer-a",
        queue_name=queue_name,
        idempotency_key=key,
        body=body,
        body_bytes=body_bytes
    )
    assert first.replayed is False

    _set_state(
        sa_session,
        queue_name=queue_name,
        state=QueueState.DRAINING,
        expected_config_version=1,
    )

    conflict_body, conflict_bytes = _body({"v": 2})
    gate_calls: list[Any] = []
    depth_calls: list[Any] = []

    with (
        patch(
            "queue_service.intake.service.evaluate_queue_state_gate",
            side_effect=lambda *a, **k: gate_calls.append((a, k)) or (_ for _ in ()).throw(
                AssertionError("state gate must not run on fingerprint conflict")
            ),
        ),
        patch(
            "queue_service.intake.service.reserve_active_depth",
            side_effect=lambda *a, **k: depth_calls.append((a, k)) or (_ for _ in ()).throw(
                AssertionError("depth must not run on fingerprint conflict")
            ),
        ),
    ):
        with pytest.raises(IntakeValidationError) as exc_info:
            service.enqueue(
                producer_id="producer-a",
                queue_name=queue_name,
                idempotency_key=key,
                body=conflict_body,
                body_bytes=conflict_bytes
            )
    assert exc_info.value.code == "idempotency_conflict"
    assert gate_calls == []
    assert depth_calls == []
    assert _counts_for_queue(sa_session, queue_name) == (1, 1, 1)
    assert _counter_depth(sa_session, int(queue.id)) == 1
    assert _instance_depth(sa_session) == baseline + 1


def test_one_session_one_commit_zero_primitive_finalization(
    session_factory: sessionmaker[Session],
    sa_session: Session,
) -> None:
    queue_name = f"uow.session.{uuid.uuid4().hex[:8]}"
    _seed_queue(sa_session, name=queue_name)
    wrapped_factory, stats = _instrument_session_factory(session_factory)
    body, body_bytes = _body({"one": "txn"})

    repo_commits = {"n": 0}
    depth_commits = {"n": 0}
    repo_rollbacks = {"n": 0}
    depth_rollbacks = {"n": 0}
    repo_sessions = {"n": 0}
    depth_sessions = {"n": 0}

    real_stage = EnqueueRepository.stage_new_under_lock
    real_resolve = EnqueueRepository.resolve_committed_dedup
    real_lock = EnqueueRepository.lock_named_queue
    real_reserve = reserve_active_depth

    def tracking_stage(self: EnqueueRepository, session: Session, *args: Any, **kwargs: Any):
        original_commit = session.commit
        original_rollback = session.rollback

        def forbidden_commit(*a: Any, **k: Any) -> None:
            repo_commits["n"] += 1
            return original_commit(*a, **k)

        def forbidden_rollback(*a: Any, **k: Any) -> None:
            repo_rollbacks["n"] += 1
            return original_rollback(*a, **k)

        session.commit = forbidden_commit  # type: ignore[method-assign]
        session.rollback = forbidden_rollback  # type: ignore[method-assign]
        try:
            return real_stage(self, session, *args, **kwargs)
        finally:
            session.commit = original_commit  # type: ignore[method-assign]
            session.rollback = original_rollback  # type: ignore[method-assign]

    def tracking_resolve(self: EnqueueRepository, session: Session, *args: Any, **kwargs: Any):
        return real_resolve(self, session, *args, **kwargs)

    def tracking_lock(self: EnqueueRepository, session: Session, *args: Any, **kwargs: Any):
        return real_lock(self, session, *args, **kwargs)

    def tracking_reserve(session: Session, *args: Any, **kwargs: Any):
        original_commit = session.commit
        original_rollback = session.rollback

        def forbidden_commit(*a: Any, **k: Any) -> None:
            depth_commits["n"] += 1
            return original_commit(*a, **k)

        def forbidden_rollback(*a: Any, **k: Any) -> None:
            depth_rollbacks["n"] += 1
            return original_rollback(*a, **k)

        session.commit = forbidden_commit  # type: ignore[method-assign]
        session.rollback = forbidden_rollback  # type: ignore[method-assign]
        try:
            return real_reserve(session, *args, **kwargs)
        finally:
            session.commit = original_commit  # type: ignore[method-assign]
            session.rollback = original_rollback  # type: ignore[method-assign]

    with (
        patch.object(EnqueueRepository, "stage_new_under_lock", tracking_stage),
        patch.object(EnqueueRepository, "resolve_committed_dedup", tracking_resolve),
        patch.object(EnqueueRepository, "lock_named_queue", tracking_lock),
        patch("queue_service.intake.service.reserve_active_depth", tracking_reserve),
        patch("queue_service.intake.service.Session", side_effect=AssertionError("no Session()")),
        patch(
            "queue_service.intake.service.sessionmaker",
            side_effect=AssertionError("no sessionmaker()"),
        ),
        patch(
            "queue_service.intake.service.create_engine",
            side_effect=AssertionError("no create_engine()"),
        ),
    ):
        service = EnqueueService(session_factory=wrapped_factory)
        result = service.enqueue(
            producer_id="producer-a",
            queue_name=queue_name,
            idempotency_key=f"idem-{uuid.uuid4().hex}",
            body=body,
            body_bytes=body_bytes
        )

    assert result.replayed is False
    assert stats["sessions_created"] == 1
    assert len(stats["commit_by_session"]) == 1
    assert stats["rollback_by_session"] == []
    assert repo_commits["n"] == 0
    assert depth_commits["n"] == 0
    assert repo_rollbacks["n"] == 0
    assert depth_rollbacks["n"] == 0
    assert repo_sessions["n"] == 0
    assert depth_sessions["n"] == 0
    assert _counts_for_queue(sa_session, queue_name) == (1, 1, 1)


def test_failure_injection_rolls_back_all_effects(
    session_factory: sessionmaker[Session],
    sa_session: Session,
) -> None:
    queue_name = f"uow.rollback.{uuid.uuid4().hex[:8]}"
    queue = _seed_queue(sa_session, name=queue_name)
    body, body_bytes = _body({"boom": True})
    key_base = uuid.uuid4().hex

    stages = (
        "after_depth",
        "after_policy_snapshot",
        "after_task_flush",
        "after_payload_flush",
        "after_dedup_flush",
    )
    for index, stage in enumerate(stages):
        hooks = EnqueueFaultHooks(
            **{
                stage: lambda s=stage: (_ for _ in ()).throw(
                    RuntimeError(f"injected failure at {s}")
                )
            }
        )
        service = EnqueueService(session_factory=session_factory, fault_hooks=hooks)
        with pytest.raises(RuntimeError, match="injected failure"):
            service.enqueue(
                producer_id="producer-a",
                queue_name=queue_name,
                idempotency_key=f"idem-{key_base}-{index}",
                body=body,
                body_bytes=body_bytes
            )
        sa_session.expire_all()
        assert _counts_for_queue(sa_session, queue_name) == (0, 0, 0)
        assert _counter_depth(sa_session, int(queue.id)) == 0


def test_active_and_paused_accept_draining_and_unknown_reject(
    session_factory: sessionmaker[Session],
    sa_session: Session,
) -> None:
    active_name = f"uow.active.{uuid.uuid4().hex[:8]}"
    paused_name = f"uow.paused.{uuid.uuid4().hex[:8]}"
    draining_name = f"uow.drain.{uuid.uuid4().hex[:8]}"
    active_q = _seed_queue(sa_session, name=active_name)
    paused_q = _seed_queue(sa_session, name=paused_name)
    draining_q = _seed_queue(sa_session, name=draining_name)

    _set_state(
        sa_session,
        queue_name=paused_name,
        state=QueueState.PAUSED,
        expected_config_version=1,
    )
    _set_state(
        sa_session,
        queue_name=draining_name,
        state=QueueState.DRAINING,
        expected_config_version=1,
    )

    service = EnqueueService(session_factory=session_factory)
    body, body_bytes = _body({"ok": True})

    active_result = service.enqueue(
        producer_id="producer-a",
        queue_name=active_name,
        idempotency_key=f"idem-active-{uuid.uuid4().hex}",
        body=body,
        body_bytes=body_bytes
    )
    assert active_result.replayed is False
    assert _counts_for_queue(sa_session, active_name) == (1, 1, 1)
    assert _counter_depth(sa_session, int(active_q.id)) == 1

    paused_result = service.enqueue(
        producer_id="producer-a",
        queue_name=paused_name,
        idempotency_key=f"idem-paused-{uuid.uuid4().hex}",
        body=body,
        body_bytes=body_bytes
    )
    assert paused_result.replayed is False
    assert _counts_for_queue(sa_session, paused_name) == (1, 1, 1)
    assert _counter_depth(sa_session, int(paused_q.id)) == 1

    with pytest.raises(IntakeValidationError) as drain_exc:
        service.enqueue(
            producer_id="producer-a",
            queue_name=draining_name,
            idempotency_key=f"idem-drain-{uuid.uuid4().hex}",
            body=body,
            body_bytes=body_bytes
        )
    assert drain_exc.value.code == "queue_draining"
    assert drain_exc.value.retryable is True
    assert _counts_for_queue(sa_session, draining_name) == (0, 0, 0)
    assert _counter_depth(sa_session, int(draining_q.id)) == 0

    with pytest.raises(IntakeValidationError) as missing_exc:
        service.enqueue(
            producer_id="producer-a",
            queue_name=f"missing.{uuid.uuid4().hex[:8]}",
            idempotency_key=f"idem-missing-{uuid.uuid4().hex}",
            body=body,
            body_bytes=body_bytes
        )
    assert missing_exc.value.code == "queue_not_found"


def test_delayed_enqueue_reserves_delayed_counter_only(
    session_factory: sessionmaker[Session],
    sa_session: Session,
) -> None:
    queue_name = f"uow.delayed.{uuid.uuid4().hex[:8]}"
    queue = _seed_queue(sa_session, name=queue_name)
    future_at = datetime.now(tz=UTC) + timedelta(minutes=20)
    body = {"payload": {"delayed": True}, "available_at": future_at}
    body_bytes = json.dumps(body, default=str, separators=(",", ":")).encode("utf-8")
    service = EnqueueService(session_factory=session_factory)
    result = service.enqueue(
        producer_id="producer-a",
        queue_name=queue_name,
        idempotency_key=f"idem-{uuid.uuid4().hex}",
        body=body,
        body_bytes=body_bytes,
    )
    assert result.replayed is False

    counter = sa_session.execute(
        select(QueueCounter).where(QueueCounter.queue_id == queue.id)
    ).scalar_one()
    assert int(counter.delayed_count) == 1
    assert int(counter.ready_count) == 0

    task = sa_session.execute(
        select(TaskActive).where(TaskActive.task_id == result.task_id)
    ).scalar_one()
    assert int(task.state_code) == 1
    assert abs((task.available_at - future_at).total_seconds()) < 1.0


def test_over_horizon_enqueue_rejected_without_counter_or_task_writes(
    session_factory: sessionmaker[Session],
    sa_session: Session,
) -> None:
    queue_name = f"uow.horizon.{uuid.uuid4().hex[:8]}"
    queue = _seed_queue(sa_session, name=queue_name)
    over_horizon = datetime.now(tz=UTC) + timedelta(days=2)
    body = {"payload": {"too_far": True}, "available_at": over_horizon}
    body_bytes = json.dumps(body, default=str, separators=(",", ":")).encode("utf-8")
    service = EnqueueService(session_factory=session_factory)
    with pytest.raises(IntakeValidationError) as exc_info:
        service.enqueue(
            producer_id="producer-a",
            queue_name=queue_name,
            idempotency_key=f"idem-{uuid.uuid4().hex}",
            body=body,
            body_bytes=body_bytes,
        )
    assert exc_info.value.code == "validation_failed"
    assert exc_info.value.retryable is False
    assert _counts_for_queue(sa_session, queue_name) == (0, 0, 0)
    assert _counter_depth(sa_session, int(queue.id)) == 0


def test_matching_replay_bypasses_tightened_service_horizon(
    session_factory: sessionmaker[Session],
    sa_session: Session,
) -> None:
    queue_name = f"uow.replay.horizon.{uuid.uuid4().hex[:8]}"
    _seed_queue(sa_session, name=queue_name)
    future_at = datetime.now(tz=UTC) + timedelta(hours=3)
    body = {"payload": {"k": "same"}, "available_at": future_at}
    body_bytes = json.dumps(body, default=str, separators=(",", ":")).encode("utf-8")
    key = f"idem-{uuid.uuid4().hex}"
    wide = EnqueueService(
        session_factory=session_factory,
        scheduling_policy=SchedulingPolicy(horizon_seconds=86_400),
    )
    first = wide.enqueue(
        producer_id="producer-a",
        queue_name=queue_name,
        idempotency_key=key,
        body=body,
        body_bytes=body_bytes,
    )
    assert first.replayed is False

    tight = EnqueueService(
        session_factory=session_factory,
        scheduling_policy=SchedulingPolicy(horizon_seconds=3_600),
    )
    replay = tight.enqueue(
        producer_id="producer-a",
        queue_name=queue_name,
        idempotency_key=key,
        body=body,
        body_bytes=body_bytes,
    )
    assert replay.replayed is True
    assert replay.task_id == first.task_id
    assert _counts_for_queue(sa_session, queue_name) == (1, 1, 1)
