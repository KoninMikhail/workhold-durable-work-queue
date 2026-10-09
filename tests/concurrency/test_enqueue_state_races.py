"""Deterministic multi-connection enqueue/state/policy race coverage.

QUAL-02 (Phase 3.4 enqueue/state slice): new enqueue versus drain, committed
replay versus queue-state flips, fingerprint conflict versus state flips, and
enqueue versus active-policy activation. PostgreSQL row locks are the
serialization point; process-local locks are never used for correctness.

Thread Events/Barriers only coordinate test start and hold points at documented
``FOR UPDATE`` boundaries. Sleeps are not coordinators.

This module uses its own Alembic schema so high-cardinality race rows do not
pollute the session-scoped ``migrated_schema`` shared by other integration tests.
"""

from __future__ import annotations

import json
import os
import re
import threading
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import patch

import psycopg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, event, func, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from workhold.domain.queue_control import (
    ActivatePolicyMutation,
    AdminRequestMetadata,
    BackoffStrategy,
    ConfigVersion,
    CreatePolicyMutation,
    CreateQueueMutation,
    PolicyVersion,
    QueueState,
    RetryPolicyDraft,
    SetQueueStateMutation,
)
from workhold.infrastructure.postgres.queue_control_repository import (
    QueueControlRepository,
)
from workhold.intake.contracts import IntakeValidationError
from workhold.intake.repository import EnqueueRepository
from workhold.intake.service import EnqueueService
from workhold.storage.models import (
    EnqueueDedup,
    Queue,
    QueueCounter,
    QueuePolicyVersion,
    TaskActive,
    TaskPayloadActive,
)

_JOIN_TIMEOUT_S = 30.0
_ROOT = Path(__file__).resolve().parents[2]
_ALEMBIC_INI = _ROOT / "alembic.ini"
_SCHEMA_NAME_RE = re.compile(r"^[a-z_][a-z0-9_]*$")
_STATE_CODE = {
    QueueState.ACTIVE: 1,
    QueueState.PAUSED: 2,
    QueueState.DRAINING: 3,
}


def _require_test_database_url() -> str:
    url = os.environ.get("TEST_DATABASE_URL", "").strip()
    if not url:
        pytest.fail(
            "TEST_DATABASE_URL is required for tests/concurrency "
            "(PostgreSQL 16). Refusing to skip or xfail."
        )
    return url


def _to_psycopg_conninfo(url: str) -> str:
    if url.startswith("postgresql+psycopg://"):
        return "postgresql://" + url.removeprefix("postgresql+psycopg://")
    return url


def _run_alembic(direction: str, target: str, *, schema: str, database_url: str) -> None:
    if not _SCHEMA_NAME_RE.fullmatch(schema):
        raise ValueError(f"refusing unsafe schema name: {schema!r}")
    previous_url = os.environ.get("DATABASE_URL")
    previous_schema = os.environ.get("ALEMBIC_VERSION_TABLE_SCHEMA")
    os.environ["DATABASE_URL"] = database_url
    os.environ["ALEMBIC_VERSION_TABLE_SCHEMA"] = schema
    try:
        cfg = Config(str(_ALEMBIC_INI))
        if direction == "upgrade":
            command.upgrade(cfg, target)
        elif direction == "downgrade":
            command.downgrade(cfg, target)
        else:
            raise ValueError(f"unknown alembic direction: {direction}")
    finally:
        if previous_url is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = previous_url
        if previous_schema is None:
            os.environ.pop("ALEMBIC_VERSION_TABLE_SCHEMA", None)
        else:
            os.environ["ALEMBIC_VERSION_TABLE_SCHEMA"] = previous_schema


@pytest.fixture(scope="module")
def race_migrated_schema() -> Iterator[str]:
    """Module-private migrated schema for enqueue/state race proofs."""
    database_url = _require_test_database_url()
    schema = f"enqrace_{uuid.uuid4().hex}"
    if not _SCHEMA_NAME_RE.fullmatch(schema):
        raise ValueError(f"refusing unsafe schema name: {schema!r}")
    admin = psycopg.connect(_to_psycopg_conninfo(database_url))
    admin.autocommit = True
    try:
        admin.execute(f'CREATE SCHEMA "{schema}"')
    finally:
        admin.close()

    _run_alembic("upgrade", "head", schema=schema, database_url=database_url)
    try:
        yield schema
    finally:
        try:
            _run_alembic(
                "downgrade", "base", schema=schema, database_url=database_url
            )
        finally:
            drop = psycopg.connect(_to_psycopg_conninfo(database_url))
            drop.autocommit = True
            try:
                drop.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            finally:
                drop.close()


@pytest.fixture
def sa_engine(race_migrated_schema: str) -> Iterator[Engine]:
    database_url = _require_test_database_url()
    schema = race_migrated_schema
    engine = create_engine(database_url, pool_pre_ping=True, pool_size=6, max_overflow=0)

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
def session_factory(sa_engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=sa_engine, expire_on_commit=False)


def _meta(*, actor_id: str) -> AdminRequestMetadata:
    return AdminRequestMetadata(
        actor_id=actor_id,
        request_id=str(uuid.uuid4()),
        idempotency_key=f"idem-{uuid.uuid4().hex}",
    )


def _unique(prefix: str) -> str:
    return f"{prefix}.{uuid.uuid4().hex[:12]}"


def _body(payload: Any | None = None) -> tuple[dict[str, Any], bytes]:
    body: dict[str, Any] = {"payload": payload if payload is not None else {"n": 1}}
    raw = json.dumps(body, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return body, raw


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
            metadata=_meta(actor_id="seed"),
        ),
    )
    session.commit()
    return session.execute(select(Queue).where(Queue.name == name)).scalar_one()


def _append_inactive_policy(
    session: Session,
    *,
    name: str,
    max_attempts: int = 11,
) -> PolicyVersion:
    control = QueueControlRepository()
    control.create_policy_version(
        session,
        queue_name=name,
        mutation=CreatePolicyMutation(
            policy=RetryPolicyDraft(
                enabled=True,
                max_attempts=max_attempts,
                backoff_strategy=BackoffStrategy.FIXED,
                retry_delay_seconds=9,
            ),
            metadata=_meta(actor_id="policy-seed"),
        ),
    )
    session.commit()
    return PolicyVersion(value=2)


def _queue_row(session: Session, name: str) -> Queue:
    return session.execute(select(Queue).where(Queue.name == name)).scalar_one()


def _counts_for_queue(session: Session, queue_pk: int) -> tuple[int, int, int]:
    tasks = int(
        session.scalar(
            select(func.count())
            .select_from(TaskActive)
            .where(TaskActive.queue_id == queue_pk)
        )
        or 0
    )
    payloads = int(
        session.scalar(
            select(func.count())
            .select_from(TaskPayloadActive)
            .join(TaskActive, TaskActive.id == TaskPayloadActive.task_id)
            .where(TaskActive.queue_id == queue_pk)
        )
        or 0
    )
    dedups = int(
        session.scalar(
            select(func.count())
            .select_from(EnqueueDedup)
            .where(EnqueueDedup.queue_id == queue_pk)
        )
        or 0
    )
    return tasks, payloads, dedups


def _counter_depth(session: Session, queue_pk: int) -> int:
    row = session.execute(
        select(QueueCounter).where(QueueCounter.queue_id == queue_pk)
    ).scalar_one_or_none()
    if row is None:
        return 0
    return int(row.delayed_count + row.ready_count + row.leased_count)


def _policy_pk_for_version(session: Session, queue_pk: int, version: int) -> int:
    row = session.execute(
        select(QueuePolicyVersion).where(
            QueuePolicyVersion.queue_id == queue_pk,
            QueuePolicyVersion.version == version,
        )
    ).scalar_one()
    return int(row.id)


@dataclass
class _ThreadResult:
    ok: bool = False
    value: Any = None
    error: BaseException | None = None


def _join(thread: threading.Thread, *, label: str) -> None:
    thread.join(timeout=_JOIN_TIMEOUT_S)
    assert not thread.is_alive(), f"{label} did not finish within {_JOIN_TIMEOUT_S}s"


def test_enqueue_wins_serialization_then_drain_both_commit(
    session_factory: sessionmaker[Session],
) -> None:
    """Enqueue FOR UPDATE first: task + drain both commit; coherent final rows."""
    name = _unique("race.enq-first.drain")
    setup = session_factory()
    try:
        queue = _seed_queue(setup, name=name)
        queue_pk = int(queue.id)
        policy_v1 = int(queue.active_policy_version_id)
    finally:
        setup.close()

    enqueue_locked = threading.Event()
    drain_started = threading.Event()
    results = {"enqueue": _ThreadResult(), "drain": _ThreadResult()}
    body, body_bytes = _body({"race": "enq-first"})
    key = f"key-{uuid.uuid4().hex}"
    original_lock = EnqueueRepository.lock_named_queue

    def lock_then_signal(
        self: EnqueueRepository,
        session: Session,
        queue_name: str,
    ) -> Queue:
        locked = original_lock(self, session, queue_name)
        enqueue_locked.set()
        assert drain_started.wait(timeout=_JOIN_TIMEOUT_S), "drain never started"
        return locked

    def run_enqueue() -> None:
        service = EnqueueService(session_factory=session_factory)
        try:
            with patch.object(EnqueueRepository, "lock_named_queue", lock_then_signal):
                result = service.enqueue(
                    producer_id="producer-a",
                    queue_name=name,
                    idempotency_key=key,
                    body=body,
                    body_bytes=body_bytes
                )
            results["enqueue"] = _ThreadResult(ok=True, value=result)
        except BaseException as exc:  # noqa: BLE001
            results["enqueue"] = _ThreadResult(ok=False, error=exc)

    def run_drain() -> None:
        assert enqueue_locked.wait(timeout=_JOIN_TIMEOUT_S), "enqueue never locked"
        session = session_factory()
        drain_started.set()
        try:
            cfg = QueueControlRepository().set_queue_state(
                session,
                queue_name=name,
                mutation=SetQueueStateMutation(
                    expected_config_version=ConfigVersion(value=1),
                    state=QueueState.DRAINING,
                    metadata=_meta(actor_id="drain-after-enq"),
                ),
            )
            session.commit()
            results["drain"] = _ThreadResult(ok=True, value=cfg)
        except BaseException as exc:  # noqa: BLE001
            session.rollback()
            results["drain"] = _ThreadResult(ok=False, error=exc)
        finally:
            session.close()

    t_enq = threading.Thread(target=run_enqueue, daemon=True)
    t_drain = threading.Thread(target=run_drain, daemon=True)
    t_enq.start()
    t_drain.start()
    _join(t_enq, label="enqueue")
    _join(t_drain, label="drain")

    assert results["enqueue"].ok, results["enqueue"].error
    assert results["drain"].ok, results["drain"].error
    enq = results["enqueue"].value
    assert enq.replayed is False
    assert enq.retry_policy_version_id == policy_v1

    verify = session_factory()
    try:
        row = _queue_row(verify, name)
        assert int(row.state_code) == _STATE_CODE[QueueState.DRAINING]
        assert int(row.config_version) == 2
        assert _counts_for_queue(verify, queue_pk) == (1, 1, 1)
        assert _counter_depth(verify, queue_pk) == 1
        task = verify.execute(
            select(TaskActive).where(TaskActive.task_id == enq.task_id)
        ).scalar_one()
        assert int(task.retry_policy_version_id) == policy_v1
    finally:
        verify.close()


def test_drain_wins_serialization_rejects_new_enqueue_without_task(
    session_factory: sessionmaker[Session],
) -> None:
    """Drain FOR UPDATE first: enqueue observes draining and creates zero rows."""
    name = _unique("race.drain-first.enq")
    setup = session_factory()
    try:
        queue = _seed_queue(setup, name=name)
        queue_pk = int(queue.id)
    finally:
        setup.close()

    drain_locked = threading.Event()
    enqueue_started = threading.Event()
    results = {"enqueue": _ThreadResult(), "drain": _ThreadResult()}
    body, body_bytes = _body({"race": "drain-first"})
    key = f"key-{uuid.uuid4().hex}"
    original_lock = QueueControlRepository._lock_queue_by_name

    def lock_then_signal(
        self: QueueControlRepository,
        session: Session,
        queue_name: str,
    ) -> Queue:
        locked = original_lock(self, session, queue_name)
        drain_locked.set()
        assert enqueue_started.wait(timeout=_JOIN_TIMEOUT_S), "enqueue never started"
        return locked

    def run_drain() -> None:
        session = session_factory()
        try:
            with patch.object(
                QueueControlRepository,
                "_lock_queue_by_name",
                lock_then_signal,
            ):
                cfg = QueueControlRepository().set_queue_state(
                    session,
                    queue_name=name,
                    mutation=SetQueueStateMutation(
                        expected_config_version=ConfigVersion(value=1),
                        state=QueueState.DRAINING,
                        metadata=_meta(actor_id="drain-first"),
                    ),
                )
            session.commit()
            results["drain"] = _ThreadResult(ok=True, value=cfg)
        except BaseException as exc:  # noqa: BLE001
            session.rollback()
            results["drain"] = _ThreadResult(ok=False, error=exc)
        finally:
            session.close()

    def run_enqueue() -> None:
        assert drain_locked.wait(timeout=_JOIN_TIMEOUT_S), "drain never locked"
        enqueue_started.set()
        service = EnqueueService(session_factory=session_factory)
        try:
            result = service.enqueue(
                producer_id="producer-a",
                queue_name=name,
                idempotency_key=key,
                body=body,
                body_bytes=body_bytes
            )
            results["enqueue"] = _ThreadResult(ok=True, value=result)
        except IntakeValidationError as exc:
            results["enqueue"] = _ThreadResult(ok=False, error=exc)
        except BaseException as exc:  # noqa: BLE001
            results["enqueue"] = _ThreadResult(ok=False, error=exc)

    t_drain = threading.Thread(target=run_drain, daemon=True)
    t_enq = threading.Thread(target=run_enqueue, daemon=True)
    t_drain.start()
    t_enq.start()
    _join(t_drain, label="drain")
    _join(t_enq, label="enqueue")

    assert results["drain"].ok, results["drain"].error
    assert results["enqueue"].ok is False
    err = results["enqueue"].error
    assert isinstance(err, IntakeValidationError)
    assert err.code == "queue_draining"
    assert err.retryable is True

    verify = session_factory()
    try:
        row = _queue_row(verify, name)
        assert int(row.state_code) == _STATE_CODE[QueueState.DRAINING]
        assert int(row.config_version) == 2
        assert _counts_for_queue(verify, queue_pk) == (0, 0, 0)
        assert _counter_depth(verify, queue_pk) == 0
    finally:
        verify.close()


@pytest.mark.parametrize(
    ("from_state", "to_state", "seed_state", "expected_versions"),
    [
        (QueueState.ACTIVE, QueueState.DRAINING, QueueState.ACTIVE, (1, 2)),
        (QueueState.DRAINING, QueueState.ACTIVE, QueueState.DRAINING, (2, 3)),
        (QueueState.ACTIVE, QueueState.PAUSED, QueueState.ACTIVE, (1, 2)),
        (QueueState.PAUSED, QueueState.ACTIVE, QueueState.PAUSED, (2, 3)),
    ],
)
def test_matching_replay_succeeds_across_state_flip(
    session_factory: sessionmaker[Session],
    from_state: QueueState,
    to_state: QueueState,
    seed_state: QueueState,
    expected_versions: tuple[int, int],
) -> None:
    """Committed matching replay wins across every normative state flip."""
    name = _unique(f"race.replay.{from_state.value}-{to_state.value}")
    setup = session_factory()
    try:
        queue = _seed_queue(setup, name=name)
        queue_pk = int(queue.id)
        body, body_bytes = _body({"replay": True})
        key = f"key-{uuid.uuid4().hex}"
        service = EnqueueService(session_factory=session_factory)
        first = service.enqueue(
            producer_id="producer-a",
            queue_name=name,
            idempotency_key=key,
            body=body,
            body_bytes=body_bytes
        )
        assert first.replayed is False
        original_task = first.task_id
        depth_before = _counter_depth(setup, queue_pk)
        counts_before = _counts_for_queue(setup, queue_pk)

        expected_at_flip = 1
        if seed_state is not QueueState.ACTIVE:
            QueueControlRepository().set_queue_state(
                setup,
                queue_name=name,
                mutation=SetQueueStateMutation(
                    expected_config_version=ConfigVersion(value=1),
                    state=seed_state,
                    metadata=_meta(actor_id="seed-state"),
                ),
            )
            setup.commit()
            expected_at_flip = 2
        assert from_state is seed_state
        assert expected_versions == (expected_at_flip, expected_at_flip + 1)
    finally:
        setup.close()

    flip_locked = threading.Event()
    replay_started = threading.Event()
    results = {"replay": _ThreadResult(), "flip": _ThreadResult()}
    original_lock = QueueControlRepository._lock_queue_by_name

    def lock_then_signal(
        self: QueueControlRepository,
        session: Session,
        queue_name: str,
    ) -> Queue:
        locked = original_lock(self, session, queue_name)
        flip_locked.set()
        assert replay_started.wait(timeout=_JOIN_TIMEOUT_S), "replay never started"
        return locked

    def run_flip() -> None:
        session = session_factory()
        try:
            with patch.object(
                QueueControlRepository,
                "_lock_queue_by_name",
                lock_then_signal,
            ):
                cfg = QueueControlRepository().set_queue_state(
                    session,
                    queue_name=name,
                    mutation=SetQueueStateMutation(
                        expected_config_version=ConfigVersion(value=expected_at_flip),
                        state=to_state,
                        metadata=_meta(actor_id=f"flip-{to_state.value}"),
                    ),
                )
            session.commit()
            results["flip"] = _ThreadResult(ok=True, value=cfg)
        except BaseException as exc:  # noqa: BLE001
            session.rollback()
            results["flip"] = _ThreadResult(ok=False, error=exc)
        finally:
            session.close()

    def run_replay() -> None:
        assert flip_locked.wait(timeout=_JOIN_TIMEOUT_S), "flip never locked"
        replay_started.set()
        service = EnqueueService(session_factory=session_factory)
        try:
            result = service.enqueue(
                producer_id="producer-a",
                queue_name=name,
                idempotency_key=key,
                body=body,
                body_bytes=body_bytes
            )
            results["replay"] = _ThreadResult(ok=True, value=result)
        except BaseException as exc:  # noqa: BLE001
            results["replay"] = _ThreadResult(ok=False, error=exc)

    t_flip = threading.Thread(target=run_flip, daemon=True)
    t_replay = threading.Thread(target=run_replay, daemon=True)
    t_flip.start()
    t_replay.start()
    _join(t_flip, label="state-flip")
    _join(t_replay, label="replay")

    assert results["flip"].ok, results["flip"].error
    assert results["replay"].ok, results["replay"].error
    replay = results["replay"].value
    assert replay.replayed is True
    assert replay.task_id == original_task

    verify = session_factory()
    try:
        row = _queue_row(verify, name)
        assert int(row.state_code) == _STATE_CODE[to_state]
        assert int(row.config_version) == expected_versions[1]
        assert _counts_for_queue(verify, queue_pk) == counts_before
        assert _counter_depth(verify, queue_pk) == depth_before
    finally:
        verify.close()


@pytest.mark.parametrize(
    "to_state",
    [QueueState.DRAINING, QueueState.PAUSED],
)
def test_changed_fingerprint_conflicts_during_state_flip(
    session_factory: sessionmaker[Session],
    to_state: QueueState,
) -> None:
    """Changed fingerprint always conflicts, independent of concurrent state winner."""
    name = _unique(f"race.conflict.{to_state.value}")
    setup = session_factory()
    try:
        queue = _seed_queue(setup, name=name)
        queue_pk = int(queue.id)
        body_ok, body_ok_bytes = _body({"v": 1})
        key = f"key-{uuid.uuid4().hex}"
        service = EnqueueService(session_factory=session_factory)
        first = service.enqueue(
            producer_id="producer-a",
            queue_name=name,
            idempotency_key=key,
            body=body_ok,
            body_bytes=body_ok_bytes
        )
        assert first.replayed is False
        counts_before = _counts_for_queue(setup, queue_pk)
        depth_before = _counter_depth(setup, queue_pk)
    finally:
        setup.close()

    body_bad, body_bad_bytes = _body({"v": 2})
    flip_locked = threading.Event()
    conflict_started = threading.Event()
    results = {"conflict": _ThreadResult(), "flip": _ThreadResult()}
    original_lock = QueueControlRepository._lock_queue_by_name

    def lock_then_signal(
        self: QueueControlRepository,
        session: Session,
        queue_name: str,
    ) -> Queue:
        locked = original_lock(self, session, queue_name)
        flip_locked.set()
        assert conflict_started.wait(timeout=_JOIN_TIMEOUT_S), "conflict never started"
        return locked

    def run_flip() -> None:
        session = session_factory()
        try:
            with patch.object(
                QueueControlRepository,
                "_lock_queue_by_name",
                lock_then_signal,
            ):
                cfg = QueueControlRepository().set_queue_state(
                    session,
                    queue_name=name,
                    mutation=SetQueueStateMutation(
                        expected_config_version=ConfigVersion(value=1),
                        state=to_state,
                        metadata=_meta(actor_id=f"flip-{to_state.value}"),
                    ),
                )
            session.commit()
            results["flip"] = _ThreadResult(ok=True, value=cfg)
        except BaseException as exc:  # noqa: BLE001
            session.rollback()
            results["flip"] = _ThreadResult(ok=False, error=exc)
        finally:
            session.close()

    def run_conflict() -> None:
        assert flip_locked.wait(timeout=_JOIN_TIMEOUT_S), "flip never locked"
        conflict_started.set()
        service = EnqueueService(session_factory=session_factory)
        try:
            result = service.enqueue(
                producer_id="producer-a",
                queue_name=name,
                idempotency_key=key,
                body=body_bad,
                body_bytes=body_bad_bytes
            )
            results["conflict"] = _ThreadResult(ok=True, value=result)
        except IntakeValidationError as exc:
            results["conflict"] = _ThreadResult(ok=False, error=exc)
        except BaseException as exc:  # noqa: BLE001
            results["conflict"] = _ThreadResult(ok=False, error=exc)

    t_flip = threading.Thread(target=run_flip, daemon=True)
    t_conflict = threading.Thread(target=run_conflict, daemon=True)
    t_flip.start()
    t_conflict.start()
    _join(t_flip, label="state-flip")
    _join(t_conflict, label="fingerprint-conflict")

    assert results["flip"].ok, results["flip"].error
    assert results["conflict"].ok is False
    err = results["conflict"].error
    assert isinstance(err, IntakeValidationError)
    assert err.code == "idempotency_conflict"
    assert err.retryable is False

    verify = session_factory()
    try:
        row = _queue_row(verify, name)
        assert int(row.state_code) == _STATE_CODE[to_state]
        assert int(row.config_version) == 2
        assert _counts_for_queue(verify, queue_pk) == counts_before
        assert _counter_depth(verify, queue_pk) == depth_before
        task = verify.execute(
            select(TaskActive).where(TaskActive.task_id == first.task_id)
        ).scalar_one()
        payload = verify.get(TaskPayloadActive, task.id)
        assert payload is not None
        assert payload.payload == {"v": 1}
    finally:
        verify.close()


def test_enqueue_wins_over_policy_activation_snapshots_old_version(
    session_factory: sessionmaker[Session],
) -> None:
    """Enqueue FOR UPDATE before activate: task snapshots the committed old policy."""
    name = _unique("race.enq-first.policy")
    setup = session_factory()
    try:
        queue = _seed_queue(setup, name=name)
        queue_pk = int(queue.id)
        v1_pk = _policy_pk_for_version(setup, queue_pk, 1)
        _append_inactive_policy(setup, name=name, max_attempts=17)
        v2_pk = _policy_pk_for_version(setup, queue_pk, 2)
        assert v1_pk != v2_pk
    finally:
        setup.close()

    enqueue_locked = threading.Event()
    activate_started = threading.Event()
    results = {"enqueue": _ThreadResult(), "activate": _ThreadResult()}
    body, body_bytes = _body({"policy": "old"})
    key = f"key-{uuid.uuid4().hex}"
    original_lock = EnqueueRepository.lock_named_queue

    def lock_then_signal(
        self: EnqueueRepository,
        session: Session,
        queue_name: str,
    ) -> Queue:
        locked = original_lock(self, session, queue_name)
        enqueue_locked.set()
        assert activate_started.wait(timeout=_JOIN_TIMEOUT_S), "activate never started"
        return locked

    def run_enqueue() -> None:
        service = EnqueueService(session_factory=session_factory)
        try:
            with patch.object(EnqueueRepository, "lock_named_queue", lock_then_signal):
                result = service.enqueue(
                    producer_id="producer-a",
                    queue_name=name,
                    idempotency_key=key,
                    body=body,
                    body_bytes=body_bytes
                )
            results["enqueue"] = _ThreadResult(ok=True, value=result)
        except BaseException as exc:  # noqa: BLE001
            results["enqueue"] = _ThreadResult(ok=False, error=exc)

    def run_activate() -> None:
        assert enqueue_locked.wait(timeout=_JOIN_TIMEOUT_S), "enqueue never locked"
        session = session_factory()
        activate_started.set()
        try:
            cfg = QueueControlRepository().activate_policy_version(
                session,
                queue_name=name,
                mutation=ActivatePolicyMutation(
                    policy_version=PolicyVersion(value=2),
                    expected_config_version=ConfigVersion(value=1),
                    metadata=_meta(actor_id="activate-after-enq"),
                ),
            )
            session.commit()
            results["activate"] = _ThreadResult(ok=True, value=cfg)
        except BaseException as exc:  # noqa: BLE001
            session.rollback()
            results["activate"] = _ThreadResult(ok=False, error=exc)
        finally:
            session.close()

    t_enq = threading.Thread(target=run_enqueue, daemon=True)
    t_act = threading.Thread(target=run_activate, daemon=True)
    t_enq.start()
    t_act.start()
    _join(t_enq, label="enqueue")
    _join(t_act, label="activate")

    assert results["enqueue"].ok, results["enqueue"].error
    assert results["activate"].ok, results["activate"].error
    enq = results["enqueue"].value
    assert enq.replayed is False
    assert enq.retry_policy_version_id == v1_pk

    verify = session_factory()
    try:
        row = _queue_row(verify, name)
        assert int(row.active_policy_version_id) == v2_pk
        assert int(row.config_version) == 2
        task = verify.execute(
            select(TaskActive).where(TaskActive.task_id == enq.task_id)
        ).scalar_one()
        assert int(task.retry_policy_version_id) == v1_pk
        assert _counts_for_queue(verify, queue_pk) == (1, 1, 1)
    finally:
        verify.close()


def test_policy_activation_wins_over_enqueue_snapshots_new_version(
    session_factory: sessionmaker[Session],
) -> None:
    """Activate FOR UPDATE before enqueue: task snapshots the committed new policy."""
    name = _unique("race.policy-first.enq")
    setup = session_factory()
    try:
        queue = _seed_queue(setup, name=name)
        queue_pk = int(queue.id)
        v1_pk = _policy_pk_for_version(setup, queue_pk, 1)
        _append_inactive_policy(setup, name=name, max_attempts=19)
        v2_pk = _policy_pk_for_version(setup, queue_pk, 2)
        assert v1_pk != v2_pk
    finally:
        setup.close()

    activate_locked = threading.Event()
    enqueue_started = threading.Event()
    results = {"enqueue": _ThreadResult(), "activate": _ThreadResult()}
    body, body_bytes = _body({"policy": "new"})
    key = f"key-{uuid.uuid4().hex}"
    original_lock = QueueControlRepository._lock_queue_by_name

    def lock_then_signal(
        self: QueueControlRepository,
        session: Session,
        queue_name: str,
    ) -> Queue:
        locked = original_lock(self, session, queue_name)
        activate_locked.set()
        assert enqueue_started.wait(timeout=_JOIN_TIMEOUT_S), "enqueue never started"
        return locked

    def run_activate() -> None:
        session = session_factory()
        try:
            with patch.object(
                QueueControlRepository,
                "_lock_queue_by_name",
                lock_then_signal,
            ):
                cfg = QueueControlRepository().activate_policy_version(
                    session,
                    queue_name=name,
                    mutation=ActivatePolicyMutation(
                        policy_version=PolicyVersion(value=2),
                        expected_config_version=ConfigVersion(value=1),
                        metadata=_meta(actor_id="activate-first"),
                    ),
                )
            session.commit()
            results["activate"] = _ThreadResult(ok=True, value=cfg)
        except BaseException as exc:  # noqa: BLE001
            session.rollback()
            results["activate"] = _ThreadResult(ok=False, error=exc)
        finally:
            session.close()

    def run_enqueue() -> None:
        assert activate_locked.wait(timeout=_JOIN_TIMEOUT_S), "activate never locked"
        enqueue_started.set()
        service = EnqueueService(session_factory=session_factory)
        try:
            result = service.enqueue(
                producer_id="producer-a",
                queue_name=name,
                idempotency_key=key,
                body=body,
                body_bytes=body_bytes
            )
            results["enqueue"] = _ThreadResult(ok=True, value=result)
        except BaseException as exc:  # noqa: BLE001
            results["enqueue"] = _ThreadResult(ok=False, error=exc)

    t_act = threading.Thread(target=run_activate, daemon=True)
    t_enq = threading.Thread(target=run_enqueue, daemon=True)
    t_act.start()
    t_enq.start()
    _join(t_act, label="activate")
    _join(t_enq, label="enqueue")

    assert results["activate"].ok, results["activate"].error
    assert results["enqueue"].ok, results["enqueue"].error
    enq = results["enqueue"].value
    assert enq.replayed is False
    assert enq.retry_policy_version_id == v2_pk

    verify = session_factory()
    try:
        row = _queue_row(verify, name)
        assert int(row.active_policy_version_id) == v2_pk
        assert int(row.config_version) == 2
        task = verify.execute(
            select(TaskActive).where(TaskActive.task_id == enq.task_id)
        ).scalar_one()
        assert int(task.retry_policy_version_id) == v2_pk
        assert task.retry_policy_version_id in {v1_pk, v2_pk}
        assert _counts_for_queue(verify, queue_pk) == (1, 1, 1)
    finally:
        verify.close()
