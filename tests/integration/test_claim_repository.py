"""Real-PostgreSQL proof of fenced claim/reclaim serialization (Phase 03.5-01).

Covers competing claimers, empty work, pause/drain gates, expiry reclaim,
token rotation, generation monotonicity, metadata equality, and a deterministic
pause-versus-claim race. PostgreSQL row locks are the serialization point.

Uses a module-private Alembic schema so claim rows do not pollute the
session-scoped ``migrated_schema`` shared by other integration tests.
"""

from __future__ import annotations

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
from uuid import UUID

import psycopg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, event, func, select, text, update
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from workhold.api.schemas.terminal import parse_complete_command
from workhold.application.claim_service import ClaimService
from workhold.application.completion import CompletionService
from workhold.application.lease_service import LeaseService
from workhold.domain.queue_control import (
    AdminRequestMetadata,
    BackoffStrategy,
    ConfigVersion,
    CreateQueueMutation,
    QueueState,
    RetryPolicyDraft,
    SetQueueStateMutation,
)
from workhold.infrastructure.postgres.claim_repository import (
    ClaimPersistenceResult,
    ClaimRepository,
)
from workhold.infrastructure.postgres.queue_control_repository import (
    QueueControlRepository,
)
from workhold.intake.contracts import normalize_enqueue_command
from workhold.intake.repository import EnqueueRepository
from workhold.storage.models import (
    ClaimRegistry,
    Queue,
    QueueCounter,
    TaskActive,
    TaskAttempt,
)

_JOIN_TIMEOUT_S = 30.0
_STATE_DELAYED = 1
_STATE_READY = 2
_STATE_LEASED = 3
_PRIORITY_MIN = -32768
_PRIORITY_MAX = 32767
_SKIP_PHASE12_CLAIM = pytest.mark.skip(
    reason="Wave 0 scaffold; implemented by 12-07",
)
_OUTCOME_ACTIVE = 1
_OUTCOME_EXPIRED = 5
_LEASE_SECONDS = 30
_ROOT = Path(__file__).resolve().parents[2]
_ALEMBIC_INI = _ROOT / "alembic.ini"
_SCHEMA_NAME_RE = re.compile(r"^[a-z_][a-z0-9_]*$")


def _require_test_database_url() -> str:
    url = os.environ.get("TEST_DATABASE_URL", "").strip()
    if not url:
        pytest.fail(
            "TEST_DATABASE_URL is required for tests/integration "
            "(PostgreSQL 16). Refusing to skip or xfail."
        )
    return url


def _to_psycopg_conninfo(url: str) -> str:
    if url.startswith("postgresql+psycopg://"):
        return "postgresql://" + url.removeprefix("postgresql+psycopg://")
    return url


def _purge_nonzero_priority_rows(database_url: str, schema: str) -> None:
    """Allow bounded-priority migration downgrade after priority claim tests."""
    conn = psycopg.connect(_to_psycopg_conninfo(database_url))
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(f'SET search_path TO "{schema}"')
            cur.execute("DELETE FROM tasks_active WHERE priority <> 0")
            cur.execute("DELETE FROM tasks_terminal WHERE priority <> 0")
    finally:
        conn.close()


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
def claim_migrated_schema() -> Iterator[str]:
    """Module-private migrated schema for claim/reclaim proofs."""
    database_url = _require_test_database_url()
    schema = f"qclaim_{uuid.uuid4().hex}"
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
            _purge_nonzero_priority_rows(database_url, schema)
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
def sa_engine(claim_migrated_schema: str) -> Iterator[Engine]:
    database_url = _require_test_database_url()
    schema = claim_migrated_schema
    engine = create_engine(database_url, pool_pre_ping=True, pool_size=8, max_overflow=0)

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
        idempotency_key=f"admin-idem-{uuid.uuid4().hex}",
    )


def _unique(prefix: str) -> str:
    return f"{prefix}.{uuid.uuid4().hex[:12]}"


def _seed_queue(session: Session, *, name: str) -> Queue:
    QueueControlRepository().create_named_queue(
        session,
        CreateQueueMutation(
            name=name,
            initial_policy=RetryPolicyDraft(
                enabled=True,
                max_attempts=3,
                backoff_strategy=BackoffStrategy.FIXED,
                retry_delay_seconds=0,
            ),
            metadata=_meta(actor_id="claim-seed"),
        ),
    )
    session.commit()
    return session.execute(select(Queue).where(Queue.name == name)).scalar_one()


def _enqueue_ready(
    session: Session,
    *,
    queue_name: str,
    producer_id: str = "producer-a",
    payload: dict[str, Any] | None = None,
) -> UUID:
    cmd = normalize_enqueue_command(
        producer_id=producer_id,
        queue_name=queue_name,
        idempotency_key=f"idem-{uuid.uuid4().hex}",
        payload=payload or {"n": 1},
        priority=0,
        available_at=None
    )
    result = EnqueueRepository().stage_enqueue(session, cmd)
    session.commit()
    assert result.replayed is False
    return result.task_id


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
            metadata=_meta(actor_id="state-flip"),
        ),
    )
    session.commit()


def _claim(
    session_factory: sessionmaker[Session],
    *,
    queue_name: str,
    worker_id: str,
    lease_seconds: int = _LEASE_SECONDS,
) -> ClaimPersistenceResult:
    return ClaimService(session_factory=session_factory).claim(
        queue_name=queue_name,
        worker_id=worker_id,
        lease_seconds=lease_seconds,
    )


@dataclass
class _ThreadResult:
    ok: bool = False
    value: Any = None
    error: BaseException | None = None


def _join(thread: threading.Thread, *, label: str) -> None:
    thread.join(timeout=_JOIN_TIMEOUT_S)
    assert not thread.is_alive(), f"{label} did not finish within {_JOIN_TIMEOUT_S}s"


def test_empty_active_queue_returns_empty_without_mutation(
    session_factory: sessionmaker[Session],
) -> None:
    name = _unique("claim.empty")
    setup = session_factory()
    try:
        _seed_queue(setup, name=name)
    finally:
        setup.close()

    result = _claim(session_factory, queue_name=name, worker_id="worker-empty")
    assert result.empty is True
    assert result.paused is False
    assert result.task_id is None
    assert result.claim_id is None
    assert result.claim_token is None

    verify = session_factory()
    try:
        queue = verify.execute(select(Queue).where(Queue.name == name)).scalar_one()
        task_ids = list(
            verify.scalars(select(TaskActive.task_id).where(TaskActive.queue_id == queue.id))
        )
        assert (
            verify.scalar(
                select(func.count())
                .select_from(ClaimRegistry)
                .where(ClaimRegistry.task_id.in_(task_ids or [uuid.uuid4()]))
            )
            == 0
        )
        assert (
            verify.scalar(
                select(func.count())
                .select_from(TaskAttempt)
                .where(TaskAttempt.task_id.in_(task_ids or [uuid.uuid4()]))
            )
            == 0
        )
    finally:
        verify.close()


def test_paused_returns_empty_without_claim_mutation(
    session_factory: sessionmaker[Session],
) -> None:
    name = _unique("claim.paused")
    setup = session_factory()
    try:
        _seed_queue(setup, name=name)
        _enqueue_ready(setup, queue_name=name)
        _set_state(
            setup,
            queue_name=name,
            state=QueueState.PAUSED,
            expected_config_version=1,
        )
    finally:
        setup.close()

    result = _claim(session_factory, queue_name=name, worker_id="worker-paused")
    assert result.empty is True
    assert result.paused is True
    assert result.task_id is None

    verify = session_factory()
    try:
        queue = verify.execute(select(Queue).where(Queue.name == name)).scalar_one()
        task = verify.execute(
            select(TaskActive).where(TaskActive.queue_id == queue.id)
        ).scalar_one()
        assert int(task.state_code) == _STATE_READY
        assert task.current_claim_id is None
        assert (
            verify.scalar(
                select(func.count())
                .select_from(ClaimRegistry)
                .where(ClaimRegistry.task_id == task.task_id)
            )
            == 0
        )
        assert (
            verify.scalar(
                select(func.count())
                .select_from(TaskAttempt)
                .where(TaskAttempt.task_id == task.task_id)
            )
            == 0
        )
    finally:
        verify.close()


def test_active_and_draining_permit_claim(
    session_factory: sessionmaker[Session],
) -> None:
    for state, version_bump in (
        (QueueState.ACTIVE, 0),
        (QueueState.DRAINING, 1),
    ):
        name = _unique(f"claim.{state.value}")
        setup = session_factory()
        try:
            _seed_queue(setup, name=name)
            task_id = _enqueue_ready(setup, queue_name=name, payload={"s": state.value})
            if state is QueueState.DRAINING:
                _set_state(
                    setup,
                    queue_name=name,
                    state=QueueState.DRAINING,
                    expected_config_version=1,
                )
        finally:
            setup.close()

        result = _claim(
            session_factory,
            queue_name=name,
            worker_id=f"worker-{state.value}",
        )
        assert result.empty is False, state
        assert result.paused is False, state
        assert result.task_id == task_id
        assert result.claim_id is not None
        assert result.claim_token is not None
        assert result.generation == 1
        assert version_bump in (0, 1)


def test_claim_and_attempt_share_queue_store_metadata(
    session_factory: sessionmaker[Session],
) -> None:
    name = _unique("claim.meta")
    worker_id = "worker-meta-1"
    setup = session_factory()
    try:
        _seed_queue(setup, name=name)
        task_id = _enqueue_ready(setup, queue_name=name)
    finally:
        setup.close()

    result = _claim(session_factory, queue_name=name, worker_id=worker_id)
    assert result.empty is False
    assert result.claimed_at is not None
    assert result.lease_expires_at is not None
    assert result.worker_id == worker_id
    assert result.generation == 1

    verify = session_factory()
    try:
        task = verify.execute(
            select(TaskActive).where(TaskActive.task_id == task_id)
        ).scalar_one()
        registry = verify.execute(
            select(ClaimRegistry).where(ClaimRegistry.claim_id == result.claim_id)
        ).scalar_one()
        attempt = verify.execute(
            select(TaskAttempt).where(TaskAttempt.claim_id == result.claim_id)
        ).scalar_one()

        assert task.current_claim_id == result.claim_id
        assert task.claimed_at == result.claimed_at == registry.claimed_at == attempt.claimed_at
        assert (
            task.lease_expires_at
            == result.lease_expires_at
            == registry.lease_expires_at
            == attempt.lease_expires_at
        )
        assert task.generation == result.generation == registry.generation == attempt.generation
        assert task.worker_id == worker_id == attempt.worker_id
        assert int(task.state_code) == _STATE_LEASED
        assert int(attempt.outcome_code) == _OUTCOME_ACTIVE
        assert attempt.ended_at is None
        assert registry.claim_token == result.claim_token
        assert registry.task_id == task_id
    finally:
        verify.close()


def test_reclaim_after_expiry_rotates_token_and_increments_generation(
    session_factory: sessionmaker[Session],
) -> None:
    name = _unique("claim.reclaim")
    setup = session_factory()
    try:
        _seed_queue(setup, name=name)
        task_id = _enqueue_ready(setup, queue_name=name)
    finally:
        setup.close()

    first = _claim(session_factory, queue_name=name, worker_id="worker-first")
    assert first.empty is False
    assert first.generation == 1
    old_claim_id = first.claim_id
    old_token = first.claim_token

    expire = session_factory()
    try:
        # Reclaim selects on tasks_active.lease_expires_at; claim_registry keeps
        # lease_expires_at > claimed_at, so only the hot task lease is aged here.
        expire.execute(
            update(TaskActive)
            .where(TaskActive.task_id == task_id)
            .values(
                lease_expires_at=func.transaction_timestamp()
                - text("interval '1 second'")
            )
        )
        expire.commit()
    finally:
        expire.close()

    second = _claim(session_factory, queue_name=name, worker_id="worker-second")
    assert second.empty is False
    assert second.task_id == task_id
    assert second.generation == 2
    assert second.claim_id != old_claim_id
    assert second.claim_token != old_token
    assert second.worker_id == "worker-second"

    verify = session_factory()
    try:
        task = verify.execute(
            select(TaskActive).where(TaskActive.task_id == task_id)
        ).scalar_one()
        assert task.current_claim_id == second.claim_id
        assert task.generation == 2
        assert task.worker_id == "worker-second"

        registries = list(
            verify.scalars(
                select(ClaimRegistry).where(ClaimRegistry.task_id == task_id)
            )
        )
        assert len(registries) == 1
        assert registries[0].claim_id == second.claim_id
        assert registries[0].claim_token == second.claim_token
        assert registries[0].generation == 2

        attempts = list(
            verify.scalars(
                select(TaskAttempt)
                .where(TaskAttempt.task_id == task_id)
                .order_by(TaskAttempt.generation)
            )
        )
        assert len(attempts) == 2
        old_attempt, new_attempt = attempts
        assert old_attempt.claim_id == old_claim_id
        assert int(old_attempt.outcome_code) == _OUTCOME_EXPIRED
        assert old_attempt.ended_at is not None
        assert new_attempt.claim_id == second.claim_id
        assert int(new_attempt.outcome_code) == _OUTCOME_ACTIVE
        assert new_attempt.ended_at is None
        assert new_attempt.generation == 2
        assert new_attempt.worker_id == "worker-second"
        assert (
            new_attempt.claimed_at
            == task.claimed_at
            == registries[0].claimed_at
            == second.claimed_at
        )
        assert (
            new_attempt.lease_expires_at
            == task.lease_expires_at
            == registries[0].lease_expires_at
            == second.lease_expires_at
        )
    finally:
        verify.close()


def test_twenty_synchronized_claim_races_produce_one_lease(
    session_factory: sessionmaker[Session],
) -> None:
    """PASS iff 20 races each yield one current lease, one attempt, ≤1 non-empty."""
    for race_index in range(20):
        name = _unique(f"claim.race.{race_index}")
        setup = session_factory()
        try:
            _seed_queue(setup, name=name)
            task_id = _enqueue_ready(setup, queue_name=name, payload={"race": race_index})
        finally:
            setup.close()

        barrier = threading.Barrier(2, timeout=_JOIN_TIMEOUT_S)
        outcomes: list[ClaimPersistenceResult | None] = [None, None]
        errors: list[BaseException | None] = [None, None]

        def worker(index: int) -> None:
            worker_name = f"w-{race_index}-{index}"
            try:
                barrier.wait()
                outcomes[index] = _claim(
                    session_factory,
                    queue_name=name,
                    worker_id=worker_name,
                )
            except BaseException as exc:  # noqa: BLE001
                errors[index] = exc

        threads = [
            threading.Thread(target=worker, args=(0,), daemon=True),
            threading.Thread(target=worker, args=(1,), daemon=True),
        ]
        for thread in threads:
            thread.start()
        for index, thread in enumerate(threads):
            _join(thread, label=f"race-{race_index}-worker-{index}")

        for index, err in enumerate(errors):
            if err is not None:
                raise AssertionError(f"race {race_index} worker {index} failed") from err

        assert outcomes[0] is not None and outcomes[1] is not None
        non_empty = [r for r in outcomes if r is not None and not r.empty]
        empty = [r for r in outcomes if r is not None and r.empty]
        assert len(non_empty) == 1, f"race {race_index}: {outcomes!r}"
        assert len(empty) == 1
        winner = non_empty[0]
        assert winner.task_id == task_id
        assert winner.generation == 1

        verify = session_factory()
        try:
            task = verify.execute(
                select(TaskActive).where(TaskActive.task_id == task_id)
            ).scalar_one()
            assert task.current_claim_id == winner.claim_id
            assert task.generation == 1
            assert (
                verify.scalar(
                    select(func.count())
                    .select_from(ClaimRegistry)
                    .where(ClaimRegistry.task_id == task_id)
                )
                == 1
            )
            attempts = list(
                verify.scalars(
                    select(TaskAttempt).where(TaskAttempt.task_id == task_id)
                )
            )
            assert len(attempts) == 1
            assert attempts[0].claim_id == winner.claim_id
            assert int(attempts[0].outcome_code) == _OUTCOME_ACTIVE
        finally:
            verify.close()


def test_pause_claim_race_serializes_on_locked_queue_state(
    session_factory: sessionmaker[Session],
) -> None:
    """Winner of queue-row lock determines claim vs paused-empty; no ambiguity."""
    # Case A: claim locks first — lease commits, then pause commits.
    name_claim_first = _unique("claim.pause.claim-first")
    setup = session_factory()
    try:
        _seed_queue(setup, name=name_claim_first)
        task_id_a = _enqueue_ready(setup, queue_name=name_claim_first)
    finally:
        setup.close()

    claim_locked = threading.Event()
    pause_started = threading.Event()
    results_a = {"claim": _ThreadResult(), "pause": _ThreadResult()}
    original_lock = ClaimRepository.lock_named_queue

    def lock_then_hold(
        self: ClaimRepository,
        session: Session,
        queue_name: str,
    ) -> Queue:
        locked = original_lock(self, session, queue_name)
        claim_locked.set()
        assert pause_started.wait(timeout=_JOIN_TIMEOUT_S), "pause never started"
        return locked

    def run_claim_a() -> None:
        try:
            with patch.object(ClaimRepository, "lock_named_queue", lock_then_hold):
                result = _claim(
                    session_factory,
                    queue_name=name_claim_first,
                    worker_id="worker-claim-first",
                )
            results_a["claim"] = _ThreadResult(ok=True, value=result)
        except BaseException as exc:  # noqa: BLE001
            results_a["claim"] = _ThreadResult(ok=False, error=exc)

    def run_pause_a() -> None:
        assert claim_locked.wait(timeout=_JOIN_TIMEOUT_S), "claim never locked"
        session = session_factory()
        pause_started.set()
        try:
            cfg = QueueControlRepository().set_queue_state(
                session,
                queue_name=name_claim_first,
                mutation=SetQueueStateMutation(
                    expected_config_version=ConfigVersion(value=1),
                    state=QueueState.PAUSED,
                    metadata=_meta(actor_id="pause-after-claim"),
                ),
            )
            session.commit()
            results_a["pause"] = _ThreadResult(ok=True, value=cfg)
        except BaseException as exc:  # noqa: BLE001
            session.rollback()
            results_a["pause"] = _ThreadResult(ok=False, error=exc)
        finally:
            session.close()

    t_claim = threading.Thread(target=run_claim_a, daemon=True)
    t_pause = threading.Thread(target=run_pause_a, daemon=True)
    t_claim.start()
    t_pause.start()
    _join(t_claim, label="claim-first")
    _join(t_pause, label="pause-second")

    assert results_a["claim"].ok, results_a["claim"].error
    assert results_a["pause"].ok, results_a["pause"].error
    claim_a = results_a["claim"].value
    assert claim_a.empty is False
    assert claim_a.task_id == task_id_a

    verify = session_factory()
    try:
        queue = verify.execute(
            select(Queue).where(Queue.name == name_claim_first)
        ).scalar_one()
        assert int(queue.state_code) == 2  # paused after claim
        task = verify.execute(
            select(TaskActive).where(TaskActive.task_id == task_id_a)
        ).scalar_one()
        assert task.current_claim_id == claim_a.claim_id
        assert task.generation == 1
        assert (
            verify.scalar(
                select(func.count())
                .select_from(ClaimRegistry)
                .where(ClaimRegistry.task_id == task_id_a)
            )
            == 1
        )
        assert (
            verify.scalar(
                select(func.count())
                .select_from(TaskAttempt)
                .where(TaskAttempt.task_id == task_id_a)
            )
            == 1
        )
    finally:
        verify.close()

    # Case B: pause locks first — claim observes paused and mutates nothing.
    name_pause_first = _unique("claim.pause.pause-first")
    setup = session_factory()
    try:
        _seed_queue(setup, name=name_pause_first)
        task_id_b = _enqueue_ready(setup, queue_name=name_pause_first)
    finally:
        setup.close()

    pause_locked = threading.Event()
    claim_started = threading.Event()
    results_b = {"claim": _ThreadResult(), "pause": _ThreadResult()}
    original_control_lock = QueueControlRepository._lock_queue_by_name

    def control_lock_then_hold(
        self: QueueControlRepository,
        session: Session,
        queue_name: str,
    ) -> Queue:
        locked = original_control_lock(self, session, queue_name)
        pause_locked.set()
        assert claim_started.wait(timeout=_JOIN_TIMEOUT_S), "claim never started"
        return locked

    def run_pause_b() -> None:
        session = session_factory()
        try:
            with patch.object(
                QueueControlRepository,
                "_lock_queue_by_name",
                control_lock_then_hold,
            ):
                cfg = QueueControlRepository().set_queue_state(
                    session,
                    queue_name=name_pause_first,
                    mutation=SetQueueStateMutation(
                        expected_config_version=ConfigVersion(value=1),
                        state=QueueState.PAUSED,
                        metadata=_meta(actor_id="pause-first"),
                    ),
                )
                session.commit()
            results_b["pause"] = _ThreadResult(ok=True, value=cfg)
        except BaseException as exc:  # noqa: BLE001
            session.rollback()
            results_b["pause"] = _ThreadResult(ok=False, error=exc)
        finally:
            session.close()

    def run_claim_b() -> None:
        assert pause_locked.wait(timeout=_JOIN_TIMEOUT_S), "pause never locked"
        claim_started.set()
        try:
            result = _claim(
                session_factory,
                queue_name=name_pause_first,
                worker_id="worker-after-pause",
            )
            results_b["claim"] = _ThreadResult(ok=True, value=result)
        except BaseException as exc:  # noqa: BLE001
            results_b["claim"] = _ThreadResult(ok=False, error=exc)

    t_pause_b = threading.Thread(target=run_pause_b, daemon=True)
    t_claim_b = threading.Thread(target=run_claim_b, daemon=True)
    t_pause_b.start()
    t_claim_b.start()
    _join(t_pause_b, label="pause-first")
    _join(t_claim_b, label="claim-second")

    assert results_b["pause"].ok, results_b["pause"].error
    assert results_b["claim"].ok, results_b["claim"].error
    claim_b = results_b["claim"].value
    assert claim_b.empty is True
    assert claim_b.paused is True
    assert claim_b.task_id is None

    verify = session_factory()
    try:
        task = verify.execute(
            select(TaskActive).where(TaskActive.task_id == task_id_b)
        ).scalar_one()
        assert int(task.state_code) == _STATE_READY
        assert task.current_claim_id is None
        # Only the claim-first race left registry/attempt rows in this schema.
        attempts_b = list(
            verify.scalars(
                select(TaskAttempt).where(TaskAttempt.task_id == task_id_b)
            )
        )
        assert attempts_b == []
        registries_b = list(
            verify.scalars(
                select(ClaimRegistry).where(ClaimRegistry.task_id == task_id_b)
            )
        )
        assert registries_b == []
    finally:
        verify.close()


def _seed_delayed_task(
    session: Session,
    *,
    queue_name: str,
    offset_seconds: int = 3600,
) -> UUID:
    """Queue-store delayed row for due-claim scaffolds (until public delayed enqueue)."""
    task_id = _enqueue_ready(session, queue_name=queue_name)
    task = session.execute(
        select(TaskActive).where(TaskActive.task_id == task_id).with_for_update()
    ).scalar_one()
    session.execute(
        update(TaskActive)
        .where(TaskActive.task_id == task_id)
        .values(
            state_code=_STATE_DELAYED,
            available_at=func.transaction_timestamp()
            + text(f"interval '{offset_seconds} seconds'"),
        )
    )
    counter = session.get(QueueCounter, int(task.queue_id))
    assert counter is not None
    counter.ready_count = max(0, int(counter.ready_count) - 1)
    counter.delayed_count = int(counter.delayed_count) + 1
    session.commit()
    return task_id


def test_delayed_future_claim_empty_without_mutation(
    session_factory: sessionmaker[Session],
) -> None:
    name = _unique("claim.delayed.future")
    setup = session_factory()
    try:
        _seed_queue(setup, name=name)
        task_id = _seed_delayed_task(setup, queue_name=name, offset_seconds=3600)
    finally:
        setup.close()

    result = _claim(session_factory, queue_name=name, worker_id="worker-not-due")
    assert result.empty is True
    assert result.paused is False
    assert result.task_id is None
    assert result.claim_id is None
    assert result.claim_token is None

    verify = session_factory()
    try:
        task = verify.execute(
            select(TaskActive).where(TaskActive.task_id == task_id)
        ).scalar_one()
        assert int(task.state_code) == _STATE_DELAYED
        assert task.current_claim_id is None
        assert task.generation == 0
        assert (
            verify.scalar(
                select(func.count())
                .select_from(ClaimRegistry)
                .where(ClaimRegistry.task_id == task_id)
            )
            == 0
        )
        assert (
            verify.scalar(
                select(func.count())
                .select_from(TaskAttempt)
                .where(TaskAttempt.task_id == task_id)
            )
            == 0
        )
        counter = verify.get(QueueCounter, int(task.queue_id))
        assert counter is not None
        assert int(counter.delayed_count) == 1
        assert int(counter.ready_count) == 0
        assert int(counter.leased_count) == 0
    finally:
        verify.close()


def test_due_delayed_claim_transitions_counters_and_metadata(
    session_factory: sessionmaker[Session],
) -> None:
    name = _unique("claim.delayed.due")
    setup = session_factory()
    try:
        _seed_queue(setup, name=name)
        task_id = _seed_delayed_task(setup, queue_name=name, offset_seconds=1)
    finally:
        setup.close()

    age = session_factory()
    try:
        age.execute(
            update(TaskActive)
            .where(TaskActive.task_id == task_id)
            .values(
                available_at=func.transaction_timestamp() - text("interval '1 second'")
            )
        )
        age.commit()
    finally:
        age.close()

    result = _claim(session_factory, queue_name=name, worker_id="worker-due-delayed")
    assert result.empty is False
    assert result.task_id == task_id
    assert result.claim_id is not None
    assert result.claim_token is not None
    assert result.generation == 1
    assert result.claimed_at is not None
    assert result.lease_expires_at is not None

    verify = session_factory()
    try:
        task = verify.execute(
            select(TaskActive).where(TaskActive.task_id == task_id)
        ).scalar_one()
        assert int(task.state_code) == _STATE_LEASED
        assert task.current_claim_id == result.claim_id
        assert task.worker_id == "worker-due-delayed"
        registry = verify.execute(
            select(ClaimRegistry).where(ClaimRegistry.claim_id == result.claim_id)
        ).scalar_one()
        attempt = verify.execute(
            select(TaskAttempt).where(TaskAttempt.claim_id == result.claim_id)
        ).scalar_one()
        assert registry.claim_token == result.claim_token
        assert int(attempt.outcome_code) == _OUTCOME_ACTIVE
        counter = verify.get(QueueCounter, int(task.queue_id))
        assert counter is not None
        assert int(counter.delayed_count) == 0
        assert int(counter.ready_count) == 0
        assert int(counter.leased_count) == 1
    finally:
        verify.close()


def test_no_work_after_claim_is_empty(
    session_factory: sessionmaker[Session],
) -> None:
    name = _unique("claim.once")
    setup = session_factory()
    try:
        _seed_queue(setup, name=name)
        _enqueue_ready(setup, queue_name=name)
    finally:
        setup.close()

    first = _claim(session_factory, queue_name=name, worker_id="worker-a")
    assert first.empty is False
    second = _claim(session_factory, queue_name=name, worker_id="worker-b")
    assert second.empty is True
    assert second.paused is False


def _insert_ready_with_priority(
    session: Session,
    *,
    queue_name: str,
    priority: int,
    available_at_offset: str,
    producer_id: str = "producer-priority",
) -> tuple[UUID, int]:
    """Direct SQL insert for post-1201 bounded priority claim scaffolds."""
    queue = session.execute(
        select(Queue).where(Queue.name == queue_name)
    ).scalar_one()
    policy_id = int(queue.active_policy_version_id)
    active_id, task_id = session.execute(
        text(
            f"""
            INSERT INTO tasks_active (
                task_id, queue_id, producer_id, state_code, priority,
                available_at, retry_policy_version_id
            ) VALUES (
                gen_random_uuid(), :queue_pk, :producer_id, :state_ready, :priority,
                transaction_timestamp() {available_at_offset}, :policy_id
            )
            RETURNING id, task_id
            """
        ),
        {
            "queue_pk": int(queue.id),
            "producer_id": producer_id,
            "state_ready": _STATE_READY,
            "priority": priority,
            "policy_id": policy_id,
        },
    ).one()
    session.execute(
        text(
            """
            INSERT INTO task_payloads_active (task_id, payload, payload_bytes)
            VALUES (:task_pk, '{"priority": true}'::jsonb, 17)
            """
        ),
        {"task_pk": int(active_id)},
    )
    session.commit()
    return task_id, int(active_id)


def _authorize_all(_queue_name: str) -> bool:
    return True


def test_higher_priority_due_wins_over_older_lower_priority(
    session_factory: sessionmaker[Session],
) -> None:
    name = _unique("claim.priority.high-first")
    setup = session_factory()
    try:
        _seed_queue(setup, name=name)
        low_id, _ = _insert_ready_with_priority(
            setup,
            queue_name=name,
            priority=_PRIORITY_MIN,
            available_at_offset="- interval '10 seconds'",
        )
        high_id, _ = _insert_ready_with_priority(
            setup,
            queue_name=name,
            priority=_PRIORITY_MAX,
            available_at_offset="- interval '1 second'",
        )
    finally:
        setup.close()

    result = _claim(session_factory, queue_name=name, worker_id="worker-high-first")
    assert result.empty is False
    assert result.task_id == high_id
    assert result.task_id != low_id


def test_same_priority_fifo_by_available_at_then_captured_id(
    session_factory: sessionmaker[Session],
) -> None:
    name = _unique("claim.priority.fifo")
    setup = session_factory()
    try:
        _seed_queue(setup, name=name)
        first_id, first_pk = _insert_ready_with_priority(
            setup,
            queue_name=name,
            priority=10,
            available_at_offset="- interval '5 seconds'",
        )
        second_id, second_pk = _insert_ready_with_priority(
            setup,
            queue_name=name,
            priority=10,
            available_at_offset="- interval '1 second'",
        )
    finally:
        setup.close()
    assert first_pk != second_pk

    first_claim = _claim(session_factory, queue_name=name, worker_id="worker-fifo-a")
    second_claim = _claim(session_factory, queue_name=name, worker_id="worker-fifo-b")
    assert first_claim.task_id == first_id
    assert second_claim.task_id == second_id


def test_future_max_priority_does_not_preempt_due_min(
    session_factory: sessionmaker[Session],
) -> None:
    name = _unique("claim.priority.future-gate")
    setup = session_factory()
    try:
        _seed_queue(setup, name=name)
        due_id, _ = _insert_ready_with_priority(
            setup,
            queue_name=name,
            priority=_PRIORITY_MIN,
            available_at_offset="- interval '1 second'",
        )
        future_id, _ = _insert_ready_with_priority(
            setup,
            queue_name=name,
            priority=_PRIORITY_MAX,
            available_at_offset="+ interval '1 hour'",
        )
    finally:
        setup.close()

    result = _claim(session_factory, queue_name=name, worker_id="worker-due-min")
    assert result.empty is False
    assert result.task_id == due_id

    verify = session_factory()
    try:
        future = verify.execute(
            select(TaskActive).where(TaskActive.task_id == future_id)
        ).scalar_one()
        assert int(future.state_code) == _STATE_READY
        assert future.current_claim_id is None
        is_future = verify.scalar(
            select(TaskActive.available_at > func.transaction_timestamp()).where(
                TaskActive.task_id == future_id
            )
        )
        assert is_future is True
        assert (
            verify.scalar(
                select(func.count())
                .select_from(ClaimRegistry)
                .where(ClaimRegistry.task_id == future_id)
            )
            == 0
        )
    finally:
        verify.close()


def test_unexpired_low_priority_lease_not_preempted_by_later_max_enqueue(
    session_factory: sessionmaker[Session],
) -> None:
    name = _unique("claim.priority.non-preempt")
    setup = session_factory()
    try:
        _seed_queue(setup, name=name)
        low_id, _ = _insert_ready_with_priority(
            setup,
            queue_name=name,
            priority=_PRIORITY_MIN,
            available_at_offset="- interval '1 second'",
        )
    finally:
        setup.close()

    leased = _claim(session_factory, queue_name=name, worker_id="worker-low")
    assert leased.empty is False
    assert leased.task_id == low_id
    original_claim_id = leased.claim_id
    original_token = leased.claim_token
    original_generation = leased.generation

    enqueue = session_factory()
    try:
        _insert_ready_with_priority(
            enqueue,
            queue_name=name,
            priority=_PRIORITY_MAX,
            available_at_offset="- interval '1 second'",
        )
    finally:
        enqueue.close()

    second = _claim(session_factory, queue_name=name, worker_id="worker-max")
    assert second.empty is False
    assert second.task_id != low_id

    verify = session_factory()
    try:
        low = verify.execute(
            select(TaskActive).where(TaskActive.task_id == low_id)
        ).scalar_one()
        assert low.current_claim_id == original_claim_id
        assert low.generation == original_generation
        assert int(low.state_code) == _STATE_LEASED
        hb = LeaseService(session_factory=session_factory).heartbeat(
            claim_id=original_claim_id,
            claim_token=original_token,
            generation=original_generation,
            lease_seconds=_LEASE_SECONDS,
            authorize_queue=_authorize_all,
        )
        assert hb.claim_id == original_claim_id
        assert hb.generation == original_generation
        complete = CompletionService(session_factory=session_factory).complete(
            claim_id=original_claim_id,
            claim_token=original_token,
            command=parse_complete_command(
                {"generation": original_generation, "spawn": []}
            ),
            authorize_queue=_authorize_all,
        )
        assert complete.replayed is False
        assert complete.task_id == low_id
    finally:
        verify.close()


def test_unexpired_leased_row_excluded_from_common_selector(
    session_factory: sessionmaker[Session],
) -> None:
    name = _unique("claim.priority.unexpired-lease")
    setup = session_factory()
    try:
        _seed_queue(setup, name=name)
        leased_id, _ = _insert_ready_with_priority(
            setup,
            queue_name=name,
            priority=_PRIORITY_MAX,
            available_at_offset="- interval '1 second'",
        )
        due_id, _ = _insert_ready_with_priority(
            setup,
            queue_name=name,
            priority=0,
            available_at_offset="- interval '1 second'",
        )
    finally:
        setup.close()

    first = _claim(session_factory, queue_name=name, worker_id="worker-lease-first")
    assert first.task_id == leased_id

    second = _claim(session_factory, queue_name=name, worker_id="worker-lease-second")
    assert second.task_id == due_id

    verify = session_factory()
    try:
        leased = verify.execute(
            select(TaskActive).where(TaskActive.task_id == leased_id)
        ).scalar_one()
        is_unexpired = verify.scalar(
            select(TaskActive.lease_expires_at > func.transaction_timestamp()).where(
                TaskActive.task_id == leased_id
            )
        )
        assert is_unexpired is True
        assert int(leased.state_code) == _STATE_LEASED
    finally:
        verify.close()


def test_expired_leased_row_competes_in_common_selector_by_priority_tuple(
    session_factory: sessionmaker[Session],
) -> None:
    name = _unique("claim.priority.expired-reclaim")
    setup = session_factory()
    try:
        _seed_queue(setup, name=name)
        expired_leased_id, _ = _insert_ready_with_priority(
            setup,
            queue_name=name,
            priority=50,
            available_at_offset="- interval '10 seconds'",
        )
        due_low_id, _ = _insert_ready_with_priority(
            setup,
            queue_name=name,
            priority=10,
            available_at_offset="- interval '1 second'",
        )
    finally:
        setup.close()

    first = _claim(session_factory, queue_name=name, worker_id="worker-expired-first")
    assert first.task_id == expired_leased_id

    expire = session_factory()
    try:
        expire.execute(
            update(TaskActive)
            .where(TaskActive.task_id == expired_leased_id)
            .values(
                lease_expires_at=func.transaction_timestamp()
                - text("interval '1 second'")
            )
        )
        expire.commit()
    finally:
        expire.close()

    reclaimed = _claim(
        session_factory, queue_name=name, worker_id="worker-expired-reclaim"
    )
    assert reclaimed.empty is False
    assert reclaimed.task_id == expired_leased_id
    assert reclaimed.generation == 2

    verify = session_factory()
    try:
        due_low = verify.execute(
            select(TaskActive).where(TaskActive.task_id == due_low_id)
        ).scalar_one()
        assert int(due_low.state_code) == _STATE_READY
        assert due_low.current_claim_id is None
    finally:
        verify.close()
