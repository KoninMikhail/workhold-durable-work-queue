"""Real-PostgreSQL lease-expiry transitions (Phase 03.6-03).

Covers WORK-05/06/08 and QUAL-02: expired leases close one attempt, apply the
task's enqueue-time policy snapshot (or cancel), and serialize concurrent
finalizers under the same row lock used by reclaim.
"""

from __future__ import annotations

import os
import re
import threading
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID

import psycopg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, event, func, select, text, update
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from workhold.application.claim_service import ClaimService
from workhold.application.lease_expiry import LeaseExpiryService
from workhold.domain.queue_control import (
    AdminRequestMetadata,
    BackoffStrategy,
    CreateQueueMutation,
    RetryPolicyDraft,
)
from workhold.domain.retry import (
    LEASE_EXPIRY_FAILURE_CODE,
    REASON_ATTEMPTS_EXHAUSTED,
    REASON_RETRY_DISABLED,
)
from workhold.infrastructure.postgres.claim_repository import ClaimRepository
from workhold.infrastructure.postgres.lease_repository import (
    FenceDecision,
    LeaseRepository,
)
from workhold.infrastructure.postgres.queue_control_repository import (
    QueueControlRepository,
)
from workhold.intake.contracts import normalize_enqueue_command
from workhold.intake.repository import EnqueueRepository
from workhold.storage.models import (
    ClaimRegistry,
    Queue,
    TaskActive,
    TaskAttempt,
    TaskTerminal,
)

_JOIN_TIMEOUT_S = 30.0
_STATE_DELAYED = 1
_STATE_READY = 2
_STATE_LEASED = 3
_OUTCOME_ACTIVE = 1
_OUTCOME_EXPIRED = 5
_TERMINAL_DEAD_LETTERED = 11
_TERMINAL_CANCELLED = 12
_LEASE_SECONDS = 30
_ROOT = Path(__file__).resolve().parents[2]
_ALEMBIC_INI = _ROOT / "alembic.ini"
_SCHEMA_NAME_RE = re.compile(r"^[a-z_][a-z0-9_]*$")
_FAILURE_CODE_RE = re.compile(r"^[a-z][a-z0-9._-]{0,127}$")


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
def expiry_migrated_schema() -> Iterator[str]:
    database_url = _require_test_database_url()
    schema = f"qexp_{uuid.uuid4().hex}"
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
def sa_engine(expiry_migrated_schema: str) -> Iterator[Engine]:
    database_url = _require_test_database_url()
    schema = expiry_migrated_schema
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


def _seed_queue(
    session: Session,
    *,
    name: str,
    enabled: bool = True,
    max_attempts: int = 3,
    retry_delay_seconds: int = 30,
) -> Queue:
    QueueControlRepository().create_named_queue(
        session,
        CreateQueueMutation(
            name=name,
            initial_policy=RetryPolicyDraft(
                enabled=enabled,
                max_attempts=max_attempts,
                backoff_strategy=BackoffStrategy.FIXED,
                retry_delay_seconds=retry_delay_seconds,
            ),
            metadata=_meta(actor_id="expiry-seed"),
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


def _claim(
    session_factory: sessionmaker[Session],
    *,
    queue_name: str,
    worker_id: str,
    lease_seconds: int = _LEASE_SECONDS,
):
    return ClaimService(session_factory=session_factory).claim(
        queue_name=queue_name,
        worker_id=worker_id,
        lease_seconds=lease_seconds,
    )


def _expire_lease(session: Session, *, task_id: UUID, claim_id: UUID) -> None:
    """Cross expiry using Queue-store timestamps only (not worker wall clock)."""
    session.execute(
        update(TaskActive)
        .where(TaskActive.task_id == task_id)
        .values(
            lease_expires_at=func.transaction_timestamp() - text("interval '1 second'")
        )
    )
    session.execute(
        update(ClaimRegistry)
        .where(ClaimRegistry.claim_id == claim_id)
        .values(
            claimed_at=func.transaction_timestamp() - text("interval '2 hours'"),
            lease_expires_at=func.transaction_timestamp()
            - text("interval '1 second'"),
        )
    )
    session.commit()


def _request_cancel(session: Session, *, task_id: UUID) -> None:
    session.execute(
        update(TaskActive)
        .where(TaskActive.task_id == task_id)
        .values(cancel_requested_at=func.transaction_timestamp())
    )
    session.commit()


@dataclass
class _ThreadResult:
    ok: bool = False
    value: Any = None
    error: BaseException | None = None


def _join(thread: threading.Thread, *, label: str) -> None:
    thread.join(timeout=_JOIN_TIMEOUT_S)
    assert not thread.is_alive(), f"{label} did not finish within {_JOIN_TIMEOUT_S}s"


def test_retryable_expiry_closes_attempt_and_schedules_from_queue_store_time(
    session_factory: sessionmaker[Session],
) -> None:
    name = _unique("exp.retry")
    delay = 45
    setup = session_factory()
    try:
        _seed_queue(
            setup,
            name=name,
            enabled=True,
            max_attempts=3,
            retry_delay_seconds=delay,
        )
        task_id = _enqueue_ready(setup, queue_name=name)
    finally:
        setup.close()

    first = _claim(session_factory, queue_name=name, worker_id="worker-a")
    assert first.empty is False
    assert first.claim_id is not None
    claim_id = first.claim_id
    claim_token = first.claim_token
    generation = first.generation

    expire = session_factory()
    try:
        _expire_lease(expire, task_id=task_id, claim_id=claim_id)
        before = expire.scalar(select(func.transaction_timestamp()))
        assert before is not None
    finally:
        expire.close()

    result = LeaseExpiryService(session_factory=session_factory).finalize_expired(
        task_id=task_id
    )
    assert result.state == "retry_scheduled"
    assert result.available_at is not None
    assert result.terminal_at is None

    verify = session_factory()
    try:
        task = verify.execute(
            select(TaskActive).where(TaskActive.task_id == task_id)
        ).scalar_one()
        assert int(task.state_code) == _STATE_DELAYED
        assert task.current_claim_id is None
        assert task.lease_expires_at is None
        assert task.available_at == result.available_at
        # available_at is Queue-store now + snapshotted delay (not worker clock).
        assert timedelta(seconds=delay - 5) <= (result.available_at - before) <= timedelta(
            seconds=delay + 5
        )

        attempts = list(
            verify.scalars(
                select(TaskAttempt)
                .where(TaskAttempt.task_id == task_id)
                .order_by(TaskAttempt.generation)
            )
        )
        assert len(attempts) == 1
        assert int(attempts[0].outcome_code) == _OUTCOME_EXPIRED
        assert attempts[0].ended_at is not None
        assert attempts[0].failure_code is None

        registries = list(
            verify.scalars(select(ClaimRegistry).where(ClaimRegistry.task_id == task_id))
        )
        assert registries == []

        terminals = list(
            verify.scalars(select(TaskTerminal).where(TaskTerminal.task_id == task_id))
        )
        assert terminals == []
    finally:
        verify.close()

    # Stale credentials cannot mutate after finalization.
    probe_session = session_factory()
    try:
        fence = LeaseRepository().validate_current_lease(
            probe_session,
            claim_id=claim_id,
            claim_token=claim_token,  # type: ignore[arg-type]
            generation=int(generation or 0),
            for_update=False,
        )
        assert fence.decision is FenceDecision.STALE
    finally:
        probe_session.close()

    reclaim = _claim(session_factory, queue_name=name, worker_id="worker-b")
    assert reclaim.empty is True


def test_disabled_expiry_dead_letters_with_canonical_failure_code(
    session_factory: sessionmaker[Session],
) -> None:
    name = _unique("exp.disabled")
    setup = session_factory()
    try:
        _seed_queue(
            setup,
            name=name,
            enabled=False,
            max_attempts=5,
            retry_delay_seconds=0,
        )
        task_id = _enqueue_ready(setup, queue_name=name)
    finally:
        setup.close()

    first = _claim(session_factory, queue_name=name, worker_id="worker-a")
    assert first.empty is False
    assert first.claim_id is not None

    expire = session_factory()
    try:
        _expire_lease(expire, task_id=task_id, claim_id=first.claim_id)
    finally:
        expire.close()

    result = LeaseExpiryService(session_factory=session_factory).finalize_expired(
        task_id=task_id
    )
    assert result.state == "dead_lettered"
    assert result.terminal_at is not None
    assert result.available_at is None

    assert _FAILURE_CODE_RE.fullmatch(LEASE_EXPIRY_FAILURE_CODE) is not None
    assert 1 <= len(LEASE_EXPIRY_FAILURE_CODE) <= 128

    verify = session_factory()
    try:
        active = verify.execute(
            select(TaskActive).where(TaskActive.task_id == task_id)
        ).scalar_one_or_none()
        assert active is None

        attempts = list(
            verify.scalars(
                select(TaskAttempt)
                .where(TaskAttempt.task_id == task_id)
                .order_by(TaskAttempt.generation)
            )
        )
        assert len(attempts) == 1
        assert int(attempts[0].outcome_code) == _OUTCOME_EXPIRED
        assert attempts[0].failure_code == LEASE_EXPIRY_FAILURE_CODE
        assert attempts[0].failure_detail == REASON_RETRY_DISABLED
        assert _FAILURE_CODE_RE.fullmatch(attempts[0].failure_code) is not None
        assert len(attempts[0].failure_detail) <= 4096

        terminals = list(
            verify.scalars(select(TaskTerminal).where(TaskTerminal.task_id == task_id))
        )
        assert len(terminals) == 1
        assert int(terminals[0].state_code) == _TERMINAL_DEAD_LETTERED
        assert terminals[0].failure_code == LEASE_EXPIRY_FAILURE_CODE
        assert terminals[0].failure_detail == REASON_RETRY_DISABLED
    finally:
        verify.close()

    reclaim = _claim(session_factory, queue_name=name, worker_id="worker-b")
    assert reclaim.empty is True


def test_exhausted_expiry_dead_letters_with_attempts_exhausted_detail(
    session_factory: sessionmaker[Session],
) -> None:
    name = _unique("exp.exhaust")
    setup = session_factory()
    try:
        _seed_queue(
            setup,
            name=name,
            enabled=True,
            max_attempts=1,
            retry_delay_seconds=0,
        )
        task_id = _enqueue_ready(setup, queue_name=name)
    finally:
        setup.close()

    first = _claim(session_factory, queue_name=name, worker_id="worker-a")
    assert first.empty is False
    assert first.claim_id is not None

    expire = session_factory()
    try:
        _expire_lease(expire, task_id=task_id, claim_id=first.claim_id)
    finally:
        expire.close()

    result = LeaseExpiryService(session_factory=session_factory).finalize_expired(
        task_id=task_id
    )
    assert result.state == "dead_lettered"

    verify = session_factory()
    try:
        attempt = verify.execute(
            select(TaskAttempt).where(TaskAttempt.task_id == task_id)
        ).scalar_one()
        assert attempt.failure_code == LEASE_EXPIRY_FAILURE_CODE
        assert attempt.failure_detail == REASON_ATTEMPTS_EXHAUSTED
        terminal = verify.execute(
            select(TaskTerminal).where(TaskTerminal.task_id == task_id)
        ).scalar_one()
        assert int(terminal.state_code) == _TERMINAL_DEAD_LETTERED
        assert terminal.failure_detail == REASON_ATTEMPTS_EXHAUSTED
    finally:
        verify.close()


def test_cancellation_requested_expiry_cancels_and_never_reclaimable(
    session_factory: sessionmaker[Session],
) -> None:
    name = _unique("exp.cancel")
    setup = session_factory()
    try:
        _seed_queue(
            setup,
            name=name,
            enabled=True,
            max_attempts=5,
            retry_delay_seconds=0,
        )
        task_id = _enqueue_ready(setup, queue_name=name)
    finally:
        setup.close()

    first = _claim(session_factory, queue_name=name, worker_id="worker-a")
    assert first.empty is False
    assert first.claim_id is not None
    claim_id = first.claim_id
    claim_token = first.claim_token
    generation = int(first.generation or 0)

    cancel = session_factory()
    try:
        _request_cancel(cancel, task_id=task_id)
    finally:
        cancel.close()

    expire = session_factory()
    try:
        _expire_lease(expire, task_id=task_id, claim_id=claim_id)
    finally:
        expire.close()

    result = LeaseExpiryService(session_factory=session_factory).finalize_expired(
        task_id=task_id
    )
    assert result.state == "cancelled"
    assert result.terminal_at is not None

    verify = session_factory()
    try:
        assert (
            verify.execute(
                select(TaskActive).where(TaskActive.task_id == task_id)
            ).scalar_one_or_none()
            is None
        )
        attempt = verify.execute(
            select(TaskAttempt).where(TaskAttempt.task_id == task_id)
        ).scalar_one()
        assert int(attempt.outcome_code) == _OUTCOME_EXPIRED
        assert attempt.failure_code is None
        terminal = verify.execute(
            select(TaskTerminal).where(TaskTerminal.task_id == task_id)
        ).scalar_one()
        assert int(terminal.state_code) == _TERMINAL_CANCELLED
        assert terminal.failure_code is None
        assert list(
            verify.scalars(select(ClaimRegistry).where(ClaimRegistry.claim_id == claim_id))
        ) == []
    finally:
        verify.close()

    reclaim = _claim(session_factory, queue_name=name, worker_id="worker-b")
    assert reclaim.empty is True

    probe = session_factory()
    try:
        fence = LeaseRepository().validate_current_lease(
            probe,
            claim_id=claim_id,
            claim_token=claim_token,  # type: ignore[arg-type]
            generation=generation,
            for_update=False,
        )
        assert fence.decision is FenceDecision.STALE
    finally:
        probe.close()


def test_concurrent_expiry_finalizers_commit_exactly_one_outcome(
    session_factory: sessionmaker[Session],
) -> None:
    name = _unique("exp.race")
    setup = session_factory()
    try:
        _seed_queue(
            setup,
            name=name,
            enabled=False,
            max_attempts=1,
            retry_delay_seconds=0,
        )
        task_id = _enqueue_ready(setup, queue_name=name)
    finally:
        setup.close()

    first = _claim(session_factory, queue_name=name, worker_id="worker-a")
    assert first.empty is False
    assert first.claim_id is not None

    expire = session_factory()
    try:
        _expire_lease(expire, task_id=task_id, claim_id=first.claim_id)
    finally:
        expire.close()

    barrier = threading.Barrier(2)
    results: dict[str, _ThreadResult] = {
        "a": _ThreadResult(),
        "b": _ThreadResult(),
    }
    original_lock = ClaimRepository._select_claimable_task

    def run(label: str) -> None:
        try:
            barrier.wait(timeout=_JOIN_TIMEOUT_S)
            outcome = LeaseExpiryService(session_factory=session_factory).finalize_expired(
                task_id=task_id
            )
            results[label] = _ThreadResult(ok=True, value=outcome)
        except BaseException as exc:  # noqa: BLE001
            results[label] = _ThreadResult(ok=False, error=exc)

    t_a = threading.Thread(target=run, args=("a",), daemon=True)
    t_b = threading.Thread(target=run, args=("b",), daemon=True)
    t_a.start()
    t_b.start()
    _join(t_a, label="expiry-a")
    _join(t_b, label="expiry-b")

    assert results["a"].ok, results["a"].error
    assert results["b"].ok, results["b"].error
    states = {results["a"].value.state, results["b"].value.state}
    # One winner finalizes; the loser observes already_finalized (or same terminal).
    assert "dead_lettered" in states
    assert states <= {"dead_lettered", "already_finalized"}

    verify = session_factory()
    try:
        terminals = list(
            verify.scalars(select(TaskTerminal).where(TaskTerminal.task_id == task_id))
        )
        assert len(terminals) == 1
        assert int(terminals[0].state_code) == _TERMINAL_DEAD_LETTERED
        attempts = list(
            verify.scalars(select(TaskAttempt).where(TaskAttempt.task_id == task_id))
        )
        assert len(attempts) == 1
        assert int(attempts[0].outcome_code) == _OUTCOME_EXPIRED
    finally:
        verify.close()

    # Silence unused binding for reclaim-lock pattern documentation.
    assert original_lock is not None


def test_zero_delay_retry_expiry_is_immediately_claimable(
    session_factory: sessionmaker[Session],
) -> None:
    name = _unique("exp.ready")
    setup = session_factory()
    try:
        _seed_queue(
            setup,
            name=name,
            enabled=True,
            max_attempts=3,
            retry_delay_seconds=0,
        )
        task_id = _enqueue_ready(setup, queue_name=name)
    finally:
        setup.close()

    first = _claim(session_factory, queue_name=name, worker_id="worker-a")
    assert first.empty is False
    assert first.claim_id is not None
    old_claim_id = first.claim_id

    expire = session_factory()
    try:
        _expire_lease(expire, task_id=task_id, claim_id=old_claim_id)
    finally:
        expire.close()

    result = LeaseExpiryService(session_factory=session_factory).finalize_expired(
        task_id=task_id
    )
    assert result.state == "retry_scheduled"

    verify = session_factory()
    try:
        task = verify.execute(
            select(TaskActive).where(TaskActive.task_id == task_id)
        ).scalar_one()
        assert int(task.state_code) == _STATE_READY
        assert task.current_claim_id is None
    finally:
        verify.close()

    second = _claim(session_factory, queue_name=name, worker_id="worker-b")
    assert second.empty is False
    assert second.task_id == task_id
    assert second.generation == 2
    assert second.claim_id != old_claim_id


def test_lease_expiry_positive_delay_empty_before_due_claimable_after(
    session_factory: sessionmaker[Session],
) -> None:
    """Expiry retry delay > 0: empty claim before due; success after Queue-store advance."""
    from workhold.storage.models import QueueCounter

    name = _unique("exp.delay.scaffold")
    delay = 90
    setup = session_factory()
    try:
        _seed_queue(
            setup,
            name=name,
            enabled=True,
            max_attempts=3,
            retry_delay_seconds=delay,
        )
        task_id = _enqueue_ready(setup, queue_name=name)
    finally:
        setup.close()

    first = _claim(session_factory, queue_name=name, worker_id="worker-a")
    assert first.empty is False
    assert first.claim_id is not None
    claim_id = first.claim_id

    expire = session_factory()
    try:
        _expire_lease(expire, task_id=task_id, claim_id=claim_id)
    finally:
        expire.close()

    result = LeaseExpiryService(session_factory=session_factory).finalize_expired(
        task_id=task_id
    )
    assert result.state == "retry_scheduled"
    assert result.available_at is not None

    before = _claim(session_factory, queue_name=name, worker_id="worker-b")
    assert before.empty is True
    assert before.task_id is None

    advance = session_factory()
    try:
        advance.execute(
            update(TaskActive)
            .where(TaskActive.task_id == task_id)
            .values(
                available_at=func.transaction_timestamp()
                - text("interval '1 second'")
            )
        )
        advance.commit()
    finally:
        advance.close()

    after = _claim(session_factory, queue_name=name, worker_id="worker-c")
    assert after.empty is False
    assert after.task_id == task_id
    assert after.generation == 2
    assert after.claim_id is not None

    verify = session_factory()
    try:
        task = verify.execute(
            select(TaskActive).where(TaskActive.task_id == task_id)
        ).scalar_one()
        assert int(task.state_code) == _STATE_LEASED
        counter = verify.get(QueueCounter, int(task.queue_id))
        assert counter is not None
        assert int(counter.leased_count) == 1
        assert int(counter.delayed_count) == 0
        assert int(counter.ready_count) >= 0
    finally:
        verify.close()
