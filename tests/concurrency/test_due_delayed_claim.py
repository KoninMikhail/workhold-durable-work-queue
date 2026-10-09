"""Due-delayed claim concurrency scaffolds (Phase 11 Plan 08 / WORK-15).

Plan 04 removes every temporary skip and implements the broadened due predicate.
"""

from __future__ import annotations

import os
import re
import threading
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime
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
from workhold.domain.queue_control import (
    AdminRequestMetadata,
    BackoffStrategy,
    CreateQueueMutation,
    RetryPolicyDraft,
)
from workhold.infrastructure.postgres.claim_repository import ClaimPersistenceResult
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
_STATE_LEASED = 3
_OUTCOME_ACTIVE = 1
_LEASE_SECONDS = 30
_ROOT = Path(__file__).resolve().parents[2]
_ALEMBIC_INI = _ROOT / "alembic.ini"
_SCHEMA_NAME_RE = re.compile(r"^[a-z_][a-z0-9_]*$")
def _require_test_database_url() -> str:
    url = os.environ.get("TEST_DATABASE_URL", "").strip()
    if not url:
        pytest.fail(
            "TEST_DATABASE_URL is required for tests/concurrency "
            "(PostgreSQL). Refusing to skip or xfail."
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
def due_delayed_migrated_schema() -> Iterator[str]:
    database_url = _require_test_database_url()
    schema = f"qduedelay_{uuid.uuid4().hex}"
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
def sa_engine(due_delayed_migrated_schema: str) -> Iterator[Engine]:
    database_url = _require_test_database_url()
    schema = due_delayed_migrated_schema
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
            metadata=_meta(actor_id="due-delayed-seed"),
        ),
    )
    session.commit()
    return session.execute(select(Queue).where(Queue.name == name)).scalar_one()


def _enqueue_ready(session: Session, *, queue_name: str) -> UUID:
    cmd = normalize_enqueue_command(
        producer_id="producer-a",
        queue_name=queue_name,
        idempotency_key=f"idem-{uuid.uuid4().hex}",
        payload={"n": 1},
        priority=0,
        available_at=None
    )
    result = EnqueueRepository().stage_enqueue(session, cmd)
    session.commit()
    assert result.replayed is False
    return result.task_id


def _seed_due_delayed_task(session: Session, *, queue_name: str) -> UUID:
    task_id = _enqueue_ready(session, queue_name=queue_name)
    task = session.execute(
        select(TaskActive).where(TaskActive.task_id == task_id).with_for_update()
    ).scalar_one()
    session.execute(
        update(TaskActive)
        .where(TaskActive.task_id == task_id)
        .values(
            state_code=_STATE_DELAYED,
            available_at=func.transaction_timestamp() - text("interval '1 second'"),
        )
    )
    counter = session.get(QueueCounter, int(task.queue_id))
    assert counter is not None
    counter.ready_count = max(0, int(counter.ready_count) - 1)
    counter.delayed_count = int(counter.delayed_count) + 1
    session.commit()
    return task_id


def _claim(
    session_factory: sessionmaker[Session],
    *,
    queue_name: str,
    worker_id: str,
) -> ClaimPersistenceResult:
    return ClaimService(session_factory=session_factory).claim(
        queue_name=queue_name,
        worker_id=worker_id,
        lease_seconds=_LEASE_SECONDS,
    )


def _join(thread: threading.Thread, *, label: str) -> None:
    thread.join(timeout=_JOIN_TIMEOUT_S)
    assert not thread.is_alive(), f"{label} did not finish within {_JOIN_TIMEOUT_S}s"


def test_two_synchronized_claimers_race_due_delayed_one_winner(
    session_factory: sessionmaker[Session],
) -> None:
    """Exactly one lease, one registry, one attempt; counters non-negative."""
    name = _unique("due.delayed.race")
    setup = session_factory()
    try:
        _seed_queue(setup, name=name)
        task_id = _seed_due_delayed_task(setup, queue_name=name)
    finally:
        setup.close()

    barrier = threading.Barrier(2, timeout=_JOIN_TIMEOUT_S)
    outcomes: list[ClaimPersistenceResult | None] = [None, None]
    errors: list[BaseException | None] = [None, None]

    def worker(index: int) -> None:
        worker_name = f"race-worker-{index}"
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
        _join(thread, label=f"due-delayed-race-worker-{index}")

    for index, err in enumerate(errors):
        if err is not None:
            raise AssertionError(f"worker {index} failed") from err

    assert outcomes[0] is not None and outcomes[1] is not None
    non_empty = [r for r in outcomes if r is not None and not r.empty]
    empty = [r for r in outcomes if r is not None and r.empty]
    assert len(non_empty) == 1
    assert len(empty) == 1
    winner = non_empty[0]
    assert winner.task_id == task_id
    assert winner.generation == 1

    verify = session_factory()
    try:
        task = verify.execute(
            select(TaskActive).where(TaskActive.task_id == task_id)
        ).scalar_one()
        assert int(task.state_code) == _STATE_LEASED
        assert task.current_claim_id == winner.claim_id
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
        assert int(attempts[0].outcome_code) == _OUTCOME_ACTIVE
        counter = verify.get(QueueCounter, int(task.queue_id))
        assert counter is not None
        assert int(counter.delayed_count) == 0
        assert int(counter.leased_count) == 1
        assert int(counter.ready_count) >= 0
    finally:
        verify.close()
