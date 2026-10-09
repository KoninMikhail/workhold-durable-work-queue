"""Bounded priority claim concurrency scaffolds (Phase 12 Plan 02 / WORK-16).

Plan 07 removes every temporary skip and implements priority ordering races.
"""

from __future__ import annotations

import os
import re
import threading
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from uuid import UUID

import psycopg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, event, func, select, text
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
from workhold.storage.models import (
    ClaimRegistry,
    Queue,
    TaskActive,
    TaskAttempt,
)

_JOIN_TIMEOUT_S = 30.0
_STATE_READY = 2
_STATE_LEASED = 3
_OUTCOME_ACTIVE = 1
_PRIORITY_MIN = -32768
_PRIORITY_MAX = 32767
_LEASE_SECONDS = 30
_ROOT = Path(__file__).resolve().parents[2]
_ALEMBIC_INI = _ROOT / "alembic.ini"
_SCHEMA_NAME_RE = re.compile(r"^[a-z_][a-z0-9_]*$")
_SKIP_PHASE12_RACE = pytest.mark.skip(
    reason="Wave 0 scaffold; implemented by 12-07",
)


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
def priority_migrated_schema() -> Iterator[str]:
    database_url = _require_test_database_url()
    schema = f"qpriority_{uuid.uuid4().hex}"
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
def sa_engine(priority_migrated_schema: str) -> Iterator[Engine]:
    database_url = _require_test_database_url()
    schema = priority_migrated_schema
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
            metadata=_meta(actor_id="priority-race-seed"),
        ),
    )
    session.commit()
    return session.execute(select(Queue).where(Queue.name == name)).scalar_one()


def _insert_ready_with_priority(
    session: Session,
    *,
    queue_name: str,
    priority: int,
    available_at_offset: str,
) -> UUID:
    queue = session.execute(
        select(Queue).where(Queue.name == queue_name)
    ).scalar_one()
    active_id, task_id = session.execute(
        text(
            f"""
            INSERT INTO tasks_active (
                task_id, queue_id, producer_id, state_code, priority,
                available_at, retry_policy_version_id
            ) VALUES (
                gen_random_uuid(), :queue_pk, 'producer-priority-race', :state_ready,
                :priority, transaction_timestamp() {available_at_offset}, :policy_id
            )
            RETURNING id, task_id
            """
        ),
        {
            "queue_pk": int(queue.id),
            "state_ready": _STATE_READY,
            "priority": priority,
            "policy_id": int(queue.active_policy_version_id),
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


def test_two_synchronized_claimers_split_due_priorities_unique_leases(
    session_factory: sessionmaker[Session],
) -> None:
    """Each claimer gets one task; global order is high priority then low."""
    name = _unique("priority.race.two-tasks")
    setup = session_factory()
    try:
        _seed_queue(setup, name=name)
        low_id = _insert_ready_with_priority(
            setup,
            queue_name=name,
            priority=_PRIORITY_MIN,
            available_at_offset="- interval '10 seconds'",
        )
        high_id = _insert_ready_with_priority(
            setup,
            queue_name=name,
            priority=_PRIORITY_MAX,
            available_at_offset="- interval '1 second'",
        )
    finally:
        setup.close()

    barrier = threading.Barrier(2, timeout=_JOIN_TIMEOUT_S)
    outcomes: list[ClaimPersistenceResult | None] = [None, None]
    errors: list[BaseException | None] = [None, None]

    def worker(index: int) -> None:
        try:
            barrier.wait()
            outcomes[index] = _claim(
                session_factory,
                queue_name=name,
                worker_id=f"race-worker-{index}",
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
        _join(thread, label=f"priority-race-worker-{index}")

    for index, err in enumerate(errors):
        if err is not None:
            raise AssertionError(f"worker {index} failed") from err

    assert outcomes[0] is not None and outcomes[1] is not None
    claimed_ids = {
        r.task_id for r in outcomes if r is not None and not r.empty and r.task_id
    }
    assert claimed_ids == {high_id, low_id}

    sequential_high = _claim(
        session_factory, queue_name=name, worker_id="sequential-probe"
    )
    assert sequential_high.empty is True

    verify = session_factory()
    try:
        for task_id in (high_id, low_id):
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
    finally:
        verify.close()


def test_expiry_race_versus_new_high_priority_serialized_without_duplicates(
    session_factory: sessionmaker[Session],
) -> None:
    """Expiry finalization and new high-priority due enqueue serialize cleanly."""
    name = _unique("priority.race.expiry-vs-high")
    setup = session_factory()
    try:
        _seed_queue(setup, name=name)
        leased_id = _insert_ready_with_priority(
            setup,
            queue_name=name,
            priority=10,
            available_at_offset="- interval '5 seconds'",
        )
    finally:
        setup.close()

    first = _claim(session_factory, queue_name=name, worker_id="worker-expire-race")
    assert first.task_id == leased_id

    prep = session_factory()
    try:
        prep.execute(
            text(
                """
                UPDATE tasks_active
                SET lease_expires_at = transaction_timestamp() - interval '1 second'
                WHERE task_id = :task_id
                """
            ),
            {"task_id": leased_id},
        )
        _insert_ready_with_priority(
            prep,
            queue_name=name,
            priority=_PRIORITY_MAX,
            available_at_offset="- interval '1 second'",
        )
        prep.commit()
    finally:
        prep.close()

    barrier = threading.Barrier(2, timeout=_JOIN_TIMEOUT_S)
    outcomes: list[ClaimPersistenceResult | None] = [None, None]
    errors: list[BaseException | None] = [None, None]

    def worker(index: int) -> None:
        try:
            barrier.wait()
            outcomes[index] = _claim(
                session_factory,
                queue_name=name,
                worker_id=f"expire-race-{index}",
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
        _join(thread, label=f"expire-race-worker-{index}")

    for index, err in enumerate(errors):
        if err is not None:
            raise AssertionError(f"worker {index} failed") from err

    non_empty = [r for r in outcomes if r is not None and not r.empty]
    assert len(non_empty) == 2
    claimed_ids = {r.task_id for r in non_empty if r.task_id is not None}
    assert len(claimed_ids) == 2

    verify = session_factory()
    try:
        queue = verify.execute(select(Queue).where(Queue.name == name)).scalar_one()
        task_ids = list(
            verify.scalars(
                select(TaskActive.task_id).where(TaskActive.queue_id == queue.id)
            )
        )
        for task_id in task_ids:
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
            active_attempts = [
                a for a in attempts if int(a.outcome_code) == _OUTCOME_ACTIVE
            ]
            assert len(active_attempts) == 1
        priorities = {
            verify.execute(
                select(TaskActive.priority).where(TaskActive.task_id == r.task_id)
            ).scalar_one()
            for r in non_empty
            if r.task_id is not None
        }
        assert _PRIORITY_MAX in priorities
        leased = verify.execute(
            select(TaskActive).where(TaskActive.task_id == leased_id)
        ).scalar_one()
        assert int(leased.state_code) in (_STATE_LEASED, _STATE_READY)
    finally:
        verify.close()
