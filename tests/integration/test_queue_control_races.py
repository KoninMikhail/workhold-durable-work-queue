"""Deterministic multi-connection PostgreSQL control-plane race coverage.

QUAL-02 (Phase 3.3 slice): state/state, policy/policy, state/policy, and
distinct-queue independence. PostgreSQL row locks are the serialization point;
process-local locks are never used for correctness. Thread barriers/events only
coordinate test start and hold points.

This module uses its own Alembic schema so high-cardinality race rows do not
pollute the session-scoped ``migrated_schema`` shared by other integration tests.
"""

from __future__ import annotations

import os
import re
import threading
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

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
    DomainValidationError,
    PolicyVersion,
    QueueState,
    RetryPolicyDraft,
    SetQueueStateMutation,
)
from workhold.infrastructure.postgres.queue_control_repository import (
    QueueControlRepository,
)
from workhold.storage.models import AdminAuditLog, Queue, QueuePolicyVersion

_AUDIT_CREATE_QUEUE = 1
_AUDIT_CREATE_POLICY = 2
_AUDIT_ACTIVATE_POLICY = 3
_AUDIT_SET_STATE = 4
_RACE_REPETITIONS = 10
_JOIN_TIMEOUT_S = 30.0
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
    """Module-private migrated schema for control-plane race proofs."""
    database_url = _require_test_database_url()
    schema = f"qrace_{uuid.uuid4().hex}"
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
    """Engine whose connections share the race-module Alembic schema."""
    database_url = _require_test_database_url()
    schema = race_migrated_schema
    engine = create_engine(database_url, pool_pre_ping=True, pool_size=4, max_overflow=0)

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


def _seed_queue(
    session: Session,
    repo: QueueControlRepository,
    name: str,
) -> None:
    repo.create_named_queue(
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


def _append_inactive_policy(
    session: Session,
    repo: QueueControlRepository,
    name: str,
    *,
    max_attempts: int = 7,
) -> PolicyVersion:
    created = repo.create_policy_version(
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
    # Newest immutable version is always the one just appended.
    cfg = repo.get_queue_configuration(session, name=name)
    assert cfg is not None
    assert cfg.active_policy.version.value == 1
    assert created.config_version.value == 1
    return PolicyVersion(value=2)


@dataclass(frozen=True, slots=True)
class _CallerOutcome:
    ok: bool
    value: Any = None
    error: DomainValidationError | None = None


def _run_racing_pair(
    factory: sessionmaker[Session],
    left: Callable[[Session], Any],
    right: Callable[[Session], Any],
) -> tuple[_CallerOutcome, _CallerOutcome]:
    """Start both callers behind a barrier; return committed outcomes."""
    barrier = threading.Barrier(2, timeout=_JOIN_TIMEOUT_S)
    outcomes: list[_CallerOutcome | None] = [None, None]
    errors: list[BaseException | None] = [None, None]

    def _worker(index: int, mutate: Callable[[Session], Any]) -> None:
        session = factory()
        try:
            barrier.wait()
            try:
                value = mutate(session)
                session.commit()
                outcomes[index] = _CallerOutcome(ok=True, value=value)
            except DomainValidationError as exc:
                session.rollback()
                outcomes[index] = _CallerOutcome(ok=False, error=exc)
            except BaseException as exc:  # noqa: BLE001 — surface unexpected races
                session.rollback()
                errors[index] = exc
        finally:
            session.close()

    threads = [
        threading.Thread(target=_worker, args=(0, left), daemon=True),
        threading.Thread(target=_worker, args=(1, right), daemon=True),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=_JOIN_TIMEOUT_S)
        assert not thread.is_alive(), "race worker did not finish within timeout"

    for index, err in enumerate(errors):
        if err is not None:
            raise AssertionError(f"caller {index} raised unexpected error") from err
    assert outcomes[0] is not None and outcomes[1] is not None
    return outcomes[0], outcomes[1]


def _assert_one_winner_one_conflict(
    left: _CallerOutcome,
    right: _CallerOutcome,
) -> _CallerOutcome:
    winners = [outcome for outcome in (left, right) if outcome.ok]
    losers = [outcome for outcome in (left, right) if not outcome.ok]
    assert len(winners) == 1, f"expected exactly one winner, got {left=!r} {right=!r}"
    assert len(losers) == 1, f"expected exactly one loser, got {left=!r} {right=!r}"
    assert losers[0].error is not None
    assert losers[0].error.code == "config_version_conflict"
    return winners[0]


def _queue_pk(session: Session, name: str) -> int:
    queue = session.execute(select(Queue).where(Queue.name == name)).scalar_one()
    return int(queue.id)


def _count_policies(session: Session, queue_pk: int) -> int:
    return int(
        session.scalar(
            select(func.count())
            .select_from(QueuePolicyVersion)
            .where(QueuePolicyVersion.queue_id == queue_pk)
        )
        or 0
    )


def _count_audits(
    session: Session,
    queue_pk: int,
    *,
    operation_code: int | None = None,
) -> int:
    stmt = select(func.count()).select_from(AdminAuditLog).where(
        AdminAuditLog.queue_id == queue_pk
    )
    if operation_code is not None:
        stmt = stmt.where(AdminAuditLog.operation_code == operation_code)
    return int(session.scalar(stmt) or 0)


def _policy_row_snapshot(session: Session, policy_pk: int) -> tuple[object, ...]:
    row = session.get(QueuePolicyVersion, policy_pk)
    assert row is not None
    return (
        row.id,
        row.queue_id,
        row.version,
        row.enabled,
        row.max_attempts,
        row.backoff_strategy_code,
        row.retry_delay_seconds,
        row.created_at,
    )


def test_state_state_race_one_winner_ten_reps(
    session_factory: sessionmaker[Session],
) -> None:
    repo = QueueControlRepository()
    for rep in range(_RACE_REPETITIONS):
        name = _unique(f"race.state-state.{rep}")
        setup = session_factory()
        try:
            _seed_queue(setup, repo, name)
            queue_pk = _queue_pk(setup, name)
            audits_before = _count_audits(setup, queue_pk, operation_code=_AUDIT_SET_STATE)
            policy_count_before = _count_policies(setup, queue_pk)
            cfg = repo.get_queue_configuration(setup, name=name)
            assert cfg is not None
            active_policy_pk = cfg.active_policy.policy_version_id
            policy_bytes = _policy_row_snapshot(setup, active_policy_pk)
        finally:
            setup.close()

        left, right = _run_racing_pair(
            session_factory,
            lambda s, n=name: repo.set_queue_state(
                s,
                queue_name=n,
                mutation=SetQueueStateMutation(
                    expected_config_version=ConfigVersion(value=1),
                    state=QueueState.PAUSED,
                    metadata=_meta(actor_id="state-left"),
                ),
            ),
            lambda s, n=name: repo.set_queue_state(
                s,
                queue_name=n,
                mutation=SetQueueStateMutation(
                    expected_config_version=ConfigVersion(value=1),
                    state=QueueState.DRAINING,
                    metadata=_meta(actor_id="state-right"),
                ),
            ),
        )
        winner = _assert_one_winner_one_conflict(left, right)
        assert winner.value.state in {QueueState.PAUSED, QueueState.DRAINING}
        assert winner.value.config_version.value == 2

        verify = session_factory()
        try:
            cfg = repo.get_queue_configuration(verify, name=name)
            assert cfg is not None
            assert cfg.config_version.value == 2
            assert cfg.state is winner.value.state
            assert cfg.active_policy.policy_version_id == active_policy_pk
            assert _policy_row_snapshot(verify, active_policy_pk) == policy_bytes
            assert _count_policies(verify, queue_pk) == policy_count_before
            assert (
                _count_audits(verify, queue_pk, operation_code=_AUDIT_SET_STATE)
                == audits_before + 1
            )
            audit = verify.execute(
                select(AdminAuditLog).where(
                    AdminAuditLog.queue_id == queue_pk,
                    AdminAuditLog.operation_code == _AUDIT_SET_STATE,
                    AdminAuditLog.new_config_version == 2,
                )
            ).scalar_one()
            assert audit.previous_config_version == 1
            assert audit.details["previous_state"] == "active"
            assert audit.details["new_state"] == winner.value.state.value
            assert audit.actor_id in {"state-left", "state-right"}
        finally:
            verify.close()


def test_policy_policy_race_one_winner_ten_reps(
    session_factory: sessionmaker[Session],
) -> None:
    repo = QueueControlRepository()
    for rep in range(_RACE_REPETITIONS):
        name = _unique(f"race.policy-policy.{rep}")
        setup = session_factory()
        try:
            _seed_queue(setup, repo, name)
            target = _append_inactive_policy(setup, repo, name, max_attempts=11)
            queue_pk = _queue_pk(setup, name)
            policy_count_before = _count_policies(setup, queue_pk)
            assert policy_count_before == 2
            cfg = repo.get_queue_configuration(setup, name=name)
            assert cfg is not None
            original_active_pk = cfg.active_policy.policy_version_id
            historical = _policy_row_snapshot(setup, original_active_pk)
            audits_before = _count_audits(
                setup, queue_pk, operation_code=_AUDIT_ACTIVATE_POLICY
            )
        finally:
            setup.close()

        left, right = _run_racing_pair(
            session_factory,
            lambda s, n=name, p=target: repo.activate_policy_version(
                s,
                queue_name=n,
                mutation=ActivatePolicyMutation(
                    expected_config_version=ConfigVersion(value=1),
                    policy_version=p,
                    metadata=_meta(actor_id="policy-left"),
                ),
            ),
            lambda s, n=name, p=target: repo.activate_policy_version(
                s,
                queue_name=n,
                mutation=ActivatePolicyMutation(
                    expected_config_version=ConfigVersion(value=1),
                    policy_version=p,
                    metadata=_meta(actor_id="policy-right"),
                ),
            ),
        )
        winner = _assert_one_winner_one_conflict(left, right)
        assert winner.value.config_version.value == 2
        assert winner.value.active_policy.version.value == 2
        assert winner.value.active_policy.policy.max_attempts == 11

        verify = session_factory()
        try:
            cfg = repo.get_queue_configuration(verify, name=name)
            assert cfg is not None
            assert cfg.config_version.value == 2
            assert cfg.active_policy.version.value == 2
            assert cfg.state is QueueState.ACTIVE
            assert _count_policies(verify, queue_pk) == 2
            assert _policy_row_snapshot(verify, original_active_pk) == historical
            assert (
                _count_audits(verify, queue_pk, operation_code=_AUDIT_ACTIVATE_POLICY)
                == audits_before + 1
            )
            audit = verify.execute(
                select(AdminAuditLog).where(
                    AdminAuditLog.queue_id == queue_pk,
                    AdminAuditLog.operation_code == _AUDIT_ACTIVATE_POLICY,
                    AdminAuditLog.new_config_version == 2,
                )
            ).scalar_one()
            assert audit.previous_config_version == 1
            assert audit.details["previous_policy_version"] == 1
            assert audit.details["new_policy_version"] == 2
            assert audit.actor_id in {"policy-left", "policy-right"}
        finally:
            verify.close()


def test_state_policy_race_one_coherent_winner_ten_reps(
    session_factory: sessionmaker[Session],
) -> None:
    repo = QueueControlRepository()
    for rep in range(_RACE_REPETITIONS):
        name = _unique(f"race.state-policy.{rep}")
        setup = session_factory()
        try:
            _seed_queue(setup, repo, name)
            target = _append_inactive_policy(setup, repo, name, max_attempts=13)
            queue_pk = _queue_pk(setup, name)
            cfg = repo.get_queue_configuration(setup, name=name)
            assert cfg is not None
            original_active_pk = cfg.active_policy.policy_version_id
            historical = _policy_row_snapshot(setup, original_active_pk)
            audits_before = _count_audits(setup, queue_pk)
            create_policy_audits = _count_audits(
                setup, queue_pk, operation_code=_AUDIT_CREATE_POLICY
            )
            create_queue_audits = _count_audits(
                setup, queue_pk, operation_code=_AUDIT_CREATE_QUEUE
            )
            assert create_queue_audits == 1
            assert create_policy_audits == 1
        finally:
            setup.close()

        left, right = _run_racing_pair(
            session_factory,
            lambda s, n=name: repo.set_queue_state(
                s,
                queue_name=n,
                mutation=SetQueueStateMutation(
                    expected_config_version=ConfigVersion(value=1),
                    state=QueueState.PAUSED,
                    metadata=_meta(actor_id="cross-state"),
                ),
            ),
            lambda s, n=name, p=target: repo.activate_policy_version(
                s,
                queue_name=n,
                mutation=ActivatePolicyMutation(
                    expected_config_version=ConfigVersion(value=1),
                    policy_version=p,
                    metadata=_meta(actor_id="cross-policy"),
                ),
            ),
        )
        winner = _assert_one_winner_one_conflict(left, right)
        assert winner.value.config_version.value == 2

        verify = session_factory()
        try:
            cfg = repo.get_queue_configuration(verify, name=name)
            assert cfg is not None
            assert cfg.config_version.value == 2
            assert _count_policies(verify, queue_pk) == 2
            assert _policy_row_snapshot(verify, original_active_pk) == historical
            # Exactly one mutation audit beyond create_queue + create_policy.
            assert _count_audits(verify, queue_pk) == audits_before + 1

            set_state_audits = _count_audits(
                verify, queue_pk, operation_code=_AUDIT_SET_STATE
            )
            activate_audits = _count_audits(
                verify, queue_pk, operation_code=_AUDIT_ACTIVATE_POLICY
            )
            if winner.value.state is QueueState.PAUSED:
                assert set_state_audits == 1
                assert activate_audits == 0
                assert cfg.active_policy.version.value == 1
                assert cfg.active_policy.policy_version_id == original_active_pk
                audit = verify.execute(
                    select(AdminAuditLog).where(
                        AdminAuditLog.queue_id == queue_pk,
                        AdminAuditLog.operation_code == _AUDIT_SET_STATE,
                    )
                ).scalar_one()
                assert audit.actor_id == "cross-state"
                assert audit.details["new_state"] == "paused"
            else:
                assert set_state_audits == 0
                assert activate_audits == 1
                assert cfg.state is QueueState.ACTIVE
                assert cfg.active_policy.version.value == 2
                assert cfg.active_policy.policy.max_attempts == 13
                audit = verify.execute(
                    select(AdminAuditLog).where(
                        AdminAuditLog.queue_id == queue_pk,
                        AdminAuditLog.operation_code == _AUDIT_ACTIVATE_POLICY,
                    )
                ).scalar_one()
                assert audit.actor_id == "cross-policy"
                assert audit.details["new_policy_version"] == 2
        finally:
            verify.close()


def test_distinct_queues_mutate_without_global_lock(
    session_factory: sessionmaker[Session],
) -> None:
    """While queue A is row-locked, queue B must still commit (row-scoped locking)."""
    repo = QueueControlRepository()
    for rep in range(_RACE_REPETITIONS):
        name_a = _unique(f"race.indep.a.{rep}")
        name_b = _unique(f"race.indep.b.{rep}")
        setup = session_factory()
        try:
            _seed_queue(setup, repo, name_a)
            _seed_queue(setup, repo, name_b)
        finally:
            setup.close()

        a_locked = threading.Event()
        b_committed = threading.Event()
        release_a = threading.Event()
        errors: list[BaseException] = []

        def _hold_a() -> None:
            session = session_factory()
            try:
                locked = session.execute(
                    select(Queue).where(Queue.name == name_a).with_for_update()
                ).scalar_one()
                assert locked.name == name_a
                a_locked.set()
                assert release_a.wait(timeout=_JOIN_TIMEOUT_S), "A never released"
                session.rollback()
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)
            finally:
                session.close()

        def _mutate_b() -> None:
            session = session_factory()
            try:
                assert a_locked.wait(timeout=_JOIN_TIMEOUT_S), "A never locked"
                result = repo.set_queue_state(
                    session,
                    queue_name=name_b,
                    mutation=SetQueueStateMutation(
                        expected_config_version=ConfigVersion(value=1),
                        state=QueueState.PAUSED,
                        metadata=_meta(actor_id="indep-b"),
                    ),
                )
                session.commit()
                assert result.state is QueueState.PAUSED
                assert result.config_version.value == 2
                b_committed.set()
            except BaseException as exc:  # noqa: BLE001
                session.rollback()
                errors.append(exc)
            finally:
                session.close()

        thread_a = threading.Thread(target=_hold_a, daemon=True)
        thread_b = threading.Thread(target=_mutate_b, daemon=True)
        thread_a.start()
        thread_b.start()
        thread_b.join(timeout=_JOIN_TIMEOUT_S)
        assert not thread_b.is_alive(), (
            "queue B mutation blocked while unrelated queue A held a row lock "
            "(global/process-local serialization suspected)"
        )
        assert b_committed.is_set()
        release_a.set()
        thread_a.join(timeout=_JOIN_TIMEOUT_S)
        assert not thread_a.is_alive()
        assert errors == [], f"unexpected worker errors: {errors!r}"

        verify = session_factory()
        try:
            cfg_a = repo.get_queue_configuration(verify, name=name_a)
            cfg_b = repo.get_queue_configuration(verify, name=name_b)
            assert cfg_a is not None and cfg_b is not None
            assert cfg_a.state is QueueState.ACTIVE
            assert cfg_a.config_version.value == 1
            assert cfg_b.state is QueueState.PAUSED
            assert cfg_b.config_version.value == 2
            pk_b = _queue_pk(verify, name_b)
            assert _count_audits(verify, pk_b, operation_code=_AUDIT_SET_STATE) == 1
        finally:
            verify.close()


def test_distinct_queues_concurrent_winners(
    session_factory: sessionmaker[Session],
) -> None:
    """Two different queues can both win concurrent mutations."""
    repo = QueueControlRepository()
    for rep in range(_RACE_REPETITIONS):
        name_a = _unique(f"race.both.a.{rep}")
        name_b = _unique(f"race.both.b.{rep}")
        setup = session_factory()
        try:
            _seed_queue(setup, repo, name_a)
            _seed_queue(setup, repo, name_b)
        finally:
            setup.close()

        left, right = _run_racing_pair(
            session_factory,
            lambda s, n=name_a: repo.set_queue_state(
                s,
                queue_name=n,
                mutation=SetQueueStateMutation(
                    expected_config_version=ConfigVersion(value=1),
                    state=QueueState.PAUSED,
                    metadata=_meta(actor_id="both-a"),
                ),
            ),
            lambda s, n=name_b: repo.set_queue_state(
                s,
                queue_name=n,
                mutation=SetQueueStateMutation(
                    expected_config_version=ConfigVersion(value=1),
                    state=QueueState.PAUSED,
                    metadata=_meta(actor_id="both-b"),
                ),
            ),
        )
        assert left.ok and right.ok, f"expected dual winners, got {left=!r} {right=!r}"
        assert left.value.config_version.value == 2
        assert right.value.config_version.value == 2

        verify = session_factory()
        try:
            cfg_a = repo.get_queue_configuration(verify, name=name_a)
            cfg_b = repo.get_queue_configuration(verify, name=name_b)
            assert cfg_a is not None and cfg_b is not None
            assert cfg_a.state is QueueState.PAUSED
            assert cfg_b.state is QueueState.PAUSED
        finally:
            verify.close()
