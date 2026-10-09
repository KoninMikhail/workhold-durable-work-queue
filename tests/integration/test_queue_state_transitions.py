"""Real-PostgreSQL proof of pause/resume/drain primitives and operation gates."""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator

import pytest
from sqlalchemy import create_engine, event, func, select
from sqlalchemy.orm import Session, sessionmaker

from workhold.application.queue_state_gate import evaluate_queue_state_gate
from workhold.domain.queue_control import (
    AdminRequestMetadata,
    BackoffStrategy,
    ConfigVersion,
    CreateQueueMutation,
    DomainValidationError,
    OperationGateOutcome,
    QueueOperation,
    QueueState,
    RetryPolicyDraft,
    SetQueueStateMutation,
)
from workhold.infrastructure.postgres.queue_control_repository import (
    QueueControlRepository,
)
from workhold.storage.models import AdminAuditLog, Queue, QueuePolicyVersion

_AUDIT_SET_STATE = 4


@pytest.fixture
def sa_session(migrated_schema) -> Iterator[Session]:
    """SQLAlchemy session bound to the isolated Alembic-migrated schema."""
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

    factory = sessionmaker(bind=engine, expire_on_commit=False)
    session = factory()
    try:
        yield session
    finally:
        session.close()
        engine.dispose()


def _meta(*, actor_id: str = "admin-actor-1") -> AdminRequestMetadata:
    return AdminRequestMetadata(
        actor_id=actor_id,
        request_id=str(uuid.uuid4()),
        idempotency_key=f"idem-{uuid.uuid4().hex}",
    )


def _seed_queue(session: Session, repo: QueueControlRepository, name: str) -> None:
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
            metadata=_meta(),
        ),
    )
    session.commit()


def test_active_paused_round_trip_increments_and_audits(sa_session: Session) -> None:
    repo = QueueControlRepository()
    _seed_queue(sa_session, repo, "state.pause-resume")

    paused = repo.set_queue_state(
        sa_session,
        queue_name="state.pause-resume",
        mutation=SetQueueStateMutation(
            expected_config_version=ConfigVersion(value=1),
            state=QueueState.PAUSED,
            metadata=_meta(actor_id="pause-actor"),
        ),
    )
    sa_session.commit()
    assert paused.state is QueueState.PAUSED
    assert paused.config_version.value == 2

    resumed = repo.set_queue_state(
        sa_session,
        queue_name="state.pause-resume",
        mutation=SetQueueStateMutation(
            expected_config_version=ConfigVersion(value=2),
            state=QueueState.ACTIVE,
            metadata=_meta(actor_id="resume-actor"),
        ),
    )
    sa_session.commit()
    assert resumed.state is QueueState.ACTIVE
    assert resumed.config_version.value == 3

    audits = list(
        sa_session.scalars(
            select(AdminAuditLog)
            .where(AdminAuditLog.operation_code == _AUDIT_SET_STATE)
            .order_by(AdminAuditLog.previous_config_version)
        )
    )
    assert len(audits) == 2
    assert audits[0].actor_id == "pause-actor"
    assert audits[0].previous_config_version == 1
    assert audits[0].new_config_version == 2
    assert audits[0].details["previous_state"] == "active"
    assert audits[0].details["new_state"] == "paused"
    assert audits[1].actor_id == "resume-actor"
    assert audits[1].details["previous_state"] == "paused"
    assert audits[1].details["new_state"] == "active"
    assert audits[0].audit_at is not None


def test_internal_draining_transitions_remain_valid(sa_session: Session) -> None:
    repo = QueueControlRepository()
    _seed_queue(sa_session, repo, "state.drain-primitive")

    draining = repo.set_queue_state(
        sa_session,
        queue_name="state.drain-primitive",
        mutation=SetQueueStateMutation(
            expected_config_version=ConfigVersion(value=1),
            state=QueueState.DRAINING,
            metadata=_meta(actor_id="drain-internal"),
        ),
    )
    sa_session.commit()
    assert draining.state is QueueState.DRAINING
    assert draining.config_version.value == 2

    paused = repo.set_queue_state(
        sa_session,
        queue_name="state.drain-primitive",
        mutation=SetQueueStateMutation(
            expected_config_version=ConfigVersion(value=2),
            state=QueueState.PAUSED,
            metadata=_meta(actor_id="leave-drain"),
        ),
    )
    sa_session.commit()
    assert paused.state is QueueState.PAUSED
    assert paused.config_version.value == 3


def test_stale_expected_version_is_conflict_without_side_effects(
    sa_session: Session,
) -> None:
    repo = QueueControlRepository()
    _seed_queue(sa_session, repo, "state.stale")
    queue_before = sa_session.execute(
        select(Queue).where(Queue.name == "state.stale")
    ).scalar_one()
    state_before = queue_before.state_code
    config_before = queue_before.config_version
    policy_before = queue_before.active_policy_version_id
    audits_before = sa_session.scalar(select(func.count()).select_from(AdminAuditLog))

    with pytest.raises(DomainValidationError) as exc_info:
        repo.set_queue_state(
            sa_session,
            queue_name="state.stale",
            mutation=SetQueueStateMutation(
                expected_config_version=ConfigVersion(value=99),
                state=QueueState.PAUSED,
                metadata=_meta(),
            ),
        )
    assert exc_info.value.code == "config_version_conflict"
    sa_session.rollback()

    queue_after = sa_session.execute(
        select(Queue).where(Queue.name == "state.stale")
    ).scalar_one()
    assert queue_after.state_code == state_before
    assert queue_after.config_version == config_before
    assert queue_after.active_policy_version_id == policy_before
    assert (
        sa_session.scalar(select(func.count()).select_from(AdminAuditLog))
        == audits_before
    )


def test_state_change_does_not_mutate_active_policy(sa_session: Session) -> None:
    repo = QueueControlRepository()
    _seed_queue(sa_session, repo, "state.side-effects")
    before = repo.get_queue_configuration(sa_session, name="state.side-effects")
    assert before is not None
    policy_pk = before.active_policy.policy_version_id
    policy_row = sa_session.get(QueuePolicyVersion, policy_pk)
    assert policy_row is not None
    snapshot = (
        policy_row.version,
        policy_row.enabled,
        policy_row.max_attempts,
        policy_row.retry_delay_seconds,
    )

    after = repo.set_queue_state(
        sa_session,
        queue_name="state.side-effects",
        mutation=SetQueueStateMutation(
            expected_config_version=ConfigVersion(value=1),
            state=QueueState.PAUSED,
            metadata=_meta(),
        ),
    )
    sa_session.commit()
    assert after.active_policy.policy_version_id == policy_pk
    policy_row = sa_session.get(QueuePolicyVersion, policy_pk)
    assert policy_row is not None
    assert (
        policy_row.version,
        policy_row.enabled,
        policy_row.max_attempts,
        policy_row.retry_delay_seconds,
    ) == snapshot


def test_operation_gate_matrix_matches_runtime_semantics() -> None:
    expected = {
        (QueueState.ACTIVE, QueueOperation.EXTERNAL_ENQUEUE): OperationGateOutcome.ALLOWED,
        (QueueState.PAUSED, QueueOperation.EXTERNAL_ENQUEUE): OperationGateOutcome.ALLOWED,
        (
            QueueState.DRAINING,
            QueueOperation.EXTERNAL_ENQUEUE,
        ): OperationGateOutcome.REJECTED,
        (QueueState.ACTIVE, QueueOperation.INTERNAL_SPAWN): OperationGateOutcome.ALLOWED,
        (QueueState.PAUSED, QueueOperation.INTERNAL_SPAWN): OperationGateOutcome.ALLOWED,
        (QueueState.DRAINING, QueueOperation.INTERNAL_SPAWN): OperationGateOutcome.ALLOWED,
        (QueueState.ACTIVE, QueueOperation.CLAIM): OperationGateOutcome.ALLOWED,
        (QueueState.PAUSED, QueueOperation.CLAIM): OperationGateOutcome.PAUSED_EMPTY,
        (QueueState.DRAINING, QueueOperation.CLAIM): OperationGateOutcome.ALLOWED,
        (QueueState.ACTIVE, QueueOperation.LEASE_MUTATION): OperationGateOutcome.ALLOWED,
        (QueueState.PAUSED, QueueOperation.LEASE_MUTATION): OperationGateOutcome.ALLOWED,
        (QueueState.DRAINING, QueueOperation.LEASE_MUTATION): OperationGateOutcome.ALLOWED,
        (QueueState.ACTIVE, QueueOperation.CANCEL): OperationGateOutcome.ALLOWED,
        (QueueState.PAUSED, QueueOperation.CANCEL): OperationGateOutcome.ALLOWED,
        (QueueState.DRAINING, QueueOperation.CANCEL): OperationGateOutcome.ALLOWED,
        (QueueState.ACTIVE, QueueOperation.DELIVERY_RELAY): OperationGateOutcome.ALLOWED,
        (QueueState.PAUSED, QueueOperation.DELIVERY_RELAY): OperationGateOutcome.ALLOWED,
        (QueueState.DRAINING, QueueOperation.DELIVERY_RELAY): OperationGateOutcome.ALLOWED,
    }
    for (state, operation), outcome in expected.items():
        assert evaluate_queue_state_gate(state, operation) is outcome
