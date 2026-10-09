"""Real-PostgreSQL proof of immutable policy create and optimistic activation."""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from datetime import datetime

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
    DomainValidationError,
    PolicyVersion,
    RetryPolicyDraft,
)
from queue_service.infrastructure.postgres.queue_control_repository import (
    QueueControlRepository,
)
from queue_service.storage.models import AdminAuditLog, Queue, QueuePolicyVersion

_AUDIT_CREATE_POLICY = 2
_AUDIT_ACTIVATE_POLICY = 3


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


def _seed_queue(
    session: Session,
    repo: QueueControlRepository,
    name: str,
    *,
    enabled: bool = True,
    max_attempts: int = 3,
    retry_delay_seconds: int = 5,
) -> None:
    repo.create_named_queue(
        session,
        CreateQueueMutation(
            name=name,
            initial_policy=RetryPolicyDraft(
                enabled=enabled,
                max_attempts=max_attempts,
                backoff_strategy=BackoffStrategy.FIXED,
                retry_delay_seconds=retry_delay_seconds,
            ),
            metadata=_meta(),
        ),
    )
    session.commit()


def _policy_snapshot(session: Session, policy_pk: int) -> tuple[object, ...]:
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


def test_create_then_activate_inserts_selects_and_increments_once(
    sa_session: Session,
) -> None:
    repo = QueueControlRepository()
    _seed_queue(sa_session, repo, "policy.flow")

    before = repo.get_queue_configuration(sa_session, name="policy.flow")
    assert before is not None
    assert before.config_version.value == 1
    assert before.active_policy.version.value == 1
    original_policy_pk = before.active_policy.policy_version_id
    original_bytes = _policy_snapshot(sa_session, original_policy_pk)

    created = repo.create_policy_version(
        sa_session,
        queue_name="policy.flow",
        mutation=CreatePolicyMutation(
            policy=RetryPolicyDraft(
                enabled=False,
                max_attempts=1,
                backoff_strategy=BackoffStrategy.FIXED,
                retry_delay_seconds=0,
            ),
            metadata=_meta(actor_id="policy-creator"),
        ),
    )
    sa_session.commit()

    assert created.config_version.value == 1
    assert created.active_policy.version.value == 1
    assert created.active_policy.policy_version_id == original_policy_pk

    queue_row = sa_session.execute(
        select(Queue).where(Queue.name == "policy.flow")
    ).scalar_one()
    policy_rows = list(
        sa_session.scalars(
            select(QueuePolicyVersion)
            .where(QueuePolicyVersion.queue_id == queue_row.id)
            .order_by(QueuePolicyVersion.version)
        )
    )
    assert len(policy_rows) == 2
    assert policy_rows[1].version == 2
    assert policy_rows[1].enabled is False
    assert policy_rows[1].max_attempts == 1
    assert policy_rows[1].retry_delay_seconds == 0

    activated = repo.activate_policy_version(
        sa_session,
        queue_name="policy.flow",
        mutation=ActivatePolicyMutation(
            expected_config_version=ConfigVersion(value=1),
            policy_version=PolicyVersion(value=2),
            metadata=_meta(actor_id="policy-activator"),
        ),
    )
    sa_session.commit()

    assert activated.config_version.value == 2
    assert activated.active_policy.version.value == 2
    assert activated.active_policy.policy.enabled is False
    assert activated.active_policy.policy.max_attempts == 1
    assert activated.active_policy.policy.retry_delay_seconds == 0
    assert activated.active_policy.policy_version_id == policy_rows[1].id

    assert _policy_snapshot(sa_session, original_policy_pk) == original_bytes


def test_previous_policy_remains_byte_for_byte_after_activation(
    sa_session: Session,
) -> None:
    repo = QueueControlRepository()
    _seed_queue(
        sa_session,
        repo,
        "policy.immutable",
        enabled=True,
        max_attempts=5,
        retry_delay_seconds=11,
    )
    before = repo.get_queue_configuration(sa_session, name="policy.immutable")
    assert before is not None
    original_pk = before.active_policy.policy_version_id
    original = _policy_snapshot(sa_session, original_pk)

    repo.create_policy_version(
        sa_session,
        queue_name="policy.immutable",
        mutation=CreatePolicyMutation(
            policy=RetryPolicyDraft(
                enabled=True,
                max_attempts=2,
                backoff_strategy=BackoffStrategy.FIXED,
                retry_delay_seconds=3,
            ),
            metadata=_meta(),
        ),
    )
    sa_session.commit()
    repo.activate_policy_version(
        sa_session,
        queue_name="policy.immutable",
        mutation=ActivatePolicyMutation(
            expected_config_version=ConfigVersion(value=1),
            policy_version=PolicyVersion(value=2),
            metadata=_meta(),
        ),
    )
    sa_session.commit()

    assert _policy_snapshot(sa_session, original_pk) == original
    reloaded = sa_session.get(QueuePolicyVersion, original_pk)
    assert reloaded is not None
    assert reloaded.enabled is True
    assert reloaded.max_attempts == 5
    assert reloaded.retry_delay_seconds == 11


def test_stale_expected_version_is_conflict_without_side_effects(
    sa_session: Session,
) -> None:
    repo = QueueControlRepository()
    _seed_queue(sa_session, repo, "policy.stale")
    repo.create_policy_version(
        sa_session,
        queue_name="policy.stale",
        mutation=CreatePolicyMutation(
            policy=RetryPolicyDraft(
                enabled=True,
                max_attempts=4,
                backoff_strategy=BackoffStrategy.FIXED,
                retry_delay_seconds=7,
            ),
            metadata=_meta(),
        ),
    )
    sa_session.commit()

    queue_before = sa_session.execute(
        select(Queue).where(Queue.name == "policy.stale")
    ).scalar_one()
    policies_before = sa_session.scalar(
        select(func.count()).select_from(QueuePolicyVersion)
    )
    audits_before = sa_session.scalar(select(func.count()).select_from(AdminAuditLog))
    active_before = queue_before.active_policy_version_id
    config_before = queue_before.config_version

    with pytest.raises(DomainValidationError) as exc_info:
        repo.activate_policy_version(
            sa_session,
            queue_name="policy.stale",
            mutation=ActivatePolicyMutation(
                expected_config_version=ConfigVersion(value=99),
                policy_version=PolicyVersion(value=2),
                metadata=_meta(actor_id="stale-actor"),
            ),
        )
    sa_session.rollback()

    assert exc_info.value.code == "config_version_conflict"
    queue_after = sa_session.execute(
        select(Queue).where(Queue.name == "policy.stale")
    ).scalar_one()
    assert queue_after.config_version == config_before
    assert queue_after.active_policy_version_id == active_before
    assert (
        sa_session.scalar(select(func.count()).select_from(QueuePolicyVersion))
        == policies_before
    )
    assert (
        sa_session.scalar(select(func.count()).select_from(AdminAuditLog))
        == audits_before
    )


def test_successful_activation_writes_one_audit_with_store_time(
    sa_session: Session,
) -> None:
    repo = QueueControlRepository()
    _seed_queue(sa_session, repo, "policy.audit")
    repo.create_policy_version(
        sa_session,
        queue_name="policy.audit",
        mutation=CreatePolicyMutation(
            policy=RetryPolicyDraft(
                enabled=True,
                max_attempts=9,
                backoff_strategy=BackoffStrategy.FIXED,
                retry_delay_seconds=2,
            ),
            metadata=_meta(),
        ),
    )
    sa_session.commit()

    before = sa_session.scalar(select(func.statement_timestamp()))
    assert before is not None
    mutation = ActivatePolicyMutation(
        expected_config_version=ConfigVersion(value=1),
        policy_version=PolicyVersion(value=2),
        metadata=_meta(actor_id="activator-42"),
    )
    repo.activate_policy_version(
        sa_session,
        queue_name="policy.audit",
        mutation=mutation,
    )
    sa_session.commit()
    after = sa_session.scalar(select(func.statement_timestamp()))
    assert after is not None

    audits = list(
        sa_session.scalars(
            select(AdminAuditLog).where(
                AdminAuditLog.request_id == uuid.UUID(mutation.metadata.request_id)
            )
        )
    )
    assert len(audits) == 1
    audit = audits[0]
    assert audit.actor_id == "activator-42"
    assert audit.operation_code == _AUDIT_ACTIVATE_POLICY
    assert audit.previous_config_version == 1
    assert audit.new_config_version == 2
    assert audit.details.get("previous_policy_version") == 1
    assert audit.details.get("new_policy_version") == 2
    assert isinstance(audit.audit_at, datetime)
    assert audit.audit_at.tzinfo is not None
    assert before <= audit.audit_at <= after


def test_create_policy_writes_create_audit_without_selecting(
    sa_session: Session,
) -> None:
    repo = QueueControlRepository()
    _seed_queue(sa_session, repo, "policy.create-only")
    mutation = CreatePolicyMutation(
        policy=RetryPolicyDraft(
            enabled=True,
            max_attempts=6,
            backoff_strategy=BackoffStrategy.FIXED,
            retry_delay_seconds=4,
        ),
        metadata=_meta(actor_id="creator-7"),
    )
    created = repo.create_policy_version(
        sa_session,
        queue_name="policy.create-only",
        mutation=mutation,
    )
    sa_session.commit()

    assert created.active_policy.version.value == 1
    assert created.config_version.value == 1

    audits = list(
        sa_session.scalars(
            select(AdminAuditLog).where(
                AdminAuditLog.request_id == uuid.UUID(mutation.metadata.request_id)
            )
        )
    )
    assert len(audits) == 1
    assert audits[0].operation_code == _AUDIT_CREATE_POLICY
    assert audits[0].actor_id == "creator-7"
    assert audits[0].details.get("policy_version") == 2


def test_injected_failure_rolls_back_activation(sa_session: Session) -> None:
    repo = QueueControlRepository()
    _seed_queue(sa_session, repo, "policy.rollback")
    repo.create_policy_version(
        sa_session,
        queue_name="policy.rollback",
        mutation=CreatePolicyMutation(
            policy=RetryPolicyDraft(
                enabled=True,
                max_attempts=2,
                backoff_strategy=BackoffStrategy.FIXED,
                retry_delay_seconds=1,
            ),
            metadata=_meta(),
        ),
    )
    sa_session.commit()

    queue_before = sa_session.execute(
        select(Queue).where(Queue.name == "policy.rollback")
    ).scalar_one()
    audits_before = sa_session.scalar(select(func.count()).select_from(AdminAuditLog))
    active_before = queue_before.active_policy_version_id
    config_before = queue_before.config_version

    with pytest.raises(RuntimeError, match="forced failure"):
        repo.activate_policy_version(
            sa_session,
            queue_name="policy.rollback",
            mutation=ActivatePolicyMutation(
                expected_config_version=ConfigVersion(value=1),
                policy_version=PolicyVersion(value=2),
                metadata=_meta(),
            ),
        )
        raise RuntimeError("forced failure")
    sa_session.rollback()

    queue_after = sa_session.execute(
        select(Queue).where(Queue.name == "policy.rollback")
    ).scalar_one()
    assert queue_after.config_version == config_before
    assert queue_after.active_policy_version_id == active_before
    assert (
        sa_session.scalar(select(func.count()).select_from(AdminAuditLog))
        == audits_before
    )
