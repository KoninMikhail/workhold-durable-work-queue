"""Real-PostgreSQL proof of atomic named-queue creation (Phase 03.3-02)."""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from datetime import datetime

import pytest
from sqlalchemy import create_engine, event, func, select
from sqlalchemy.orm import Session, sessionmaker

from queue_service.domain.queue_control import (
    AdminRequestMetadata,
    BackoffStrategy,
    CreateQueueMutation,
    DomainValidationError,
    QueueState,
    RetryPolicyDraft,
)
from queue_service.infrastructure.postgres.queue_control_repository import (
    QueueControlRepository,
)
from queue_service.storage.models import AdminAuditLog, Queue, QueuePolicyVersion


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
        # SET must commit: a later rollback would otherwise undo session GUCs.
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


def _mutation(
    name: str,
    *,
    actor_id: str = "admin-actor-1",
    enabled: bool = True,
    max_attempts: int = 3,
    retry_delay_seconds: int = 5,
) -> CreateQueueMutation:
    return CreateQueueMutation(
        name=name,
        initial_policy=RetryPolicyDraft(
            enabled=enabled,
            max_attempts=max_attempts,
            backoff_strategy=BackoffStrategy.FIXED,
            retry_delay_seconds=retry_delay_seconds,
        ),
        metadata=AdminRequestMetadata(
            actor_id=actor_id,
            request_id=str(uuid.uuid4()),
            idempotency_key=f"idem-{uuid.uuid4().hex}",
        ),
    )


def _counts(session: Session) -> tuple[int, int, int]:
    queues = session.scalar(select(func.count()).select_from(Queue)) or 0
    policies = session.scalar(select(func.count()).select_from(QueuePolicyVersion)) or 0
    audits = session.scalar(select(func.count()).select_from(AdminAuditLog)) or 0
    return int(queues), int(policies), int(audits)


def test_two_distinct_names_create_independent_queues(sa_session: Session) -> None:
    repo = QueueControlRepository()
    first = repo.create_named_queue(sa_session, _mutation("orders.intake"))
    second = repo.create_named_queue(sa_session, _mutation("orders.retry"))
    sa_session.commit()

    assert first.name == "orders.intake"
    assert second.name == "orders.retry"
    assert first.queue_id != second.queue_id
    assert first.state is QueueState.ACTIVE
    assert second.state is QueueState.ACTIVE
    assert first.config_version.value == 1
    assert second.config_version.value == 1
    assert first.active_policy.version.value == 1
    assert second.active_policy.version.value == 1
    assert first.active_policy.policy_version_id != second.active_policy.policy_version_id

    by_name = repo.get_queue_configuration(sa_session, name="orders.intake")
    by_id = repo.get_queue_configuration(sa_session, queue_id=second.queue_id)
    assert by_name is not None and by_name.queue_id == first.queue_id
    assert by_id is not None and by_id.name == "orders.retry"
    assert by_name.active_policy.policy_version_id == first.active_policy.policy_version_id
    assert by_id.active_policy.policy_version_id == second.active_policy.policy_version_id


def test_created_policy_is_immutable_and_selected(sa_session: Session) -> None:
    repo = QueueControlRepository()
    created = repo.create_named_queue(
        sa_session,
        _mutation("policy.lock", enabled=False, max_attempts=1, retry_delay_seconds=0),
    )
    sa_session.commit()

    policy_row = sa_session.get(QueuePolicyVersion, created.active_policy.policy_version_id)
    assert policy_row is not None
    assert policy_row.version == 1
    assert policy_row.enabled is False
    assert policy_row.max_attempts == 1
    assert policy_row.backoff_strategy_code == 1
    assert policy_row.retry_delay_seconds == 0

    queue_row = sa_session.execute(
        select(Queue).where(Queue.queue_id == created.queue_id)
    ).scalar_one()
    assert queue_row.active_policy_version_id == policy_row.id
    assert queue_row.config_version == 1
    assert queue_row.state_code == 1


def test_create_commits_one_audit_row_with_store_time_and_actor(
    sa_session: Session,
) -> None:
    repo = QueueControlRepository()
    before = sa_session.scalar(select(func.statement_timestamp()))
    assert before is not None

    mutation = _mutation("audit.queue", actor_id="principal-audit-42")
    created = repo.create_named_queue(sa_session, mutation)
    sa_session.commit()

    after = sa_session.scalar(select(func.statement_timestamp()))
    assert after is not None

    audits = list(
        sa_session.scalars(
            select(AdminAuditLog).where(AdminAuditLog.request_id == uuid.UUID(mutation.metadata.request_id))
        )
    )
    assert len(audits) == 1
    audit = audits[0]
    assert audit.actor_id == "principal-audit-42"
    assert audit.operation_code == 1  # create_queue
    assert audit.previous_config_version is None
    assert audit.new_config_version == 1
    assert audit.request_id == uuid.UUID(mutation.metadata.request_id)
    assert audit.queue_id is not None

    queue_pk = sa_session.execute(
        select(Queue.id).where(Queue.queue_id == created.queue_id)
    ).scalar_one()
    assert audit.queue_id == queue_pk

    assert isinstance(audit.audit_at, datetime)
    assert audit.audit_at.tzinfo is not None
    # Queue-store statement_timestamp() must land inside the surrounding DB clock window.
    assert before <= audit.audit_at <= after


def test_duplicate_name_is_conflict_without_partial_rows(sa_session: Session) -> None:
    repo = QueueControlRepository()
    repo.create_named_queue(sa_session, _mutation("dup.name"))
    sa_session.commit()
    baseline = _counts(sa_session)

    with pytest.raises(DomainValidationError) as exc_info:
        repo.create_named_queue(sa_session, _mutation("dup.name", actor_id="other-actor"))
    sa_session.rollback()

    assert exc_info.value.code == "idempotency_conflict"
    assert _counts(sa_session) == baseline


def test_injected_failure_rolls_back_queue_policy_and_audit(sa_session: Session) -> None:
    repo = QueueControlRepository()
    baseline = _counts(sa_session)

    with pytest.raises(RuntimeError, match="forced failure"):
        repo.create_named_queue(sa_session, _mutation("rollback.queue"))
        raise RuntimeError("forced failure")
    sa_session.rollback()

    assert _counts(sa_session) == baseline
    assert (
        sa_session.scalar(select(func.count()).select_from(Queue).where(Queue.name == "rollback.queue"))
        or 0
    ) == 0
