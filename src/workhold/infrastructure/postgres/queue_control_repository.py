"""PostgreSQL repository for named-queue control-plane persistence."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Final
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from workhold.domain.queue_control import (
    ActivatePolicyMutation,
    SetQueueStateMutation,
    BackoffStrategy,
    ConfigVersion,
    CreatePolicyMutation,
    CreateQueueMutation,
    DomainValidationError,
    PolicyVersion,
    QueueState,
    RetryPolicyDraft,
    assert_expected_config_version,
)
from workhold.storage.models import (
    AdminAuditLog,
    Queue,
    QueueCounter,
    QueuePolicyVersion,
)

_STATE_CODE_ACTIVE: Final[int] = 1
_STATE_BY_CODE: Final[dict[int, QueueState]] = {
    1: QueueState.ACTIVE,
    2: QueueState.PAUSED,
    3: QueueState.DRAINING,
}
_STATE_CODE_BY_STATE: Final[dict[QueueState, int]] = {
    state: code for code, state in _STATE_BY_CODE.items()
}
_ALLOWED_STATE_TRANSITIONS: Final[frozenset[tuple[QueueState, QueueState]]] = (
    frozenset(
        {
            (QueueState.ACTIVE, QueueState.PAUSED),
            (QueueState.PAUSED, QueueState.ACTIVE),
            (QueueState.ACTIVE, QueueState.DRAINING),
            (QueueState.PAUSED, QueueState.DRAINING),
            (QueueState.DRAINING, QueueState.ACTIVE),
            (QueueState.DRAINING, QueueState.PAUSED),
        }
    )
)
_BACKOFF_FIXED_CODE: Final[int] = 1
_AUDIT_OP_CREATE_QUEUE: Final[int] = 1
_AUDIT_OP_CREATE_POLICY: Final[int] = 2
_AUDIT_OP_ACTIVATE_POLICY: Final[int] = 3
_AUDIT_OP_SET_STATE: Final[int] = 4
_INITIAL_CONFIG_VERSION: Final[int] = 1
_INITIAL_POLICY_VERSION: Final[int] = 1
_QUEUES_NAME_UNIQUE: Final[str] = "queues_name_key"


@dataclass(frozen=True, slots=True)
class ActivePolicyView:
    """Selected immutable retry-policy version for a named queue."""

    policy_version_id: int
    version: PolicyVersion
    policy: RetryPolicyDraft
    created_at: datetime


@dataclass(frozen=True, slots=True)
class QueueConfiguration:
    """Bounded named-queue configuration read model."""

    queue_id: UUID
    name: str
    state: QueueState
    config_version: ConfigVersion
    active_policy: ActivePolicyView
    created_at: datetime
    updated_at: datetime


class QueueControlRepository:
    """Transactional named-queue create and configuration reads.

    Callers own the SQLAlchemy session/transaction. Methods flush when PostgreSQL
    must assign identities or evaluate constraints; they never commit or roll back.
    """

    def create_named_queue(
        self,
        session: Session,
        mutation: CreateQueueMutation,
    ) -> QueueConfiguration:
        """Create a named queue, initial policy version, and audit row atomically.

        Initial contract values (storage-contract / models defaults):
        ``state_code=1`` (active), ``config_version=1``, policy ``version=1``,
        audit ``operation_code=1`` (create_queue) with null previous version.
        """
        public_queue_id = uuid.uuid4()
        queue = Queue(
            queue_id=public_queue_id,
            name=mutation.name,
            state_code=_STATE_CODE_ACTIVE,
            config_version=_INITIAL_CONFIG_VERSION,
            active_policy_version_id=None,
        )
        session.add(queue)
        try:
            session.flush()
        except IntegrityError as exc:
            self._raise_duplicate_name_conflict(exc)

        session.add(
            QueueCounter(
                queue_id=queue.id,
                delayed_count=0,
                ready_count=0,
                leased_count=0,
            )
        )

        policy = QueuePolicyVersion(
            queue_id=queue.id,
            version=_INITIAL_POLICY_VERSION,
            enabled=mutation.initial_policy.enabled,
            max_attempts=mutation.initial_policy.max_attempts,
            backoff_strategy_code=_BACKOFF_FIXED_CODE,
            retry_delay_seconds=mutation.initial_policy.retry_delay_seconds,
        )
        session.add(policy)
        session.flush()

        queue.active_policy_version_id = policy.id

        audit = AdminAuditLog(
            audit_at=func.statement_timestamp(),
            queue_id=queue.id,
            actor_id=mutation.metadata.actor_id,
            operation_code=_AUDIT_OP_CREATE_QUEUE,
            previous_config_version=None,
            new_config_version=_INITIAL_CONFIG_VERSION,
            request_id=uuid.UUID(mutation.metadata.request_id),
            details={
                "name": mutation.name,
                "policy_version": _INITIAL_POLICY_VERSION,
            },
        )
        session.add(audit)
        try:
            session.flush()
        except IntegrityError as exc:
            self._raise_duplicate_name_conflict(exc)

        return self._load_configuration(session, queue_pk=queue.id)

    def get_queue_configuration(
        self,
        session: Session,
        *,
        queue_id: UUID | None = None,
        name: str | None = None,
    ) -> QueueConfiguration | None:
        """Read queue configuration by public ``queue_id`` or exact ``name``."""
        if (queue_id is None) == (name is None):
            raise DomainValidationError(
                "validation_failed",
                "exactly one of queue_id or name is required",
            )
        if queue_id is not None:
            queue = session.execute(
                select(Queue).where(Queue.queue_id == queue_id)
            ).scalar_one_or_none()
        else:
            queue = session.execute(
                select(Queue).where(Queue.name == name)
            ).scalar_one_or_none()
        if queue is None:
            return None
        return self._load_configuration(session, queue_pk=queue.id)

    def list_queue_configurations(
        self,
        session: Session,
        *,
        limit: int = 50,
        after_name: str | None = None,
    ) -> tuple[list[QueueConfiguration], str | None]:
        """List named-queue configurations ordered by name with keyset pagination."""
        if limit < 1 or limit > 100:
            raise DomainValidationError(
                "validation_failed",
                "limit must be between 1 and 100",
            )
        stmt = select(Queue).order_by(Queue.name.asc())
        if after_name is not None:
            stmt = stmt.where(Queue.name > after_name)
        rows = list(session.scalars(stmt.limit(limit + 1)).all())
        next_cursor: str | None = None
        if len(rows) > limit:
            next_cursor = rows[limit - 1].name
            rows = rows[:limit]
        configs = [self._load_configuration(session, queue_pk=row.id) for row in rows]
        return configs, next_cursor

    def create_policy_version(
        self,
        session: Session,
        *,
        queue_name: str,
        mutation: CreatePolicyMutation,
    ) -> QueueConfiguration:
        """Append an immutable retry-policy version without selecting it."""
        queue = self._lock_queue_by_name(session, queue_name)
        next_version = self._next_policy_version(session, queue_pk=queue.id)
        policy = QueuePolicyVersion(
            queue_id=queue.id,
            version=next_version,
            enabled=mutation.policy.enabled,
            max_attempts=mutation.policy.max_attempts,
            backoff_strategy_code=_BACKOFF_FIXED_CODE,
            retry_delay_seconds=mutation.policy.retry_delay_seconds,
        )
        session.add(policy)
        session.flush()

        audit = AdminAuditLog(
            audit_at=func.statement_timestamp(),
            queue_id=queue.id,
            actor_id=mutation.metadata.actor_id,
            operation_code=_AUDIT_OP_CREATE_POLICY,
            previous_config_version=None,
            new_config_version=None,
            request_id=uuid.UUID(mutation.metadata.request_id),
            details={"policy_version": next_version},
        )
        session.add(audit)
        session.flush()
        return self._load_configuration(session, queue_pk=queue.id)

    def activate_policy_version(
        self,
        session: Session,
        *,
        queue_name: str,
        mutation: ActivatePolicyMutation,
    ) -> QueueConfiguration:
        """Select an existing policy version under optimistic config concurrency."""
        queue = self._lock_queue_by_name(session, queue_name)
        assert_expected_config_version(
            current=ConfigVersion(value=queue.config_version),
            expected=mutation.expected_config_version,
        )

        policy = session.execute(
            select(QueuePolicyVersion).where(
                QueuePolicyVersion.queue_id == queue.id,
                QueuePolicyVersion.version == mutation.policy_version.value,
            )
        ).scalar_one_or_none()
        if policy is None:
            raise DomainValidationError(
                "validation_failed",
                "policy_version does not exist for this queue",
            )

        previous_policy = session.get(QueuePolicyVersion, queue.active_policy_version_id)
        if previous_policy is None:
            raise DomainValidationError(
                "internal_error",
                "active policy version row is missing",
            )

        previous_config = queue.config_version
        new_config = previous_config + 1
        queue.active_policy_version_id = policy.id
        queue.config_version = new_config
        queue.updated_at = func.statement_timestamp()

        audit = AdminAuditLog(
            audit_at=func.statement_timestamp(),
            queue_id=queue.id,
            actor_id=mutation.metadata.actor_id,
            operation_code=_AUDIT_OP_ACTIVATE_POLICY,
            previous_config_version=previous_config,
            new_config_version=new_config,
            request_id=uuid.UUID(mutation.metadata.request_id),
            details={
                "previous_policy_version": previous_policy.version,
                "new_policy_version": policy.version,
            },
        )
        session.add(audit)
        session.flush()
        return self._load_configuration(session, queue_pk=queue.id)


    def set_queue_state(
        self,
        session: Session,
        *,
        queue_name: str,
        mutation: SetQueueStateMutation,
    ) -> QueueConfiguration:
        """Persist a queue-state transition under optimistic config concurrency.

        Supports the full active/paused/draining matrix for internal callers and
        Phase 4 admin drain tools (CTRL-06 / CTRL-09).
        """
        queue = self._lock_queue_by_name(session, queue_name)
        current_state = _STATE_BY_CODE.get(queue.state_code)
        if current_state is None:
            raise DomainValidationError(
                "internal_error",
                f"unknown queue state_code={queue.state_code}",
            )
        assert_expected_config_version(
            current=ConfigVersion(value=queue.config_version),
            expected=mutation.expected_config_version,
        )
        if (current_state, mutation.state) not in _ALLOWED_STATE_TRANSITIONS:
            raise DomainValidationError(
                "validation_failed",
                (
                    f"transition {current_state.value} -> "
                    f"{mutation.state.value} is not allowed"
                ),
            )

        previous_config = queue.config_version
        new_config = previous_config + 1
        previous_state = current_state
        queue.state_code = _STATE_CODE_BY_STATE[mutation.state]
        queue.config_version = new_config
        queue.updated_at = func.statement_timestamp()

        audit = AdminAuditLog(
            audit_at=func.statement_timestamp(),
            queue_id=queue.id,
            actor_id=mutation.metadata.actor_id,
            operation_code=_AUDIT_OP_SET_STATE,
            previous_config_version=previous_config,
            new_config_version=new_config,
            request_id=uuid.UUID(mutation.metadata.request_id),
            details={
                "previous_state": previous_state.value,
                "new_state": mutation.state.value,
            },
        )
        session.add(audit)
        session.flush()
        return self._load_configuration(session, queue_pk=queue.id)

    def _lock_queue_by_name(self, session: Session, queue_name: str) -> Queue:
        queue = session.execute(
            select(Queue).where(Queue.name == queue_name).with_for_update()
        ).scalar_one_or_none()
        if queue is None:
            raise DomainValidationError("queue_not_found", "queue not found")
        return queue

    @staticmethod
    def _next_policy_version(session: Session, *, queue_pk: int) -> int:
        current_max = session.scalar(
            select(func.max(QueuePolicyVersion.version)).where(
                QueuePolicyVersion.queue_id == queue_pk
            )
        )
        if current_max is None:
            raise DomainValidationError(
                "internal_error",
                "queue has no policy versions to extend",
            )
        return int(current_max) + 1

    def _load_configuration(self, session: Session, *, queue_pk: int) -> QueueConfiguration:
        queue = session.get(Queue, queue_pk)
        if queue is None or queue.active_policy_version_id is None:
            raise DomainValidationError(
                "internal_error",
                "queue configuration is incomplete after write",
            )
        policy = session.get(QueuePolicyVersion, queue.active_policy_version_id)
        if policy is None:
            raise DomainValidationError(
                "internal_error",
                "active policy version row is missing",
            )
        if policy.queue_id != queue.id:
            raise DomainValidationError(
                "internal_error",
                "active policy does not belong to the queue",
            )
        state = _STATE_BY_CODE.get(queue.state_code)
        if state is None:
            raise DomainValidationError(
                "internal_error",
                f"unknown queue state_code={queue.state_code}",
            )
        if policy.backoff_strategy_code != _BACKOFF_FIXED_CODE:
            raise DomainValidationError(
                "internal_error",
                f"unsupported backoff_strategy_code={policy.backoff_strategy_code}",
            )
        return QueueConfiguration(
            queue_id=queue.queue_id,
            name=queue.name,
            state=state,
            config_version=ConfigVersion(value=queue.config_version),
            active_policy=ActivePolicyView(
                policy_version_id=policy.id,
                version=PolicyVersion(value=policy.version),
                policy=RetryPolicyDraft(
                    enabled=policy.enabled,
                    max_attempts=policy.max_attempts,
                    backoff_strategy=BackoffStrategy.FIXED,
                    retry_delay_seconds=policy.retry_delay_seconds,
                ),
                created_at=policy.created_at,
            ),
            created_at=queue.created_at,
            updated_at=queue.updated_at,
        )

    @staticmethod
    def _raise_duplicate_name_conflict(exc: IntegrityError) -> None:
        """Map ``queues_name_key`` to the Phase 3.1 createQueue 409 conflict code."""
        orig = getattr(exc, "orig", None)
        diag = getattr(orig, "diag", None)
        constraint = getattr(diag, "constraint_name", None) if diag is not None else None
        message = str(orig) if orig is not None else str(exc)
        if constraint == _QUEUES_NAME_UNIQUE or _QUEUES_NAME_UNIQUE in message:
            raise DomainValidationError(
                "idempotency_conflict",
                "queue name already exists",
            ) from exc
        raise DomainValidationError(
            "internal_error",
            "queue persistence integrity failure",
        ) from exc
