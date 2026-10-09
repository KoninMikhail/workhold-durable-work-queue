"""PostgreSQL single-transaction claim and reclaim primitive."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Final
from uuid import UUID

from sqlalchemy import and_, desc, func, or_, select
from sqlalchemy.orm import Session

from workhold.application.queue_state_gate import (
    OperationGateOutcome,
    QueueOperation,
    QueueState,
    evaluate_queue_state_gate,
)
from workhold.domain.queue_control import DomainValidationError
from workhold.storage.models import (
    ClaimRegistry,
    Queue,
    QueueCounter,
    QueuePolicyVersion,
    TaskActive,
    TaskAttempt,
    TaskPayloadActive,
)

_STATE_CODE_BY_INT: Final[dict[int, QueueState]] = {
    1: QueueState.ACTIVE,
    2: QueueState.PAUSED,
    3: QueueState.DRAINING,
}
_TASK_DELAYED: Final[int] = 1
_TASK_READY: Final[int] = 2
_TASK_LEASED: Final[int] = 3
_OUTCOME_ACTIVE: Final[int] = 1
_WORKER_ID_MAX_LEN: Final[int] = 128


@dataclass(frozen=True, slots=True)
class ClaimPersistenceResult:
    """Committed claim outcome exposed only after the transaction commits.

    Successful claims carry the OpenAPI ClaimedTask projection fields so the HTTP
    adapter can map the response without a second credential or payload lookup.
    """

    empty: bool
    paused: bool = False
    queue_name: str | None = None
    queue_state: str | None = None
    server_time: datetime | None = None
    task_id: UUID | None = None
    claim_id: UUID | None = None
    claim_token: UUID | None = None
    generation: int | None = None
    claimed_at: datetime | None = None
    lease_expires_at: datetime | None = None
    worker_id: str | None = None
    cancel_requested: bool = False
    producer_id: str | None = None
    priority: int | None = None
    available_at: datetime | None = None
    retry_policy_version: int | None = None
    created_at: datetime | None = None
    payload: object | None = None


class ClaimRepository:
    """Short-transaction fenced claim/reclaim against the Queue store.

    Callers own the session and must commit before exposing claim credentials.
    """

    def lock_named_queue(self, session: Session, queue_name: str) -> Queue:
        """Lock the named queue row so claim and state transitions serialize."""
        queue = session.execute(
            select(Queue).where(Queue.name == queue_name).with_for_update()
        ).scalar_one_or_none()
        if queue is None:
            raise DomainValidationError("queue_not_found", "queue not found")
        return queue

    def claim_one(
        self,
        session: Session,
        *,
        queue_name: str,
        worker_id: str,
        lease_seconds: int,
    ) -> ClaimPersistenceResult:
        """Claim or reclaim at most one task inside the caller's open transaction."""
        self._validate_worker_id(worker_id)
        if not isinstance(lease_seconds, int) or lease_seconds < 1:
            raise DomainValidationError(
                "validation_failed",
                "lease_seconds must be a positive integer",
            )

        queue = self.lock_named_queue(session, queue_name)
        state = _STATE_CODE_BY_INT.get(int(queue.state_code))
        if state is None:
            raise DomainValidationError(
                "internal_error",
                "queue has an unknown state_code",
            )
        server_time = session.scalar(select(func.transaction_timestamp()))
        if server_time is None:
            raise DomainValidationError(
                "internal_error",
                "transaction_timestamp() returned null",
            )

        gate = evaluate_queue_state_gate(state, QueueOperation.CLAIM)
        if gate is OperationGateOutcome.PAUSED_EMPTY:
            return ClaimPersistenceResult(
                empty=True,
                paused=True,
                queue_name=queue_name,
                queue_state=state.value,
                server_time=server_time,
            )
        if gate is not OperationGateOutcome.ALLOWED:
            raise DomainValidationError(
                "internal_error",
                f"unexpected claim gate outcome: {gate.value}",
            )

        task = self._select_claimable_task(session, queue_pk=queue.id)
        # Expired leased rows finalize under the same FOR UPDATE lock before any
        # new claim. Delayed / cancelled / dead-lettered outcomes are not claimed
        # in this transaction; keep selecting until ready work is found or none.
        # Lazy import avoids claim_repository ↔ task_transitions ↔ api cycles.
        from workhold.infrastructure.postgres.task_transitions import (
            TaskTransitionRepository,
        )

        while task is not None and int(task.state_code) == _TASK_LEASED:
            expiry = TaskTransitionRepository().expire_locked_lease(session, task=task)
            if expiry.state == "retry_scheduled" and expiry.claimable:
                session.refresh(task)
                break
            task = self._select_claimable_task(session, queue_pk=queue.id)

        if task is None:
            return ClaimPersistenceResult(
                empty=True,
                paused=False,
                queue_name=queue_name,
                queue_state=state.value,
                server_time=server_time,
            )

        return self._fence_task(
            session,
            task=task,
            queue_name=queue_name,
            queue_state=state.value,
            worker_id=worker_id,
            lease_seconds=lease_seconds,
        )

    def _select_claimable_task(self, session: Session, *, queue_pk: int) -> TaskActive | None:
        now = func.transaction_timestamp()
        return session.execute(
            select(TaskActive)
            .where(
                TaskActive.queue_id == queue_pk,
                or_(
                    and_(
                        TaskActive.state_code.in_((_TASK_DELAYED, _TASK_READY)),
                        TaskActive.available_at <= now,
                    ),
                    and_(
                        TaskActive.state_code == _TASK_LEASED,
                        TaskActive.lease_expires_at <= now,
                    ),
                ),
            )
            .order_by(
                desc(TaskActive.priority),
                TaskActive.available_at,
                TaskActive.id,
            )
            .with_for_update(skip_locked=True)
            .limit(1)
        ).scalar_one_or_none()

    def _fence_task(
        self,
        session: Session,
        *,
        task: TaskActive,
        queue_name: str,
        queue_state: str,
        worker_id: str,
        lease_seconds: int,
    ) -> ClaimPersistenceResult:
        claimed_at = session.scalar(select(func.transaction_timestamp()))
        if claimed_at is None:
            raise DomainValidationError(
                "internal_error",
                "transaction_timestamp() returned null",
            )
        lease_expires_at = claimed_at + timedelta(seconds=lease_seconds)
        claim_id = uuid.uuid4()
        claim_token = uuid.uuid4()
        new_generation = int(task.generation) + 1

        prior_state = int(task.state_code)
        if prior_state == _TASK_LEASED:
            raise DomainValidationError(
                "internal_error",
                "expired leased task must be finalized before fencing a new claim",
            )

        task.state_code = _TASK_LEASED
        task.generation = new_generation
        task.current_claim_id = claim_id
        task.claimed_at = claimed_at
        task.lease_expires_at = lease_expires_at
        task.worker_id = worker_id
        task.updated_at = claimed_at

        if prior_state == _TASK_DELAYED:
            self._move_delayed_to_leased_counter(
                session,
                queue_id=int(task.queue_id),
                at=claimed_at,
            )
        elif prior_state == _TASK_READY:
            self._move_ready_to_leased_counter(
                session,
                queue_id=int(task.queue_id),
                at=claimed_at,
            )

        session.add(
            ClaimRegistry(
                claim_id=claim_id,
                task_id=task.task_id,
                claim_token=claim_token,
                generation=new_generation,
                claimed_at=claimed_at,
                lease_expires_at=lease_expires_at,
                created_at=claimed_at,
            )
        )
        session.add(
            TaskAttempt(
                task_id=task.task_id,
                claim_id=claim_id,
                generation=new_generation,
                claimed_at=claimed_at,
                worker_id=worker_id,
                lease_expires_at=lease_expires_at,
                ended_at=None,
                outcome_code=_OUTCOME_ACTIVE,
                failure_code=None,
                failure_detail=None,
            )
        )
        session.flush()

        payload_row = session.get(TaskPayloadActive, task.id)
        if payload_row is None:
            raise DomainValidationError(
                "internal_error",
                "claimed task is missing active payload row",
            )
        policy = session.get(QueuePolicyVersion, task.retry_policy_version_id)
        if policy is None:
            raise DomainValidationError(
                "internal_error",
                "claimed task is missing retry policy version",
            )

        return ClaimPersistenceResult(
            empty=False,
            paused=False,
            queue_name=queue_name,
            queue_state=queue_state,
            server_time=claimed_at,
            task_id=task.task_id,
            claim_id=claim_id,
            claim_token=claim_token,
            generation=new_generation,
            claimed_at=claimed_at,
            lease_expires_at=lease_expires_at,
            worker_id=worker_id,
            cancel_requested=task.cancel_requested_at is not None,
            producer_id=str(task.producer_id),
            priority=int(task.priority),
            available_at=task.available_at,
            retry_policy_version=int(policy.version),
            created_at=task.created_at,
            payload=payload_row.payload,
        )

    @staticmethod
    def _move_delayed_to_leased_counter(
        session: Session,
        *,
        queue_id: int,
        at: datetime,
    ) -> None:
        """Move one delayed unit to leased under the already-locked queue row."""
        counter = session.get(QueueCounter, queue_id)
        if counter is None:
            session.add(
                QueueCounter(
                    queue_id=queue_id,
                    delayed_count=0,
                    ready_count=0,
                    leased_count=1,
                    as_of=at,
                )
            )
            return
        if int(counter.delayed_count) >= 1:
            counter.delayed_count = int(counter.delayed_count) - 1
        counter.leased_count = int(counter.leased_count) + 1
        counter.as_of = at

    @staticmethod
    def _move_ready_to_leased_counter(
        session: Session,
        *,
        queue_id: int,
        at: datetime,
    ) -> None:
        """Move one ready unit to leased under the already-locked queue row.

        EnqueueService maintains ``ready_count``; lower-level staging helpers used
        by older integration tests may leave counters at zero. Prefer a correct
        ready→leased move when ready stock exists; otherwise still account the
        lease so reclaim/claim paths never fail closed on bookkeeping alone.
        """
        counter = session.get(QueueCounter, queue_id)
        if counter is None:
            session.add(
                QueueCounter(
                    queue_id=queue_id,
                    delayed_count=0,
                    ready_count=0,
                    leased_count=1,
                    as_of=at,
                )
            )
            return
        if int(counter.ready_count) >= 1:
            counter.ready_count = int(counter.ready_count) - 1
        counter.leased_count = int(counter.leased_count) + 1
        counter.as_of = at

    @staticmethod
    def _validate_worker_id(worker_id: str) -> None:
        if not isinstance(worker_id, str) or not (1 <= len(worker_id) <= _WORKER_ID_MAX_LEN):
            raise DomainValidationError(
                "validation_failed",
                "worker_id must be a non-empty string up to 128 characters",
            )
