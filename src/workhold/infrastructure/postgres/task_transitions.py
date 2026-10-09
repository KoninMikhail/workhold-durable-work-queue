"""PostgreSQL atomic worker fail / lease-expiry / retry / dead-letter / cancel transitions."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Final
from uuid import UUID

from sqlalchemy import delete, func, select, update
from sqlalchemy.orm import Session

from workhold.api.schemas.tasks import format_task_datetime
from workhold.api.schemas.terminal import AckCancelCommand, FailCommand
from workhold.domain.queue_control import (
    BackoffStrategy,
    DomainValidationError,
    OperationGateOutcome,
    PolicyVersion,
    QueueOperation,
    QueueState,
    evaluate_operation_gate,
)
from workhold.domain.retry import (
    LEASE_EXPIRY_FAILURE_CODE,
    DeadLettered,
    EnqueuedRetryPolicy,
    RetryCause,
    RetryOutcomeKind,
    RetryScheduled,
    decide_retry_or_dead_letter,
)
from workhold.storage.leases import FenceDecision, validate_current_lease
from workhold.storage.models import (
    ClaimRegistry,
    CompleteReplay,
    Queue,
    QueueCounter,
    QueuePolicyVersion,
    TaskActive,
    TaskAttempt,
    TaskPayloadActive,
    TaskTerminal,
)

_OP_COMPLETE: Final[int] = 1
_OP_FAIL: Final[int] = 2
_OP_ACK_CANCEL: Final[int] = 3

_OUTCOME_ACTIVE: Final[int] = 1
_OUTCOME_RETRY_SCHEDULED: Final[int] = 3
_OUTCOME_DEAD_LETTERED: Final[int] = 4
_OUTCOME_EXPIRED: Final[int] = 5
_OUTCOME_CANCELLED: Final[int] = 6

_TASK_DELAYED: Final[int] = 1
_TASK_READY: Final[int] = 2
_TASK_LEASED: Final[int] = 3

_RESULT_RETRY_SCHEDULED: Final[int] = 3
_RESULT_DEAD_LETTERED: Final[int] = 11
_RESULT_CANCELLED: Final[int] = 12
_RESULT_SUCCEEDED: Final[int] = 10

_BACKOFF_FIXED: Final[int] = 1
_REPLAY_TTL_DAYS: Final[int] = 7

_QUEUE_ACTIVE: Final[int] = 1
_QUEUE_PAUSED: Final[int] = 2
_QUEUE_DRAINING: Final[int] = 3

_QUEUE_STATE_BY_CODE: Final[dict[int, QueueState]] = {
    _QUEUE_ACTIVE: QueueState.ACTIVE,
    _QUEUE_PAUSED: QueueState.PAUSED,
    _QUEUE_DRAINING: QueueState.DRAINING,
}

_TERMINAL_STATE_BY_CODE: Final[dict[int, str]] = {
    _RESULT_SUCCEEDED: "succeeded",
    _RESULT_DEAD_LETTERED: "dead_lettered",
    _RESULT_CANCELLED: "cancelled",
}


@dataclass(frozen=True, slots=True)
class FailPersistenceResult:
    """Committed fail projection after the Queue-store transaction commits."""

    task_id: UUID
    state: str
    available_at: datetime | None
    terminal_at: datetime | None
    replayed: bool
    queue_name: str


@dataclass(frozen=True, slots=True)
class ExpiryPersistenceResult:
    """Committed lease-expiry projection after the Queue-store transaction commits."""

    task_id: UUID
    state: str
    available_at: datetime | None
    terminal_at: datetime | None
    claimable: bool


@dataclass(frozen=True, slots=True)
class CancelPersistenceResult:
    """Committed producer-cancel projection after the Queue-store transaction commits."""

    task: dict[str, Any]
    replayed: bool


@dataclass(frozen=True, slots=True)
class AckCancelPersistenceResult:
    """Committed ack_cancel projection after the Queue-store transaction commits."""

    task_id: UUID
    state: str
    terminal_at: datetime
    replayed: bool
    queue_name: str


class TaskTransitionRepository:
    """Short-transaction fail / lease-expiry / cancel / ack_cancel primitives."""

    def expire_lease(
        self,
        session: Session,
        *,
        task_id: UUID,
    ) -> ExpiryPersistenceResult:
        """Lock the task row and finalize an expired current lease, if any."""
        task = session.execute(
            select(TaskActive)
            .where(TaskActive.task_id == task_id)
            .with_for_update()
        ).scalar_one_or_none()
        if task is None:
            return ExpiryPersistenceResult(
                task_id=task_id,
                state="already_finalized",
                available_at=None,
                terminal_at=None,
                claimable=False,
            )
        return self.expire_locked_lease(session, task=task)

    def expire_locked_lease(
        self,
        session: Session,
        *,
        task: TaskActive,
    ) -> ExpiryPersistenceResult:
        """Finalize expiry for a task row already locked by the caller (e.g. reclaim).

        Closes the current attempt as ``expired``. Cancellation-requested leases
        become terminal cancelled without granting a new claim. Otherwise the
        enqueue-time policy snapshot decides retry vs dead-letter. Never consults
        the queue's active policy version.
        """
        now = session.scalar(select(func.transaction_timestamp()))
        if now is None:
            raise DomainValidationError(
                "internal_error",
                "transaction_timestamp() returned null",
            )

        if int(task.state_code) != _TASK_LEASED:
            return ExpiryPersistenceResult(
                task_id=task.task_id,
                state="already_finalized",
                available_at=task.available_at if int(task.state_code) in (
                    _TASK_DELAYED,
                    _TASK_READY,
                ) else None,
                terminal_at=None,
                claimable=int(task.state_code) == _TASK_READY
                and task.available_at <= now
                and task.current_claim_id is None,
            )

        if task.lease_expires_at is None or task.lease_expires_at > now:
            raise DomainValidationError(
                "validation_failed",
                "lease is not expired under Queue-store time",
            )

        prior_claim_id = task.current_claim_id
        if prior_claim_id is None:
            raise DomainValidationError(
                "internal_error",
                "leased task is missing current_claim_id",
            )

        # Serialize against reclaim/heartbeat with the same claim_registry row.
        registry = session.execute(
            select(ClaimRegistry)
            .where(ClaimRegistry.claim_id == prior_claim_id)
            .with_for_update()
        ).scalar_one_or_none()
        if registry is None:
            return ExpiryPersistenceResult(
                task_id=task.task_id,
                state="already_finalized",
                available_at=None,
                terminal_at=None,
                claimable=False,
            )

        active_attempt = session.execute(
            select(TaskAttempt)
            .where(
                TaskAttempt.task_id == task.task_id,
                TaskAttempt.claim_id == prior_claim_id,
                TaskAttempt.outcome_code == _OUTCOME_ACTIVE,
            )
            .with_for_update()
        ).scalar_one_or_none()
        if active_attempt is None:
            return ExpiryPersistenceResult(
                task_id=task.task_id,
                state="already_finalized",
                available_at=None,
                terminal_at=None,
                claimable=False,
            )

        cancel_requested = task.cancel_requested_at is not None
        policy_row = session.get(QueuePolicyVersion, task.retry_policy_version_id)
        if policy_row is None:
            raise DomainValidationError(
                "internal_error",
                "task is missing enqueue-time retry policy version",
            )

        if cancel_requested:
            self._close_expired_attempt(
                session,
                task=task,
                claim_id=prior_claim_id,
                ended_at=now,
                failure_code=None,
                failure_detail=None,
            )
            self._cancel_terminal(
                session,
                task=task,
                policy_row=policy_row,
                terminal_at=now,
                prior_state_code=_TASK_LEASED,
            )
            return ExpiryPersistenceResult(
                task_id=task.task_id,
                state="cancelled",
                available_at=None,
                terminal_at=now,
                claimable=False,
            )

        completed = session.scalar(
            select(func.count())
            .select_from(TaskAttempt)
            .where(
                TaskAttempt.task_id == task.task_id,
                TaskAttempt.outcome_code != _OUTCOME_ACTIVE,
            )
        )
        completed_attempt_count = int(completed or 0) + 1

        if int(policy_row.backoff_strategy_code) != _BACKOFF_FIXED:
            raise DomainValidationError(
                "internal_error",
                f"unsupported backoff_strategy_code={policy_row.backoff_strategy_code}",
            )
        policy = EnqueuedRetryPolicy(
            policy_version=PolicyVersion(int(policy_row.version)),
            enabled=bool(policy_row.enabled),
            max_attempts=int(policy_row.max_attempts),
            backoff_strategy=BackoffStrategy.FIXED,
            retry_delay_seconds=int(policy_row.retry_delay_seconds),
        )
        decision = decide_retry_or_dead_letter(
            policy=policy,
            completed_attempt_count=completed_attempt_count,
            queue_now=now,
            cause=RetryCause.LEASE_EXPIRY,
        )

        if isinstance(decision, RetryScheduled):
            self._close_expired_attempt(
                session,
                task=task,
                claim_id=prior_claim_id,
                ended_at=now,
                failure_code=None,
                failure_detail=None,
            )
            self._schedule_retry(
                session,
                task=task,
                available_at=decision.available_at,
                queue_now=now,
            )
            claimable = (
                int(task.state_code) == _TASK_READY
                and task.available_at <= now
                and task.current_claim_id is None
            )
            return ExpiryPersistenceResult(
                task_id=task.task_id,
                state="retry_scheduled",
                available_at=decision.available_at,
                terminal_at=None,
                claimable=claimable,
            )

        assert isinstance(decision, DeadLettered)
        self._close_expired_attempt(
            session,
            task=task,
            claim_id=prior_claim_id,
            ended_at=now,
            failure_code=LEASE_EXPIRY_FAILURE_CODE,
            failure_detail=decision.reason,
        )
        self._dead_letter_expiry(
            session,
            task=task,
            policy_row=policy_row,
            terminal_at=now,
            failure_code=LEASE_EXPIRY_FAILURE_CODE,
            failure_detail=decision.reason,
        )
        return ExpiryPersistenceResult(
            task_id=task.task_id,
            state="dead_lettered",
            available_at=None,
            terminal_at=now,
            claimable=False,
        )

    @staticmethod
    def _close_expired_attempt(
        session: Session,
        *,
        task: TaskActive,
        claim_id: UUID,
        ended_at: datetime,
        failure_code: str | None,
        failure_detail: str | None,
    ) -> None:
        closed = session.execute(
            update(TaskAttempt)
            .where(
                TaskAttempt.task_id == task.task_id,
                TaskAttempt.claim_id == claim_id,
                TaskAttempt.outcome_code == _OUTCOME_ACTIVE,
            )
            .values(
                ended_at=ended_at,
                outcome_code=_OUTCOME_EXPIRED,
                failure_code=failure_code,
                failure_detail=failure_detail,
            )
        )
        if closed.rowcount != 1:
            raise DomainValidationError(
                "internal_error",
                "expected exactly one active attempt to expire",
            )
        deleted = session.execute(
            delete(ClaimRegistry).where(ClaimRegistry.claim_id == claim_id)
        )
        if deleted.rowcount != 1:
            raise DomainValidationError(
                "internal_error",
                "expected exactly one claim_registry row to delete on expiry",
            )

    def _cancel_terminal(
        self,
        session: Session,
        *,
        task: TaskActive,
        policy_row: QueuePolicyVersion,
        terminal_at: datetime,
        prior_state_code: int,
    ) -> None:
        payload_row = session.get(TaskPayloadActive, task.id)
        if payload_row is None:
            raise DomainValidationError(
                "internal_error",
                "task is missing active payload row",
            )
        session.add(
            TaskTerminal(
                task_id=task.task_id,
                queue_id=int(task.queue_id),
                producer_id=str(task.producer_id),
                state_code=_RESULT_CANCELLED,
                priority=int(task.priority),
                available_at=task.available_at,
                retry_policy_version=int(policy_row.version),
                payload=payload_row.payload,
                payload_bytes=int(payload_row.payload_bytes),
                created_at=task.created_at,
                terminal_at=terminal_at,
                failure_code=None,
                failure_detail=None,
                source_task_id=task.source_task_id,
                spawn_ordinal=task.spawn_ordinal,
            )
        )
        queue_id = int(task.queue_id)
        session.delete(task)
        session.flush()
        delayed_delta = -1 if prior_state_code == _TASK_DELAYED else 0
        ready_delta = -1 if prior_state_code == _TASK_READY else 0
        leased_delta = -1 if prior_state_code == _TASK_LEASED else 0
        self._adjust_counters(
            session,
            queue_id=queue_id,
            at=terminal_at,
            leased_delta=leased_delta,
            delayed_delta=delayed_delta,
            ready_delta=ready_delta,
        )

    def cancel_task(
        self,
        session: Session,
        *,
        task_id: UUID,
        producer_id: str,
        authorize_queue: Callable[[str], bool],
    ) -> CancelPersistenceResult:
        """State-aware producer cancel: immediate for delayed/ready, request for leased."""

        now = session.scalar(select(func.transaction_timestamp()))
        if now is None:
            raise DomainValidationError(
                "internal_error",
                "transaction_timestamp() returned null",
            )

        task = session.execute(
            select(TaskActive)
            .where(TaskActive.task_id == task_id)
            .with_for_update()
        ).scalar_one_or_none()

        if task is None:
            return self._cancel_missing_active(
                session,
                task_id=task_id,
                producer_id=producer_id,
                authorize_queue=authorize_queue,
            )

        queue = session.execute(
            select(Queue).where(Queue.id == task.queue_id).with_for_update()
        ).scalar_one_or_none()
        if queue is None:
            raise DomainValidationError("internal_error", "task queue row is missing")

        queue_name = str(queue.name)
        if not authorize_queue(queue_name):
            raise DomainValidationError("permission_denied", "permission denied")
        if str(task.producer_id) != producer_id:
            # Non-disclosing ownership: same as resolveSubmission out-of-scope.
            raise DomainValidationError("task_not_found", "task not found")

        queue_state = _QUEUE_STATE_BY_CODE.get(int(queue.state_code))
        if queue_state is None:
            raise DomainValidationError(
                "internal_error",
                f"unknown queue state_code={queue.state_code}",
            )
        gate = evaluate_operation_gate(queue_state, QueueOperation.CANCEL)
        if gate is not OperationGateOutcome.ALLOWED:
            raise DomainValidationError(
                "internal_error",
                f"cancel gate unexpectedly {gate.value}",
            )

        policy_row = session.get(QueuePolicyVersion, task.retry_policy_version_id)
        if policy_row is None:
            raise DomainValidationError(
                "internal_error",
                "task is missing enqueue-time retry policy version",
            )

        state_code = int(task.state_code)
        if state_code in (_TASK_DELAYED, _TASK_READY):
            prior = state_code
            self._cancel_terminal(
                session,
                task=task,
                policy_row=policy_row,
                terminal_at=now,
                prior_state_code=prior,
            )
            terminal = session.execute(
                select(TaskTerminal).where(TaskTerminal.task_id == task_id)
            ).scalar_one()
            return CancelPersistenceResult(
                task=self._project_terminal_task(
                    terminal=terminal,
                    queue_name=queue_name,
                ),
                replayed=False,
            )

        if state_code != _TASK_LEASED:
            raise DomainValidationError(
                "internal_error",
                f"unsupported active task state_code={state_code}",
            )

        if task.cancel_requested_at is None:
            task.cancel_requested_at = now
            task.updated_at = now
            session.flush()
            replayed = False
        else:
            replayed = True

        return CancelPersistenceResult(
            task=self._project_active_task(
                task=task,
                queue_name=queue_name,
                policy_version=int(policy_row.version),
            ),
            replayed=replayed,
        )

    def _cancel_missing_active(
        self,
        session: Session,
        *,
        task_id: UUID,
        producer_id: str,
        authorize_queue: Callable[[str], bool],
    ) -> CancelPersistenceResult:
        terminal = session.execute(
            select(TaskTerminal)
            .where(TaskTerminal.task_id == task_id)
            .with_for_update()
        ).scalar_one_or_none()
        if terminal is None:
            raise DomainValidationError("task_not_found", "task not found")

        queue = session.execute(
            select(Queue).where(Queue.id == terminal.queue_id)
        ).scalar_one_or_none()
        if queue is None:
            raise DomainValidationError("internal_error", "terminal queue row is missing")
        queue_name = str(queue.name)
        if not authorize_queue(queue_name):
            raise DomainValidationError("permission_denied", "permission denied")
        if str(terminal.producer_id) != producer_id:
            raise DomainValidationError("task_not_found", "task not found")

        if int(terminal.state_code) == _RESULT_CANCELLED:
            return CancelPersistenceResult(
                task=self._project_terminal_task(
                    terminal=terminal,
                    queue_name=queue_name,
                ),
                replayed=True,
            )
        raise DomainValidationError(
            "task_already_terminal",
            "another terminal outcome already committed for this task",
        )

    @staticmethod
    def _project_terminal_task(
        *,
        terminal: TaskTerminal,
        queue_name: str,
    ) -> dict[str, Any]:
        state = _TERMINAL_STATE_BY_CODE.get(int(terminal.state_code))
        if state is None:
            raise DomainValidationError(
                "internal_error",
                f"unknown terminal state_code={terminal.state_code}",
            )
        return {
            "task_id": str(terminal.task_id),
            "queue_name": queue_name,
            "producer_id": str(terminal.producer_id),
            "state": state,
            "priority": int(terminal.priority),
            "available_at": format_task_datetime(terminal.available_at),
            "retry_policy_version": int(terminal.retry_policy_version),
            "created_at": format_task_datetime(terminal.created_at),
            "terminal_at": format_task_datetime(terminal.terminal_at),
            "spawned_task_ids": [],
            "delivery_event_ids": [],
        }

    @staticmethod
    def _project_active_task(
        *,
        task: TaskActive,
        queue_name: str,
        policy_version: int,
    ) -> dict[str, Any]:
        state_by_code = {
            _TASK_DELAYED: "delayed",
            _TASK_READY: "ready",
            _TASK_LEASED: "leased",
        }
        state = state_by_code.get(int(task.state_code))
        if state is None:
            raise DomainValidationError(
                "internal_error",
                f"unknown active state_code={task.state_code}",
            )
        body: dict[str, Any] = {
            "task_id": str(task.task_id),
            "queue_name": queue_name,
            "producer_id": str(task.producer_id),
            "state": state,
            "priority": int(task.priority),
            "available_at": format_task_datetime(task.available_at),
            "retry_policy_version": int(policy_version),
            "created_at": format_task_datetime(task.created_at),
            "spawned_task_ids": [],
            "delivery_event_ids": [],
        }
        if int(task.state_code) == _TASK_LEASED:
            if (
                task.current_claim_id is None
                or task.claimed_at is None
                or task.lease_expires_at is None
                or task.worker_id is None
            ):
                raise DomainValidationError(
                    "internal_error",
                    "leased task is missing claim projection fields",
                )
            body["current_claim"] = {
                "claim_id": str(task.current_claim_id),
                "generation": int(task.generation),
                "claimed_at": format_task_datetime(task.claimed_at),
                "lease_expires_at": format_task_datetime(task.lease_expires_at),
                "worker_id": str(task.worker_id),
                "cancel_requested": task.cancel_requested_at is not None,
            }
        return body

    def _dead_letter_expiry(
        self,
        session: Session,
        *,
        task: TaskActive,
        policy_row: QueuePolicyVersion,
        terminal_at: datetime,
        failure_code: str,
        failure_detail: str,
    ) -> None:
        payload_row = session.get(TaskPayloadActive, task.id)
        if payload_row is None:
            raise DomainValidationError(
                "internal_error",
                "task is missing active payload row",
            )
        session.add(
            TaskTerminal(
                task_id=task.task_id,
                queue_id=int(task.queue_id),
                producer_id=str(task.producer_id),
                state_code=_RESULT_DEAD_LETTERED,
                priority=int(task.priority),
                available_at=task.available_at,
                retry_policy_version=int(policy_row.version),
                payload=payload_row.payload,
                payload_bytes=int(payload_row.payload_bytes),
                created_at=task.created_at,
                terminal_at=terminal_at,
                failure_code=failure_code,
                failure_detail=failure_detail,
                source_task_id=task.source_task_id,
                spawn_ordinal=task.spawn_ordinal,
            )
        )
        queue_id = int(task.queue_id)
        session.delete(task)
        session.flush()
        self._adjust_counters(
            session,
            queue_id=queue_id,
            at=terminal_at,
            leased_delta=-1,
            delayed_delta=0,
            ready_delta=0,
        )

    def fail_claim(
        self,
        session: Session,
        *,
        claim_id: UUID,
        claim_token: UUID,
        command: FailCommand,
        authorize_queue: Callable[[str], bool] | None = None,
    ) -> FailPersistenceResult:
        """Atomically apply worker fail with generic terminal replay semantics."""
        existing = self._load_fail_replay(session, claim_id=claim_id)
        if existing is not None:
            return self._replay_or_conflict(existing, fingerprint=command.fingerprint)

        other = self._load_other_terminal_replay(session, claim_id=claim_id)
        if other is not None:
            if int(other.operation_code) == _OP_ACK_CANCEL:
                raise DomainValidationError(
                    "cancel_race_lost",
                    "cancellation already recorded for this claim",
                )
            raise DomainValidationError(
                "task_already_terminal",
                "another terminal outcome already committed for this claim",
            )

        fence = validate_current_lease(
            session,
            claim_id=claim_id,
            claim_token=claim_token,
            generation=command.generation,
            for_update=True,
        )
        if fence.decision is FenceDecision.STALE:
            self._raise_stale_fail(session, claim_id=claim_id)
            raise AssertionError("unreachable")

        assert fence.task_id is not None
        assert fence.queue_name is not None
        assert fence.queue_id is not None
        assert fence.claim_id is not None

        if fence.cancel_requested:
            raise DomainValidationError(
                "cancel_race_lost",
                "cancellation already recorded for this claim",
            )

        if authorize_queue is not None and not authorize_queue(fence.queue_name):
            raise DomainValidationError(
                "permission_denied",
                "permission denied",
            )

        now = session.scalar(select(func.transaction_timestamp()))
        if now is None:
            raise DomainValidationError(
                "internal_error",
                "transaction_timestamp() returned null",
            )

        task = session.execute(
            select(TaskActive)
            .where(TaskActive.task_id == fence.task_id)
            .with_for_update()
        ).scalar_one()
        if (
            task.current_claim_id != fence.claim_id
            or int(task.state_code) != _TASK_LEASED
            or int(task.generation) != command.generation
        ):
            raise DomainValidationError(
                "lease_lost",
                "claim is no longer current",
            )

        policy_row = session.get(QueuePolicyVersion, task.retry_policy_version_id)
        if policy_row is None:
            raise DomainValidationError(
                "internal_error",
                "task is missing enqueue-time retry policy version",
            )

        outcome_code, result_state, available_at, terminal_at = self._decide_outcome(
            session,
            task=task,
            policy_row=policy_row,
            command=command,
            queue_now=now,
        )

        closed = session.execute(
            update(TaskAttempt)
            .where(
                TaskAttempt.task_id == task.task_id,
                TaskAttempt.claim_id == fence.claim_id,
                TaskAttempt.outcome_code == _OUTCOME_ACTIVE,
            )
            .values(
                ended_at=now,
                outcome_code=outcome_code,
                failure_code=command.failure_code,
                failure_detail=command.failure_detail,
            )
        )
        if closed.rowcount != 1:
            raise DomainValidationError(
                "internal_error",
                "expected exactly one active attempt to close on fail",
            )

        deleted = session.execute(
            delete(ClaimRegistry).where(ClaimRegistry.claim_id == fence.claim_id)
        )
        if deleted.rowcount != 1:
            raise DomainValidationError(
                "internal_error",
                "expected exactly one claim_registry row to delete on fail",
            )

        if result_state == _RESULT_RETRY_SCHEDULED:
            assert available_at is not None
            self._schedule_retry(
                session,
                task=task,
                available_at=available_at,
                queue_now=now,
            )
            state_name = "retry_scheduled"
        else:
            assert terminal_at is not None
            self._dead_letter(
                session,
                task=task,
                policy_row=policy_row,
                command=command,
                terminal_at=terminal_at,
            )
            state_name = "dead_lettered"

        replay = CompleteReplay(
            claim_id=fence.claim_id,
            operation_code=_OP_FAIL,
            request_fingerprint=command.fingerprint,
            task_id=fence.task_id,
            result_state_code=result_state,
            available_at=available_at,
            terminal_at=terminal_at,
            spawned_task_ids=[],
            event_ids=[],
            created_at=now,
            expires_at=now + timedelta(days=_REPLAY_TTL_DAYS),
        )
        session.add(replay)
        session.flush()

        return FailPersistenceResult(
            task_id=fence.task_id,
            state=state_name,
            available_at=available_at,
            terminal_at=terminal_at,
            replayed=False,
            queue_name=fence.queue_name,
        )

    def ack_cancel_claim(
        self,
        session: Session,
        *,
        claim_id: UUID,
        claim_token: UUID,
        command: AckCancelCommand,
        authorize_queue: Callable[[str], bool] | None = None,
    ) -> AckCancelPersistenceResult:
        """Atomically acknowledge cooperative cancellation with generic terminal replay."""
        existing = self._load_ack_cancel_replay(session, claim_id=claim_id)
        if existing is not None:
            return self._replay_or_conflict_ack_cancel(
                existing, fingerprint=command.fingerprint
            )

        other = self._load_other_terminal_replay_for_ack(session, claim_id=claim_id)
        if other is not None:
            raise DomainValidationError(
                "task_already_terminal",
                "another terminal outcome already committed for this claim",
            )

        fence = validate_current_lease(
            session,
            claim_id=claim_id,
            claim_token=claim_token,
            generation=command.generation,
            for_update=True,
        )
        if fence.decision is FenceDecision.STALE:
            self._raise_stale_fail(session, claim_id=claim_id)
            raise AssertionError("unreachable")

        assert fence.task_id is not None
        assert fence.queue_name is not None
        assert fence.queue_id is not None
        assert fence.claim_id is not None

        if authorize_queue is not None and not authorize_queue(fence.queue_name):
            raise DomainValidationError(
                "permission_denied",
                "permission denied",
            )

        if not fence.cancel_requested:
            raise DomainValidationError(
                "cancel_race_lost",
                "cancellation request is not recorded for this claim",
            )

        now = session.scalar(select(func.transaction_timestamp()))
        if now is None:
            raise DomainValidationError(
                "internal_error",
                "transaction_timestamp() returned null",
            )

        task = session.execute(
            select(TaskActive)
            .where(TaskActive.task_id == fence.task_id)
            .with_for_update()
        ).scalar_one()
        if (
            task.current_claim_id != fence.claim_id
            or int(task.state_code) != _TASK_LEASED
            or int(task.generation) != command.generation
            or task.cancel_requested_at is None
        ):
            raise DomainValidationError(
                "lease_lost",
                "claim is no longer current",
            )

        policy_row = session.get(QueuePolicyVersion, task.retry_policy_version_id)
        if policy_row is None:
            raise DomainValidationError(
                "internal_error",
                "task is missing enqueue-time retry policy version",
            )

        closed = session.execute(
            update(TaskAttempt)
            .where(
                TaskAttempt.task_id == task.task_id,
                TaskAttempt.claim_id == fence.claim_id,
                TaskAttempt.outcome_code == _OUTCOME_ACTIVE,
            )
            .values(
                ended_at=now,
                outcome_code=_OUTCOME_CANCELLED,
                failure_code=None,
                failure_detail=None,
            )
        )
        if closed.rowcount != 1:
            raise DomainValidationError(
                "internal_error",
                "expected exactly one active attempt to close on ack_cancel",
            )

        deleted = session.execute(
            delete(ClaimRegistry).where(ClaimRegistry.claim_id == fence.claim_id)
        )
        if deleted.rowcount != 1:
            raise DomainValidationError(
                "internal_error",
                "expected exactly one claim_registry row to delete on ack_cancel",
            )

        self._cancel_terminal(
            session,
            task=task,
            policy_row=policy_row,
            terminal_at=now,
            prior_state_code=_TASK_LEASED,
        )

        replay = CompleteReplay(
            claim_id=fence.claim_id,
            operation_code=_OP_ACK_CANCEL,
            request_fingerprint=command.fingerprint,
            task_id=fence.task_id,
            result_state_code=_RESULT_CANCELLED,
            available_at=None,
            terminal_at=now,
            spawned_task_ids=[],
            event_ids=[],
            created_at=now,
            expires_at=now + timedelta(days=_REPLAY_TTL_DAYS),
        )
        session.add(replay)
        session.flush()

        return AckCancelPersistenceResult(
            task_id=fence.task_id,
            state="cancelled",
            terminal_at=now,
            replayed=False,
            queue_name=fence.queue_name,
        )

    def _decide_outcome(
        self,
        session: Session,
        *,
        task: TaskActive,
        policy_row: QueuePolicyVersion,
        command: FailCommand,
        queue_now: datetime,
    ) -> tuple[int, int, datetime | None, datetime | None]:
        if not command.retryable:
            return (
                _OUTCOME_DEAD_LETTERED,
                _RESULT_DEAD_LETTERED,
                None,
                queue_now,
            )

        completed = session.scalar(
            select(func.count())
            .select_from(TaskAttempt)
            .where(
                TaskAttempt.task_id == task.task_id,
                TaskAttempt.outcome_code != _OUTCOME_ACTIVE,
            )
        )
        # Include the attempt about to close.
        completed_attempt_count = int(completed or 0) + 1

        if int(policy_row.backoff_strategy_code) != _BACKOFF_FIXED:
            raise DomainValidationError(
                "internal_error",
                f"unsupported backoff_strategy_code={policy_row.backoff_strategy_code}",
            )
        policy = EnqueuedRetryPolicy(
            policy_version=PolicyVersion(int(policy_row.version)),
            enabled=bool(policy_row.enabled),
            max_attempts=int(policy_row.max_attempts),
            backoff_strategy=BackoffStrategy.FIXED,
            retry_delay_seconds=int(policy_row.retry_delay_seconds),
        )
        decision = decide_retry_or_dead_letter(
            policy=policy,
            completed_attempt_count=completed_attempt_count,
            queue_now=queue_now,
            cause=RetryCause.WORKER_FAILURE,
        )
        if isinstance(decision, RetryScheduled):
            assert decision.kind is RetryOutcomeKind.RETRY_SCHEDULED
            return (
                _OUTCOME_RETRY_SCHEDULED,
                _RESULT_RETRY_SCHEDULED,
                decision.available_at,
                None,
            )
        assert isinstance(decision, DeadLettered)
        return (
            _OUTCOME_DEAD_LETTERED,
            _RESULT_DEAD_LETTERED,
            None,
            queue_now,
        )

    def _schedule_retry(
        self,
        session: Session,
        *,
        task: TaskActive,
        available_at: datetime,
        queue_now: datetime,
    ) -> None:
        state_code = _TASK_READY if available_at <= queue_now else _TASK_DELAYED
        task.state_code = state_code
        task.available_at = available_at
        task.current_claim_id = None
        task.claimed_at = None
        task.lease_expires_at = None
        task.worker_id = None
        task.cancel_requested_at = None
        task.updated_at = queue_now
        self._adjust_counters(
            session,
            queue_id=int(task.queue_id),
            at=queue_now,
            leased_delta=-1,
            delayed_delta=1 if state_code == _TASK_DELAYED else 0,
            ready_delta=1 if state_code == _TASK_READY else 0,
        )

    def _dead_letter(
        self,
        session: Session,
        *,
        task: TaskActive,
        policy_row: QueuePolicyVersion,
        command: FailCommand,
        terminal_at: datetime,
    ) -> None:
        payload_row = session.get(TaskPayloadActive, task.id)
        if payload_row is None:
            raise DomainValidationError(
                "internal_error",
                "task is missing active payload row",
            )
        session.add(
            TaskTerminal(
                task_id=task.task_id,
                queue_id=int(task.queue_id),
                producer_id=str(task.producer_id),
                state_code=_RESULT_DEAD_LETTERED,
                priority=int(task.priority),
                available_at=task.available_at,
                retry_policy_version=int(policy_row.version),
                payload=payload_row.payload,
                payload_bytes=int(payload_row.payload_bytes),
                created_at=task.created_at,
                terminal_at=terminal_at,
                failure_code=command.failure_code,
                failure_detail=command.failure_detail,
                source_task_id=task.source_task_id,
                spawn_ordinal=task.spawn_ordinal,
            )
        )
        queue_id = int(task.queue_id)
        session.delete(task)
        session.flush()
        self._adjust_counters(
            session,
            queue_id=queue_id,
            at=terminal_at,
            leased_delta=-1,
            delayed_delta=0,
            ready_delta=0,
        )

    @staticmethod
    def _adjust_counters(
        session: Session,
        *,
        queue_id: int,
        at: datetime,
        leased_delta: int,
        delayed_delta: int,
        ready_delta: int,
    ) -> None:
        counter = session.get(QueueCounter, queue_id)
        if counter is None:
            session.add(
                QueueCounter(
                    queue_id=queue_id,
                    delayed_count=max(0, delayed_delta),
                    ready_count=max(0, ready_delta),
                    leased_count=max(0, leased_delta),
                    as_of=at,
                )
            )
            return
        counter.leased_count = max(0, int(counter.leased_count) + leased_delta)
        counter.delayed_count = max(0, int(counter.delayed_count) + delayed_delta)
        counter.ready_count = max(0, int(counter.ready_count) + ready_delta)
        counter.as_of = at

    @staticmethod
    def _load_fail_replay(
        session: Session,
        *,
        claim_id: UUID,
    ) -> CompleteReplay | None:
        return session.execute(
            select(CompleteReplay).where(
                CompleteReplay.claim_id == claim_id,
                CompleteReplay.operation_code == _OP_FAIL,
            )
        ).scalar_one_or_none()

    @staticmethod
    def _load_ack_cancel_replay(
        session: Session,
        *,
        claim_id: UUID,
    ) -> CompleteReplay | None:
        return session.execute(
            select(CompleteReplay).where(
                CompleteReplay.claim_id == claim_id,
                CompleteReplay.operation_code == _OP_ACK_CANCEL,
            )
        ).scalar_one_or_none()

    @staticmethod
    def _load_other_terminal_replay(
        session: Session,
        *,
        claim_id: UUID,
    ) -> CompleteReplay | None:
        return session.execute(
            select(CompleteReplay).where(
                CompleteReplay.claim_id == claim_id,
                CompleteReplay.operation_code.in_((_OP_COMPLETE, _OP_ACK_CANCEL)),
            )
        ).scalar_one_or_none()

    @staticmethod
    def _load_other_terminal_replay_for_ack(
        session: Session,
        *,
        claim_id: UUID,
    ) -> CompleteReplay | None:
        return session.execute(
            select(CompleteReplay).where(
                CompleteReplay.claim_id == claim_id,
                CompleteReplay.operation_code.in_((_OP_COMPLETE, _OP_FAIL)),
            )
        ).scalar_one_or_none()

    @staticmethod
    def _replay_or_conflict(
        replay: CompleteReplay,
        *,
        fingerprint: bytes,
    ) -> FailPersistenceResult:
        if bytes(replay.request_fingerprint) != fingerprint:
            raise DomainValidationError(
                "idempotency_conflict",
                "fail request fingerprint conflicts with stored replay",
            )
        result_code = int(replay.result_state_code)
        if result_code == _RESULT_RETRY_SCHEDULED:
            assert replay.available_at is not None
            return FailPersistenceResult(
                task_id=replay.task_id,
                state="retry_scheduled",
                available_at=replay.available_at,
                terminal_at=None,
                replayed=True,
                queue_name="",
            )
        if result_code == _RESULT_DEAD_LETTERED:
            assert replay.terminal_at is not None
            return FailPersistenceResult(
                task_id=replay.task_id,
                state="dead_lettered",
                available_at=None,
                terminal_at=replay.terminal_at,
                replayed=True,
                queue_name="",
            )
        raise DomainValidationError(
            "internal_error",
            f"unexpected fail replay result_state_code={result_code}",
        )

    @staticmethod
    def _replay_or_conflict_ack_cancel(
        replay: CompleteReplay,
        *,
        fingerprint: bytes,
    ) -> AckCancelPersistenceResult:
        if bytes(replay.request_fingerprint) != fingerprint:
            raise DomainValidationError(
                "idempotency_conflict",
                "ack_cancel request fingerprint conflicts with stored replay",
            )
        if int(replay.result_state_code) != _RESULT_CANCELLED:
            raise DomainValidationError(
                "internal_error",
                f"unexpected ack_cancel replay result_state_code={replay.result_state_code}",
            )
        assert replay.terminal_at is not None
        return AckCancelPersistenceResult(
            task_id=replay.task_id,
            state="cancelled",
            terminal_at=replay.terminal_at,
            replayed=True,
            queue_name="",
        )

    @staticmethod
    def _raise_stale_fail(session: Session, *, claim_id: UUID) -> None:
        registry = session.execute(
            select(ClaimRegistry).where(ClaimRegistry.claim_id == claim_id)
        ).scalar_one_or_none()
        if registry is not None:
            raise DomainValidationError(
                "lease_lost",
                "claim is no longer current",
            )
        # Superseded/expired reclaim deletes the registry but retains the attempt.
        prior_attempt = session.execute(
            select(TaskAttempt.claim_id)
            .where(TaskAttempt.claim_id == claim_id)
            .limit(1)
        ).scalar_one_or_none()
        if prior_attempt is not None:
            raise DomainValidationError(
                "lease_lost",
                "claim is no longer current",
            )
        raise DomainValidationError(
            "claim_not_found",
            "claim not found",
        )
