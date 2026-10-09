"""Producer enqueue application service — sole SQLAlchemy unit-of-work owner.

Owns exactly one session/transaction per enqueue call. Flush-only intake
primitives (dedup, depth, staging) never commit or roll back; this service
performs the single final commit on success and rolls the whole transaction
back on any failure or rejected new operation.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from workhold.application.queue_state_gate import evaluate_queue_state_gate
from workhold.domain.queue_control import (
    OperationGateOutcome,
    QueueOperation,
    QueueState,
)
from workhold.intake.admission import (
    EnqueueAdmissionLimits,
    validate_producer_enqueue_admission,
)
from workhold.intake.contracts import (
    EnqueueCommand,
    IntakeValidationError,
    normalize_enqueue_command,
)
from workhold.intake.depth import DepthCeilings, reserve_active_depth
from workhold.intake.repository import (
    DedupScope,
    EnqueuePersistenceResult,
    EnqueueRepository,
)
from workhold.scheduling import SchedulingPolicy, SchedulingValidationError
from workhold.settings import SCHEDULE_HORIZON_SECONDS_DEFAULT
from workhold.storage.models import Queue

if TYPE_CHECKING:
    from workhold.admission.enqueue import AdaptiveEnqueueGate
# Re-exported names exist so tests can prove the service never allocates them.
_ = (create_engine, Session, sessionmaker)

_STATE_BY_CODE: Final[dict[int, QueueState]] = {
    1: QueueState.ACTIVE,
    2: QueueState.PAUSED,
    3: QueueState.DRAINING,
}
_CODE_QUEUE_DRAINING: Final[str] = "queue_draining"
_CODE_INTERNAL: Final[str] = "internal_error"


@dataclass(frozen=True, slots=True)
class EnqueueFaultHooks:
    """Optional test-only seams for proving all-or-nothing rollback."""

    after_depth: Callable[[], None] | None = None
    after_policy_snapshot: Callable[[], None] | None = None
    after_task_flush: Callable[[], None] | None = None
    after_payload_flush: Callable[[], None] | None = None
    after_dedup_flush: Callable[[], None] | None = None


class EnqueueService:
    """Ordered producer-intake use case with one service-owned transaction."""

    def __init__(
        self,
        *,
        session_factory: sessionmaker[Session],
        repository: EnqueueRepository | None = None,
        admission_limits: EnqueueAdmissionLimits | None = None,
        depth_ceilings: DepthCeilings | None = None,
        fault_hooks: EnqueueFaultHooks | None = None,
        adaptive_gate: AdaptiveEnqueueGate | None = None,
        scheduling_policy: SchedulingPolicy | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._repository = repository or EnqueueRepository()
        self._admission_limits = admission_limits
        self._depth_ceilings = depth_ceilings
        self._fault_hooks = fault_hooks or EnqueueFaultHooks()
        self._adaptive_gate = adaptive_gate
        self._scheduling_policy = scheduling_policy or SchedulingPolicy(
            horizon_seconds=SCHEDULE_HORIZON_SECONDS_DEFAULT
        )

    def enqueue(
        self,
        *,
        producer_id: str,
        queue_name: str,
        idempotency_key: str | None,
        body: Mapping[str, Any],
        body_bytes: bytes,
    ) -> EnqueuePersistenceResult:
        """Validate, resolve committed replay, then gate/depth/stage new work.

        Ordering invariants:
        1. Pure admission + command normalization (no SQLAlchemy).
        2. Committed matching replay / fingerprint conflict before state & depth.
        3. New operations: serialize queue state, reject unknown/draining, reserve
           depth, snapshot policy, stage rows, then one service commit.
        """
        validate_producer_enqueue_admission(
            idempotency_key=idempotency_key,
            body=body,
            body_bytes=body_bytes,
            limits=self._admission_limits,
        )
        command = normalize_enqueue_command(
            producer_id=producer_id,
            queue_name=queue_name,
            idempotency_key=idempotency_key,
            payload=body["payload"],
            priority=body.get("priority"),
            available_at=body.get("available_at"),
        )

        session = self._session_factory()
        try:
            result = self._enqueue_in_session(session, command)
            session.commit()
            return result
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def _enqueue_in_session(
        self,
        session: Session,
        command: EnqueueCommand,
    ) -> EnqueuePersistenceResult:
        existing = self._repository.resolve_committed_dedup(session, command)
        if existing is not None:
            return existing

        locked = self._repository.lock_named_queue(session, command.queue_name)
        existing = self._repository.resolve_committed_dedup(
            session,
            command,
            queue=locked,
        )
        if existing is not None:
            return existing

        # Soft adaptive throttle after idempotent replay resolution; never
        # relaxes hard byte/depth ceilings applied below.
        if self._adaptive_gate is not None:
            from workhold.admission.enqueue import check_adaptive_new_enqueue

            check_adaptive_new_enqueue(
                self._adaptive_gate,
                queue_name=command.queue_name,
                is_idempotent_replay=False,
            )

        self._assert_external_enqueue_allowed(locked)
        store_now = EnqueueRepository.transaction_timestamp(session)
        try:
            scheduling_decision = self._scheduling_policy.resolve(
                command.available_at,
                store_now=store_now,
            )
        except SchedulingValidationError as exc:
            raise IntakeValidationError(
                exc.code,
                exc.message,
                retryable=exc.retryable,
                details=exc.details,
            ) from exc

        reserve_active_depth(
            session,
            queue_id=int(locked.id),
            units=1,
            delayed=scheduling_decision.is_delayed,
            ceilings=self._depth_ceilings,
        )
        if self._fault_hooks.after_depth is not None:
            self._fault_hooks.after_depth()

        scope = DedupScope(
            producer_id=command.producer_id,
            queue_id=int(locked.id),
            key_hash=EnqueueRepository.key_hash_for(command.idempotency_key),
        )
        return self._repository.stage_new_under_lock(
            session,
            command,
            locked,
            scope=scope,
            scheduling_decision=scheduling_decision,
            store_now=store_now,
            after_policy_snapshot=self._fault_hooks.after_policy_snapshot,
            after_task_flush=self._fault_hooks.after_task_flush,
            after_payload_flush=self._fault_hooks.after_payload_flush,
            after_dedup_flush=self._fault_hooks.after_dedup_flush,
        )

    @staticmethod
    def _assert_external_enqueue_allowed(queue: Queue) -> None:
        state = _STATE_BY_CODE.get(int(queue.state_code))
        if state is None:
            raise IntakeValidationError(
                _CODE_INTERNAL,
                f"unknown queue state_code={queue.state_code}",
            )
        outcome = evaluate_queue_state_gate(
            state,
            QueueOperation.EXTERNAL_ENQUEUE,
        )
        if outcome is OperationGateOutcome.REJECTED:
            raise IntakeValidationError(
                _CODE_QUEUE_DRAINING,
                "queue is draining; new external enqueue is closed",
                retryable=True,
            )
        if outcome is not OperationGateOutcome.ALLOWED:
            raise IntakeValidationError(
                _CODE_INTERNAL,
                f"unexpected external enqueue gate outcome={outcome.value}",
            )
