"""PostgreSQL atomic Complete-with-spawn persistence (COMP-01 / API-04 lineage)."""

from __future__ import annotations

import json
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Final
from uuid import UUID

from sqlalchemy import delete, func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from queue_service.api.schemas.terminal import CompleteCommand, SpawnCommand
from queue_service.application.queue_state_gate import evaluate_queue_state_gate
from queue_service.delivery.repository import (
    DeliveryEventFaultHooks,
    DeliveryEventRepository,
)
from queue_service.domain.queue_control import (
    DomainValidationError,
    OperationGateOutcome,
    QueueOperation,
    QueueState,
)
from queue_service.intake.depth import DepthCeilings, reserve_active_depth
from queue_service.scheduling import (
    SchedulingDecision,
    SchedulingPolicy,
    SchedulingValidationError,
)
from queue_service.settings import SCHEDULE_HORIZON_SECONDS_DEFAULT
from queue_service.storage.leases import FenceDecision, validate_current_lease
from queue_service.storage.models import (
    ClaimRegistry,
    CompleteReplay,
    CompletionEffect,
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
_OUTCOME_SUCCEEDED: Final[int] = 2

_TASK_DELAYED: Final[int] = 1
_TASK_READY: Final[int] = 2
_TASK_LEASED: Final[int] = 3
_RESULT_SUCCEEDED: Final[int] = 10
_REPLAY_TTL_DAYS: Final[int] = 7

_EFFECT_KIND_SPAWN: Final[int] = 1

_STATE_BY_CODE: Final[dict[int, QueueState]] = {
    1: QueueState.ACTIVE,
    2: QueueState.PAUSED,
    3: QueueState.DRAINING,
}


@dataclass(frozen=True, slots=True)
class CompletePersistenceResult:
    """Committed complete projection after the Queue-store transaction commits."""

    task_id: UUID
    state: str
    spawned_task_ids: tuple[UUID, ...]
    event_ids: tuple[UUID, ...]
    terminal_at: datetime
    replayed: bool
    queue_name: str


@dataclass(frozen=True, slots=True)
class CompleteFaultHooks:
    """Optional test-only seams for proving all-or-nothing rollback."""

    after_attempt_close: Callable[[], None] | None = None
    after_claim_delete: Callable[[], None] | None = None
    after_terminal_insert: Callable[[], None] | None = None
    after_active_delete: Callable[[], None] | None = None
    after_counter_adjust: Callable[[], None] | None = None
    after_spawn_effect: Callable[[], None] | None = None
    after_spawns: Callable[[], None] | None = None
    after_event_effect: Callable[[], None] | None = None
    after_events: Callable[[], None] | None = None
    after_replay_flush: Callable[[], None] | None = None


class CompleteRepository:
    """Short-transaction Complete with ordered spawn[] descendants."""

    def __init__(
        self,
        *,
        depth_ceilings: DepthCeilings | None = None,
        scheduling_policy: SchedulingPolicy | None = None,
    ) -> None:
        self._depth_ceilings = depth_ceilings
        self._scheduling_policy = scheduling_policy or SchedulingPolicy(
            horizon_seconds=SCHEDULE_HORIZON_SECONDS_DEFAULT,
        )

    def complete_claim(
        self,
        session: Session,
        *,
        claim_id: UUID,
        claim_token: UUID,
        command: CompleteCommand,
        authorize_queue: Callable[[str], bool] | None = None,
        fault_hooks: CompleteFaultHooks | None = None,
    ) -> CompletePersistenceResult:
        """Atomically succeed the leased source task and persist ordered spawns."""
        hooks = fault_hooks or CompleteFaultHooks()

        existing = self._load_complete_replay(session, claim_id=claim_id)
        if existing is not None:
            return self._resolve_complete_replay(
                session,
                replay=existing,
                fingerprint=command.fingerprint,
                authorize_queue=authorize_queue,
            )

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
            # After waiting on the claim/task row lock, a concurrent Completer
            # (or another terminal command) may have committed. Re-consult the
            # durable registry so same-body races replay and different Complete
            # fingerprints return exact idempotency_conflict — not lease_lost /
            # a terminal-race code reserved for a distinct winning command.
            existing_after_wait = self._load_complete_replay(
                session, claim_id=claim_id
            )
            if existing_after_wait is not None:
                return self._resolve_complete_replay(
                    session,
                    replay=existing_after_wait,
                    fingerprint=command.fingerprint,
                    authorize_queue=authorize_queue,
                )
            other_after_wait = self._load_other_terminal_replay(
                session, claim_id=claim_id
            )
            if other_after_wait is not None:
                if int(other_after_wait.operation_code) == _OP_ACK_CANCEL:
                    raise DomainValidationError(
                        "cancel_race_lost",
                        "cancellation already recorded for this claim",
                    )
                raise DomainValidationError(
                    "task_already_terminal",
                    "another terminal outcome already committed for this claim",
                )
            self._raise_stale_complete(session, claim_id=claim_id)
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

        spawn_decisions = self._resolve_spawn_schedules(command.spawn, store_now=now)

        closed = session.execute(
            update(TaskAttempt)
            .where(
                TaskAttempt.task_id == task.task_id,
                TaskAttempt.claim_id == fence.claim_id,
                TaskAttempt.outcome_code == _OUTCOME_ACTIVE,
            )
            .values(
                ended_at=now,
                outcome_code=_OUTCOME_SUCCEEDED,
                failure_code=None,
                failure_detail=None,
            )
        )
        if closed.rowcount != 1:
            raise DomainValidationError(
                "internal_error",
                "expected exactly one active attempt to close on complete",
            )
        if hooks.after_attempt_close is not None:
            hooks.after_attempt_close()

        deleted = session.execute(
            delete(ClaimRegistry).where(ClaimRegistry.claim_id == fence.claim_id)
        )
        if deleted.rowcount != 1:
            raise DomainValidationError(
                "internal_error",
                "expected exactly one claim_registry row to delete on complete",
            )
        if hooks.after_claim_delete is not None:
            hooks.after_claim_delete()

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
                state_code=_RESULT_SUCCEEDED,
                priority=int(task.priority),
                available_at=task.available_at,
                retry_policy_version=int(policy_row.version),
                payload=payload_row.payload,
                payload_bytes=int(payload_row.payload_bytes),
                created_at=task.created_at,
                terminal_at=now,
                failure_code=None,
                failure_detail=None,
                source_task_id=task.source_task_id,
                spawn_ordinal=task.spawn_ordinal,
            )
        )
        session.flush()
        if hooks.after_terminal_insert is not None:
            hooks.after_terminal_insert()

        source_task_id = task.task_id
        source_producer_id = str(task.producer_id)
        queue_id = int(task.queue_id)
        session.delete(task)
        session.flush()
        if hooks.after_active_delete is not None:
            hooks.after_active_delete()

        self._adjust_leased_counter(session, queue_id=queue_id, at=now)
        if hooks.after_counter_adjust is not None:
            hooks.after_counter_adjust()

        spawned_task_ids = self._persist_spawns(
            session,
            command=command,
            spawn_decisions=spawn_decisions,
            source_claim_id=fence.claim_id,
            source_task_id=source_task_id,
            source_producer_id=source_producer_id,
            now=now,
            hooks=hooks,
        )
        if hooks.after_spawns is not None:
            hooks.after_spawns()

        event_ids = DeliveryEventRepository().insert_pending_for_complete(
            session,
            source_claim_id=fence.claim_id,
            source_task_id=source_task_id,
            events=command.events,
            now=now,
            fault_hooks=DeliveryEventFaultHooks(
                after_event_effect=hooks.after_event_effect,
            ),
        )
        if hooks.after_events is not None:
            hooks.after_events()

        replay = CompleteReplay(
            claim_id=fence.claim_id,
            operation_code=_OP_COMPLETE,
            request_fingerprint=command.fingerprint,
            task_id=fence.task_id,
            result_state_code=_RESULT_SUCCEEDED,
            available_at=None,
            terminal_at=now,
            spawned_task_ids=list(spawned_task_ids),
            event_ids=list(event_ids),
            created_at=now,
            expires_at=now + timedelta(days=_REPLAY_TTL_DAYS),
        )
        session.add(replay)
        session.flush()
        if hooks.after_replay_flush is not None:
            hooks.after_replay_flush()

        return CompletePersistenceResult(
            task_id=fence.task_id,
            state="succeeded",
            spawned_task_ids=tuple(spawned_task_ids),
            event_ids=tuple(event_ids),
            terminal_at=now,
            replayed=False,
            queue_name=fence.queue_name,
        )

    def _resolve_spawn_schedules(
        self,
        spawn: tuple[SpawnCommand, ...],
        *,
        store_now: datetime,
    ) -> tuple[SchedulingDecision, ...]:
        decisions: list[SchedulingDecision] = []
        for index, item in enumerate(spawn):
            try:
                decisions.append(
                    self._scheduling_policy.resolve(
                        item.available_at,
                        store_now=store_now,
                    )
                )
            except SchedulingValidationError as exc:
                raise DomainValidationError(exc.code, exc.message) from exc
        return tuple(decisions)

    def _persist_spawns(
        self,
        session: Session,
        *,
        command: CompleteCommand,
        spawn_decisions: tuple[SchedulingDecision, ...],
        source_claim_id: UUID,
        source_task_id: UUID,
        source_producer_id: str,
        now: datetime,
        hooks: CompleteFaultHooks,
    ) -> list[UUID]:
        spawned: list[UUID] = []
        for ordinal, (item, decision) in enumerate(
            zip(command.spawn, spawn_decisions, strict=True)
        ):
            spawned.append(
                self._persist_one_spawn(
                    session,
                    item=item,
                    decision=decision,
                    ordinal=ordinal,
                    source_claim_id=source_claim_id,
                    source_task_id=source_task_id,
                    source_producer_id=source_producer_id,
                    now=now,
                    hooks=hooks,
                )
            )
        return spawned

    def _persist_one_spawn(
        self,
        session: Session,
        *,
        item: SpawnCommand,
        decision: SchedulingDecision,
        ordinal: int,
        source_claim_id: UUID,
        source_task_id: UUID,
        source_producer_id: str,
        now: datetime,
        hooks: CompleteFaultHooks,
    ) -> UUID:
        queue = session.execute(
            select(Queue).where(Queue.name == item.queue_name).with_for_update()
        ).scalar_one_or_none()
        if queue is None:
            raise DomainValidationError(
                "queue_not_found",
                "queue not found",
            )
        self._assert_internal_spawn_allowed(queue)
        if queue.active_policy_version_id is None:
            raise DomainValidationError(
                "internal_error",
                "queue has no active retry-policy version",
            )

        reserve_active_depth(
            session,
            queue_id=int(queue.id),
            units=1,
            delayed=decision.is_delayed,
            ceilings=self._depth_ceilings,
        )

        state_code = _TASK_DELAYED if decision.is_delayed else _TASK_READY
        public_task_id = uuid.uuid4()
        payload_bytes = _payload_bytes(item.payload)
        try:
            with session.begin_nested():
                child = TaskActive(
                    task_id=public_task_id,
                    queue_id=int(queue.id),
                    producer_id=source_producer_id,
                    state_code=state_code,
                    priority=item.priority,
                    available_at=decision.available_at,
                    retry_policy_version_id=int(queue.active_policy_version_id),
                    generation=0,
                    source_task_id=source_task_id,
                    spawn_ordinal=ordinal,
                    created_at=now,
                    updated_at=now,
                )
                session.add(child)
                session.flush()
                session.add(
                    TaskPayloadActive(
                        task_id=child.id,
                        payload=item.payload,
                        payload_bytes=payload_bytes,
                    )
                )
                session.add(
                    CompletionEffect(
                        source_claim_id=source_claim_id,
                        effect_kind_code=_EFFECT_KIND_SPAWN,
                        ordinal=ordinal,
                        resource_id=public_task_id,
                        created_at=now,
                    )
                )
                session.flush()
        except IntegrityError as exc:
            raise DomainValidationError(
                "internal_error",
                "completion effect or spawn lineage uniqueness conflict",
            ) from exc

        if hooks.after_spawn_effect is not None:
            hooks.after_spawn_effect()
        return public_task_id

    @staticmethod
    def _assert_internal_spawn_allowed(queue: Queue) -> None:
        state = _STATE_BY_CODE.get(int(queue.state_code))
        if state is None:
            raise DomainValidationError(
                "internal_error",
                f"unknown queue state_code={queue.state_code}",
            )
        outcome = evaluate_queue_state_gate(state, QueueOperation.INTERNAL_SPAWN)
        if outcome is not OperationGateOutcome.ALLOWED:
            raise DomainValidationError(
                "internal_error",
                f"unexpected internal spawn gate outcome={outcome.value}",
            )

    @staticmethod
    def _adjust_leased_counter(
        session: Session,
        *,
        queue_id: int,
        at: datetime,
    ) -> None:
        counter = session.get(QueueCounter, queue_id)
        if counter is None:
            session.add(
                QueueCounter(
                    queue_id=queue_id,
                    delayed_count=0,
                    ready_count=0,
                    leased_count=0,
                    as_of=at,
                )
            )
            return
        counter.leased_count = max(0, int(counter.leased_count) - 1)
        counter.as_of = at


    def _resolve_complete_replay(
        self,
        session: Session,
        *,
        replay: CompleteReplay,
        fingerprint: bytes,
        authorize_queue: Callable[[str], bool] | None,
    ) -> CompletePersistenceResult:
        """Return stored success, conflict, or claim_not_found for expired registry."""
        now = session.scalar(select(func.transaction_timestamp()))
        if now is None:
            raise DomainValidationError(
                "internal_error",
                "transaction_timestamp() returned null",
            )
        if replay.expires_at <= now:
            raise DomainValidationError(
                "claim_not_found",
                "claim not found",
            )
        queue_name = self._queue_name_for_task(session, task_id=replay.task_id)
        if authorize_queue is not None and not authorize_queue(queue_name):
            raise DomainValidationError(
                "permission_denied",
                "permission denied",
            )
        result = self._replay_or_conflict(replay, fingerprint=fingerprint)
        return CompletePersistenceResult(
            task_id=result.task_id,
            state=result.state,
            spawned_task_ids=result.spawned_task_ids,
            event_ids=result.event_ids,
            terminal_at=result.terminal_at,
            replayed=True,
            queue_name=queue_name,
        )

    @staticmethod
    def _queue_name_for_task(session: Session, *, task_id: UUID) -> str:
        row = session.execute(
            select(Queue.name)
            .select_from(TaskTerminal)
            .join(Queue, Queue.id == TaskTerminal.queue_id)
            .where(TaskTerminal.task_id == task_id)
        ).scalar_one_or_none()
        if row is None:
            raise DomainValidationError(
                "internal_error",
                "complete replay is missing terminal queue projection",
            )
        return str(row)

    @staticmethod
    def _load_complete_replay(
        session: Session,
        *,
        claim_id: UUID,
    ) -> CompleteReplay | None:
        return session.execute(
            select(CompleteReplay).where(
                CompleteReplay.claim_id == claim_id,
                CompleteReplay.operation_code == _OP_COMPLETE,
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
                CompleteReplay.operation_code.in_((_OP_FAIL, _OP_ACK_CANCEL)),
            )
        ).scalar_one_or_none()

    @staticmethod
    def _replay_or_conflict(
        replay: CompleteReplay,
        *,
        fingerprint: bytes,
    ) -> CompletePersistenceResult:
        if bytes(replay.request_fingerprint) != fingerprint:
            raise DomainValidationError(
                "idempotency_conflict",
                "complete request fingerprint conflicts with stored replay",
            )
        if int(replay.result_state_code) != _RESULT_SUCCEEDED:
            raise DomainValidationError(
                "internal_error",
                f"unexpected complete replay result_state_code={replay.result_state_code}",
            )
        assert replay.terminal_at is not None
        return CompletePersistenceResult(
            task_id=replay.task_id,
            state="succeeded",
            spawned_task_ids=tuple(replay.spawned_task_ids or ()),
            event_ids=tuple(replay.event_ids or ()),
            terminal_at=replay.terminal_at,
            replayed=True,
            queue_name="",
        )

    @staticmethod
    def _raise_stale_complete(session: Session, *, claim_id: UUID) -> None:
        registry = session.execute(
            select(ClaimRegistry).where(ClaimRegistry.claim_id == claim_id)
        ).scalar_one_or_none()
        if registry is not None:
            raise DomainValidationError(
                "lease_lost",
                "claim is no longer current",
            )
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


def _payload_bytes(payload: Any) -> int:
    return len(
        json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
            sort_keys=True,
        ).encode("utf-8")
    )
