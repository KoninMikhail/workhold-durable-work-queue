"""Session-bound PostgreSQL enqueue and dedup primitives (flush-only).

Callers own the SQLAlchemy session/transaction. Methods flush when PostgreSQL
must assign identities or evaluate uniqueness; they never commit or roll back.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Final
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from workhold.intake.contracts import EnqueueCommand, IntakeValidationError
from workhold.scheduling import (
    SchedulingDecision,
    SchedulingPolicy,
    SchedulingValidationError,
)
from workhold.settings import SCHEDULE_HORIZON_SECONDS_DEFAULT
from workhold.storage.models import (
    EnqueueDedup,
    Queue,
    TaskActive,
    TaskPayloadActive,
)

_STATE_READY: Final[int] = 2
_STATE_DELAYED: Final[int] = 1
_DEDUP_TTL: Final[timedelta] = timedelta(days=90)
_DEDUP_UNIQUE: Final[str] = "enqueue_dedup_producer_id_queue_id_key_hash_key"
_CODE_IDEMPOTENCY_CONFLICT: Final[str] = "idempotency_conflict"
_CODE_QUEUE_NOT_FOUND: Final[str] = "queue_not_found"
_CODE_INTERNAL: Final[str] = "internal_error"


@dataclass(frozen=True, slots=True)
class DedupScope:
    """Producer + queue + hashed idempotency key uniqueness scope."""

    producer_id: str
    queue_id: int
    key_hash: bytes


@dataclass(frozen=True, slots=True)
class EnqueuePersistenceResult:
    """Outcome of staging or replaying an enqueue inside a caller transaction."""

    task_id: UUID
    replayed: bool
    retry_policy_version_id: int


def _probe_dedup(session: Session, scope: DedupScope) -> EnqueueDedup | None:
    """Lookup the correctness-registry row for a scoped idempotency key.

    Single initial-lookup seam used before and after the queue-row lock; tests
    may wrap this to synchronize concurrent miss observations.
    """
    return session.execute(
        select(EnqueueDedup).where(
            EnqueueDedup.producer_id == scope.producer_id,
            EnqueueDedup.queue_id == scope.queue_id,
            EnqueueDedup.key_hash == scope.key_hash,
        )
    ).scalar_one_or_none()


def _key_hash(idempotency_key: str) -> bytes:
    return hashlib.sha256(idempotency_key.encode("utf-8")).digest()


def _payload_bytes(payload: Any) -> int:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
        sort_keys=True,
    ).encode("utf-8")
    return len(encoded)


class EnqueueRepository:
    """Flush-only enqueue staging against a caller-owned SQLAlchemy session."""

    @staticmethod
    def transaction_timestamp(session: Session) -> datetime:
        """Return the Queue-store timestamp for the caller's open transaction."""
        store_now = session.scalar(select(func.statement_timestamp()))
        if store_now is None:
            raise IntakeValidationError(
                _CODE_INTERNAL,
                "Queue-store statement_timestamp() is unavailable",
            )
        return store_now

    @staticmethod
    def key_hash_for(idempotency_key: str) -> bytes:
        """SHA-256 digest used as ``enqueue_dedup.key_hash``."""
        return _key_hash(idempotency_key)

    def resolve_committed_dedup(
        self,
        session: Session,
        command: EnqueueCommand,
        *,
        queue: Queue | None = None,
    ) -> EnqueuePersistenceResult | None:
        """Return matching replay/conflict for a committed dedup row, else None.

        Never commits or rolls back. Does not evaluate draining or depth gates —
        callers must apply those only after a miss.
        """
        resolved_queue = queue or self._load_queue_by_name(session, command.queue_name)
        scope = DedupScope(
            producer_id=command.producer_id,
            queue_id=int(resolved_queue.id),
            key_hash=_key_hash(command.idempotency_key),
        )
        existing = _probe_dedup(session, scope)
        if existing is None:
            return None
        return self._replay_or_conflict(session, existing, command.fingerprint)

    def lock_named_queue(self, session: Session, queue_name: str) -> Queue:
        """Serialize on the named queue row (``FOR UPDATE``). Flush-only."""
        queue = session.execute(
            select(Queue).where(Queue.name == queue_name).with_for_update()
        ).scalar_one_or_none()
        if queue is None:
            raise IntakeValidationError(
                _CODE_QUEUE_NOT_FOUND,
                "queue not found",
            )
        return queue

    def stage_enqueue(
        self,
        session: Session,
        command: EnqueueCommand,
    ) -> EnqueuePersistenceResult:
        """Stage a new task or return a matching replay inside the caller's UoW.

        Never commits or rolls back. Flushes only when identifiers or uniqueness
        constraints must be evaluated by PostgreSQL.
        """
        existing = self.resolve_committed_dedup(session, command)
        if existing is not None:
            return existing

        locked = self.lock_named_queue(session, command.queue_name)
        existing = self.resolve_committed_dedup(session, command, queue=locked)
        if existing is not None:
            return existing

        scope = DedupScope(
            producer_id=command.producer_id,
            queue_id=int(locked.id),
            key_hash=_key_hash(command.idempotency_key),
        )
        store_now = self.transaction_timestamp(session)
        try:
            scheduling_decision = SchedulingPolicy(
                horizon_seconds=SCHEDULE_HORIZON_SECONDS_DEFAULT
            ).resolve(command.available_at, store_now=store_now)
        except SchedulingValidationError as exc:
            raise IntakeValidationError(
                exc.code,
                exc.message,
                retryable=exc.retryable,
                details=exc.details,
            ) from exc
        return self.stage_new_under_lock(
            session,
            command,
            locked,
            scope=scope,
            scheduling_decision=scheduling_decision,
            store_now=store_now,
        )

    def stage_new_under_lock(
        self,
        session: Session,
        command: EnqueueCommand,
        queue: Queue,
        *,
        scope: DedupScope | None = None,
        scheduling_decision: SchedulingDecision,
        store_now: datetime,
        after_policy_snapshot: Callable[[], None] | None = None,
        after_task_flush: Callable[[], None] | None = None,
        after_payload_flush: Callable[[], None] | None = None,
        after_dedup_flush: Callable[[], None] | None = None,
    ) -> EnqueuePersistenceResult:
        """Stage task/payload/dedup under a caller-held queue lock.

        Never commits or rolls back. Optional callables are test seams only.
        """
        effective_scope = scope or DedupScope(
            producer_id=command.producer_id,
            queue_id=int(queue.id),
            key_hash=_key_hash(command.idempotency_key),
        )
        if queue.active_policy_version_id is None:
            raise IntakeValidationError(
                _CODE_INTERNAL,
                "queue has no active retry-policy version",
            )
        policy_version_id = int(queue.active_policy_version_id)
        if after_policy_snapshot is not None:
            after_policy_snapshot()

        available_at = scheduling_decision.available_at
        state_code = _STATE_DELAYED if scheduling_decision.is_delayed else _STATE_READY
        if command.available_at is None:
            assert available_at == store_now
        elif scheduling_decision.is_delayed:
            assert available_at == command.available_at
        else:
            assert available_at == command.available_at
        public_task_id = uuid.uuid4()
        expires_at = store_now + _DEDUP_TTL
        payload_bytes = _payload_bytes(command.payload)

        try:
            with session.begin_nested():
                task = TaskActive(
                    task_id=public_task_id,
                    queue_id=queue.id,
                    producer_id=command.producer_id,
                    state_code=state_code,
                    priority=command.priority,
                    available_at=available_at,
                    retry_policy_version_id=policy_version_id,
                    generation=0,
                    created_at=store_now,
                    updated_at=store_now,
                )
                session.add(task)
                session.flush()
                if after_task_flush is not None:
                    after_task_flush()

                session.add(
                    TaskPayloadActive(
                        task_id=task.id,
                        payload=command.payload,
                        payload_bytes=payload_bytes,
                    )
                )
                session.flush()
                if after_payload_flush is not None:
                    after_payload_flush()

                session.add(
                    EnqueueDedup(
                        producer_id=effective_scope.producer_id,
                        queue_id=effective_scope.queue_id,
                        key_hash=effective_scope.key_hash,
                        request_fingerprint=command.fingerprint,
                        task_id=public_task_id,
                        created_at=store_now,
                        expires_at=expires_at,
                    )
                )
                session.flush()
                if after_dedup_flush is not None:
                    after_dedup_flush()
        except IntegrityError as exc:
            return self._recover_uniqueness_conflict(
                session,
                scope=effective_scope,
                fingerprint=command.fingerprint,
                exc=exc,
            )

        return EnqueuePersistenceResult(
            task_id=public_task_id,
            replayed=False,
            retry_policy_version_id=policy_version_id,
        )

    def _recover_uniqueness_conflict(
        self,
        session: Session,
        *,
        scope: DedupScope,
        fingerprint: bytes,
        exc: IntegrityError,
    ) -> EnqueuePersistenceResult:
        if not self._is_dedup_unique_violation(exc):
            raise
        existing = _probe_dedup(session, scope)
        if existing is None:
            raise IntakeValidationError(
                _CODE_INTERNAL,
                "dedup uniqueness conflict without visible winner row",
            ) from exc
        return self._replay_or_conflict(session, existing, fingerprint)

    def _replay_or_conflict(
        self,
        session: Session,
        existing: EnqueueDedup,
        fingerprint: bytes,
    ) -> EnqueuePersistenceResult:
        if existing.request_fingerprint != fingerprint:
            raise IntakeValidationError(
                _CODE_IDEMPOTENCY_CONFLICT,
                "idempotency key reused with a different request fingerprint",
            )
        policy_id = self._policy_id_for_task(session, existing.task_id)
        return EnqueuePersistenceResult(
            task_id=existing.task_id,
            replayed=True,
            retry_policy_version_id=policy_id,
        )

    @staticmethod
    def _policy_id_for_task(session: Session, public_task_id: UUID) -> int:
        task = session.execute(
            select(TaskActive).where(TaskActive.task_id == public_task_id)
        ).scalar_one_or_none()
        if task is None:
            raise IntakeValidationError(
                _CODE_INTERNAL,
                "dedup row references a missing active task",
            )
        return int(task.retry_policy_version_id)

    @staticmethod
    def _load_queue_by_name(session: Session, queue_name: str) -> Queue:
        queue = session.execute(
            select(Queue).where(Queue.name == queue_name)
        ).scalar_one_or_none()
        if queue is None:
            raise IntakeValidationError(
                _CODE_QUEUE_NOT_FOUND,
                "queue not found",
            )
        return queue

    @staticmethod
    def _is_dedup_unique_violation(exc: IntegrityError) -> bool:
        orig = getattr(exc, "orig", None)
        diag = getattr(orig, "diag", None)
        constraint = getattr(diag, "constraint_name", None) if diag is not None else None
        if constraint == _DEDUP_UNIQUE:
            return True
        message = str(orig) if orig is not None else str(exc)
        return _DEDUP_UNIQUE in message
