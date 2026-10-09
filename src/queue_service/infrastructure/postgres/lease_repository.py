"""Reusable current-lease fence and atomic heartbeat against PostgreSQL."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Final
from uuid import UUID

from sqlalchemy import and_, func, select, update
from sqlalchemy.orm import Session

from queue_service.domain.queue_control import DomainValidationError
from queue_service.storage.models import ClaimRegistry, Queue, TaskActive

_TASK_LEASED: Final[int] = 3
_LEASE_MIN: Final[int] = 1
_LEASE_MAX: Final[int] = 3600


class FenceDecision(str, Enum):
    """Current-lease authority decision shared by heartbeat and terminal probes."""

    CURRENT = "current"
    STALE = "stale"


@dataclass(frozen=True, slots=True)
class LeaseFenceResult:
    """Outcome of the reusable full-fence validator.

    ``CURRENT`` carries the locked lease snapshot. ``STALE`` carries no row
    fields and must not mutate Queue state. Terminal handlers in later phases
    consume this same decision without a weaker duplicate check.
    """

    decision: FenceDecision
    claim_id: UUID | None = None
    task_id: UUID | None = None
    claim_token: UUID | None = None
    generation: int | None = None
    claimed_at: datetime | None = None
    lease_expires_at: datetime | None = None
    worker_id: str | None = None
    queue_name: str | None = None
    queue_id: int | None = None
    cancel_requested: bool = False
    server_time: datetime | None = None


@dataclass(frozen=True, slots=True)
class HeartbeatPersistenceResult:
    """Committed heartbeat projection after the Queue-store transaction commits."""

    claim_id: UUID
    generation: int
    claimed_at: datetime
    lease_expires_at: datetime
    worker_id: str
    cancel_requested: bool
    server_time: datetime
    queue_name: str
    lease_seconds: int


class LeaseRepository:
    """Short-transaction current-lease fence and heartbeat primitive.

    Callers own the session and must commit before exposing heartbeat results.
    """

    def validate_current_lease(
        self,
        session: Session,
        *,
        claim_id: UUID,
        claim_token: UUID,
        generation: int,
        for_update: bool = True,
    ) -> LeaseFenceResult:
        """Return CURRENT or STALE for the full fence; never mutates rows.

        Fence requires matching ``claim_registry`` and ``tasks_active`` rows with
        equal generation, ``tasks_active.current_claim_id``, state code 3, and
        ``lease_expires_at > transaction_timestamp()``.
        """
        self._validate_generation(generation)
        now = session.scalar(select(func.transaction_timestamp()))
        if now is None:
            raise DomainValidationError(
                "internal_error",
                "transaction_timestamp() returned null",
            )

        stmt = (
            select(ClaimRegistry, TaskActive, Queue)
            .join(TaskActive, TaskActive.task_id == ClaimRegistry.task_id)
            .join(Queue, Queue.id == TaskActive.queue_id)
            .where(
                ClaimRegistry.claim_id == claim_id,
                ClaimRegistry.claim_token == claim_token,
                ClaimRegistry.generation == generation,
                TaskActive.current_claim_id == claim_id,
                TaskActive.generation == generation,
                TaskActive.state_code == _TASK_LEASED,
                TaskActive.lease_expires_at > func.transaction_timestamp(),
                ClaimRegistry.lease_expires_at > func.transaction_timestamp(),
            )
        )
        if for_update:
            stmt = stmt.with_for_update(of=(ClaimRegistry, TaskActive))

        row = session.execute(stmt).one_or_none()
        if row is None:
            return LeaseFenceResult(decision=FenceDecision.STALE, server_time=now)

        registry, task, queue = row
        return LeaseFenceResult(
            decision=FenceDecision.CURRENT,
            claim_id=registry.claim_id,
            task_id=task.task_id,
            claim_token=registry.claim_token,
            generation=int(registry.generation),
            claimed_at=registry.claimed_at,
            lease_expires_at=task.lease_expires_at,
            worker_id=task.worker_id,
            queue_name=str(queue.name),
            queue_id=int(queue.id),
            cancel_requested=task.cancel_requested_at is not None,
            server_time=now,
        )

    def heartbeat(
        self,
        session: Session,
        *,
        claim_id: UUID,
        claim_token: UUID,
        generation: int,
        lease_seconds: int,
        authorize_queue: Callable[[str], bool] | None = None,
    ) -> HeartbeatPersistenceResult:
        """Atomically fence, optionally authorize queue scope, and reset expiry.

        Raises ``DomainValidationError(lease_lost)`` when the fence is stale and
        ``permission_denied`` when ``authorize_queue`` rejects the named queue.
        Preserves claim identity, token, generation, claimed_at, worker_id, and
        attempt count; does not append attempts or rotate credentials.
        """
        self._validate_lease_seconds(lease_seconds)
        fence = self.validate_current_lease(
            session,
            claim_id=claim_id,
            claim_token=claim_token,
            generation=generation,
            for_update=True,
        )
        if fence.decision is FenceDecision.STALE:
            raise DomainValidationError(
                "lease_lost",
                "claim is no longer current",
            )

        assert fence.claim_id is not None
        assert fence.task_id is not None
        assert fence.generation is not None
        assert fence.claimed_at is not None
        assert fence.worker_id is not None
        assert fence.queue_name is not None

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
        new_expiry = now + timedelta(seconds=lease_seconds)

        task_updated = session.execute(
            update(TaskActive)
            .where(
                and_(
                    TaskActive.task_id == fence.task_id,
                    TaskActive.current_claim_id == fence.claim_id,
                    TaskActive.generation == fence.generation,
                    TaskActive.state_code == _TASK_LEASED,
                    TaskActive.lease_expires_at > func.transaction_timestamp(),
                )
            )
            .values(
                lease_expires_at=new_expiry,
                updated_at=now,
            )
        )
        registry_updated = session.execute(
            update(ClaimRegistry)
            .where(
                and_(
                    ClaimRegistry.claim_id == fence.claim_id,
                    ClaimRegistry.claim_token == claim_token,
                    ClaimRegistry.generation == fence.generation,
                    ClaimRegistry.lease_expires_at > func.transaction_timestamp(),
                )
            )
            .values(lease_expires_at=new_expiry)
        )
        if task_updated.rowcount != 1 or registry_updated.rowcount != 1:
            raise DomainValidationError(
                "lease_lost",
                "claim is no longer current",
            )

        session.flush()
        return HeartbeatPersistenceResult(
            claim_id=fence.claim_id,
            generation=fence.generation,
            claimed_at=fence.claimed_at,
            lease_expires_at=new_expiry,
            worker_id=fence.worker_id,
            cancel_requested=fence.cancel_requested,
            server_time=now,
            queue_name=fence.queue_name,
            lease_seconds=lease_seconds,
        )

    @staticmethod
    def _validate_generation(generation: int) -> None:
        if type(generation) is not int or isinstance(generation, bool) or generation < 1:
            raise DomainValidationError(
                "validation_failed",
                "generation must be an integer >= 1",
            )

    @staticmethod
    def _validate_lease_seconds(lease_seconds: int) -> None:
        if (
            type(lease_seconds) is not int
            or isinstance(lease_seconds, bool)
            or not (_LEASE_MIN <= lease_seconds <= _LEASE_MAX)
        ):
            raise DomainValidationError(
                "validation_failed",
                "lease_seconds is outside the deployment hard ceiling",
            )
