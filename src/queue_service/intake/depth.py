"""Transactional queue and instance active-depth reservation.

Reserves depth against ``queue_counters`` rows inside a caller-owned SQLAlchemy
session. Never commits, rolls back, allocates a session, or opens an independent
top-level transaction. Never scans ``tasks_active``. The enqueue service (or
other application UoW owner) supplies the session and performs the sole final
commit or rollback so depth stays atomic with dedup/task/payload staging.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from queue_service.intake.contracts import IntakeValidationError
from queue_service.storage.models import QueueCounter

# admission-control.md Phase 3 hard baseline.
DEFAULT_QUEUE_ACTIVE_DEPTH: Final[int] = 100_000
DEFAULT_INSTANCE_ACTIVE_DEPTH: Final[int] = 500_000
DEFAULT_DEPTH_RETRY_AFTER_MS: Final[int] = 250

# Transaction-scoped advisory lock: serialize instance-depth checks without
# scanning tasks_active. Key is stable and unrelated to queue identity.
_INSTANCE_DEPTH_LOCK_KEY: Final[int] = 734_001_034

_CODE_RESOURCE_EXHAUSTED: Final[str] = "resource_exhausted"
_CODE_VALIDATION_FAILED: Final[str] = "validation_failed"


@dataclass(frozen=True, slots=True)
class DepthCeilings:
    """Independent queue and instance active-depth hard ceilings."""

    queue_active_depth: int = DEFAULT_QUEUE_ACTIVE_DEPTH
    instance_active_depth: int = DEFAULT_INSTANCE_ACTIVE_DEPTH
    retry_after_ms: int = DEFAULT_DEPTH_RETRY_AFTER_MS

    def __post_init__(self) -> None:
        if self.queue_active_depth < 1:
            raise ValueError("queue_active_depth must be >= 1")
        if self.instance_active_depth < 1:
            raise ValueError("instance_active_depth must be >= 1")
        if self.queue_active_depth > DEFAULT_QUEUE_ACTIVE_DEPTH:
            raise ValueError(
                "queue_active_depth cannot exceed deployment hard ceiling "
                f"{DEFAULT_QUEUE_ACTIVE_DEPTH}"
            )
        if self.instance_active_depth > DEFAULT_INSTANCE_ACTIVE_DEPTH:
            raise ValueError(
                "instance_active_depth cannot exceed deployment hard ceiling "
                f"{DEFAULT_INSTANCE_ACTIVE_DEPTH}"
            )
        if self.retry_after_ms < 0:
            raise ValueError("retry_after_ms must be >= 0")

    def with_queue_ceiling(self, runtime_queue_ceiling: int) -> DepthCeilings:
        """Return ceilings with a runtime queue limit that may only tighten."""
        if runtime_queue_ceiling < 1:
            raise ValueError("runtime_queue_ceiling must be >= 1")
        if runtime_queue_ceiling > self.queue_active_depth:
            raise ValueError(
                "runtime queue ceiling cannot raise the deployment hard ceiling"
            )
        return DepthCeilings(
            queue_active_depth=runtime_queue_ceiling,
            instance_active_depth=self.instance_active_depth,
            retry_after_ms=self.retry_after_ms,
        )


@dataclass(frozen=True, slots=True)
class DepthReservation:
    """Outcome of a successful counter reservation inside the caller transaction."""

    queue_id: int
    units: int
    queue_depth_after: int
    instance_depth_after: int


def reserve_active_depth(
    session: Session,
    *,
    queue_id: int,
    units: int = 1,
    delayed: bool = False,
    ceilings: DepthCeilings | None = None,
) -> DepthReservation:
    """Conditionally reserve active depth on ``queue_counters`` and flush.

    Depth is ``delayed_count + ready_count + leased_count``. Immediate producer
    enqueue increments ``ready_count``; delayed work increments ``delayed_count``.

    Raises:
        IntakeValidationError: ``resource_exhausted`` (retryable) when either
            ceiling would be exceeded; ``validation_failed`` for bad inputs.
    """
    if not isinstance(queue_id, int) or isinstance(queue_id, bool) or queue_id < 1:
        raise IntakeValidationError(
            _CODE_VALIDATION_FAILED,
            "queue_id must be a positive integer",
        )
    if not isinstance(units, int) or isinstance(units, bool) or units < 1:
        raise IntakeValidationError(
            _CODE_VALIDATION_FAILED,
            "units must be a positive integer",
        )

    effective = ceilings or DepthCeilings()

    # Serialize instance-wide depth checks without scanning tasks_active.
    session.execute(
        text("SELECT pg_advisory_xact_lock(:lock_key)"),
        {"lock_key": _INSTANCE_DEPTH_LOCK_KEY},
    )

    counter = _lock_or_create_counter(session, queue_id=queue_id)
    queue_depth = int(
        counter.delayed_count + counter.ready_count + counter.leased_count
    )
    instance_depth = _instance_active_depth(session)

    if queue_depth + units > effective.queue_active_depth:
        raise IntakeValidationError(
            _CODE_RESOURCE_EXHAUSTED,
            "queue active-depth ceiling reached",
            retryable=True,
            retry_after_ms=effective.retry_after_ms,
            details={
                "scope": "queue",
                "limit": effective.queue_active_depth,
                "observed": queue_depth,
            },
        )
    if instance_depth + units > effective.instance_active_depth:
        raise IntakeValidationError(
            _CODE_RESOURCE_EXHAUSTED,
            "instance active-depth ceiling reached",
            retryable=True,
            retry_after_ms=effective.retry_after_ms,
            details={
                "scope": "instance",
                "limit": effective.instance_active_depth,
                "observed": instance_depth,
            },
        )

    if delayed:
        counter.delayed_count = int(counter.delayed_count) + units
    else:
        counter.ready_count = int(counter.ready_count) + units
    counter.as_of = func.statement_timestamp()
    session.flush()

    queue_depth_after = int(
        counter.delayed_count + counter.ready_count + counter.leased_count
    )
    instance_depth_after = instance_depth + units
    return DepthReservation(
        queue_id=queue_id,
        units=units,
        queue_depth_after=queue_depth_after,
        instance_depth_after=instance_depth_after,
    )


def _lock_or_create_counter(session: Session, *, queue_id: int) -> QueueCounter:
    existing = session.execute(
        select(QueueCounter)
        .where(QueueCounter.queue_id == queue_id)
        .with_for_update()
    ).scalar_one_or_none()
    if existing is not None:
        return existing

    session.add(
        QueueCounter(
            queue_id=queue_id,
            delayed_count=0,
            ready_count=0,
            leased_count=0,
        )
    )
    session.flush()
    locked = session.execute(
        select(QueueCounter)
        .where(QueueCounter.queue_id == queue_id)
        .with_for_update()
    ).scalar_one()
    return locked


def _instance_active_depth(session: Session) -> int:
    total = session.execute(
        select(
            func.coalesce(
                func.sum(
                    QueueCounter.delayed_count
                    + QueueCounter.ready_count
                    + QueueCounter.leased_count
                ),
                0,
            )
        )
    ).scalar_one()
    return int(total)
