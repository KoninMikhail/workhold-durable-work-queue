"""Allowlisted break-glass emergency repairs (REC-03 / OPS-08).

Separate short-lived role, mandatory incident/reason/risk acknowledgement, and
stronger immutable audit. Never issues claim tokens, never mutates terminal
history in place, never deletes audit, and never exposes SQL/credentials.
"""

from __future__ import annotations

import hashlib
import logging
import re
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Final
from uuid import UUID

from sqlalchemy import func, select, text, update
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.orm import Session

from workhold.delivery.repository import DeliveryEventRepository
from workhold.domain.queue_control import DomainValidationError
from workhold.infrastructure.postgres import partition_catalog
from workhold.infrastructure.postgres.history_retention import (
    _detach_and_drop,
    _is_fully_expired,
    _require_no_open_transaction,
    _store_utc_now,
)
from workhold.infrastructure.postgres.task_transitions import (
    ExpiryPersistenceResult,
    TaskTransitionRepository,
)
from workhold.observability.context import emit_correlation, project_correlation
from workhold.observability.metrics import KernelMetrics
from workhold.operations.bulk import BulkReplayRateGate
from workhold.security.payload_policy import PayloadRetentionPolicy
from workhold.storage.models import (
    AdminAuditLog,
    BreakGlassElevation,
    ClaimRegistry,
    DeliveryEventActive,
    EnqueueDedup,
    Queue,
    QueueCounter,
    TaskActive,
    TaskTerminal,
)

logger = logging.getLogger(__name__)

_AUDIT_FORCE_LEASE_EXPIRY: Final[int] = 9
_AUDIT_RECONCILE_COUNTERS: Final[int] = 10
_AUDIT_RAISE_REPLAY_LIMIT: Final[int] = 11
_AUDIT_DROP_EXPIRED_PARTITION: Final[int] = 12
_AUDIT_REPAIR_REGISTRY: Final[int] = 13
# Phase 14 delivery break-glass (RESEARCH: codes 9–13 used → 14+)
_AUDIT_FORCE_DELIVERY_RECLAIM: Final[int] = 14
_AUDIT_FORCE_DELIVERY_DEAD_LETTER: Final[int] = 15

_STATE_DELAYED: Final[int] = 1
_STATE_READY: Final[int] = 2
_STATE_LEASED: Final[int] = 3
_STATE_PAUSED: Final[int] = 2

_REASON_MIN: Final[int] = 1
_REASON_MAX: Final[int] = 512
_INCIDENT_MIN: Final[int] = 1
_INCIDENT_MAX: Final[int] = 128
_PARTITION_NAME_RE: Final[re.Pattern[str]] = re.compile(
    r"^[a-z][a-z0-9_]*_[0-9]{8}$"
)

_BREAK_GLASS_CORRELATION_KEYS: Final[frozenset[str]] = frozenset(
    {
        "request_id",
        "trace_id",
        "actor_id",
        "operation",
        "queue",
        "incident_ref_hash",
        "target_id",
        "generation",
        "result",
        "code",
    }
)


@dataclass(frozen=True, slots=True)
class BreakGlassAck:
    """Mandatory acknowledgement fields for every break-glass request."""

    reason: str
    incident_reference: str
    risk_acknowledged: bool


@dataclass(frozen=True, slots=True)
class BreakGlassResult:
    """Bounded success projection; never includes secrets or repair values."""

    operation: str
    queue: str | None
    target_id: str
    outcome: str
    generation: int | None = None
    effective_rps: float | None = None
    delayed_count: int | None = None
    ready_count: int | None = None
    leased_count: int | None = None


def _hash_incident(incident_reference: str) -> str:
    digest = hashlib.sha256(incident_reference.encode("utf-8")).hexdigest()
    return digest[:16]


def _bound_text(value: object, *, field: str, min_len: int, max_len: int) -> str:
    if not isinstance(value, str):
        raise DomainValidationError("validation_failed", f"{field} must be a string")
    cleaned = value.strip()
    if len(cleaned) < min_len or len(cleaned) > max_len:
        raise DomainValidationError(
            "validation_failed",
            f"{field} length must be between {min_len} and {max_len}",
        )
    return cleaned


def parse_break_glass_ack(body: Mapping[str, Any]) -> BreakGlassAck:
    """Validate shared break-glass acknowledgement fields."""
    reason = _bound_text(
        body.get("reason"), field="reason", min_len=_REASON_MIN, max_len=_REASON_MAX
    )
    incident = _bound_text(
        body.get("incident_reference"),
        field="incident_reference",
        min_len=_INCIDENT_MIN,
        max_len=_INCIDENT_MAX,
    )
    ack = body.get("risk_acknowledged")
    if ack is not True:
        raise DomainValidationError(
            "validation_failed",
            "risk_acknowledged must be true",
        )
    return BreakGlassAck(
        reason=reason,
        incident_reference=incident,
        risk_acknowledged=True,
    )


def project_break_glass_correlation(
    *,
    operation: str,
    request_id: str | None,
    trace_id: str | None,
    actor_id: str | None,
    queue: str | None,
    incident_ref_hash: str | None,
    target_id: str | None,
    result: str,
    code: str | None,
    generation: int | None = None,
    extras: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Project break-glass diagnostics through the shared OPS-08 allowlist."""
    fields: dict[str, Any] = {
        "request_id": request_id,
        "trace_id": trace_id,
        "actor_id": actor_id,
        "operation": operation,
        "queue": queue,
        "incident_ref_hash": incident_ref_hash,
        "target_id": target_id,
        "result": result,
        "code": code,
    }
    if generation is not None:
        fields["generation"] = generation
    polluted: dict[str, Any] = dict(extras or {})
    polluted.update(fields)
    projected = project_correlation(polluted)
    return {k: v for k, v in projected.items() if k in _BREAK_GLASS_CORRELATION_KEYS}


def emit_break_glass_correlation(
    logger_: logging.Logger,
    *,
    operation: str,
    request_id: str | None,
    trace_id: str | None,
    actor_id: str | None,
    queue: str | None,
    incident_ref_hash: str | None,
    target_id: str | None,
    result: str,
    code: str | None,
    generation: int | None = None,
    extras: Mapping[str, Any] | None = None,
    span: Any | None = None,
) -> dict[str, Any]:
    """Project then emit break-glass correlation to logs and optional spans."""
    projected = project_break_glass_correlation(
        operation=operation,
        request_id=request_id,
        trace_id=trace_id,
        actor_id=actor_id,
        queue=queue,
        incident_ref_hash=incident_ref_hash,
        target_id=target_id,
        result=result,
        code=code,
        generation=generation,
        extras=extras,
    )
    return emit_correlation(logger_, "break_glass_admin", projected, span=span)


def record_break_glass_success(
    metrics: KernelMetrics | None,
    *,
    operation: str,
    result: str,
    queue: str | None = None,
) -> None:
    """OPS-09 loud-use counter after a committed break-glass mutation (D-07/D-08).

    Additive to OPS-08 correlation. Labels never include reason/incident/token/payload.
    """
    if metrics is None:
        return
    metrics.record_break_glass(operation=operation, result=result, queue=queue)


def _require_queue(session: Session, queue_name: str) -> Queue:
    queue = session.execute(
        select(Queue).where(Queue.name == queue_name).with_for_update()
    ).scalar_one_or_none()
    if queue is None:
        raise DomainValidationError("queue_not_found", "queue not found")
    return queue


def _store_now(session: Session) -> datetime:
    now = session.scalar(select(func.transaction_timestamp()))
    if now is None:
        raise DomainValidationError("internal_error", "transaction_timestamp() null")
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    return now


def _write_audit(
    session: Session,
    *,
    audit_code: int,
    actor_id: str,
    request_id: str,
    queue_id: int | None,
    store_now: datetime,
    details: Mapping[str, Any],
) -> None:
    session.add(
        AdminAuditLog(
            audit_at=store_now,
            queue_id=queue_id,
            actor_id=actor_id,
            operation_code=audit_code,
            previous_config_version=None,
            new_config_version=None,
            request_id=UUID(request_id),
            details=dict(details),
        )
    )
    session.flush()


def _age_lease_for_force_expiry(session: Session, *, task: TaskActive) -> None:
    """Age lease under Queue-store time so fencing-aware expiry can run."""
    claim_id = task.current_claim_id
    if claim_id is None:
        raise DomainValidationError(
            "validation_failed",
            "leased task is missing current_claim_id",
        )
    # Satisfy claim_registry CHECK (lease_expires_at > claimed_at) while ensuring
    # lease_expires_at < transaction_timestamp() for expire_locked_lease.
    session.execute(
        update(ClaimRegistry)
        .where(ClaimRegistry.claim_id == claim_id)
        .values(
            lease_expires_at=ClaimRegistry.claimed_at + text("interval '1 second'")
        )
    )
    session.execute(
        update(TaskActive)
        .where(TaskActive.task_id == task.task_id)
        .values(
            lease_expires_at=func.transaction_timestamp() - text("interval '1 second'")
        )
    )
    session.flush()
    session.refresh(task)


def force_lease_expiry(
    session: Session,
    *,
    queue_name: str,
    task_id: UUID,
    actor_id: str,
    request_id: str,
    ack: BreakGlassAck,
    repository: TaskTransitionRepository | None = None,
) -> BreakGlassResult:
    """Force-expire a stuck lease via fencing-aware reclaim path; no new claim."""
    queue = _require_queue(session, queue_name)
    task = session.execute(
        select(TaskActive)
        .where(TaskActive.task_id == task_id, TaskActive.queue_id == queue.id)
        .with_for_update()
    ).scalar_one_or_none()
    if task is None:
        raise DomainValidationError("task_not_found", "task not found")
    if int(task.state_code) != _STATE_LEASED:
        raise DomainValidationError(
            "validation_failed",
            "task is not leased",
        )
    generation_before = int(task.generation)
    _age_lease_for_force_expiry(session, task=task)
    repo = repository if repository is not None else TaskTransitionRepository()
    result: ExpiryPersistenceResult = repo.expire_locked_lease(session, task=task)
    store_now = _store_now(session)
    incident_hash = _hash_incident(ack.incident_reference)
    _write_audit(
        session,
        audit_code=_AUDIT_FORCE_LEASE_EXPIRY,
        actor_id=actor_id,
        request_id=request_id,
        queue_id=int(queue.id),
        store_now=store_now,
        details={
            "operation": "force_lease_expiry",
            "task_id": str(task_id),
            "generation": generation_before,
            "expiry_state": result.state,
            "incident_reference": ack.incident_reference,
            "incident_ref_hash": incident_hash,
            "reason": ack.reason,
            "risk_acknowledged": True,
            "claim_token_issued": False,
        },
    )
    return BreakGlassResult(
        operation="forceLeaseExpiry",
        queue=queue_name,
        target_id=str(task_id),
        outcome=result.state,
        generation=generation_before,
    )


def reconcile_counters(
    session: Session,
    *,
    queue_name: str,
    actor_id: str,
    request_id: str,
    ack: BreakGlassAck,
) -> BreakGlassResult:
    """Reconcile non-authoritative queue_counters from tasks_active."""
    queue = _require_queue(session, queue_name)
    delayed = int(
        session.scalar(
            select(func.count())
            .select_from(TaskActive)
            .where(
                TaskActive.queue_id == queue.id,
                TaskActive.state_code == _STATE_DELAYED,
            )
        )
        or 0
    )
    ready = int(
        session.scalar(
            select(func.count())
            .select_from(TaskActive)
            .where(
                TaskActive.queue_id == queue.id,
                TaskActive.state_code == _STATE_READY,
            )
        )
        or 0
    )
    leased = int(
        session.scalar(
            select(func.count())
            .select_from(TaskActive)
            .where(
                TaskActive.queue_id == queue.id,
                TaskActive.state_code == _STATE_LEASED,
            )
        )
        or 0
    )
    counter = session.execute(
        select(QueueCounter)
        .where(QueueCounter.queue_id == queue.id)
        .with_for_update()
    ).scalar_one_or_none()
    if counter is None:
        counter = QueueCounter(
            queue_id=int(queue.id),
            delayed_count=delayed,
            ready_count=ready,
            leased_count=leased,
        )
        session.add(counter)
    else:
        counter.delayed_count = delayed
        counter.ready_count = ready
        counter.leased_count = leased
        counter.as_of = func.statement_timestamp()
    store_now = _store_now(session)
    incident_hash = _hash_incident(ack.incident_reference)
    _write_audit(
        session,
        audit_code=_AUDIT_RECONCILE_COUNTERS,
        actor_id=actor_id,
        request_id=request_id,
        queue_id=int(queue.id),
        store_now=store_now,
        details={
            "operation": "reconcile_counters",
            "delayed_count": delayed,
            "ready_count": ready,
            "leased_count": leased,
            "incident_reference": ack.incident_reference,
            "incident_ref_hash": incident_hash,
            "reason": ack.reason,
            "risk_acknowledged": True,
        },
    )
    session.flush()
    return BreakGlassResult(
        operation="reconcileCounters",
        queue=queue_name,
        target_id=queue_name,
        outcome="reconciled",
        delayed_count=delayed,
        ready_count=ready,
        leased_count=leased,
    )


def raise_replay_limit(
    session: Session,
    *,
    queue_name: str,
    actor_id: str,
    request_id: str,
    ack: BreakGlassAck,
    rate_gate: BulkReplayRateGate,
    factor: float,
    ttl_seconds: int,
) -> BreakGlassResult:
    """Temporarily raise replay rate for one queue (durable TTL + local cache).

    Persists ``break_glass_elevations`` so every API worker honors the raise until
    Queue-store ``expires_at`` (D-10..D-12). Still applies process-local
    ``temporarily_raise`` for the raising worker. Audit semantics unchanged.
    """
    queue = _require_queue(session, queue_name)
    effective = rate_gate.temporarily_raise(
        queue_name, factor=factor, ttl_seconds=ttl_seconds
    )
    store_now = _store_now(session)
    expires_at = store_now + timedelta(seconds=int(ttl_seconds))
    incident_hash = _hash_incident(ack.incident_reference)
    existing = session.execute(
        select(BreakGlassElevation).where(
            BreakGlassElevation.queue_name == queue_name
        )
    ).scalar_one_or_none()
    if existing is None:
        session.add(
            BreakGlassElevation(
                queue_name=queue_name,
                factor=float(factor),
                expires_at=expires_at,
                actor_id=actor_id,
                incident_ref_hash=incident_hash,
                raised_at=store_now,
            )
        )
    else:
        existing.factor = float(factor)
        existing.expires_at = expires_at
        existing.actor_id = actor_id
        existing.incident_ref_hash = incident_hash
        existing.raised_at = store_now
    session.flush()
    _write_audit(
        session,
        audit_code=_AUDIT_RAISE_REPLAY_LIMIT,
        actor_id=actor_id,
        request_id=request_id,
        queue_id=int(queue.id),
        store_now=store_now,
        details={
            "operation": "raise_replay_limit",
            "factor": factor,
            "ttl_seconds": ttl_seconds,
            "effective_rps": effective,
            "expires_at": expires_at.isoformat(),
            "incident_reference": ack.incident_reference,
            "incident_ref_hash": incident_hash,
            "reason": ack.reason,
            "risk_acknowledged": True,
            "persistent_hard_limit_changed": False,
        },
    )
    return BreakGlassResult(
        operation="raiseReplayLimit",
        queue=queue_name,
        target_id=queue_name,
        outcome="raised",
        effective_rps=effective,
    )


def drop_expired_partition(
    engine: Engine,
    session: Session,
    *,
    partition_name: str,
    actor_id: str,
    request_id: str,
    ack: BreakGlassAck,
    payload_retention_policy: PayloadRetentionPolicy,
) -> BreakGlassResult:
    """Force-drop one explicitly named already-expired history partition."""
    if not isinstance(partition_name, str) or not _PARTITION_NAME_RE.fullmatch(
        partition_name
    ):
        raise DomainValidationError(
            "validation_failed",
            "partition_name must be an explicit history child name",
        )
    # Audit first in the business session, then perform catalog drop outside txn.
    store_now = _store_now(session)
    incident_hash = _hash_incident(ack.incident_reference)
    _write_audit(
        session,
        audit_code=_AUDIT_DROP_EXPIRED_PARTITION,
        actor_id=actor_id,
        request_id=request_id,
        queue_id=None,
        store_now=store_now,
        details={
            "operation": "drop_expired_partition",
            "partition_name_hash": hashlib.sha256(
                partition_name.encode("utf-8")
            ).hexdigest()[:16],
            "incident_reference": ack.incident_reference,
            "incident_ref_hash": incident_hash,
            "reason": ack.reason,
            "risk_acknowledged": True,
        },
    )
    session.commit()

    connection = engine.connect()
    try:
        _require_no_open_transaction(connection)
        autocommit = connection.execution_options(isolation_level="AUTOCOMMIT")
        parents = partition_catalog.inspect_history_parents(autocommit)
        existing = partition_catalog.list_existing_children(autocommit)
        matched: tuple[str, Any] | None = None
        for parent_name, children in existing.items():
            if parent_name not in parents:
                continue
            for day, spec in children.items():
                if spec.child_name == partition_name:
                    matched = (parent_name, day, spec)
                    break
            if matched is not None:
                break
        if matched is None:
            raise DomainValidationError(
                "task_not_found",
                "partition not found",
            )
        parent_name, day, spec = matched
        store_utc = _store_utc_now(autocommit)
        if not _is_fully_expired(
            payload_retention_policy, store_now=store_utc, bound_to=spec.bound_to
        ):
            raise DomainValidationError(
                "validation_failed",
                "partition is not fully expired",
            )
        _detach_and_drop(
            autocommit,
            parent_name=parent_name,
            child_name=spec.child_name,
            day=day,
        )
    finally:
        connection.close()

    return BreakGlassResult(
        operation="dropExpiredPartition",
        queue=None,
        target_id=partition_name,
        outcome="dropped",
    )


def repair_registry_entry(
    session: Session,
    *,
    queue_name: str,
    actor_id: str,
    request_id: str,
    ack: BreakGlassAck,
    registry: str,
    entry_id: int,
    extend_seconds: int,
    acknowledge_duplicate_window: bool,
) -> BreakGlassResult:
    """Repair one enqueue_dedup registry row by widening its expiry window.

    Requires the named queue to be paused and explicit duplicate-window ack.
    """
    if acknowledge_duplicate_window is not True:
        raise DomainValidationError(
            "validation_failed",
            "acknowledge_duplicate_window must be true",
        )
    if registry != "enqueue_dedup":
        raise DomainValidationError(
            "validation_failed",
            "only enqueue_dedup registry repair is supported",
        )
    if not isinstance(entry_id, int) or isinstance(entry_id, bool) or entry_id < 1:
        raise DomainValidationError("validation_failed", "entry_id must be positive")
    if (
        not isinstance(extend_seconds, int)
        or isinstance(extend_seconds, bool)
        or extend_seconds < 1
        or extend_seconds > 86400 * 30
    ):
        raise DomainValidationError(
            "validation_failed",
            "extend_seconds must be between 1 and 2592000",
        )

    queue = _require_queue(session, queue_name)
    if int(queue.state_code) != _STATE_PAUSED:
        raise DomainValidationError(
            "validation_failed",
            "queue must be paused for registry repair",
        )

    row = session.execute(
        select(EnqueueDedup)
        .where(
            EnqueueDedup.id == entry_id,
            EnqueueDedup.queue_id == queue.id,
        )
        .with_for_update()
    ).scalar_one_or_none()
    if row is None:
        raise DomainValidationError("task_not_found", "registry entry not found")

    created = row.created_at
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)
    current_expires = row.expires_at
    if current_expires.tzinfo is None:
        current_expires = current_expires.replace(tzinfo=timezone.utc)
    proposed = current_expires + timedelta(seconds=extend_seconds)
    max_expires = created + timedelta(days=365)
    min_expires = created + timedelta(days=30)
    if proposed > max_expires:
        proposed = max_expires
    if proposed < min_expires:
        proposed = min_expires
    if proposed <= current_expires:
        raise DomainValidationError(
            "validation_failed",
            "registry expiry cannot be extended within bounds",
        )
    row.expires_at = proposed
    store_now = _store_now(session)
    incident_hash = _hash_incident(ack.incident_reference)
    _write_audit(
        session,
        audit_code=_AUDIT_REPAIR_REGISTRY,
        actor_id=actor_id,
        request_id=request_id,
        queue_id=int(queue.id),
        store_now=store_now,
        details={
            "operation": "repair_registry_entry",
            "registry": "enqueue_dedup",
            "entry_id": entry_id,
            "extend_seconds": extend_seconds,
            "acknowledge_duplicate_window": True,
            "incident_reference": ack.incident_reference,
            "incident_ref_hash": incident_hash,
            "reason": ack.reason,
            "risk_acknowledged": True,
            # Never store raw key_hash / fingerprint / repair values.
        },
    )
    session.flush()
    return BreakGlassResult(
        operation="repairRegistryEntry",
        queue=queue_name,
        target_id=str(entry_id),
        outcome="repaired",
    )


def _require_delivery_event_in_queue(
    session: Session,
    *,
    queue: Queue,
    event_id: UUID,
) -> DeliveryEventActive:
    """Lock active delivery event and verify its source task belongs to queue."""
    active = session.execute(
        select(DeliveryEventActive)
        .where(DeliveryEventActive.event_id == event_id)
        .with_for_update()
    ).scalar_one_or_none()
    if active is None:
        raise DomainValidationError("task_not_found", "delivery event not found")
    source_task_id = active.source_task_id
    in_active = session.execute(
        select(TaskActive.task_id).where(
            TaskActive.task_id == source_task_id,
            TaskActive.queue_id == queue.id,
        )
    ).scalar_one_or_none()
    if in_active is not None:
        return active
    in_terminal = session.execute(
        select(TaskTerminal.task_id).where(
            TaskTerminal.task_id == source_task_id,
            TaskTerminal.queue_id == queue.id,
        )
    ).scalar_one_or_none()
    if in_terminal is not None:
        return active
    raise DomainValidationError("task_not_found", "delivery event not found")


def force_delivery_reclaim(
    session: Session,
    *,
    queue_name: str,
    event_id: UUID,
    actor_id: str,
    request_id: str,
    ack: BreakGlassAck,
    repository: DeliveryEventRepository | None = None,
) -> BreakGlassResult:
    """Force reclaim stuck publishing delivery to pending without a claim token.

    Preserves ``generation`` and ``delivery_attempt`` (D-04 / D-06).
    """
    queue = _require_queue(session, queue_name)
    # Pre-lock + queue scope check before mutation helper re-locks the row.
    _require_delivery_event_in_queue(session, queue=queue, event_id=event_id)
    repo = repository if repository is not None else DeliveryEventRepository()
    generation, _attempt = repo.force_reclaim_to_pending(session, event_id=event_id)
    store_now = _store_now(session)
    incident_hash = _hash_incident(ack.incident_reference)
    _write_audit(
        session,
        audit_code=_AUDIT_FORCE_DELIVERY_RECLAIM,
        actor_id=actor_id,
        request_id=request_id,
        queue_id=int(queue.id),
        store_now=store_now,
        details={
            "operation": "force_delivery_reclaim",
            "event_id": str(event_id),
            "generation": generation,
            "incident_reference": ack.incident_reference,
            "incident_ref_hash": incident_hash,
            "reason": ack.reason,
            "risk_acknowledged": True,
            "claim_token_issued": False,
        },
    )
    return BreakGlassResult(
        operation="forceDeliveryReclaim",
        queue=queue_name,
        target_id=str(event_id),
        outcome="reclaimed",
        generation=generation,
    )


def force_delivery_dead_letter(
    session: Session,
    *,
    queue_name: str,
    event_id: UUID,
    actor_id: str,
    request_id: str,
    ack: BreakGlassAck,
    failure_code: str,
    repository: DeliveryEventRepository | None = None,
) -> BreakGlassResult:
    """Force terminal dead-letter of an active delivery event without a claim token.

    Retains generation/attempt on the terminal row per delivery model (D-04).
    """
    queue = _require_queue(session, queue_name)
    _require_delivery_event_in_queue(session, queue=queue, event_id=event_id)
    repo = repository if repository is not None else DeliveryEventRepository()
    generation, _attempt = repo.force_dead_letter(
        session, event_id=event_id, failure_code=failure_code
    )
    store_now = _store_now(session)
    incident_hash = _hash_incident(ack.incident_reference)
    _write_audit(
        session,
        audit_code=_AUDIT_FORCE_DELIVERY_DEAD_LETTER,
        actor_id=actor_id,
        request_id=request_id,
        queue_id=int(queue.id),
        store_now=store_now,
        details={
            "operation": "force_delivery_dead_letter",
            "event_id": str(event_id),
            "generation": generation,
            "failure_code": failure_code,
            "incident_reference": ack.incident_reference,
            "incident_ref_hash": incident_hash,
            "reason": ack.reason,
            "risk_acknowledged": True,
            "claim_token_issued": False,
        },
    )
    return BreakGlassResult(
        operation="forceDeliveryDeadLetter",
        queue=queue_name,
        target_id=str(event_id),
        outcome="dead_lettered",
        generation=generation,
    )
