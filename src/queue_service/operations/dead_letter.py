"""Single-task dead-letter replay (CTRL-06 / REC-01 / OPS-08).

Creates one new linked active task from an immutable dead-lettered terminal
row. Idempotency uses ``admin_replay`` operation_code=6. Never mutates source
history, never issues a claim token, and never promises exactly-once effects.
"""

from __future__ import annotations

import hashlib
import json
import logging
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Final
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from queue_service.application.queue_state_gate import evaluate_queue_state_gate
from queue_service.domain.queue_control import (
    DomainValidationError,
    OperationGateOutcome,
    QueueOperation,
    QueueState,
)
from queue_service.intake.contracts import IntakeValidationError
from queue_service.intake.depth import DepthCeilings, reserve_active_depth
from queue_service.observability.context import emit_correlation, project_correlation
from queue_service.security.payload_policy import (
    DEFAULT_PAYLOAD_BYTES,
    HARD_PAYLOAD_CEILING_BYTES,
)
from queue_service.storage.models import (
    AdminAuditLog,
    AdminReplay,
    Queue,
    QueuePolicyVersion,
    TaskActive,
    TaskAttempt,
    TaskPayloadActive,
    TaskTerminal,
)

logger = logging.getLogger(__name__)

_AUDIT_OP_REPLAY_DEAD_LETTER: Final[int] = 6
_ADMIN_REPLAY_OP_REPLAY_DEAD_LETTER: Final[int] = 6
_TERMINAL_DEAD_LETTERED: Final[int] = 11
_TASK_READY: Final[int] = 2

_REASON_MIN_LEN: Final[int] = 1
_REASON_MAX_LEN: Final[int] = 512
_AT_LEAST_ONCE_WARNING: Final[str] = (
    "Replay is at-least-once and may repeat external effects."
)

_STATE_BY_CODE: Final[dict[int, QueueState]] = {
    1: QueueState.ACTIVE,
    2: QueueState.PAUSED,
    3: QueueState.DRAINING,
}

_DEAD_LETTER_CORRELATION_KEYS: Final[frozenset[str]] = frozenset(
    {
        "request_id",
        "trace_id",
        "actor_id",
        "operation",
        "queue",
        "task_id",
        "source_task_id",
        "policy_version",
        "result",
        "code",
    }
)


@dataclass(frozen=True, slots=True)
class DeadLetterReplayResult:
    """Outcome of a single dead-letter replay inside one Queue transaction."""

    task_id: UUID
    source_task_id: UUID
    queue_name: str
    policy_version: int
    replayed: bool
    admin_replay_expires_at: datetime | None
    warning: str = _AT_LEAST_ONCE_WARNING


def _sha256_utf8(value: str) -> bytes:
    return hashlib.sha256(value.encode("utf-8")).digest()


def _format_dt(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _request_fingerprint(*, queue_name: str, source_task_id: UUID, reason: str) -> bytes:
    canonical = json.dumps(
        {
            "queue_name": queue_name,
            "source_task_id": str(source_task_id),
            "reason": reason,
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return _sha256_utf8(canonical)


def validate_replay_reason(reason: object) -> str:
    """Return a bounded non-empty reason or raise ``validation_failed``."""
    if not isinstance(reason, str):
        raise DomainValidationError("validation_failed", "reason must be a string")
    stripped = reason.strip()
    if not (_REASON_MIN_LEN <= len(stripped) <= _REASON_MAX_LEN):
        raise DomainValidationError(
            "validation_failed",
            f"reason length must be between {_REASON_MIN_LEN} and {_REASON_MAX_LEN}",
        )
    return stripped


def project_dead_letter_admin_correlation(
    *,
    operation: str,
    request_id: str | None,
    trace_id: str | None,
    actor_id: str | None,
    queue: str | None,
    task_id: str | None,
    source_task_id: str | None,
    policy_version: int | None,
    result: str,
    code: str | None,
    extras: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Project dead-letter replay diagnostics through the shared allowlist."""
    fields: dict[str, Any] = {
        "request_id": request_id,
        "trace_id": trace_id,
        "actor_id": actor_id,
        "operation": operation,
        "queue": queue,
        "task_id": task_id,
        "source_task_id": source_task_id,
        "policy_version": policy_version,
        "result": result,
        "code": code,
    }
    polluted: dict[str, Any] = dict(extras or {})
    polluted.update(fields)
    projected = project_correlation(polluted)
    return {k: v for k, v in projected.items() if k in _DEAD_LETTER_CORRELATION_KEYS}


def emit_dead_letter_admin_correlation(
    logger_: logging.Logger,
    *,
    operation: str,
    request_id: str | None,
    trace_id: str | None,
    actor_id: str | None,
    queue: str | None,
    task_id: str | None,
    source_task_id: str | None,
    policy_version: int | None,
    result: str,
    code: str | None,
    extras: Mapping[str, Any] | None = None,
    span: Any | None = None,
) -> dict[str, Any]:
    """Project then emit dead-letter replay correlation to logs and optional spans."""
    projected = project_dead_letter_admin_correlation(
        operation=operation,
        request_id=request_id,
        trace_id=trace_id,
        actor_id=actor_id,
        queue=queue,
        task_id=task_id,
        source_task_id=source_task_id,
        policy_version=policy_version,
        result=result,
        code=code,
        extras=extras,
    )
    return emit_correlation(logger_, "dead_letter_admin", projected, span=span)


def _lookup_admin_replay(
    session: Session,
    *,
    principal_id: str,
    key_hash: bytes,
    fingerprint: bytes,
) -> dict[str, Any] | None:
    existing = session.execute(
        select(AdminReplay).where(
            AdminReplay.admin_principal_id == principal_id,
            AdminReplay.operation_code == _ADMIN_REPLAY_OP_REPLAY_DEAD_LETTER,
            AdminReplay.key_hash == key_hash,
        )
    ).scalar_one_or_none()
    if existing is None:
        return None
    if bytes(existing.request_fingerprint) != fingerprint:
        raise DomainValidationError(
            "idempotency_conflict",
            "idempotency key reused with a different request fingerprint",
        )
    body = dict(existing.response_body)
    body["replayed"] = True
    return body


def _store_admin_replay(
    session: Session,
    *,
    principal_id: str,
    key_hash: bytes,
    fingerprint: bytes,
    response_body: Mapping[str, Any],
    admin_replay_ttl_seconds: int,
    created_at: datetime,
) -> datetime:
    expires_at = created_at + timedelta(seconds=admin_replay_ttl_seconds)
    session.add(
        AdminReplay(
            admin_principal_id=principal_id,
            operation_code=_ADMIN_REPLAY_OP_REPLAY_DEAD_LETTER,
            key_hash=key_hash,
            request_fingerprint=fingerprint,
            http_status=200,
            response_body=dict(response_body),
            created_at=created_at,
            expires_at=expires_at,
        )
    )
    return expires_at


def _next_spawn_ordinal(session: Session, *, source_task_id: UUID) -> int:
    active_max = session.execute(
        select(func.max(TaskActive.spawn_ordinal)).where(
            TaskActive.source_task_id == source_task_id
        )
    ).scalar_one()
    terminal_max = session.execute(
        select(func.max(TaskTerminal.spawn_ordinal)).where(
            TaskTerminal.source_task_id == source_task_id
        )
    ).scalar_one()
    candidates = [v for v in (active_max, terminal_max) if v is not None]
    if not candidates:
        return 0
    return int(max(candidates)) + 1


def _assert_external_enqueue_allowed(queue: Queue) -> None:
    state = _STATE_BY_CODE.get(int(queue.state_code))
    if state is None:
        raise DomainValidationError(
            "internal_error",
            f"unknown queue state_code={queue.state_code}",
        )
    outcome = evaluate_queue_state_gate(state, QueueOperation.EXTERNAL_ENQUEUE)
    if outcome is OperationGateOutcome.REJECTED:
        raise DomainValidationError(
            "queue_draining",
            "queue is draining; dead-letter replay is closed",
        )
    if outcome is not OperationGateOutcome.ALLOWED:
        raise DomainValidationError(
            "internal_error",
            f"unexpected dead-letter replay gate outcome={outcome.value}",
        )


def _load_source_dead_letter(
    session: Session,
    *,
    queue: Queue,
    source_task_id: UUID,
) -> TaskTerminal:
    row = session.execute(
        select(TaskTerminal)
        .where(
            TaskTerminal.task_id == source_task_id,
            TaskTerminal.queue_id == int(queue.id),
            TaskTerminal.state_code == _TERMINAL_DEAD_LETTERED,
        )
        .order_by(TaskTerminal.terminal_at.desc())
        .limit(1)
    ).scalar_one_or_none()
    if row is None:
        raise DomainValidationError("task_not_found", "dead-letter task not found")
    return row


def _snapshot_source_immutability(
    session: Session,
    *,
    source: TaskTerminal,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Capture source terminal + attempts for post-commit byte-equality checks."""
    terminal_snap = {
        "task_id": str(source.task_id),
        "queue_id": int(source.queue_id),
        "producer_id": str(source.producer_id),
        "state_code": int(source.state_code),
        "priority": int(source.priority),
        "retry_policy_version": int(source.retry_policy_version),
        "payload": dict(source.payload),
        "payload_bytes": int(source.payload_bytes),
        "failure_code": source.failure_code,
        "failure_detail": source.failure_detail,
        "source_task_id": (
            None if source.source_task_id is None else str(source.source_task_id)
        ),
        "spawn_ordinal": (
            None if source.spawn_ordinal is None else int(source.spawn_ordinal)
        ),
        "terminal_at": source.terminal_at,
        "created_at": source.created_at,
        "available_at": source.available_at,
    }
    attempts = list(
        session.execute(
            select(TaskAttempt)
            .where(TaskAttempt.task_id == source.task_id)
            .order_by(TaskAttempt.claimed_at, TaskAttempt.id)
        ).scalars()
    )
    attempt_snaps = [
        {
            "id": int(a.id),
            "claim_id": str(a.claim_id),
            "generation": int(a.generation),
            "claimed_at": a.claimed_at,
            "worker_id": str(a.worker_id),
            "lease_expires_at": a.lease_expires_at,
            "ended_at": a.ended_at,
            "outcome_code": int(a.outcome_code),
            "failure_code": a.failure_code,
            "failure_detail": a.failure_detail,
        }
        for a in attempts
    ]
    return terminal_snap, attempt_snaps


def assert_source_unchanged(
    session: Session,
    *,
    source_task_id: UUID,
    terminal_snapshot: Mapping[str, Any],
    attempt_snapshots: list[Mapping[str, Any]],
) -> None:
    """Raise if the immutable dead-letter source or attempts drifted."""
    row = session.execute(
        select(TaskTerminal)
        .where(TaskTerminal.task_id == source_task_id)
        .order_by(TaskTerminal.terminal_at.desc())
        .limit(1)
    ).scalar_one_or_none()
    if row is None:
        raise DomainValidationError("internal_error", "source terminal disappeared")
    current, current_attempts = _snapshot_source_immutability(session, source=row)
    if current != dict(terminal_snapshot):
        raise DomainValidationError(
            "internal_error",
            "dead-letter source terminal mutated during replay",
        )
    if current_attempts != [dict(a) for a in attempt_snapshots]:
        raise DomainValidationError(
            "internal_error",
            "dead-letter source attempts mutated during replay",
        )


def replay_dead_letter(
    session: Session,
    *,
    queue_name: str,
    source_task_id: UUID,
    reason: str,
    principal_id: str,
    actor_id: str,
    request_id: str,
    idempotency_key: str,
    admin_replay_ttl_seconds: int,
    max_payload_bytes: int = DEFAULT_PAYLOAD_BYTES,
    depth_ceilings: DepthCeilings | None = None,
) -> DeadLetterReplayResult:
    """Replay one dead letter as a new linked task in the caller's transaction.

    Ordering: resolve admin replay → lock queue → validate source → gate/depth →
    stage task/payload/lineage → audit + admin_replay. Caller commits once.
    """
    bounded_reason = validate_replay_reason(reason)
    if not (1 <= len(idempotency_key) <= 256):
        raise DomainValidationError(
            "validation_failed",
            "Idempotency-Key length must be between 1 and 256",
        )
    if not (1 <= max_payload_bytes <= HARD_PAYLOAD_CEILING_BYTES):
        raise DomainValidationError(
            "internal_error",
            "max_payload_bytes outside hard payload ceiling",
        )

    key_hash = _sha256_utf8(idempotency_key)
    fingerprint = _request_fingerprint(
        queue_name=queue_name,
        source_task_id=source_task_id,
        reason=bounded_reason,
    )

    replayed_body = _lookup_admin_replay(
        session,
        principal_id=principal_id,
        key_hash=key_hash,
        fingerprint=fingerprint,
    )
    if replayed_body is not None:
        return DeadLetterReplayResult(
            task_id=UUID(str(replayed_body["task_id"])),
            source_task_id=UUID(str(replayed_body["source_task_id"])),
            queue_name=str(replayed_body["queue"]),
            policy_version=int(replayed_body["policy_version"]),
            replayed=True,
            admin_replay_expires_at=None,
            warning=str(replayed_body.get("warning") or _AT_LEAST_ONCE_WARNING),
        )

    queue = session.execute(
        select(Queue).where(Queue.name == queue_name).with_for_update()
    ).scalar_one_or_none()
    if queue is None:
        raise DomainValidationError("queue_not_found", "queue not found")

    # Re-check replay under the queue lock (same key may race).
    replayed_body = _lookup_admin_replay(
        session,
        principal_id=principal_id,
        key_hash=key_hash,
        fingerprint=fingerprint,
    )
    if replayed_body is not None:
        return DeadLetterReplayResult(
            task_id=UUID(str(replayed_body["task_id"])),
            source_task_id=UUID(str(replayed_body["source_task_id"])),
            queue_name=str(replayed_body["queue"]),
            policy_version=int(replayed_body["policy_version"]),
            replayed=True,
            admin_replay_expires_at=None,
            warning=str(replayed_body.get("warning") or _AT_LEAST_ONCE_WARNING),
        )

    source = _load_source_dead_letter(
        session, queue=queue, source_task_id=source_task_id
    )
    terminal_snap, attempt_snaps = _snapshot_source_immutability(session, source=source)

    _assert_external_enqueue_allowed(queue)
    if queue.active_policy_version_id is None:
        raise DomainValidationError(
            "internal_error",
            "queue has no active retry-policy version",
        )

    payload = dict(source.payload)
    payload_bytes = int(source.payload_bytes)
    if payload_bytes < 1 or payload_bytes > max_payload_bytes:
        raise DomainValidationError(
            "payload_too_large",
            "dead-letter payload exceeds configured payload limit",
        )

    store_now = session.scalar(select(func.statement_timestamp()))
    if store_now is None:
        raise DomainValidationError(
            "internal_error",
            "Queue-store statement_timestamp() is unavailable",
        )

    try:
        reserve_active_depth(
            session,
            queue_id=int(queue.id),
            units=1,
            delayed=False,
            ceilings=depth_ceilings,
        )
    except IntakeValidationError as exc:
        raise DomainValidationError(exc.code, exc.message) from exc

    policy_row = session.get(QueuePolicyVersion, int(queue.active_policy_version_id))
    if policy_row is None:
        raise DomainValidationError(
            "internal_error",
            "active retry-policy version row missing",
        )
    policy_version = int(policy_row.version)
    policy_version_id = int(policy_row.id)

    public_task_id = uuid.uuid4()
    spawn_ordinal = _next_spawn_ordinal(session, source_task_id=source.task_id)

    active = TaskActive(
        task_id=public_task_id,
        queue_id=int(queue.id),
        producer_id=str(source.producer_id),
        state_code=_TASK_READY,
        priority=int(source.priority),
        available_at=store_now,
        retry_policy_version_id=policy_version_id,
        generation=0,
        source_task_id=source.task_id,
        spawn_ordinal=spawn_ordinal,
        created_at=store_now,
        updated_at=store_now,
    )
    session.add(active)
    session.flush()
    session.add(
        TaskPayloadActive(
            task_id=active.id,
            payload=payload,
            payload_bytes=payload_bytes,
        )
    )

    response_body = {
        "task_id": str(public_task_id),
        "source_task_id": str(source.task_id),
        "queue": queue_name,
        "policy_version": policy_version,
        "replayed": False,
        "admin_replay_expires_at": None,
        "warning": _AT_LEAST_ONCE_WARNING,
    }
    expires_at = _store_admin_replay(
        session,
        principal_id=principal_id,
        key_hash=key_hash,
        fingerprint=fingerprint,
        response_body=response_body,
        admin_replay_ttl_seconds=admin_replay_ttl_seconds,
        created_at=store_now,
    )
    response_body["admin_replay_expires_at"] = _format_dt(expires_at)
    # Patch stored body with concrete expires_at for future replays.
    stored = session.execute(
        select(AdminReplay).where(
            AdminReplay.admin_principal_id == principal_id,
            AdminReplay.operation_code == _ADMIN_REPLAY_OP_REPLAY_DEAD_LETTER,
            AdminReplay.key_hash == key_hash,
        )
    ).scalar_one()
    stored.response_body = dict(response_body)

    session.add(
        AdminAuditLog(
            audit_at=store_now,
            queue_id=int(queue.id),
            actor_id=actor_id,
            operation_code=_AUDIT_OP_REPLAY_DEAD_LETTER,
            previous_config_version=None,
            new_config_version=None,
            request_id=uuid.UUID(request_id),
            details={
                "source_task_id": str(source.task_id),
                "task_id": str(public_task_id),
                "policy_version": policy_version,
                "spawn_ordinal": spawn_ordinal,
                "reason": bounded_reason,
                "outcome": "succeeded",
            },
        )
    )
    session.flush()

    # Source must remain byte-stable through staging (no in-place rewrite).
    assert_source_unchanged(
        session,
        source_task_id=source.task_id,
        terminal_snapshot=terminal_snap,
        attempt_snapshots=attempt_snaps,
    )

    return DeadLetterReplayResult(
        task_id=public_task_id,
        source_task_id=source.task_id,
        queue_name=queue_name,
        policy_version=policy_version,
        replayed=False,
        admin_replay_expires_at=expires_at,
        warning=_AT_LEAST_ONCE_WARNING,
    )
