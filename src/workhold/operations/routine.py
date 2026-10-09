"""Routine drain and partition-maintenance operator tools (CTRL-06 / OPS-08).

Wires Phase 3.3 optimistic queue-state control and Phase 3.8 advisory-locked
maintainer without duplicating transitions or bypassing the lock. Admin logs and
traces share Plan 01 ``project_correlation`` (no payload / claim-token / DSN /
SQL leakage).
"""

from __future__ import annotations

import hashlib
import logging
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Final, Literal

from sqlalchemy import func, select, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.orm import Session

from workhold.domain.queue_control import (
    DomainValidationError,
    QueueState,
    SetQueueStateMutation,
)
from workhold.health import DEFAULT_PARTITION_PREMAKE_DAYS
from workhold.infrastructure.postgres.maintenance import (
    StorageMaintenanceReport,
    run_storage_maintenance,
)
from workhold.infrastructure.postgres.queue_control_repository import (
    QueueConfiguration,
    QueueControlRepository,
)
from workhold.observability.context import emit_correlation, project_correlation
from workhold.roles.maintain import MAINTENANCE_LOCK_KEY
from workhold.security.payload_policy import PayloadRetentionPolicy
from workhold.storage.models import AdminAuditLog, AdminReplay, Queue, QueueCounter

_AUDIT_OP_RUN_MAINTENANCE: Final[int] = 5
_ADMIN_REPLAY_OP_RUN_MAINTENANCE: Final[int] = 5
_TRY_LOCK_SQL = text("SELECT pg_try_advisory_lock(:key)")
_UNLOCK_SQL = text("SELECT pg_advisory_unlock(:key)")

MaintenanceTriggerOutcome = Literal["succeeded", "skipped_lock", "failed", "replayed"]

_ROUTINE_CORRELATION_KEYS: Final[frozenset[str]] = frozenset(
    {
        "request_id",
        "trace_id",
        "actor_id",
        "operation",
        "queue",
        "config_version",
        "maintenance_run_id",
        "result",
        "code",
    }
)


@dataclass(frozen=True, slots=True)
class DrainProgress:
    """Bounded drain observation from keyed ``queue_counters``."""

    state: QueueState
    config_version: int
    active_depth: int
    ready_count: int
    delayed_count: int
    leased_count: int
    drain_complete: bool


@dataclass(frozen=True, slots=True)
class MaintenanceTriggerResult:
    """Outcome of an admin maintenance trigger (single-winner)."""

    outcome: MaintenanceTriggerOutcome
    status: dict[str, Any]
    replayed: bool
    admin_replay_expires_at: datetime | None
    maintenance_run_id: str | None
    error_code: str | None = None


def _sha256_utf8(value: str) -> bytes:
    return hashlib.sha256(value.encode("utf-8")).digest()


def _format_dt(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _format_date(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def project_routine_admin_correlation(
    *,
    operation: str,
    request_id: str | None,
    trace_id: str | None,
    actor_id: str | None,
    queue: str | None,
    config_version: int | None,
    maintenance_run_id: str | None,
    result: str,
    code: str | None,
    extras: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Project drain/maintenance admin diagnostics through the shared allowlist."""
    fields: dict[str, Any] = {
        "request_id": request_id,
        "trace_id": trace_id,
        "actor_id": actor_id,
        "operation": operation,
        "queue": queue,
        "config_version": config_version,
        "maintenance_run_id": maintenance_run_id,
        "result": result,
        "code": code,
    }
    polluted: dict[str, Any] = dict(extras or {})
    polluted.update(fields)
    projected = project_correlation(polluted)
    return {k: v for k, v in projected.items() if k in _ROUTINE_CORRELATION_KEYS}


def emit_routine_admin_correlation(
    logger: logging.Logger,
    *,
    operation: str,
    request_id: str | None,
    trace_id: str | None,
    actor_id: str | None,
    queue: str | None,
    config_version: int | None,
    maintenance_run_id: str | None,
    result: str,
    code: str | None,
    extras: Mapping[str, Any] | None = None,
    span: Any | None = None,
) -> dict[str, Any]:
    """Project then emit routine admin correlation to logs and optional spans."""
    projected = project_routine_admin_correlation(
        operation=operation,
        request_id=request_id,
        trace_id=trace_id,
        actor_id=actor_id,
        queue=queue,
        config_version=config_version,
        maintenance_run_id=maintenance_run_id,
        result=result,
        code=code,
        extras=extras,
    )
    return emit_correlation(logger, "routine_admin", projected, span=span)


def read_drain_progress(
    session: Session,
    *,
    queue_name: str,
) -> DrainProgress:
    """Return drain progress for ``queue_name`` from state + counters."""
    row = session.execute(
        select(
            Queue.state_code,
            Queue.config_version,
            QueueCounter.ready_count,
            QueueCounter.delayed_count,
            QueueCounter.leased_count,
        )
        .select_from(Queue)
        .outerjoin(QueueCounter, QueueCounter.queue_id == Queue.id)
        .where(Queue.name == queue_name)
    ).one_or_none()
    if row is None:
        raise DomainValidationError("queue_not_found", "queue not found")

    state_code, config_version, ready, delayed, leased = row
    state = {
        1: QueueState.ACTIVE,
        2: QueueState.PAUSED,
        3: QueueState.DRAINING,
    }.get(int(state_code))
    if state is None:
        raise DomainValidationError(
            "internal_error",
            f"unknown queue state_code={state_code}",
        )
    ready_i = int(ready or 0)
    delayed_i = int(delayed or 0)
    leased_i = int(leased or 0)
    active_depth = ready_i + delayed_i + leased_i
    return DrainProgress(
        state=state,
        config_version=int(config_version),
        active_depth=active_depth,
        ready_count=ready_i,
        delayed_count=delayed_i,
        leased_count=leased_i,
        drain_complete=state is QueueState.DRAINING and active_depth == 0,
    )


def enrich_queue_response(
    base: dict[str, Any],
    progress: DrainProgress,
) -> dict[str, Any]:
    """Attach bounded drain progress fields to a Queue JSON projection."""
    out = dict(base)
    out["active_depth"] = progress.active_depth
    out["ready_count"] = progress.ready_count
    out["delayed_count"] = progress.delayed_count
    out["leased_count"] = progress.leased_count
    out["drain_complete"] = progress.drain_complete
    return out


def start_drain(
    session: Session,
    *,
    repository: QueueControlRepository,
    queue_name: str,
    mutation: SetQueueStateMutation,
) -> tuple[QueueConfiguration, DrainProgress]:
    """Transition a named queue to draining under optimistic config concurrency."""
    if mutation.state is not QueueState.DRAINING:
        raise DomainValidationError(
            "validation_failed",
            "start_drain requires state=draining",
        )
    config = repository.set_queue_state(
        session,
        queue_name=queue_name,
        mutation=mutation,
    )
    progress = read_drain_progress(session, queue_name=queue_name)
    return config, progress


def read_maintenance_status(session: Session) -> dict[str, Any]:
    """Project singleton ``partition_maintenance_status`` (no error_detail)."""
    row = session.execute(
        text(
            """
            SELECT
                last_started_at,
                last_succeeded_at,
                premade_through,
                retained_from,
                last_error_code,
                updated_at
            FROM partition_maintenance_status
            WHERE singleton_id = 1
            """
        )
    ).mappings().first()
    if row is None:
        now = session.execute(text("SELECT CURRENT_TIMESTAMP")).scalar_one()
        return {
            "last_started_at": None,
            "last_succeeded_at": None,
            "premade_through": None,
            "retained_from": None,
            "last_error_code": None,
            "updated_at": _format_dt(now),
        }
    return {
        "last_started_at": _format_dt(row["last_started_at"]),
        "last_succeeded_at": _format_dt(row["last_succeeded_at"]),
        "premade_through": _format_date(row["premade_through"]),
        "retained_from": _format_date(row["retained_from"]),
        "last_error_code": row["last_error_code"],
        "updated_at": _format_dt(row["updated_at"]),
    }


def _maintenance_fingerprint() -> bytes:
    # runMaintenance has no request body; fingerprint is the stable operation identity.
    return _sha256_utf8("runMaintenance")


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
            AdminReplay.operation_code == _ADMIN_REPLAY_OP_RUN_MAINTENANCE,
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
) -> datetime:
    created_at = session.execute(select(func.statement_timestamp())).scalar_one()
    expires_at = created_at + timedelta(seconds=admin_replay_ttl_seconds)
    session.add(
        AdminReplay(
            admin_principal_id=principal_id,
            operation_code=_ADMIN_REPLAY_OP_RUN_MAINTENANCE,
            key_hash=key_hash,
            request_fingerprint=fingerprint,
            http_status=200,
            response_body=dict(response_body),
            created_at=created_at,
            expires_at=expires_at,
        )
    )
    return expires_at


def _record_maintenance_audit(
    session: Session,
    *,
    actor_id: str,
    request_id: str,
    maintenance_run_id: str,
    outcome: str,
    error_code: str | None,
) -> None:
    session.add(
        AdminAuditLog(
            audit_at=func.statement_timestamp(),
            queue_id=None,
            actor_id=actor_id,
            operation_code=_AUDIT_OP_RUN_MAINTENANCE,
            previous_config_version=None,
            new_config_version=None,
            request_id=uuid.UUID(request_id),
            details={
                "maintenance_run_id": maintenance_run_id,
                "outcome": outcome,
                "error_code": error_code,
            },
        )
    )


def _run_locked_maintenance(
    connection: Connection,
    *,
    payload_retention_policy: PayloadRetentionPolicy,
    registry_purge_batch_size: int,
    horizon_days: int,
) -> tuple[bool, StorageMaintenanceReport | None]:
    """Try the Phase 3.8 advisory lock once; run maintenance on success."""
    locked = bool(
        connection.execute(_TRY_LOCK_SQL, {"key": MAINTENANCE_LOCK_KEY}).scalar_one()
    )
    if not locked:
        return False, None
    try:
        report = run_storage_maintenance(
            connection,
            horizon_days=horizon_days,
            payload_retention_policy=payload_retention_policy,
            registry_purge_batch_size=registry_purge_batch_size,
        )
        return True, report
    finally:
        try:
            connection.execute(_UNLOCK_SQL, {"key": MAINTENANCE_LOCK_KEY})
            if connection.in_transaction():
                connection.commit()
        except Exception:
            pass


def trigger_partition_maintenance(
    session: Session,
    *,
    engine: Engine,
    principal_id: str,
    actor_id: str,
    request_id: str,
    idempotency_key: str,
    payload_retention_policy: PayloadRetentionPolicy,
    registry_purge_batch_size: int,
    admin_replay_ttl_seconds: int,
    horizon_days: int = DEFAULT_PARTITION_PREMAKE_DAYS,
) -> MaintenanceTriggerResult:
    """Idempotent single-winner maintenance trigger for the private control plane.

    Lock losers return current status without DDL. Accepted runs record audit and
    ``admin_replay`` (operation_code=5) in the caller's ORM session transaction.
    """
    key_hash = _sha256_utf8(idempotency_key)
    fingerprint = _maintenance_fingerprint()

    replayed = _lookup_admin_replay(
        session,
        principal_id=principal_id,
        key_hash=key_hash,
        fingerprint=fingerprint,
    )
    if replayed is not None:
        run_id = None
        status = replayed.get("status")
        if isinstance(status, dict):
            run_id = status.get("maintenance_run_id")  # type: ignore[assignment]
        if not isinstance(run_id, str):
            run_id = replayed.get("maintenance_run_id")  # type: ignore[assignment]
        return MaintenanceTriggerResult(
            outcome="replayed",
            status=dict(replayed.get("status") or read_maintenance_status(session)),
            replayed=True,
            admin_replay_expires_at=None,
            maintenance_run_id=run_id if isinstance(run_id, str) else None,
            error_code=None,
        )

    maintenance_run_id = str(uuid.uuid4())
    # Use a dedicated connection so advisory lock lifetime matches Phase 3.8.
    with engine.connect() as connection:
        acquired, report = _run_locked_maintenance(
            connection,
            payload_retention_policy=payload_retention_policy,
            registry_purge_batch_size=registry_purge_batch_size,
            horizon_days=horizon_days,
        )

    if not acquired:
        status = read_maintenance_status(session)
        return MaintenanceTriggerResult(
            outcome="skipped_lock",
            status=status,
            replayed=False,
            admin_replay_expires_at=None,
            maintenance_run_id=None,
            error_code="lock_held",
        )

    assert report is not None
    outcome: MaintenanceTriggerOutcome = (
        "succeeded" if report.outcome == "succeeded" else "failed"
    )
    # Re-read status after the locked run mutated the singleton.
    status = read_maintenance_status(session)
    status_with_run = dict(status)
    status_with_run["maintenance_run_id"] = maintenance_run_id
    status_with_run["outcome"] = outcome

    response_body = {
        "status": status_with_run,
        "replayed": False,
        "admin_replay_expires_at": None,  # filled after store
        "maintenance_run_id": maintenance_run_id,
        "outcome": outcome,
    }
    expires_at = _store_admin_replay(
        session,
        principal_id=principal_id,
        key_hash=key_hash,
        fingerprint=fingerprint,
        response_body={
            "status": status_with_run,
            "replayed": False,
            "admin_replay_expires_at": None,
            "maintenance_run_id": maintenance_run_id,
            "outcome": outcome,
        },
        admin_replay_ttl_seconds=admin_replay_ttl_seconds,
    )
    response_body["admin_replay_expires_at"] = _format_dt(expires_at)
    # Patch stored body with concrete expires_at for future replays.
    stored = session.execute(
        select(AdminReplay).where(
            AdminReplay.admin_principal_id == principal_id,
            AdminReplay.operation_code == _ADMIN_REPLAY_OP_RUN_MAINTENANCE,
            AdminReplay.key_hash == key_hash,
        )
    ).scalar_one()
    stored.response_body = {
        "status": status_with_run,
        "replayed": False,
        "admin_replay_expires_at": _format_dt(expires_at),
        "maintenance_run_id": maintenance_run_id,
        "outcome": outcome,
    }

    _record_maintenance_audit(
        session,
        actor_id=actor_id,
        request_id=request_id,
        maintenance_run_id=maintenance_run_id,
        outcome=outcome,
        error_code=report.error_code,
    )

    return MaintenanceTriggerResult(
        outcome=outcome,
        status=status_with_run,
        replayed=False,
        admin_replay_expires_at=expires_at,
        maintenance_run_id=maintenance_run_id,
        error_code=report.error_code,
    )
