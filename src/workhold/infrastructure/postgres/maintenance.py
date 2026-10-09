"""Composite held-session storage maintenance orchestration (STOR-03/04/08).

Runs premake, bound verification, history retention, and one incremental
registry purge batch on a caller-held SQLAlchemy ``Connection``. Never acquires
advisory locks, never creates engines/connections, and never owns the outer
session lifetime — ``roles.maintain`` is the sole lock/session owner.

Persists durable status only on the Phase 3.1 singleton
``partition_maintenance_status`` (row ``singleton_id=1``). Detailed counts stay
in the typed process report.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Final, Literal

from sqlalchemy import text
from sqlalchemy.engine import Connection

from workhold.health import (
    DAILY_RANGE_PARENTS,
    DEFAULT_PARTITION_PREMAKE_DAYS,
    ReasonCode,
)
from workhold.infrastructure.postgres.history_retention import (
    HistoryRetentionResult,
    retain_expired_history,
)
from workhold.infrastructure.postgres.partition_catalog import (
    list_existing_children,
    required_day_specs,
    store_utc_today,
    validate_horizon_days,
)
from workhold.infrastructure.postgres.partition_premake import (
    PremakeResult,
    premake_daily_partitions,
)
from workhold.infrastructure.postgres.registry_retention import (
    RegistryPurgeResult,
    purge_expired_registries,
)
from workhold.security.payload_policy import PayloadRetentionPolicy
from workhold.security.redaction import sanitize_for_diagnostics

MaintenanceOutcome = Literal["succeeded", "failed"]

MAX_ERROR_CODE_LEN: Final[int] = 128
MAX_ERROR_DETAIL_LEN: Final[int] = 4096

_ENSURE_SINGLETON_SQL = text(
    """
    INSERT INTO partition_maintenance_status (singleton_id, updated_at)
    VALUES (1, CURRENT_TIMESTAMP)
    ON CONFLICT (singleton_id) DO NOTHING
    """
)

_MARK_STARTED_SQL = text(
    """
    UPDATE partition_maintenance_status
    SET last_started_at = CURRENT_TIMESTAMP,
        updated_at = CURRENT_TIMESTAMP
    WHERE singleton_id = 1
    RETURNING last_started_at, last_succeeded_at, premade_through, retained_from
    """
)

_MARK_SUCCEEDED_SQL = text(
    """
    UPDATE partition_maintenance_status
    SET last_succeeded_at = CURRENT_TIMESTAMP,
        premade_through = :premade_through,
        retained_from = :retained_from,
        last_error_code = NULL,
        last_error_detail = NULL,
        updated_at = CURRENT_TIMESTAMP
    WHERE singleton_id = 1
    RETURNING last_succeeded_at, premade_through, retained_from
    """
)

_MARK_FAILED_SQL = text(
    """
    UPDATE partition_maintenance_status
    SET last_error_code = :error_code,
        last_error_detail = :error_detail,
        updated_at = CURRENT_TIMESTAMP
    WHERE singleton_id = 1
    RETURNING last_succeeded_at, premade_through, retained_from,
              last_error_code, last_error_detail
    """
)

# Diagnostic allowlist for maintenance error projection (low-cardinality).
_MAINT_DIAG_ALLOWLIST: Final[frozenset[str]] = frozenset(
    {
        "status",
        "reason",
        "outcome",
        "code",
        "message",
        "operation",
        "role",
    }
)


@dataclass(frozen=True, slots=True)
class StorageMaintenanceReport:
    """Typed composite maintenance outcome for the maintain role report."""

    outcome: MaintenanceOutcome
    premade_through: date | None
    retained_from: date | None
    last_started_at: datetime | None
    last_succeeded_at: datetime | None
    premake: PremakeResult | None
    retention: HistoryRetentionResult | None
    purge: RegistryPurgeResult | None
    verification_reason: str | None
    error_code: str | None
    error_detail: str | None
    partitions_created: int
    partitions_detached: int
    partitions_dropped: int
    purge_examined_total: int
    purge_deleted_total: int
    purge_by_registry: Mapping[str, Mapping[str, object]] | None


def run_storage_maintenance(
    connection: Connection,
    *,
    horizon_days: int = DEFAULT_PARTITION_PREMAKE_DAYS,
    retention_days_by_parent: Mapping[str, int] | None = None,
    payload_retention_policy: PayloadRetentionPolicy,
    registry_purge_batch_size: int,
) -> StorageMaintenanceReport:
    """Execute the bounded maintenance lifecycle on ``connection``.

    Caller must hold the maintenance advisory lock and own session lifetime.
    This function commits between stages when required (premake txn → retention
    AUTOCOMMIT) but never closes ``connection`` or takes locks.
    """
    days_map = dict(retention_days_by_parent or {})
    if not days_map:
        from workhold.maintenance.delivery_retention import (
            default_retention_days_by_parent,
        )

        days_map = default_retention_days_by_parent(
            payload_retention_days=payload_retention_policy.retention_days
        )

    started_at: datetime | None = None
    prior_succeeded: datetime | None = None
    prior_premade: date | None = None
    prior_retained: date | None = None
    premake_result: PremakeResult | None = None
    retention_result: HistoryRetentionResult | None = None
    purge_result: RegistryPurgeResult | None = None

    try:
        _ensure_singleton(connection)
        started_at, prior_succeeded, prior_premade, prior_retained = _mark_started(
            connection
        )
        connection.commit()

        premake_result = premake_daily_partitions(
            connection, horizon_days=horizon_days
        )
        connection.commit()

        verify_code = _verify_premake_bounds(connection, horizon_days=horizon_days)
        if verify_code is not None:
            return _fail(
                connection,
                error_code=_bound_code(verify_code),
                error_detail=_bound_detail(f"verification failed: {verify_code}"),
                prior_succeeded=prior_succeeded,
                prior_premade=prior_premade,
                prior_retained=prior_retained,
                started_at=started_at,
                premake=premake_result,
                retention=None,
                purge=None,
                verification_reason=verify_code,
            )

        # DETACH CONCURRENTLY requires no open transaction.
        if connection.in_transaction():
            connection.commit()

        retention_result = retain_expired_history(
            connection,
            retention_days_by_parent=days_map,
            payload_retention_policy=payload_retention_policy,
        )

        # Leave AUTOCOMMIT isolation from retention; begin explicit txn for
        # published-row purge then registry purge.
        if not connection.in_transaction():
            connection.begin()

        from workhold.maintenance.delivery_retention import (
            DEFAULT_PUBLISHED_RETENTION_DAYS,
            purge_expired_published_events,
        )

        published_purge = purge_expired_published_events(
            connection,
            published_retention_days=DEFAULT_PUBLISHED_RETENTION_DAYS,
        )

        purge_result = purge_expired_registries(
            connection, batch_size=registry_purge_batch_size
        )
        connection.commit()

        # Published 30d purge failures must fail the maintain outcome — never
        # report pure success after a failed published-history purge (OPS-05).
        if published_purge.failures > 0:
            failed = _fail(
                connection,
                error_code=_bound_code(
                    published_purge.failure_code or "published_purge_failed"
                ),
                error_detail=_bound_detail(
                    "published purge failed: "
                    f"{published_purge.failure_code or 'published_purge_failed'}"
                ),
                prior_succeeded=prior_succeeded,
                prior_premade=prior_premade,
                prior_retained=prior_retained,
                started_at=started_at,
                premake=premake_result,
                retention=retention_result,
                purge=purge_result,
                verification_reason=None,
            )
            return StorageMaintenanceReport(
                outcome=failed.outcome,
                premade_through=failed.premade_through,
                retained_from=failed.retained_from,
                last_started_at=failed.last_started_at,
                last_succeeded_at=failed.last_succeeded_at,
                premake=failed.premake,
                retention=failed.retention,
                purge=failed.purge,
                verification_reason=failed.verification_reason,
                error_code=failed.error_code,
                error_detail=failed.error_detail,
                partitions_created=failed.partitions_created,
                partitions_detached=failed.partitions_detached,
                partitions_dropped=failed.partitions_dropped,
                purge_examined_total=failed.purge_examined_total,
                purge_deleted_total=failed.purge_deleted_total
                + published_purge.deleted,
                purge_by_registry=_with_published_purge(
                    failed.purge_by_registry, published_purge
                ),
            )

        retained_from = _compute_retained_from(connection) or prior_retained
        premade_through = premake_result.through_day
        succeeded_at, _, _ = _mark_succeeded(
            connection,
            premade_through=premade_through,
            retained_from=retained_from,
        )
        connection.commit()

        success = _success_report(
            started_at=started_at,
            succeeded_at=succeeded_at,
            premade_through=premade_through,
            retained_from=retained_from,
            premake=premake_result,
            retention=retention_result,
            purge=purge_result,
            verification_reason=None,
        )
        # Fold published-row deletes into aggregate purge count for operators.
        return StorageMaintenanceReport(
            outcome=success.outcome,
            premade_through=success.premade_through,
            retained_from=success.retained_from,
            last_started_at=success.last_started_at,
            last_succeeded_at=success.last_succeeded_at,
            premake=success.premake,
            retention=success.retention,
            purge=success.purge,
            verification_reason=success.verification_reason,
            error_code=success.error_code,
            error_detail=success.error_detail,
            partitions_created=success.partitions_created,
            partitions_detached=success.partitions_detached,
            partitions_dropped=success.partitions_dropped,
            purge_examined_total=success.purge_examined_total,
            purge_deleted_total=success.purge_deleted_total
            + published_purge.deleted,
            purge_by_registry=_with_published_purge(
                success.purge_by_registry, published_purge
            ),
        )
    except Exception as exc:  # noqa: BLE001 — typed failure for durable status
        try:
            if connection.in_transaction():
                connection.rollback()
        except Exception:
            pass
        code, detail = _classify_failure(exc)
        try:
            return _fail(
                connection,
                error_code=code,
                error_detail=detail,
                prior_succeeded=prior_succeeded,
                prior_premade=prior_premade,
                prior_retained=prior_retained,
                started_at=started_at,
                premake=premake_result,
                retention=retention_result,
                purge=purge_result,
                verification_reason=None,
            )
        except Exception:
            return StorageMaintenanceReport(
                outcome="failed",
                premade_through=prior_premade,
                retained_from=prior_retained,
                last_started_at=started_at,
                last_succeeded_at=prior_succeeded,
                premake=premake_result,
                retention=retention_result,
                purge=purge_result,
                verification_reason=None,
                error_code=code,
                error_detail=detail,
                partitions_created=_count_created(premake_result),
                partitions_detached=_count_status(retention_result, "detached"),
                partitions_dropped=_count_status(retention_result, "dropped"),
                purge_examined_total=_purge_examined(purge_result),
                purge_deleted_total=_purge_deleted(purge_result),
                purge_by_registry=_purge_map(purge_result),
            )


def _ensure_singleton(connection: Connection) -> None:
    connection.execute(_ENSURE_SINGLETON_SQL)


def _mark_started(
    connection: Connection,
) -> tuple[datetime | None, datetime | None, date | None, date | None]:
    row = connection.execute(_MARK_STARTED_SQL).one()
    return (
        _as_aware(row[0]),
        _as_aware(row[1]),
        row[2],
        row[3],
    )


def _mark_succeeded(
    connection: Connection,
    *,
    premade_through: date,
    retained_from: date | None,
) -> tuple[datetime | None, date | None, date | None]:
    row = connection.execute(
        _MARK_SUCCEEDED_SQL,
        {"premade_through": premade_through, "retained_from": retained_from},
    ).one()
    return _as_aware(row[0]), row[1], row[2]


def _fail(
    connection: Connection,
    *,
    error_code: str,
    error_detail: str,
    prior_succeeded: datetime | None,
    prior_premade: date | None,
    prior_retained: date | None,
    started_at: datetime | None,
    premake: PremakeResult | None,
    retention: HistoryRetentionResult | None,
    purge: RegistryPurgeResult | None,
    verification_reason: str | None,
) -> StorageMaintenanceReport:
    try:
        if connection.in_transaction():
            connection.rollback()
        connection.execute(
            _MARK_FAILED_SQL,
            {"error_code": error_code, "error_detail": error_detail},
        )
        connection.commit()
    except Exception:
        pass
    return StorageMaintenanceReport(
        outcome="failed",
        premade_through=prior_premade,
        retained_from=prior_retained,
        last_started_at=started_at,
        last_succeeded_at=prior_succeeded,
        premake=premake,
        retention=retention,
        purge=purge,
        verification_reason=verification_reason,
        error_code=error_code,
        error_detail=error_detail,
        partitions_created=_count_created(premake),
        partitions_detached=_count_status(retention, "detached"),
        partitions_dropped=_count_status(retention, "dropped"),
        purge_examined_total=_purge_examined(purge),
        purge_deleted_total=_purge_deleted(purge),
        purge_by_registry=_purge_map(purge),
    )


def _success_report(
    *,
    started_at: datetime | None,
    succeeded_at: datetime | None,
    premade_through: date | None,
    retained_from: date | None,
    premake: PremakeResult | None,
    retention: HistoryRetentionResult | None,
    purge: RegistryPurgeResult | None,
    verification_reason: str | None,
) -> StorageMaintenanceReport:
    return StorageMaintenanceReport(
        outcome="succeeded",
        premade_through=premade_through,
        retained_from=retained_from,
        last_started_at=started_at,
        last_succeeded_at=succeeded_at,
        premake=premake,
        retention=retention,
        purge=purge,
        verification_reason=verification_reason,
        error_code=None,
        error_detail=None,
        partitions_created=_count_created(premake),
        partitions_detached=_count_status(retention, "detached"),
        partitions_dropped=_count_status(retention, "dropped"),
        purge_examined_total=_purge_examined(purge),
        purge_deleted_total=_purge_deleted(purge),
        purge_by_registry=_purge_map(purge),
    )


def _verify_premake_bounds(connection: Connection, *, horizon_days: int) -> str | None:
    """Return a stable reason code when the held-session horizon is incomplete."""
    try:
        validate_horizon_days(horizon_days)
        store_today = store_utc_today(connection)
        existing = list_existing_children(connection)
    except ValueError:
        return ReasonCode.PARTITION_MISSING

    for parent in DAILY_RANGE_PARENTS:
        required = required_day_specs(
            parent, store_today=store_today, horizon_days=horizon_days
        )
        present = existing.get(parent, {})
        for spec in required:
            if spec.day not in present:
                # Interior gap vs short max-day: both unsafe for correctness paths.
                if any(d > spec.day for d in present):
                    return ReasonCode.PARTITION_MISSING
                return ReasonCode.PARTITION_HORIZON_UNSAFE
    return None


def _compute_retained_from(connection: Connection) -> date | None:
    existing = list_existing_children(connection)
    days: list[date] = []
    for parent in DAILY_RANGE_PARENTS:
        days.extend(existing.get(parent, {}).keys())
    if not days:
        return store_utc_today(connection)
    return min(days)


def _classify_failure(exc: BaseException) -> tuple[str, str]:
    name = type(exc).__name__
    code = _bound_code(name if name else "maintenance_failed")
    # Never echo raw exception text — it may contain SQL, payloads, or secrets.
    # Keep only the stable exception class name in the bounded detail.
    safe = sanitize_for_diagnostics(
        {"status": "failed", "reason": code, "outcome": name, "operation": "maintain"},
        allowlist=_MAINT_DIAG_ALLOWLIST,
    )
    outcome = str(safe.get("outcome") or code)
    detail = _bound_detail(f"{code}: {outcome}")
    return code, detail


def _bound_code(code: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in "_-" else "_" for ch in code.strip())
    if not cleaned:
        cleaned = "maintenance_failed"
    return cleaned[:MAX_ERROR_CODE_LEN]


def _bound_detail(detail: str) -> str:
    text_value = detail.strip() or "maintenance_failed"
    encoded = text_value.encode("utf-8", errors="replace")
    if len(encoded) > MAX_ERROR_DETAIL_LEN:
        text_value = encoded[:MAX_ERROR_DETAIL_LEN].decode("utf-8", errors="ignore")
    return text_value[:MAX_ERROR_DETAIL_LEN]


def _as_aware(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    return None


def _count_created(premake: PremakeResult | None) -> int:
    if premake is None:
        return 0
    return sum(len(p.created) for p in premake.by_parent.values())


def _count_status(retention: HistoryRetentionResult | None, status: str) -> int:
    if retention is None:
        return 0
    return sum(1 for o in retention.outcomes if o.status == status)


def _purge_examined(purge: RegistryPurgeResult | None) -> int:
    if purge is None:
        return 0
    return sum(o.examined for o in purge.outcomes)


def _purge_deleted(purge: RegistryPurgeResult | None) -> int:
    if purge is None:
        return 0
    return sum(o.deleted for o in purge.outcomes)


def _purge_map(
    purge: RegistryPurgeResult | None,
) -> Mapping[str, Mapping[str, object]] | None:
    if purge is None:
        return None
    out: dict[str, Mapping[str, object]] = {}
    for outcome in purge.outcomes:
        out[outcome.registry] = {
            "examined": outcome.examined,
            "deleted": outcome.deleted,
            "more_work": outcome.more_work,
            "oldest_remaining_expires_at": outcome.oldest_remaining_expires_at,
        }
    return out


def _with_published_purge(
    purge_by_registry: Mapping[str, Mapping[str, object]] | None,
    published_purge: object,
) -> Mapping[str, Mapping[str, object]]:
    """Fold published-row purge counts into the maintain purge registry map."""
    out: dict[str, Mapping[str, object]] = dict(purge_by_registry or {})
    deleted = int(getattr(published_purge, "deleted", 0) or 0)
    failures = int(getattr(published_purge, "failures", 0) or 0)
    failure_code = getattr(published_purge, "failure_code", None)
    out["delivery_published"] = {
        "examined": deleted + failures,
        "deleted": deleted,
        "more_work": False,
        "failures": failures,
        "failure_code": failure_code,
        "oldest_remaining_expires_at": None,
    }
    return out
