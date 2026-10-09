"""Held-session history partition detach/drop retention (STOR-04, SEC-05).

Expires allowlisted daily UTC RANGE history children whose complete upper bound
is past the configured retention cutoff. Operates only through the caller-held
SQLAlchemy ``Connection``: no advisory locks, no engine creation, and no ownership
of the outer business transaction. ``DETACH PARTITION CONCURRENTLY`` requires the
session to have no open transaction (caller must commit first); this module raises
rather than committing on the caller's behalf.

Terminal task / payload expiry for ``tasks_terminal`` is decided exclusively by
Phase 3.2 ``PayloadRetentionPolicy.expires_at`` / ``is_expired``. Other history
parents reuse the same policy type for Queue-store expiry arithmetic so day math
is not duplicated.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Final, Literal

from sqlalchemy import text
from sqlalchemy.engine import Connection

from queue_service.infrastructure.postgres import partition_catalog
from queue_service.security.payload_policy import (
    PAYLOAD_RETENTION_DAYS_MAX,
    PAYLOAD_RETENTION_DAYS_MIN,
    PayloadRetentionPolicy,
)

RetentionStatus = Literal[
    "detached",
    "dropped",
    "skipped_not_expired",
    "resumed",
    "failed",
]

MIN_HISTORY_RETENTION_DAYS: Final[int] = PAYLOAD_RETENTION_DAYS_MIN
MAX_HISTORY_RETENTION_DAYS: Final[int] = PAYLOAD_RETENTION_DAYS_MAX

_TASKS_TERMINAL: Final[str] = "tasks_terminal"

_STORE_NOW_SQL = text("SELECT CURRENT_TIMESTAMP")

_PENDING_DETACH_SQL = text(
    """
    SELECT parent.relname AS parent_name,
           child.relname AS child_name,
           to_date(right(child.relname, 8), 'YYYYMMDD') AS day
    FROM pg_inherits i
    JOIN pg_class child ON child.oid = i.inhrelid
    JOIN pg_class parent ON parent.oid = i.inhparent
    JOIN pg_namespace n ON n.oid = parent.relnamespace
    WHERE n.nspname = current_schema()
      AND parent.relkind = 'p'
      AND child.relkind = 'r'
      AND parent.relname = ANY(:parents)
      AND i.inhdetachpending
    ORDER BY parent.relname, child.relname
    """
)

_DETACHED_ORPHANS_SQL = text(
    """
    SELECT c.relname AS child_name,
           to_date(right(c.relname, 8), 'YYYYMMDD') AS day
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = current_schema()
      AND c.relkind = 'r'
      AND NOT c.relispartition
      AND NOT EXISTS (
            SELECT 1 FROM pg_inherits i WHERE i.inhrelid = c.oid
      )
      AND c.relname LIKE :prefix
    ORDER BY c.relname
    """
)

_FORMAT_DETACH_CONCURRENTLY_SQL = text(
    """
    SELECT format(
        'ALTER TABLE %I DETACH PARTITION %I CONCURRENTLY',
        CAST(:parent_name AS text),
        CAST(:child_name AS text)
    )
    """
)

_FORMAT_DETACH_FINALIZE_SQL = text(
    """
    SELECT format(
        'ALTER TABLE %I DETACH PARTITION %I FINALIZE',
        CAST(:parent_name AS text),
        CAST(:child_name AS text)
    )
    """
)

_FORMAT_DROP_SQL = text(
    """
    SELECT format('DROP TABLE %I', CAST(:child_name AS text))
    """
)


@dataclass(frozen=True, slots=True)
class PartitionRetentionOutcome:
    """Typed per-partition retention outcome for maintenance reporting."""

    parent_name: str
    child_name: str
    day: date
    status: RetentionStatus
    detail: str | None = None


@dataclass(frozen=True, slots=True)
class HistoryRetentionResult:
    """Aggregate retention run over allowlisted history parents."""

    store_now: datetime
    outcomes: tuple[PartitionRetentionOutcome, ...]


def retain_expired_history(
    connection: Connection,
    *,
    retention_days_by_parent: Mapping[str, int],
    payload_retention_policy: PayloadRetentionPolicy,
) -> HistoryRetentionResult:
    """Detach/drop fully expired history partitions on ``connection``.

    ``tasks_terminal`` expiry uses ``payload_retention_policy`` only. Other
    parents use ``retention_days_by_parent`` values clamped to 30–90 inclusive.
    """
    # CONCURRENTLY cannot run inside a transaction. Raise rather than commit for
    # the caller; then use AUTOCOMMIT so catalog SELECTs do not open a txn block.
    _require_no_open_transaction(connection)
    session = connection.execution_options(isolation_level="AUTOCOMMIT")

    parents = partition_catalog.inspect_history_parents(session)
    validated = _validate_retention_days(
        retention_days_by_parent,
        payload_retention_policy=payload_retention_policy,
        parents=frozenset(parents),
    )
    store_now = _store_utc_now(session)
    outcomes: list[PartitionRetentionOutcome] = []

    # Resume pending FINALIZE and already-detached orphans first.
    for parent_name, child_name, day in _list_pending_detach(session):
        if parent_name not in parents:
            continue
        outcomes.extend(
            _resume_pending_detach(
                session,
                parent_name=parent_name,
                child_name=child_name,
                day=day,
            )
        )

    for parent_name in parents:
        for child_name, day in _list_detached_orphans(session, parent_name):
            outcomes.extend(
                _drop_detached_child(
                    session,
                    parent_name=parent_name,
                    child_name=child_name,
                    day=day,
                    resumed=True,
                )
            )

    existing = partition_catalog.list_existing_children(session)
    for parent_name, children in existing.items():
        days = validated[parent_name]
        policy = (
            payload_retention_policy
            if parent_name == _TASKS_TERMINAL
            else PayloadRetentionPolicy(retention_days=days)
        )
        for day, spec in sorted(children.items()):
            if not _is_fully_expired(policy, store_now=store_now, bound_to=spec.bound_to):
                outcomes.append(
                    PartitionRetentionOutcome(
                        parent_name=parent_name,
                        child_name=spec.child_name,
                        day=day,
                        status="skipped_not_expired",
                    )
                )
                continue
            outcomes.extend(
                _detach_and_drop(
                    session,
                    parent_name=parent_name,
                    child_name=spec.child_name,
                    day=day,
                )
            )

    return HistoryRetentionResult(store_now=store_now, outcomes=tuple(outcomes))


def _validate_retention_days(
    retention_days_by_parent: Mapping[str, int],
    *,
    payload_retention_policy: PayloadRetentionPolicy,
    parents: frozenset[str],
) -> dict[str, int]:
    if not isinstance(payload_retention_policy, PayloadRetentionPolicy):
        raise TypeError("payload_retention_policy must be PayloadRetentionPolicy")
    missing = parents - set(retention_days_by_parent)
    if missing:
        raise ValueError(f"missing retention days for parents: {sorted(missing)}")
    unknown = set(retention_days_by_parent) - parents
    if unknown:
        raise ValueError(f"unknown history parents in retention map: {sorted(unknown)}")

    out: dict[str, int] = {}
    for parent in parents:
        days = retention_days_by_parent[parent]
        if not isinstance(days, int) or isinstance(days, bool):
            raise ValueError(f"retention days for {parent!r} must be an int")
        if days < MIN_HISTORY_RETENTION_DAYS or days > MAX_HISTORY_RETENTION_DAYS:
            raise ValueError(
                f"retention days for {parent!r} must be between "
                f"{MIN_HISTORY_RETENTION_DAYS} and {MAX_HISTORY_RETENTION_DAYS} "
                f"inclusive, got {days}"
            )
        if parent == _TASKS_TERMINAL and days != payload_retention_policy.retention_days:
            raise ValueError(
                "tasks_terminal retention days must equal "
                "payload_retention_policy.retention_days"
            )
        out[parent] = days
    return out


def _store_utc_now(connection: Connection) -> datetime:
    value = connection.execute(_STORE_NOW_SQL).scalar_one()
    if not isinstance(value, datetime):
        raise TypeError("CURRENT_TIMESTAMP did not return datetime")
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def _is_fully_expired(
    policy: PayloadRetentionPolicy,
    *,
    store_now: datetime,
    bound_to: datetime,
) -> bool:
    """True when the exclusive upper bound itself is expired under ``policy``."""
    expires = policy.expires_at(bound_to)
    return policy.is_expired(store_now, expires)


def _list_pending_detach(
    connection: Connection,
) -> list[tuple[str, str, date]]:
    parents = [s.parent_name for s in partition_catalog.history_parent_specs()]
    rows = connection.execute(_PENDING_DETACH_SQL, {"parents": parents}).all()
    out: list[tuple[str, str, date]] = []
    for parent_name, child_name, day in rows:
        if day is None:
            continue
        day_value = day if isinstance(day, date) else date.fromisoformat(str(day))
        out.append((str(parent_name), str(child_name), day_value))
    return out


def _list_detached_orphans(
    connection: Connection,
    parent_name: str,
) -> list[tuple[str, date]]:
    if parent_name not in {s.parent_name for s in partition_catalog.history_parent_specs()}:
        raise ValueError(f"refusing unknown history parent: {parent_name!r}")
    rows = connection.execute(
        _DETACHED_ORPHANS_SQL, {"prefix": f"{parent_name}_%"}
    ).all()
    out: list[tuple[str, date]] = []
    for child_name, day in rows:
        name = str(child_name)
        suffix = name.rsplit("_", 1)[-1]
        if len(suffix) != 8 or not suffix.isdigit():
            continue
        if day is None:
            continue
        day_value = day if isinstance(day, date) else date.fromisoformat(str(day))
        expected = partition_catalog.child_name_for(parent_name, day_value)
        if name != expected:
            continue
        out.append((name, day_value))
    return out


def _require_no_open_transaction(connection: Connection) -> None:
    if connection.in_transaction():
        raise ValueError(
            "DETACH PARTITION CONCURRENTLY requires no open transaction; "
            "caller must commit before history retention"
        )


def _detach_and_drop(
    connection: Connection,
    *,
    parent_name: str,
    child_name: str,
    day: date,
) -> list[PartitionRetentionOutcome]:
    outcomes: list[PartitionRetentionOutcome] = []
    try:
        detach_sql = connection.execute(
            _FORMAT_DETACH_CONCURRENTLY_SQL,
            {"parent_name": parent_name, "child_name": child_name},
        ).scalar_one()
        connection.execute(text(str(detach_sql)))
        outcomes.append(
            PartitionRetentionOutcome(
                parent_name=parent_name,
                child_name=child_name,
                day=day,
                status="detached",
            )
        )
        outcomes.extend(
            _drop_detached_child(
                connection,
                parent_name=parent_name,
                child_name=child_name,
                day=day,
                resumed=False,
            )
        )
    except Exception as exc:  # noqa: BLE001 — typed failed outcome for report
        outcomes.append(
            PartitionRetentionOutcome(
                parent_name=parent_name,
                child_name=child_name,
                day=day,
                status="failed",
                detail=str(exc),
            )
        )
    return outcomes


def _resume_pending_detach(
    connection: Connection,
    *,
    parent_name: str,
    child_name: str,
    day: date,
) -> list[PartitionRetentionOutcome]:
    outcomes: list[PartitionRetentionOutcome] = [
        PartitionRetentionOutcome(
            parent_name=parent_name,
            child_name=child_name,
            day=day,
            status="resumed",
            detail="finalize_pending_detach",
        )
    ]
    try:
        finalize_sql = connection.execute(
            _FORMAT_DETACH_FINALIZE_SQL,
            {"parent_name": parent_name, "child_name": child_name},
        ).scalar_one()
        connection.execute(text(str(finalize_sql)))
        outcomes.append(
            PartitionRetentionOutcome(
                parent_name=parent_name,
                child_name=child_name,
                day=day,
                status="detached",
                detail="finalize",
            )
        )
        outcomes.extend(
            _drop_detached_child(
                connection,
                parent_name=parent_name,
                child_name=child_name,
                day=day,
                resumed=True,
            )
        )
    except Exception as exc:  # noqa: BLE001
        outcomes.append(
            PartitionRetentionOutcome(
                parent_name=parent_name,
                child_name=child_name,
                day=day,
                status="failed",
                detail=str(exc),
            )
        )
    return outcomes


def _drop_detached_child(
    connection: Connection,
    *,
    parent_name: str,
    child_name: str,
    day: date,
    resumed: bool,
) -> list[PartitionRetentionOutcome]:
    outcomes: list[PartitionRetentionOutcome] = []
    if resumed:
        outcomes.append(
            PartitionRetentionOutcome(
                parent_name=parent_name,
                child_name=child_name,
                day=day,
                status="resumed",
                detail="drop_detached",
            )
        )
    try:
        drop_sql = connection.execute(
            _FORMAT_DROP_SQL, {"child_name": child_name}
        ).scalar_one()
        connection.execute(text(str(drop_sql)))
        outcomes.append(
            PartitionRetentionOutcome(
                parent_name=parent_name,
                child_name=child_name,
                day=day,
                status="dropped",
            )
        )
    except Exception as exc:  # noqa: BLE001
        outcomes.append(
            PartitionRetentionOutcome(
                parent_name=parent_name,
                child_name=child_name,
                day=day,
                status="failed",
                detail=str(exc),
            )
        )
    return outcomes
