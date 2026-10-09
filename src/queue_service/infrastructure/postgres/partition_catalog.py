"""Validated daily UTC RANGE history partition catalog (STOR-03).

Inspects allowlisted Phase 3.1 history parents and derives UTC calendar partition
specifications from Queue-store time. Callers supply an already-open SQLAlchemy
connection; this module never opens engines or acquires advisory locks.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Final, Mapping

from sqlalchemy import text
from sqlalchemy.engine import Connection

from queue_service.health import DAILY_RANGE_PARENTS

# Exact Phase 3.1 parents and immutable Queue-store partition keys.
_PARENT_KEYS: Final[Mapping[str, str]] = {
    "admin_audit_log": "audit_at",
    "task_attempts": "claimed_at",
    "tasks_terminal": "terminal_at",
    "delivery_events_terminal": "terminal_at",
}

MIN_PREMAKE_HORIZON_DAYS: Final[int] = 14
MAX_PREMAKE_HORIZON_DAYS: Final[int] = 30

_UTC_TODAY_SQL = text("SELECT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date")

_PARENT_META_SQL = text(
    """
    SELECT c.relname AS parent_name,
           pt.partstrat,
           pg_get_partkeydef(c.oid) AS partkey
    FROM pg_partitioned_table pt
    JOIN pg_class c ON c.oid = pt.partrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = current_schema()
      AND c.relkind = 'p'
      AND c.relname = ANY(:parents)
    ORDER BY c.relname
    """
)

_CHILDREN_SQL = text(
    """
    SELECT parent.relname AS parent_name,
           child.relname AS child_name,
           to_date(right(child.relname, 8), 'YYYYMMDD') AS day,
           pg_get_expr(child.relpartbound, child.oid) AS bound
    FROM pg_inherits i
    JOIN pg_class child ON child.oid = i.inhrelid
    JOIN pg_class parent ON parent.oid = i.inhparent
    JOIN pg_namespace n ON n.oid = parent.relnamespace
    WHERE n.nspname = current_schema()
      AND parent.relkind = 'p'
      AND child.relkind = 'r'
      AND parent.relname = ANY(:parents)
    ORDER BY parent.relname, child.relname
    """
)


@dataclass(frozen=True, slots=True)
class HistoryParentSpec:
    """Allowlisted daily UTC RANGE history parent."""

    parent_name: str
    partition_key: str


@dataclass(frozen=True, slots=True)
class PartitionDaySpec:
    """One UTC calendar-day child partition specification."""

    day: date
    child_name: str
    bound_from: datetime
    bound_to: datetime


def history_parent_specs() -> tuple[HistoryParentSpec, ...]:
    """Return the Phase 3.1 allowlisted history parents in stable order."""
    return tuple(
        HistoryParentSpec(parent_name=name, partition_key=_PARENT_KEYS[name])
        for name in DAILY_RANGE_PARENTS
    )


def validate_horizon_days(horizon_days: int) -> int:
    """Accept only the deployment-configured 14–30 day premake window."""
    if not isinstance(horizon_days, int) or isinstance(horizon_days, bool):
        raise ValueError("horizon_days must be an int")
    if horizon_days < MIN_PREMAKE_HORIZON_DAYS or horizon_days > MAX_PREMAKE_HORIZON_DAYS:
        raise ValueError(
            f"horizon_days must be between {MIN_PREMAKE_HORIZON_DAYS} and "
            f"{MAX_PREMAKE_HORIZON_DAYS} inclusive, got {horizon_days}"
        )
    return horizon_days


def store_utc_today(connection: Connection) -> date:
    """Read Queue-store UTC calendar day from the caller-held session."""
    return connection.execute(_UTC_TODAY_SQL).scalar_one()


def utc_day_bounds(day: date) -> tuple[datetime, datetime]:
    """Half-open UTC timestamptz bounds for one calendar day (Phase 3.1)."""
    bound_from = datetime.combine(day, time.min, tzinfo=timezone.utc)
    bound_to = datetime.combine(day + timedelta(days=1), time.min, tzinfo=timezone.utc)
    return bound_from, bound_to


def child_name_for(parent_name: str, day: date) -> str:
    """Canonical child relation name ``{parent}_{YYYYMMDD}``."""
    if parent_name not in _PARENT_KEYS:
        raise ValueError(f"refusing unknown history parent: {parent_name!r}")
    return f"{parent_name}_{day.strftime('%Y%m%d')}"


def day_spec_for(parent_name: str, day: date) -> PartitionDaySpec:
    """Build a trusted partition spec from allowlisted parent + UTC day."""
    bound_from, bound_to = utc_day_bounds(day)
    return PartitionDaySpec(
        day=day,
        child_name=child_name_for(parent_name, day),
        bound_from=bound_from,
        bound_to=bound_to,
    )


def required_day_specs(
    parent_name: str,
    *,
    store_today: date,
    horizon_days: int,
) -> tuple[PartitionDaySpec, ...]:
    """Inclusive UTC horizon: store_today through store_today + horizon_days."""
    horizon_days = validate_horizon_days(horizon_days)
    if parent_name not in _PARENT_KEYS:
        raise ValueError(f"refusing unknown history parent: {parent_name!r}")
    return tuple(
        day_spec_for(parent_name, store_today + timedelta(days=offset))
        for offset in range(horizon_days + 1)
    )


def inspect_history_parents(connection: Connection) -> dict[str, HistoryParentSpec]:
    """Validate allowlisted parents exist as RANGE tables in current_schema()."""
    expected = {spec.parent_name: spec for spec in history_parent_specs()}
    rows = connection.execute(
        _PARENT_META_SQL, {"parents": list(expected)}
    ).all()
    found: dict[str, HistoryParentSpec] = {}
    for parent_name, partstrat, partkey in rows:
        name = str(parent_name)
        if name not in expected:
            continue
        if partstrat != "r":
            raise ValueError(f"history parent {name!r} is not RANGE-partitioned")
        key = expected[name].partition_key
        partkey_text = str(partkey or "")
        if key not in partkey_text:
            raise ValueError(
                f"history parent {name!r} partition key mismatch: expected {key!r}"
            )
        found[name] = expected[name]
    missing = set(expected) - set(found)
    if missing:
        raise ValueError(f"missing history partition parents: {sorted(missing)}")
    return found


def list_existing_children(
    connection: Connection,
) -> dict[str, dict[date, PartitionDaySpec]]:
    """Map parent → day → spec for existing non-DEFAULT children."""
    parents = [spec.parent_name for spec in history_parent_specs()]
    rows = connection.execute(_CHILDREN_SQL, {"parents": parents}).all()
    out: dict[str, dict[date, PartitionDaySpec]] = {name: {} for name in parents}
    for parent_name, child_name, day, bound in rows:
        parent = str(parent_name)
        if parent not in out:
            continue
        bound_text = str(bound or "")
        if "DEFAULT" in bound_text.upper():
            raise ValueError(
                f"DEFAULT partition forbidden on {parent}: {child_name}"
            )
        if day is None:
            continue
        day_value = day if isinstance(day, date) else date.fromisoformat(str(day))
        out[parent][day_value] = day_spec_for(parent, day_value)
        # Prefer catalog child name when it matches the canonical form.
        if str(child_name) != out[parent][day_value].child_name:
            # Still record under canonical day; creation path uses canonical names.
            pass
    return out
