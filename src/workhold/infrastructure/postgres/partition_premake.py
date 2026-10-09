"""Held-session idempotent daily UTC history partition premake (STOR-03).

Requires the caller's already-open SQLAlchemy ``Connection``. Never acquires
advisory locks, never creates engines/connections, and never commits, rolls
back, or closes the caller session — Plan 03.8-06 owns lock and session lifetime.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Mapping

from sqlalchemy import text
from sqlalchemy.engine import Connection

from workhold.infrastructure.postgres import partition_catalog

# format() produces driver-safe DDL from allowlisted identifiers + typed bounds.
_FORMAT_CREATE_SQL = text(
    """
    SELECT format(
        'CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
        CAST(:child_name AS text),
        CAST(:parent_name AS text),
        CAST(:bound_from AS timestamptz),
        CAST(:bound_to AS timestamptz)
    )
    """
)


@dataclass(frozen=True, slots=True)
class ParentPremakeResult:
    """Created vs already-present children for one history parent."""

    parent_name: str
    created: tuple[partition_catalog.PartitionDaySpec, ...]
    existing: tuple[partition_catalog.PartitionDaySpec, ...]


@dataclass(frozen=True, slots=True)
class PremakeResult:
    """Typed premake outcome with UTC bounds for every history parent."""

    store_today: date
    through_day: date
    horizon_days: int
    by_parent: Mapping[str, ParentPremakeResult]


def premake_daily_partitions(
    connection: Connection,
    *,
    horizon_days: int,
) -> PremakeResult:
    """Create missing daily UTC children through ``horizon_days`` on ``connection``.

    Operates only through the provided session (``current_schema()``). DDL runs
    inside the caller's transaction; this function does not commit, roll back,
    or close ``connection``.
    """
    horizon_days = partition_catalog.validate_horizon_days(horizon_days)
    parents = partition_catalog.inspect_history_parents(connection)
    store_today = partition_catalog.store_utc_today(connection)
    through_day = store_today + timedelta(days=horizon_days)
    existing = partition_catalog.list_existing_children(connection)

    by_parent: dict[str, ParentPremakeResult] = {}
    for parent_name in parents:
        required = partition_catalog.required_day_specs(
            parent_name,
            store_today=store_today,
            horizon_days=horizon_days,
        )
        present = existing.get(parent_name, {})
        created: list[partition_catalog.PartitionDaySpec] = []
        already: list[partition_catalog.PartitionDaySpec] = []
        for spec in required:
            if spec.day in present:
                already.append(present[spec.day])
                continue
            _create_child(connection, spec, parent_name=parent_name)
            created.append(spec)
        by_parent[parent_name] = ParentPremakeResult(
            parent_name=parent_name,
            created=tuple(created),
            existing=tuple(already),
        )

    return PremakeResult(
        store_today=store_today,
        through_day=through_day,
        horizon_days=horizon_days,
        by_parent=by_parent,
    )


def _create_child(
    connection: Connection,
    spec: partition_catalog.PartitionDaySpec,
    *,
    parent_name: str,
) -> None:
    """Create one PARTITION OF child using PostgreSQL format() escaping."""
    if parent_name not in parents_allowlist():
        raise ValueError(f"refusing unknown history parent: {parent_name!r}")
    if spec.child_name != partition_catalog.child_name_for(parent_name, spec.day):
        raise ValueError(f"refusing non-canonical child name: {spec.child_name!r}")

    ddl = connection.execute(
        _FORMAT_CREATE_SQL,
        {
            "child_name": spec.child_name,
            "parent_name": parent_name,
            "bound_from": spec.bound_from,
            "bound_to": spec.bound_to,
        },
    ).scalar_one()
    # Server-built DDL string; identifiers/literals already escaped by format().
    connection.execute(text(str(ddl)))


def parents_allowlist() -> frozenset[str]:
    """Allowlisted parent relation names (no runtime-selected relations)."""
    return frozenset(s.parent_name for s in partition_catalog.history_parent_specs())
