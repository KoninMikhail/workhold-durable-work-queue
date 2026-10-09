"""Framework-neutral process liveness and correctness-path readiness.

Liveness is process/event-loop only and never opens a database connection.
Readiness acquires from a caller-owned API-role pool and runs bounded
read-only probes: Alembic revision compatibility and continuous UTC daily
partition coverage through the configured premake horizon.

Failure reasons are stable low-cardinality codes with no DSN or SQL detail.
Statistics freshness, relay destination, and retention lag are intentionally
out of scope for Work Queue API readiness.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Final

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import DBAPIError, OperationalError, TimeoutError as SATimeoutError

# Binary-declared schema compatibility window (inclusive ranks in revision_order).
# Must reach alembic head so migrate → api readiness admits traffic after Phase 4–5
# revisions (040–0502). Unknown future heads still fail closed as schema_above_range.
BINARY_COMPATIBLE_MIN: Final[str] = "0001_physical_contract_foundations"
BINARY_COMPATIBLE_MAX: Final[str] = "2001_claim_long_poll_wakeup"
BINARY_REVISION_ORDER: Final[tuple[str, ...]] = (
    "0001_physical_contract_foundations",
    "039_apply_qualified_storage_layout",
    "040_admin_dead_letter_replay_ops",
    "041_admin_bulk_ops",
    "042_admin_break_glass_ops",
    "0501_delivery_outbox",
    "0502_delivery_pending_generation",
    "1201_bounded_priority_claim_ordering",
    "043_break_glass_delivery_ops",
    "044_break_glass_elevations",
    "2001_claim_long_poll_wakeup",
)

DEFAULT_PARTITION_PREMAKE_DAYS: Final[int] = 30

DAILY_RANGE_PARENTS: Final[tuple[str, ...]] = (
    "admin_audit_log",
    "task_attempts",
    "tasks_terminal",
    "delivery_events_terminal",
)

_SCHEMA_NAME_RE = re.compile(r"^[a-z_][a-z0-9_]*$")

_READINESS_PING_SQL = text("SELECT 1")
_SERVER_VERSION_NUM_SQL = text("SHOW server_version_num")
_ALEMBIC_REVISION_SQL = text("SELECT version_num FROM alembic_version LIMIT 1")


class ReasonCode:
    """Bounded readiness failure labels (never include DSN/SQL/secrets)."""

    POSTGRES_UNAVAILABLE: Final[str] = "postgres_unavailable"
    POOL_TIMEOUT: Final[str] = "pool_timeout"
    STATEMENT_TIMEOUT: Final[str] = "statement_timeout"
    SCHEMA_BELOW_RANGE: Final[str] = "schema_below_range"
    SCHEMA_ABOVE_RANGE: Final[str] = "schema_above_range"
    PARTITION_MISSING: Final[str] = "partition_missing"
    PARTITION_HORIZON_UNSAFE: Final[str] = "partition_horizon_unsafe"
    OVERLOAD_PRESSURE: Final[str] = "overload_pressure"
    POSTGRES_VERSION_UNSUPPORTED: Final[str] = "postgres_version_unsupported"


@dataclass(frozen=True, slots=True)
class PartitionReadiness:
    """Catalog-bounded premake headroom component of readiness."""

    safe: bool
    limiting_parent: str | None
    through_day: date | None
    remaining_utc_days: int | None
    required_horizon_days: int
    reason_code: str | None = None


@dataclass(frozen=True, slots=True)
class HealthStatus:
    """Outcome of a liveness or readiness evaluation."""

    ok: bool
    reason_code: str | None = None
    partition: PartitionReadiness | None = None

    def __repr__(self) -> str:
        return (
            f"HealthStatus(ok={self.ok!r}, reason_code={self.reason_code!r}, "
            f"partition={self.partition!r})"
        )


def check_liveness() -> HealthStatus:
    """Return ok when the process can evaluate health (no database I/O)."""
    return HealthStatus(ok=True, reason_code=None, partition=None)


def check_readiness(
    engine: Engine,
    *,
    schema: str | None = None,
    premake_days: int = DEFAULT_PARTITION_PREMAKE_DAYS,
    compatible_min: str = BINARY_COMPATIBLE_MIN,
    compatible_max: str = BINARY_COMPATIBLE_MAX,
    revision_order: Sequence[str] = BINARY_REVISION_ORDER,
    pressure_controller: object | None = None,
) -> HealthStatus:
    """Evaluate correctness-path readiness against PostgreSQL catalogs.

    Acquires one connection from ``engine`` (API-role bounded pool), runs
    read-only probes, and maps failures to :class:`ReasonCode` values.
    Partition headroom reuses plan-01 catalog inspection only (no history-row
    scans). Retention lag is not a readiness input.

    When ``pressure_controller`` is supplied and reports readiness failure
    (Phase 4 adaptive overload), readiness fails with ``overload_pressure``
    before opening a database connection.
    """
    if pressure_controller is not None:
        readiness_ok = getattr(pressure_controller, "readiness_ok", True)
        if readiness_ok is False:
            return HealthStatus(ok=False, reason_code=ReasonCode.OVERLOAD_PRESSURE)

    if premake_days < 0:
        raise ValueError("premake_days must be non-negative")
    if schema is not None and not _SCHEMA_NAME_RE.fullmatch(schema):
        raise ValueError(f"refusing unsafe schema name: {schema!r}")

    try:
        with engine.connect() as conn:
            if schema is not None:
                # Identifier validated above; SET is session-scoped for this connection.
                conn.execute(text(f"SET search_path TO {schema}"))

            conn.execute(_READINESS_PING_SQL)

            version_num = int(conn.execute(_SERVER_VERSION_NUM_SQL).scalar_one())
            major, minor = divmod(version_num, 10_000)
            if (major, minor) != (18, 6):
                return HealthStatus(
                    ok=False,
                    reason_code=ReasonCode.POSTGRES_VERSION_UNSUPPORTED,
                )

            revision_row = conn.execute(_ALEMBIC_REVISION_SQL).one_or_none()
            if revision_row is None:
                return HealthStatus(ok=False, reason_code=ReasonCode.SCHEMA_BELOW_RANGE)
            current_revision = str(revision_row[0])

            schema_status = _classify_revision(
                current_revision,
                compatible_min=compatible_min,
                compatible_max=compatible_max,
                revision_order=revision_order,
            )
            if schema_status is not None:
                return schema_status

            return _evaluate_partition_readiness(conn, premake_days=premake_days)
    except Exception as exc:
        return HealthStatus(ok=False, reason_code=_map_db_failure(exc))


def _classify_revision(
    current: str,
    *,
    compatible_min: str,
    compatible_max: str,
    revision_order: Sequence[str],
) -> HealthStatus | None:
    order = tuple(revision_order)
    try:
        current_idx = order.index(current)
        min_idx = order.index(compatible_min)
        max_idx = order.index(compatible_max)
    except ValueError:
        # Unknown revision relative to the binary catalog: treat as above-range
        # when it is not the declared minimum (roll-forward unknown), else below.
        if current == compatible_min or current == compatible_max:
            return None
        if compatible_min in order and current not in order:
            max_rev = compatible_max
            if current > max_rev:
                return HealthStatus(ok=False, reason_code=ReasonCode.SCHEMA_ABOVE_RANGE)
            return HealthStatus(ok=False, reason_code=ReasonCode.SCHEMA_BELOW_RANGE)
        return HealthStatus(ok=False, reason_code=ReasonCode.SCHEMA_BELOW_RANGE)

    if current_idx < min_idx:
        return HealthStatus(ok=False, reason_code=ReasonCode.SCHEMA_BELOW_RANGE)
    if current_idx > max_idx:
        return HealthStatus(ok=False, reason_code=ReasonCode.SCHEMA_ABOVE_RANGE)
    return None


def _evaluate_partition_readiness(
    connection: Connection,
    *,
    premake_days: int,
) -> HealthStatus:
    """Fail closed from plan-01 catalog inspection before the safety horizon."""
    # Lazy import avoids the partition_catalog → health cycle at module import.
    from workhold.infrastructure.postgres import partition_catalog

    try:
        partition_catalog.validate_horizon_days(premake_days)
        partition_catalog.inspect_history_parents(connection)
        store_today = partition_catalog.store_utc_today(connection)
        existing = partition_catalog.list_existing_children(connection)
    except ValueError:
        component = PartitionReadiness(
            safe=False,
            limiting_parent=DAILY_RANGE_PARENTS[0],
            through_day=None,
            remaining_utc_days=None,
            required_horizon_days=premake_days,
            reason_code=ReasonCode.PARTITION_MISSING,
        )
        return HealthStatus(
            ok=False,
            reason_code=ReasonCode.PARTITION_MISSING,
            partition=component,
        )

    required_last = store_today + timedelta(days=premake_days)
    missing_parent: str | None = None
    min_remaining: int | None = None
    limiting_parent = DAILY_RANGE_PARENTS[0]
    limiting_through: date | None = required_last

    for parent in DAILY_RANGE_PARENTS:
        days_map: Mapping[date, object] = existing.get(parent, {})
        days = set(days_map)
        through_day, remaining, gap = _parent_headroom(
            days,
            store_today=store_today,
            required_last=required_last,
        )
        if gap:
            missing_parent = parent
            limiting_parent = parent
            limiting_through = through_day
            min_remaining = remaining
            break
        if min_remaining is None or remaining < min_remaining or (
            remaining == min_remaining and parent < limiting_parent
        ):
            min_remaining = remaining
            limiting_parent = parent
            limiting_through = through_day

    assert min_remaining is not None  # DAILY_RANGE_PARENTS is non-empty

    if missing_parent is not None:
        component = PartitionReadiness(
            safe=False,
            limiting_parent=missing_parent,
            through_day=limiting_through,
            remaining_utc_days=min_remaining,
            required_horizon_days=premake_days,
            reason_code=ReasonCode.PARTITION_MISSING,
        )
        return HealthStatus(
            ok=False,
            reason_code=ReasonCode.PARTITION_MISSING,
            partition=component,
        )

    if min_remaining < premake_days:
        component = PartitionReadiness(
            safe=False,
            limiting_parent=limiting_parent,
            through_day=limiting_through,
            remaining_utc_days=min_remaining,
            required_horizon_days=premake_days,
            reason_code=ReasonCode.PARTITION_HORIZON_UNSAFE,
        )
        return HealthStatus(
            ok=False,
            reason_code=ReasonCode.PARTITION_HORIZON_UNSAFE,
            partition=component,
        )

    component = PartitionReadiness(
        safe=True,
        limiting_parent=limiting_parent,
        through_day=limiting_through,
        remaining_utc_days=min_remaining,
        required_horizon_days=premake_days,
        reason_code=None,
    )
    return HealthStatus(ok=True, reason_code=None, partition=component)


def _parent_headroom(
    days: set[date],
    *,
    store_today: date,
    required_last: date,
) -> tuple[date | None, int, bool]:
    """Return (through_day, remaining_utc_days, has_interior_gap)."""
    if store_today not in days:
        later_exists = any(day > store_today for day in days)
        if later_exists:
            return None, -1, True
        return None, -1, False

    cursor = store_today
    while cursor in days:
        cursor = cursor + timedelta(days=1)
    through_day = cursor - timedelta(days=1)
    remaining = (through_day - store_today).days

    expected = store_today
    while expected <= required_last:
        if expected not in days:
            later_exists = any(day > expected for day in days)
            if later_exists:
                return through_day, remaining, True
            return through_day, remaining, False
        expected = expected + timedelta(days=1)
    return through_day, remaining, False


def _map_db_failure(exc: BaseException) -> str:
    """Map SQLAlchemy/DBAPI failures to bounded reason codes (no SQL/DSN)."""
    if isinstance(exc, SATimeoutError):
        return ReasonCode.POOL_TIMEOUT

    text_blob = _safe_error_text(exc).lower()

    if (
        "statement timeout" in text_blob
        or "canceling statement due to statement timeout" in text_blob
        or "querycanceled" in text_blob
        or "query_canceled" in text_blob
    ):
        return ReasonCode.STATEMENT_TIMEOUT

    # SQLAlchemy QueuePool acquisition timeout (distinct from TCP connect timeout).
    if "queuepool limit" in text_blob or (
        "pool" in text_blob and "timed out" in text_blob
    ):
        return ReasonCode.POOL_TIMEOUT

    if isinstance(exc, (OperationalError, DBAPIError, OSError, TimeoutError, ConnectionError)):
        return ReasonCode.POSTGRES_UNAVAILABLE

    return ReasonCode.POSTGRES_UNAVAILABLE


def _safe_error_text(exc: BaseException) -> str:
    parts: list[str] = [type(exc).__name__]
    current: BaseException | None = exc
    depth = 0
    while current is not None and depth < 4:
        try:
            parts.append(str(current))
        except Exception:
            parts.append(type(current).__name__)
        current = current.__cause__ or current.__context__
        depth += 1
    return " | ".join(parts)
