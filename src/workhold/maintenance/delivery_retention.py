"""Queue-owned Delivery Outbox terminal retention (OPS-05 / DLVR-02).

Published history defaults to 30 days (row purge). Dead-letter history defaults
to 90 days and gates ``delivery_events_terminal`` partition detach/drop via the
existing Phase 3.8 maintainer. Active pending/publishing rows are never
retention candidates.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Final

from sqlalchemy import text
from sqlalchemy.engine import Connection

from workhold.delivery.models import STATE_PUBLISHED
from workhold.health import DAILY_RANGE_PARENTS
from workhold.infrastructure.postgres.maintenance import StorageMaintenanceReport
from workhold.security.payload_policy import (
    PAYLOAD_RETENTION_DAYS_MAX,
    PAYLOAD_RETENTION_DAYS_MIN,
)

DEFAULT_PUBLISHED_RETENTION_DAYS: Final[int] = 30
DEFAULT_DEAD_LETTER_RETENTION_DAYS: Final[int] = 90

_STORE_NOW_SQL = text("SELECT CURRENT_TIMESTAMP")

_PURGE_PUBLISHED_SQL = text(
    """
    DELETE FROM delivery_events_terminal
    WHERE state_code = :published
      AND terminal_at < :cutoff
    """
)


@dataclass(frozen=True, slots=True)
class PublishedPurgeResult:
    """Aggregate published-row purge outcome (never touches active rows)."""

    deleted: int
    failures: int
    failure_code: str | None
    last_success_at: datetime | None
    published_policy_days: int
    store_now: datetime


@dataclass(frozen=True, slots=True)
class DeliveryRetentionReport:
    """Operator-facing delivery retention bounds and drop health."""

    published_policy_days: int
    dead_letter_policy_days: int
    partitions_detached: int
    partitions_dropped: int
    published_rows_deleted: int
    failures: int
    failure_code: str | None
    last_success_at: datetime | None
    retained_from: object | None
    premade_through: object | None


def default_retention_days_by_parent(
    *,
    payload_retention_days: int = DEFAULT_DEAD_LETTER_RETENTION_DAYS,
    dead_letter_retention_days: int = DEFAULT_DEAD_LETTER_RETENTION_DAYS,
) -> dict[str, int]:
    """Build the Phase 3.8 ``retention_days_by_parent`` map with delivery at 90d.

    Partition detach for ``delivery_events_terminal`` uses the dead-letter window
    so published 30d expiry is enforced by :func:`purge_expired_published_events`
    rather than early partition drop.
    """
    if not isinstance(payload_retention_days, int) or isinstance(
        payload_retention_days, bool
    ):
        raise ValueError("payload_retention_days must be an int")
    if not (
        PAYLOAD_RETENTION_DAYS_MIN
        <= payload_retention_days
        <= PAYLOAD_RETENTION_DAYS_MAX
    ):
        raise ValueError(
            "payload_retention_days must be between "
            f"{PAYLOAD_RETENTION_DAYS_MIN} and {PAYLOAD_RETENTION_DAYS_MAX}"
        )
    if not isinstance(dead_letter_retention_days, int) or isinstance(
        dead_letter_retention_days, bool
    ):
        raise ValueError("dead_letter_retention_days must be an int")
    if not (
        PAYLOAD_RETENTION_DAYS_MIN
        <= dead_letter_retention_days
        <= PAYLOAD_RETENTION_DAYS_MAX
    ):
        raise ValueError(
            "dead_letter_retention_days must be between "
            f"{PAYLOAD_RETENTION_DAYS_MIN} and {PAYLOAD_RETENTION_DAYS_MAX}"
        )
    out = {parent: payload_retention_days for parent in DAILY_RANGE_PARENTS}
    out["delivery_events_terminal"] = dead_letter_retention_days
    out["tasks_terminal"] = payload_retention_days
    return out


def validate_published_retention_days(days: int) -> int:
    if not isinstance(days, int) or isinstance(days, bool):
        raise ValueError("published_retention_days must be an int")
    if not (PAYLOAD_RETENTION_DAYS_MIN <= days <= PAYLOAD_RETENTION_DAYS_MAX):
        raise ValueError(
            "published_retention_days must be between "
            f"{PAYLOAD_RETENTION_DAYS_MIN} and {PAYLOAD_RETENTION_DAYS_MAX}"
        )
    return days


def purge_expired_published_events(
    connection: Connection,
    *,
    published_retention_days: int = DEFAULT_PUBLISHED_RETENTION_DAYS,
) -> PublishedPurgeResult:
    """Delete fully expired published terminal rows; never touch active work.

    Dead-lettered rows and ``delivery_events_active`` are excluded by
    ``state_code`` / table selection.
    """
    days = validate_published_retention_days(published_retention_days)
    store_now = _store_utc_now(connection)
    cutoff = store_now - timedelta(days=days)
    try:
        result = connection.execute(
            _PURGE_PUBLISHED_SQL,
            {"published": STATE_PUBLISHED, "cutoff": cutoff},
        )
        deleted = int(result.rowcount or 0)
        return PublishedPurgeResult(
            deleted=deleted,
            failures=0,
            failure_code=None,
            last_success_at=store_now,
            published_policy_days=days,
            store_now=store_now,
        )
    except Exception:  # noqa: BLE001 — bounded failure for operator report
        return PublishedPurgeResult(
            deleted=0,
            failures=1,
            failure_code="published_purge_failed",
            last_success_at=None,
            published_policy_days=days,
            store_now=store_now,
        )


def build_delivery_retention_report(
    *,
    storage_report: StorageMaintenanceReport,
    published_purge: PublishedPurgeResult,
    dead_letter_policy_days: int = DEFAULT_DEAD_LETTER_RETENTION_DAYS,
) -> DeliveryRetentionReport:
    """Combine partition maintenance + published purge into one operator report."""
    failures = published_purge.failures
    failure_code = published_purge.failure_code
    if storage_report.outcome == "failed":
        failures += 1
        failure_code = storage_report.error_code or failure_code
    last_success = published_purge.last_success_at
    if storage_report.outcome == "succeeded" and storage_report.last_succeeded_at:
        last_success = storage_report.last_succeeded_at
    return DeliveryRetentionReport(
        published_policy_days=published_purge.published_policy_days,
        dead_letter_policy_days=dead_letter_policy_days,
        partitions_detached=storage_report.partitions_detached,
        partitions_dropped=storage_report.partitions_dropped,
        published_rows_deleted=published_purge.deleted,
        failures=failures,
        failure_code=failure_code,
        last_success_at=last_success,
        retained_from=storage_report.retained_from,
        premade_through=storage_report.premade_through,
    )


def _store_utc_now(connection: Connection) -> datetime:
    value = connection.execute(_STORE_NOW_SQL).scalar_one()
    if not isinstance(value, datetime):
        raise TypeError("CURRENT_TIMESTAMP did not return datetime")
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value