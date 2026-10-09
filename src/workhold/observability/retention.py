"""Retention and partition-headroom telemetry with alert predicates (OPS-05/09).

Adapts Phase 3.8 :class:`StorageMaintenanceReport` into Plan 01 metric
conventions: aggregate-only gauges, closed window enums, Queue-store
timestamps, and bounded failure codes. Maintenance diagnostics share
:func:`workhold.observability.context.project_correlation`.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import Any, Final

from workhold.infrastructure.postgres.maintenance import StorageMaintenanceReport
from workhold.observability.context import project_correlation
from workhold.observability.metrics import KernelMetrics

# Baseline retention windows (storage-topology.md / ADR 017).
DEFAULT_TASK_TERMINAL_RETENTION_DAYS: Final[int] = 90
DEFAULT_ATTEMPT_RETENTION_DAYS: Final[int] = 90
DEFAULT_ADMIN_AUDIT_RETENTION_DAYS: Final[int] = 90
DEFAULT_ENQUEUE_DEDUP_DAYS: Final[int] = 90
DEFAULT_COMPLETE_REPLAY_DAYS: Final[int] = 7
DEFAULT_ADMIN_REPLAY_DAYS: Final[int] = 30
DEFAULT_DELIVERY_PUBLISHED_DAYS: Final[int] = 30
DEFAULT_DELIVERY_DEAD_LETTER_DAYS: Final[int] = 90

# Alert before partition horizon reaches the readiness failure window.
PREMAKE_HEADROOM_WARN_DAYS: Final[int] = 3
SUSTAINED_FAILURE_THRESHOLD: Final[int] = 2

# Maintenance correlation keys (subset of Plan 01 allowlist).
_MAINT_CORRELATION_KEYS: Final[frozenset[str]] = frozenset(
    {
        "request_id",
        "trace_id",
        "operation",
        "process_role",
        "store_now",
        "result",
        "code",
    }
)


class RetentionWindow(Enum):
    """Distinct retention correctness windows (never relation/partition names)."""

    TASK_TERMINAL = "task_terminal"
    ATTEMPT = "attempt"
    ADMIN_AUDIT = "admin_audit"
    CORRECTNESS_ENQUEUE_DEDUP = "correctness_enqueue_dedup"
    CORRECTNESS_COMPLETE_REPLAY = "correctness_complete_replay"
    CORRECTNESS_ADMIN_REPLAY = "correctness_admin_replay"
    DELIVERY_PUBLISHED = "delivery_published"
    DELIVERY_DEAD_LETTER = "delivery_dead_letter"


class MaintenanceSignalOutcome(Enum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED_LOCK = "skipped_lock"


class RetentionAlertSeverity(Enum):
    NONE = "none"
    WARNING = "warning"
    CRITICAL = "critical"


class RetentionAlertKind(Enum):
    PREMAKE_HEADROOM_LOW = "premake_headroom_low"
    PREMAKE_HEADROOM_EXHAUSTED = "premake_headroom_exhausted"
    SUSTAINED_MAINTENANCE_FAILURE = "sustained_maintenance_failure"
    STATS_STALE = "stats_stale"


class MaintenanceDiagEvent(Enum):
    """Bounded maintenance diagnostic operations for logs/traces."""

    START = "maintain.start"
    SUCCESS = "maintain.success"
    FAILURE = "maintain.failure"
    LOCK_LOSER = "maintain.lock_loser"
    DETACH_DROP = "maintain.detach_drop"
    REGISTRY_PURGE = "maintain.registry_purge"


_DEFAULT_POLICY_DAYS: Final[dict[RetentionWindow, int]] = {
    RetentionWindow.TASK_TERMINAL: DEFAULT_TASK_TERMINAL_RETENTION_DAYS,
    RetentionWindow.ATTEMPT: DEFAULT_ATTEMPT_RETENTION_DAYS,
    RetentionWindow.ADMIN_AUDIT: DEFAULT_ADMIN_AUDIT_RETENTION_DAYS,
    RetentionWindow.CORRECTNESS_ENQUEUE_DEDUP: DEFAULT_ENQUEUE_DEDUP_DAYS,
    RetentionWindow.CORRECTNESS_COMPLETE_REPLAY: DEFAULT_COMPLETE_REPLAY_DAYS,
    RetentionWindow.CORRECTNESS_ADMIN_REPLAY: DEFAULT_ADMIN_REPLAY_DAYS,
    RetentionWindow.DELIVERY_PUBLISHED: DEFAULT_DELIVERY_PUBLISHED_DAYS,
    RetentionWindow.DELIVERY_DEAD_LETTER: DEFAULT_DELIVERY_DEAD_LETTER_DAYS,
}

_SEVERITY_RANK: Final[dict[RetentionAlertSeverity, int]] = {
    RetentionAlertSeverity.NONE: 0,
    RetentionAlertSeverity.WARNING: 1,
    RetentionAlertSeverity.CRITICAL: 2,
}

_HISTORY_WINDOWS: Final[frozenset[RetentionWindow]] = frozenset(
    {
        RetentionWindow.TASK_TERMINAL,
        RetentionWindow.ATTEMPT,
        RetentionWindow.ADMIN_AUDIT,
    }
)


@dataclass(frozen=True, slots=True)
class RetentionAlert:
    kind: RetentionAlertKind
    severity: RetentionAlertSeverity


@dataclass(frozen=True, slots=True)
class WindowRetentionBounds:
    window: RetentionWindow
    policy_days: int
    oldest_retained_age_days: int | None
    newest_retained_age_days: int | None


@dataclass(frozen=True, slots=True)
class RetentionHealthSnapshot:
    """Constant-size retention health boundary for operators and alerts."""

    observed_at: datetime
    store_today: date
    configured_horizon_days: int
    premake_headroom_days: int | None
    last_maintenance_started_at: datetime | None
    last_maintenance_success_at: datetime | None
    outcome: MaintenanceSignalOutcome
    failure_code: str | None
    partitions_created: int
    partitions_detached: int
    partitions_dropped: int
    purge_examined_total: int
    purge_deleted_total: int
    consecutive_failures: int
    windows: tuple[WindowRetentionBounds, ...]
    alerts: tuple[RetentionAlert, ...]

    def as_bounded_dict(self) -> dict[str, Any]:
        """Serialize without partition names, payloads, IDs, or free text."""
        return {
            "observed_at": self.observed_at.isoformat(),
            "store_today": self.store_today.isoformat(),
            "configured_horizon_days": self.configured_horizon_days,
            "premake_headroom_days": self.premake_headroom_days,
            "last_maintenance_started_at": (
                self.last_maintenance_started_at.isoformat()
                if self.last_maintenance_started_at is not None
                else None
            ),
            "last_maintenance_success_at": (
                self.last_maintenance_success_at.isoformat()
                if self.last_maintenance_success_at is not None
                else None
            ),
            "outcome": self.outcome.value,
            "failure_code": self.failure_code,
            "partitions_created": self.partitions_created,
            "partitions_detached": self.partitions_detached,
            "partitions_dropped": self.partitions_dropped,
            "purge_examined_total": self.purge_examined_total,
            "purge_deleted_total": self.purge_deleted_total,
            "consecutive_failures": self.consecutive_failures,
            "windows": [
                {
                    "window": w.window.value,
                    "policy_days": w.policy_days,
                    "oldest_retained_age_days": w.oldest_retained_age_days,
                    "newest_retained_age_days": w.newest_retained_age_days,
                }
                for w in self.windows
            ],
            "alerts": [
                {"kind": a.kind.value, "severity": a.severity.value}
                for a in self.alerts
            ],
        }


def severity_rank(severity: RetentionAlertSeverity) -> int:
    return _SEVERITY_RANK[severity]


def default_policy_days() -> dict[RetentionWindow, int]:
    return dict(_DEFAULT_POLICY_DAYS)


def is_noop_success(report: StorageMaintenanceReport) -> bool:
    """True when a succeeded cycle performed no detach/drop/purge deletes."""
    return (
        report.outcome == "succeeded"
        and report.partitions_detached == 0
        and report.partitions_dropped == 0
        and report.purge_deleted_total == 0
    )


def is_noop_success_alertable(snapshot: RetentionHealthSnapshot) -> bool:
    """Successful no-op cycles must not fire retention alerts by themselves."""
    return False


def evaluate_alerts(
    *,
    premake_headroom_days: int | None,
    configured_horizon_days: int,
    outcome: MaintenanceSignalOutcome,
    consecutive_failures: int,
    stats_stale: bool,
    is_noop_success: bool,
    failure_code: str | None = None,
) -> tuple[RetentionAlert, ...]:
    """Machine-testable retention alert predicates (observability.md).

    - Premake headroom warns at ``PREMAKE_HEADROOM_WARN_DAYS`` and goes critical
      at exhaustion (``<= 0``) — before readiness failure is inevitable.
    - Sustained maintenance failure alerts at ``SUSTAINED_FAILURE_THRESHOLD``;
      a single failure or one successful no-op cycle does not.
    - Stats staleness is WARNING and always less severe than CRITICAL
      correctness-path alerts.
    """
    del configured_horizon_days, failure_code, is_noop_success  # documented inputs
    alerts: list[RetentionAlert] = []

    if premake_headroom_days is not None:
        if premake_headroom_days <= 0:
            alerts.append(
                RetentionAlert(
                    kind=RetentionAlertKind.PREMAKE_HEADROOM_EXHAUSTED,
                    severity=RetentionAlertSeverity.CRITICAL,
                )
            )
        elif premake_headroom_days <= PREMAKE_HEADROOM_WARN_DAYS:
            alerts.append(
                RetentionAlert(
                    kind=RetentionAlertKind.PREMAKE_HEADROOM_LOW,
                    severity=RetentionAlertSeverity.WARNING,
                )
            )

    if (
        outcome is MaintenanceSignalOutcome.FAILED
        and consecutive_failures >= SUSTAINED_FAILURE_THRESHOLD
    ):
        alerts.append(
            RetentionAlert(
                kind=RetentionAlertKind.SUSTAINED_MAINTENANCE_FAILURE,
                severity=RetentionAlertSeverity.CRITICAL,
            )
        )

    if stats_stale:
        alerts.append(
            RetentionAlert(
                kind=RetentionAlertKind.STATS_STALE,
                severity=RetentionAlertSeverity.WARNING,
            )
        )

    return tuple(alerts)


def build_retention_health(
    report: StorageMaintenanceReport,
    *,
    observed_at: datetime,
    store_today: date,
    configured_horizon_days: int,
    consecutive_failures: int = 0,
    policy_days: Mapping[RetentionWindow, int] | None = None,
    stats_stale: bool = False,
) -> RetentionHealthSnapshot:
    """Adapt a Phase 3.8 maintenance report into a bounded retention snapshot."""
    if observed_at.tzinfo is None:
        raise ValueError("observed_at must be timezone-aware Queue-store time")

    days = default_policy_days()
    if policy_days:
        days.update(dict(policy_days))

    outcome = (
        MaintenanceSignalOutcome.FAILED
        if report.outcome == "failed"
        else MaintenanceSignalOutcome.SUCCEEDED
    )
    headroom: int | None = None
    if report.premade_through is not None:
        headroom = (report.premade_through - store_today).days

    oldest_age: int | None = None
    newest_age: int | None = None
    if report.retained_from is not None:
        oldest_age = (store_today - report.retained_from).days
        newest_age = 0

    windows: list[WindowRetentionBounds] = []
    for window in RetentionWindow:
        policy = days[window]
        if window in _HISTORY_WINDOWS:
            windows.append(
                WindowRetentionBounds(
                    window=window,
                    policy_days=policy,
                    oldest_retained_age_days=oldest_age,
                    newest_retained_age_days=newest_age,
                )
            )
        else:
            # Correctness registries are TTL-purged, not partition-detached;
            # age bounds are unavailable from the aggregate maintenance report.
            windows.append(
                WindowRetentionBounds(
                    window=window,
                    policy_days=policy,
                    oldest_retained_age_days=None,
                    newest_retained_age_days=None,
                )
            )

    failure_code = report.error_code if outcome is MaintenanceSignalOutcome.FAILED else None
    # Never copy free-text error_detail into the snapshot.
    alerts = evaluate_alerts(
        premake_headroom_days=headroom,
        configured_horizon_days=configured_horizon_days,
        outcome=outcome,
        consecutive_failures=consecutive_failures,
        stats_stale=stats_stale,
        is_noop_success=is_noop_success(report),
        failure_code=failure_code,
    )

    return RetentionHealthSnapshot(
        observed_at=observed_at,
        store_today=store_today,
        configured_horizon_days=configured_horizon_days,
        premake_headroom_days=headroom,
        last_maintenance_started_at=report.last_started_at,
        last_maintenance_success_at=report.last_succeeded_at,
        outcome=outcome,
        failure_code=failure_code,
        partitions_created=report.partitions_created,
        partitions_detached=report.partitions_detached,
        partitions_dropped=report.partitions_dropped,
        purge_examined_total=report.purge_examined_total,
        purge_deleted_total=report.purge_deleted_total,
        consecutive_failures=consecutive_failures,
        windows=tuple(windows),
        alerts=alerts,
    )


def project_maintenance_correlation(
    *,
    event: MaintenanceDiagEvent,
    request_id: str | None,
    trace_id: str | None,
    process_role: str,
    store_now: datetime | None,
    result: str,
    code: str | None,
    extras: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Project maintenance diagnostics through the shared OPS-08 allowlist.

    ``extras`` may contain hostile fields (payload, DSN, SQL, partition names);
    only the maintenance key subset survives, via Plan 01 ``project_correlation``.
    """
    fields: dict[str, Any] = {
        "request_id": request_id,
        "trace_id": trace_id,
        "operation": event.value,
        "process_role": process_role,
        "store_now": store_now.isoformat() if store_now is not None else None,
        "result": result,
        "code": code,
    }
    polluted: dict[str, Any] = dict(extras or {})
    polluted.update(fields)
    projected = project_correlation(polluted)
    return {k: v for k, v in projected.items() if k in _MAINT_CORRELATION_KEYS}


def record_retention_metrics(
    metrics: KernelMetrics,
    snapshot: RetentionHealthSnapshot,
) -> None:
    """Record aggregate retention gauges/counters with allowlisted labels only."""
    if snapshot.premake_headroom_days is not None:
        metrics.set_premake_headroom_days(float(snapshot.premake_headroom_days))

    if snapshot.last_maintenance_success_at is not None:
        age = (
            snapshot.observed_at - snapshot.last_maintenance_success_at
        ).total_seconds()
        if age < 0:
            age = 0.0
        metrics.set_maintenance_last_success_age_seconds(age)

    metrics.record_retention_partition_counts(
        result=snapshot.outcome.value,
        created=snapshot.partitions_created,
        detached=snapshot.partitions_detached,
        dropped=snapshot.partitions_dropped,
        purge_deleted=snapshot.purge_deleted_total,
        failure_code=snapshot.failure_code,
    )

    for window in snapshot.windows:
        metrics.set_retention_window_bounds(
            retention_window=window.window.value,
            policy_days=window.policy_days,
            oldest_retained_age_days=(
                float(window.oldest_retained_age_days)
                if window.oldest_retained_age_days is not None
                else None
            ),
            newest_retained_age_days=(
                float(window.newest_retained_age_days)
                if window.newest_retained_age_days is not None
                else None
            ),
        )
