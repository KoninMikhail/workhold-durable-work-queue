"""Retention health telemetry and alert-condition coverage (OPS-05, OPS-09).

Proves bounded retention signals for terminal tasks, attempts, audit history,
and correctness registries; alert predicates distinguish approaching premake
bounds from sustained maintenance failure; metrics never carry partition names,
task IDs, payload, or free text.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from workhold.infrastructure.postgres.maintenance import StorageMaintenanceReport
from workhold.observability import retention as retention_obs
from workhold.observability.metrics import KernelMetrics


def _aware(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _report(
    *,
    outcome: str = "succeeded",
    premade_through: date | None = None,
    retained_from: date | None = None,
    last_started_at: datetime | None = None,
    last_succeeded_at: datetime | None = None,
    partitions_created: int = 0,
    partitions_detached: int = 0,
    partitions_dropped: int = 0,
    purge_examined_total: int = 0,
    purge_deleted_total: int = 0,
    error_code: str | None = None,
    error_detail: str | None = None,
) -> StorageMaintenanceReport:
    now = _aware(datetime(2026, 9, 19, 12, 0, 0))
    return StorageMaintenanceReport(
        outcome=outcome,  # type: ignore[arg-type]
        premade_through=premade_through or date(2026, 10, 19),
        retained_from=retained_from or date(2026, 6, 21),
        last_started_at=last_started_at or now,
        last_succeeded_at=last_succeeded_at or (now if outcome == "succeeded" else None),
        premake=None,
        retention=None,
        purge=None,
        verification_reason=None,
        error_code=error_code,
        error_detail=error_detail,
        partitions_created=partitions_created,
        partitions_detached=partitions_detached,
        partitions_dropped=partitions_dropped,
        purge_examined_total=purge_examined_total,
        purge_deleted_total=purge_deleted_total,
        purge_by_registry=None,
    )


def test_windows_remain_distinct_with_baseline_policy_days() -> None:
    store_today = date(2026, 9, 19)
    report = _report(
        retained_from=date(2026, 6, 21),
        premade_through=date(2026, 10, 19),
    )
    snapshot = retention_obs.build_retention_health(
        report,
        observed_at=_aware(datetime(2026, 9, 19, 12, 0, 0)),
        store_today=store_today,
        configured_horizon_days=30,
        consecutive_failures=0,
    )

    by_window = {w.window: w for w in snapshot.windows}
    assert set(by_window) == {
        retention_obs.RetentionWindow.TASK_TERMINAL,
        retention_obs.RetentionWindow.ATTEMPT,
        retention_obs.RetentionWindow.ADMIN_AUDIT,
        retention_obs.RetentionWindow.CORRECTNESS_ENQUEUE_DEDUP,
        retention_obs.RetentionWindow.CORRECTNESS_COMPLETE_REPLAY,
        retention_obs.RetentionWindow.CORRECTNESS_ADMIN_REPLAY,
        retention_obs.RetentionWindow.DELIVERY_PUBLISHED,
        retention_obs.RetentionWindow.DELIVERY_DEAD_LETTER,
    }
    assert by_window[retention_obs.RetentionWindow.TASK_TERMINAL].policy_days == 90
    assert by_window[retention_obs.RetentionWindow.ATTEMPT].policy_days == 90
    assert by_window[retention_obs.RetentionWindow.ADMIN_AUDIT].policy_days == 90
    assert (
        by_window[retention_obs.RetentionWindow.CORRECTNESS_ENQUEUE_DEDUP].policy_days
        == 90
    )
    assert (
        by_window[
            retention_obs.RetentionWindow.CORRECTNESS_COMPLETE_REPLAY
        ].policy_days
        == 7
    )
    assert (
        by_window[retention_obs.RetentionWindow.CORRECTNESS_ADMIN_REPLAY].policy_days
        == 30
    )
    assert by_window[retention_obs.RetentionWindow.DELIVERY_PUBLISHED].policy_days == 30
    assert (
        by_window[retention_obs.RetentionWindow.DELIVERY_DEAD_LETTER].policy_days == 90
    )

    # Oldest/newest are day offsets — never physical relation/partition names.
    bounded = snapshot.as_bounded_dict()
    serialized = str(bounded)
    for forbidden in (
        "tasks_terminal",
        "task_attempts",
        "admin_audit_log",
        "delivery_events_terminal",
        "payload",
        "claim_token",
        "partition_name",
    ):
        assert forbidden not in serialized
    # Closed enum values only — not PostgreSQL relation names.
    for window_entry in bounded["windows"]:
        assert window_entry["window"] in {w.value for w in retention_obs.RetentionWindow}


def test_audit_retention_is_independently_configurable() -> None:
    snapshot = retention_obs.build_retention_health(
        _report(),
        observed_at=_aware(datetime(2026, 9, 19, 12, 0, 0)),
        store_today=date(2026, 9, 19),
        configured_horizon_days=30,
        policy_days={
            retention_obs.RetentionWindow.ADMIN_AUDIT: 60,
        },
    )
    audit = next(
        w
        for w in snapshot.windows
        if w.window == retention_obs.RetentionWindow.ADMIN_AUDIT
    )
    assert audit.policy_days == 60
    terminal = next(
        w
        for w in snapshot.windows
        if w.window == retention_obs.RetentionWindow.TASK_TERMINAL
    )
    assert terminal.policy_days == 90


def test_telemetry_reports_headroom_bounds_counts_and_last_success() -> None:
    started = _aware(datetime(2026, 9, 19, 11, 55, 0))
    succeeded = _aware(datetime(2026, 9, 19, 11, 56, 0))
    report = _report(
        outcome="succeeded",
        premade_through=date(2026, 10, 10),
        retained_from=date(2026, 6, 21),
        last_started_at=started,
        last_succeeded_at=succeeded,
        partitions_created=2,
        partitions_detached=1,
        partitions_dropped=1,
        purge_examined_total=10,
        purge_deleted_total=3,
    )
    observed = _aware(datetime(2026, 9, 19, 12, 0, 0))
    snapshot = retention_obs.build_retention_health(
        report,
        observed_at=observed,
        store_today=date(2026, 9, 19),
        configured_horizon_days=30,
    )

    assert snapshot.premake_headroom_days == (date(2026, 10, 10) - date(2026, 9, 19)).days
    assert snapshot.last_maintenance_success_at == succeeded
    assert snapshot.last_maintenance_started_at == started
    assert snapshot.partitions_created == 2
    assert snapshot.partitions_detached == 1
    assert snapshot.partitions_dropped == 1
    assert snapshot.purge_examined_total == 10
    assert snapshot.purge_deleted_total == 3
    assert snapshot.outcome == retention_obs.MaintenanceSignalOutcome.SUCCEEDED
    assert snapshot.failure_code is None

    history = next(
        w
        for w in snapshot.windows
        if w.window == retention_obs.RetentionWindow.TASK_TERMINAL
    )
    assert history.oldest_retained_age_days == (
        date(2026, 9, 19) - date(2026, 6, 21)
    ).days
    assert history.newest_retained_age_days == 0


def test_successful_noop_cycle_does_not_alert() -> None:
    report = _report(
        outcome="succeeded",
        partitions_created=0,
        partitions_detached=0,
        partitions_dropped=0,
        purge_deleted_total=0,
    )
    snapshot = retention_obs.build_retention_health(
        report,
        observed_at=_aware(datetime(2026, 9, 19, 12, 0, 0)),
        store_today=date(2026, 9, 19),
        configured_horizon_days=30,
        consecutive_failures=0,
    )
    assert snapshot.premake_headroom_days is not None
    assert snapshot.premake_headroom_days >= 30
    assert snapshot.alerts == ()
    assert not retention_obs.is_noop_success_alertable(snapshot)


@pytest.mark.parametrize(
    ("headroom", "expected_kinds", "expected_severity"),
    [
        (
            3,
            {retention_obs.RetentionAlertKind.PREMAKE_HEADROOM_LOW},
            retention_obs.RetentionAlertSeverity.WARNING,
        ),
        (
            1,
            {retention_obs.RetentionAlertKind.PREMAKE_HEADROOM_LOW},
            retention_obs.RetentionAlertSeverity.WARNING,
        ),
        (
            0,
            {retention_obs.RetentionAlertKind.PREMAKE_HEADROOM_EXHAUSTED},
            retention_obs.RetentionAlertSeverity.CRITICAL,
        ),
        (
            -1,
            {retention_obs.RetentionAlertKind.PREMAKE_HEADROOM_EXHAUSTED},
            retention_obs.RetentionAlertSeverity.CRITICAL,
        ),
    ],
)
def test_premake_headroom_alert_boundaries(
    headroom: int,
    expected_kinds: set[retention_obs.RetentionAlertKind],
    expected_severity: retention_obs.RetentionAlertSeverity,
) -> None:
    store_today = date(2026, 9, 19)
    premade = store_today + timedelta(days=headroom)
    report = _report(premade_through=premade, outcome="succeeded")
    snapshot = retention_obs.build_retention_health(
        report,
        observed_at=_aware(datetime(2026, 9, 19, 12, 0, 0)),
        store_today=store_today,
        configured_horizon_days=30,
        consecutive_failures=0,
    )
    kinds = {a.kind for a in snapshot.alerts}
    assert expected_kinds <= kinds
    matching = [a for a in snapshot.alerts if a.kind in expected_kinds]
    assert matching
    assert all(a.severity == expected_severity for a in matching)


def test_sustained_failure_alerts_single_failure_does_not() -> None:
    failed = _report(
        outcome="failed",
        error_code="partition_horizon_unsafe",
        error_detail="verification failed: partition_horizon_unsafe",
        last_succeeded_at=_aware(datetime(2026, 9, 18, 12, 0, 0)),
    )
    single = retention_obs.build_retention_health(
        failed,
        observed_at=_aware(datetime(2026, 9, 19, 12, 0, 0)),
        store_today=date(2026, 9, 19),
        configured_horizon_days=30,
        consecutive_failures=1,
    )
    assert retention_obs.RetentionAlertKind.SUSTAINED_MAINTENANCE_FAILURE not in {
        a.kind for a in single.alerts
    }
    assert single.failure_code == "partition_horizon_unsafe"
    # Free-text detail never enters the snapshot.
    assert "verification failed" not in str(single.as_bounded_dict())

    sustained = retention_obs.build_retention_health(
        failed,
        observed_at=_aware(datetime(2026, 9, 19, 12, 0, 0)),
        store_today=date(2026, 9, 19),
        configured_horizon_days=30,
        consecutive_failures=2,
    )
    kinds = {a.kind for a in sustained.alerts}
    assert retention_obs.RetentionAlertKind.SUSTAINED_MAINTENANCE_FAILURE in kinds
    alert = next(
        a
        for a in sustained.alerts
        if a.kind == retention_obs.RetentionAlertKind.SUSTAINED_MAINTENANCE_FAILURE
    )
    assert alert.severity == retention_obs.RetentionAlertSeverity.CRITICAL


def test_stats_staleness_is_less_severe_than_correctness_path_failure() -> None:
    stale_only = retention_obs.evaluate_alerts(
        premake_headroom_days=30,
        configured_horizon_days=30,
        outcome=retention_obs.MaintenanceSignalOutcome.SUCCEEDED,
        consecutive_failures=0,
        stats_stale=True,
        is_noop_success=True,
    )
    assert len(stale_only) == 1
    assert stale_only[0].kind == retention_obs.RetentionAlertKind.STATS_STALE
    assert stale_only[0].severity == retention_obs.RetentionAlertSeverity.WARNING

    correctness = retention_obs.evaluate_alerts(
        premake_headroom_days=0,
        configured_horizon_days=30,
        outcome=retention_obs.MaintenanceSignalOutcome.FAILED,
        consecutive_failures=2,
        stats_stale=True,
        is_noop_success=False,
        failure_code="partition_horizon_unsafe",
    )
    by_kind = {a.kind: a for a in correctness}
    assert (
        by_kind[retention_obs.RetentionAlertKind.STATS_STALE].severity
        == retention_obs.RetentionAlertSeverity.WARNING
    )
    assert (
        by_kind[
            retention_obs.RetentionAlertKind.SUSTAINED_MAINTENANCE_FAILURE
        ].severity
        == retention_obs.RetentionAlertSeverity.CRITICAL
    )
    assert (
        by_kind[retention_obs.RetentionAlertKind.PREMAKE_HEADROOM_EXHAUSTED].severity
        == retention_obs.RetentionAlertSeverity.CRITICAL
    )
    # Severity ordering: stats staleness < correctness-path failure.
    assert retention_obs.severity_rank(
        retention_obs.RetentionAlertSeverity.WARNING
    ) < retention_obs.severity_rank(retention_obs.RetentionAlertSeverity.CRITICAL)


def test_record_retention_metrics_are_aggregate_only() -> None:
    report = _report(
        partitions_detached=2,
        partitions_dropped=2,
        purge_deleted_total=5,
        error_code=None,
    )
    snapshot = retention_obs.build_retention_health(
        report,
        observed_at=_aware(datetime(2026, 9, 19, 12, 0, 0)),
        store_today=date(2026, 9, 19),
        configured_horizon_days=30,
    )
    metrics = KernelMetrics(process_role="maintain")
    retention_obs.record_retention_metrics(metrics, snapshot)

    samples = metrics.snapshot()
    assert samples
    for sample in samples:
        label_keys = set(sample.labels)
        assert "task_id" not in label_keys
        assert "claim_id" not in label_keys
        assert "partition" not in label_keys
        assert "payload" not in label_keys
        for key, value in sample.labels.items():
            assert key in {
                "process_role",
                "operation",
                "result",
                "failure_code",
                "retention_window",
                "queue",
                "terminal_outcome",
            }
            assert "token" not in value.lower()
            assert "payload" not in value.lower()
            # Closed retention_window enum values only.
            if key == "retention_window":
                assert value in {w.value for w in retention_obs.RetentionWindow}

    names = {s.name for s in samples}
    assert "queue_premake_headroom_days" in names
    assert "queue_retention_partitions_detached_total" in names
    assert "queue_retention_window_oldest_age_days" in names


def test_alert_predicates_are_machine_testable_at_documented_thresholds() -> None:
    """Boundary values match docs/05-operations/observability.md retention alerts."""
    assert retention_obs.PREMAKE_HEADROOM_WARN_DAYS == 3
    assert retention_obs.SUSTAINED_FAILURE_THRESHOLD == 2

    # Exactly at warn threshold → warning; one above → no headroom alert.
    at_warn = retention_obs.evaluate_alerts(
        premake_headroom_days=retention_obs.PREMAKE_HEADROOM_WARN_DAYS,
        configured_horizon_days=30,
        outcome=retention_obs.MaintenanceSignalOutcome.SUCCEEDED,
        consecutive_failures=0,
        stats_stale=False,
        is_noop_success=True,
    )
    assert any(
        a.kind == retention_obs.RetentionAlertKind.PREMAKE_HEADROOM_LOW for a in at_warn
    )

    above_warn = retention_obs.evaluate_alerts(
        premake_headroom_days=retention_obs.PREMAKE_HEADROOM_WARN_DAYS + 1,
        configured_horizon_days=30,
        outcome=retention_obs.MaintenanceSignalOutcome.SUCCEEDED,
        consecutive_failures=0,
        stats_stale=False,
        is_noop_success=True,
    )
    assert not any(
        a.kind
        in {
            retention_obs.RetentionAlertKind.PREMAKE_HEADROOM_LOW,
            retention_obs.RetentionAlertKind.PREMAKE_HEADROOM_EXHAUSTED,
        }
        for a in above_warn
    )


def test_failed_report_bounded_failure_code_only() -> None:
    report = _report(
        outcome="failed",
        error_code="OperationalError",
        error_detail="connection to host=db.internal password=s3cret failed",
    )
    snapshot = retention_obs.build_retention_health(
        report,
        observed_at=_aware(datetime(2026, 9, 19, 12, 0, 0)),
        store_today=date(2026, 9, 19),
        configured_horizon_days=30,
        consecutive_failures=2,
    )
    assert snapshot.failure_code == "OperationalError"
    blob = str(snapshot.as_bounded_dict())
    assert "s3cret" not in blob
    assert "password" not in blob
    assert "db.internal" not in blob
    assert "error_detail" not in blob
