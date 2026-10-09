"""Pressure observation bounds, freshness, and unavailable-signal coverage (OPS-02)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from workhold.observability import pressure


def test_pressure_snapshot_is_constant_size_and_typed() -> None:
    observed_at = datetime(2026, 9, 19, 6, 0, 0, tzinfo=UTC)
    snap = pressure.PressureSnapshot(
        observed_at=observed_at,
        collected_monotonic_ns=1_000_000_000,
        freshness=pressure.Freshness.FRESH,
        pool=pressure.PoolPressure(
            status=pressure.PressureStatus.OK,
            reason=pressure.PressureReason.WITHIN_BUDGET,
            acquisition_wait_seconds=0.012,
            saturation_ratio=0.25,
        ),
        wal=pressure.SignalPressure(
            status=pressure.PressureStatus.WARNING,
            reason=pressure.PressureReason.WAL_GROWTH,
            value=0.72,
        ),
        disk=pressure.SignalPressure(
            status=pressure.PressureStatus.OK,
            reason=pressure.PressureReason.WITHIN_BUDGET,
            value=0.41,
        ),
        autovacuum=pressure.SignalPressure(
            status=pressure.PressureStatus.CRITICAL,
            reason=pressure.PressureReason.AUTOVACUUM_LAG,
            value=0.95,
        ),
    )

    assert snap.freshness is pressure.Freshness.FRESH
    assert snap.pool.acquisition_wait_seconds == pytest.approx(0.012)
    assert snap.pool.saturation_ratio == pytest.approx(0.25)
    assert snap.wal.status is pressure.PressureStatus.WARNING
    assert snap.disk.status is pressure.PressureStatus.OK
    assert snap.autovacuum.status is pressure.PressureStatus.CRITICAL

    as_dict = snap.as_bounded_dict()
    assert set(as_dict) == {
        "observed_at",
        "collected_monotonic_ns",
        "freshness",
        "pool",
        "wal",
        "disk",
        "autovacuum",
    }
    assert set(as_dict["pool"]) == {
        "status",
        "reason",
        "acquisition_wait_seconds",
        "saturation_ratio",
    }
    # No high-cardinality / secret surfaces.
    blob = repr(as_dict).lower()
    for forbidden in (
        "select ",
        "from ",
        "/var/",
        "c:\\",
        "relation",
        "payload",
        "claim_token",
        "task_id",
        "pg_stat",
    ):
        assert forbidden not in blob


def test_unavailable_signals_are_explicit_and_bounded() -> None:
    snap = pressure.unavailable_snapshot(
        observed_at=datetime(2026, 9, 19, 6, 1, 0, tzinfo=UTC),
        collected_monotonic_ns=2_000_000_000,
        reason=pressure.PressureReason.COLLECTOR_UNAVAILABLE,
    )
    assert snap.freshness is pressure.Freshness.UNAVAILABLE
    for signal in (snap.pool, snap.wal, snap.disk, snap.autovacuum):
        assert signal.status is pressure.PressureStatus.UNAVAILABLE
        assert signal.reason is pressure.PressureReason.COLLECTOR_UNAVAILABLE
    assert snap.pool.acquisition_wait_seconds is None
    assert snap.pool.saturation_ratio is None
    assert snap.wal.value is None
    assert snap.disk.value is None
    assert snap.autovacuum.value is None


def test_freshness_classification_uses_observed_at_and_now() -> None:
    observed_at = datetime(2026, 9, 19, 6, 0, 0, tzinfo=UTC)
    fresh = pressure.classify_freshness(
        observed_at=observed_at,
        now=observed_at + timedelta(seconds=2),
        max_age_seconds=5.0,
    )
    stale = pressure.classify_freshness(
        observed_at=observed_at,
        now=observed_at + timedelta(seconds=10),
        max_age_seconds=5.0,
    )
    assert fresh is pressure.Freshness.FRESH
    assert stale is pressure.Freshness.STALE


def test_build_snapshot_rejects_out_of_range_ratios() -> None:
    observed_at = datetime(2026, 9, 19, 6, 0, 0, tzinfo=UTC)
    with pytest.raises(pressure.PressureBoundError):
        pressure.build_snapshot(
            observed_at=observed_at,
            collected_monotonic_ns=1,
            freshness=pressure.Freshness.FRESH,
            pool_wait_seconds=-0.1,
            pool_saturation_ratio=0.2,
            wal_value=0.1,
            disk_value=0.1,
            autovacuum_value=0.1,
        )
    with pytest.raises(pressure.PressureBoundError):
        pressure.build_snapshot(
            observed_at=observed_at,
            collected_monotonic_ns=1,
            freshness=pressure.Freshness.FRESH,
            pool_wait_seconds=0.1,
            pool_saturation_ratio=1.5,
            wal_value=0.1,
            disk_value=0.1,
            autovacuum_value=0.1,
        )
    with pytest.raises(pressure.PressureBoundError):
        pressure.build_snapshot(
            observed_at=observed_at,
            collected_monotonic_ns=1,
            freshness=pressure.Freshness.FRESH,
            pool_wait_seconds=0.1,
            pool_saturation_ratio=0.2,
            wal_value=2.0,
            disk_value=0.1,
            autovacuum_value=0.1,
        )


def test_build_snapshot_normalizes_status_from_thresholds() -> None:
    observed_at = datetime(2026, 9, 19, 6, 0, 0, tzinfo=UTC)
    snap = pressure.build_snapshot(
        observed_at=observed_at,
        collected_monotonic_ns=42,
        freshness=pressure.Freshness.FRESH,
        pool_wait_seconds=0.05,
        pool_saturation_ratio=0.9,
        wal_value=0.85,
        disk_value=0.2,
        autovacuum_value=0.4,
    )
    assert snap.pool.status is pressure.PressureStatus.CRITICAL
    assert snap.pool.reason is pressure.PressureReason.POOL_SATURATION
    assert snap.wal.status is pressure.PressureStatus.WARNING
    assert snap.wal.reason is pressure.PressureReason.WAL_GROWTH
    assert snap.disk.status is pressure.PressureStatus.OK
    assert snap.autovacuum.status is pressure.PressureStatus.OK


def test_pressure_enums_are_closed_sets() -> None:
    assert set(pressure.Freshness) == {
        pressure.Freshness.FRESH,
        pressure.Freshness.STALE,
        pressure.Freshness.UNAVAILABLE,
    }
    assert set(pressure.PressureStatus) == {
        pressure.PressureStatus.OK,
        pressure.PressureStatus.WARNING,
        pressure.PressureStatus.CRITICAL,
        pressure.PressureStatus.UNAVAILABLE,
    }
    # Reasons stay bounded — adaptive control (04-03) depends on this closed set.
    assert pressure.PressureReason.WITHIN_BUDGET in pressure.PressureReason
    assert pressure.PressureReason.COLLECTOR_UNAVAILABLE in pressure.PressureReason
    assert len(pressure.PressureReason) <= 16
