"""Bounded PostgreSQL pressure observations for adaptive control (OPS-02).

Plan 04-03 consumes :class:`PressureSnapshot` directly. Snapshots are
constant-size, use closed status/reason enums, and never carry SQL,
filesystem paths, relation names, payloads, or identifiers.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Final


class Freshness(Enum):
    FRESH = "fresh"
    STALE = "stale"
    UNAVAILABLE = "unavailable"


class PressureStatus(Enum):
    OK = "ok"
    WARNING = "warning"
    CRITICAL = "critical"
    UNAVAILABLE = "unavailable"


class PressureReason(Enum):
    WITHIN_BUDGET = "within_budget"
    POOL_WAIT = "pool_wait"
    POOL_SATURATION = "pool_saturation"
    WAL_GROWTH = "wal_growth"
    DISK_PRESSURE = "disk_pressure"
    AUTOVACUUM_LAG = "autovacuum_lag"
    COLLECTOR_UNAVAILABLE = "collector_unavailable"
    SIGNAL_STALE = "signal_stale"


class PressureBoundError(ValueError):
    """Raised when a pressure observation is outside its declared bounds."""


# Thresholds for normalized 0..1 pressure ratios (enter warning / critical).
_WARN_RATIO: Final[float] = 0.7
_CRIT_RATIO: Final[float] = 0.9
# Pool acquisition wait (seconds) warning / critical enter thresholds.
_WARN_POOL_WAIT: Final[float] = 0.05
_CRIT_POOL_WAIT: Final[float] = 0.2


@dataclass(frozen=True, slots=True)
class PoolPressure:
    status: PressureStatus
    reason: PressureReason
    acquisition_wait_seconds: float | None
    saturation_ratio: float | None


@dataclass(frozen=True, slots=True)
class SignalPressure:
    status: PressureStatus
    reason: PressureReason
    value: float | None


@dataclass(frozen=True, slots=True)
class PressureSnapshot:
    """Typed, constant-size pressure boundary for Plan 04-03."""

    observed_at: datetime
    collected_monotonic_ns: int
    freshness: Freshness
    pool: PoolPressure
    wal: SignalPressure
    disk: SignalPressure
    autovacuum: SignalPressure

    def as_bounded_dict(self) -> dict[str, Any]:
        """Serialize without paths, SQL, identifiers, or free text."""
        return {
            "observed_at": self.observed_at.isoformat(),
            "collected_monotonic_ns": self.collected_monotonic_ns,
            "freshness": self.freshness.value,
            "pool": {
                "status": self.pool.status.value,
                "reason": self.pool.reason.value,
                "acquisition_wait_seconds": self.pool.acquisition_wait_seconds,
                "saturation_ratio": self.pool.saturation_ratio,
            },
            "wal": {
                "status": self.wal.status.value,
                "reason": self.wal.reason.value,
                "value": self.wal.value,
            },
            "disk": {
                "status": self.disk.status.value,
                "reason": self.disk.reason.value,
                "value": self.disk.value,
            },
            "autovacuum": {
                "status": self.autovacuum.status.value,
                "reason": self.autovacuum.reason.value,
                "value": self.autovacuum.value,
            },
        }


def classify_freshness(
    *,
    observed_at: datetime,
    now: datetime,
    max_age_seconds: float,
) -> Freshness:
    """Classify observation freshness from Queue-store timestamps."""
    if max_age_seconds < 0:
        raise PressureBoundError("max_age_seconds must be >= 0")
    age = (now - observed_at).total_seconds()
    if age < 0:
        # Clock skew: treat as stale rather than inventing future authority.
        return Freshness.STALE
    if age <= max_age_seconds:
        return Freshness.FRESH
    return Freshness.STALE


def unavailable_snapshot(
    *,
    observed_at: datetime,
    collected_monotonic_ns: int,
    reason: PressureReason = PressureReason.COLLECTOR_UNAVAILABLE,
) -> PressureSnapshot:
    """Return an explicit unavailable snapshot with null numeric signals."""
    unavailable = SignalPressure(
        status=PressureStatus.UNAVAILABLE,
        reason=reason,
        value=None,
    )
    return PressureSnapshot(
        observed_at=observed_at,
        collected_monotonic_ns=collected_monotonic_ns,
        freshness=Freshness.UNAVAILABLE,
        pool=PoolPressure(
            status=PressureStatus.UNAVAILABLE,
            reason=reason,
            acquisition_wait_seconds=None,
            saturation_ratio=None,
        ),
        wal=unavailable,
        disk=unavailable,
        autovacuum=unavailable,
    )


def _require_unit_interval(name: str, value: float) -> float:
    if value < 0.0 or value > 1.0:
        raise PressureBoundError(f"{name} must be in [0, 1], got {value}")
    return value


def _require_non_negative(name: str, value: float) -> float:
    if value < 0.0:
        raise PressureBoundError(f"{name} must be >= 0, got {value}")
    return value


def _status_from_ratio(
    value: float,
    *,
    warn_reason: PressureReason,
    crit_reason: PressureReason,
) -> tuple[PressureStatus, PressureReason]:
    if value >= _CRIT_RATIO:
        return PressureStatus.CRITICAL, crit_reason
    if value >= _WARN_RATIO:
        return PressureStatus.WARNING, warn_reason
    return PressureStatus.OK, PressureReason.WITHIN_BUDGET


def _pool_status(
    wait_seconds: float, saturation_ratio: float
) -> tuple[PressureStatus, PressureReason]:
    # Saturation and wait are evaluated independently; worst wins.
    sat_status, sat_reason = _status_from_ratio(
        saturation_ratio,
        warn_reason=PressureReason.POOL_SATURATION,
        crit_reason=PressureReason.POOL_SATURATION,
    )
    if wait_seconds >= _CRIT_POOL_WAIT:
        wait_status, wait_reason = (
            PressureStatus.CRITICAL,
            PressureReason.POOL_WAIT,
        )
    elif wait_seconds >= _WARN_POOL_WAIT:
        wait_status, wait_reason = (
            PressureStatus.WARNING,
            PressureReason.POOL_WAIT,
        )
    else:
        wait_status, wait_reason = (
            PressureStatus.OK,
            PressureReason.WITHIN_BUDGET,
        )

    rank = {
        PressureStatus.OK: 0,
        PressureStatus.WARNING: 1,
        PressureStatus.CRITICAL: 2,
        PressureStatus.UNAVAILABLE: 3,
    }
    if rank[sat_status] >= rank[wait_status]:
        return sat_status, sat_reason
    return wait_status, wait_reason


def build_snapshot(
    *,
    observed_at: datetime,
    collected_monotonic_ns: int,
    freshness: Freshness,
    pool_wait_seconds: float,
    pool_saturation_ratio: float,
    wal_value: float,
    disk_value: float,
    autovacuum_value: float,
) -> PressureSnapshot:
    """Build a bounded pressure snapshot from normalized collector inputs."""
    wait = _require_non_negative("pool_wait_seconds", pool_wait_seconds)
    sat = _require_unit_interval("pool_saturation_ratio", pool_saturation_ratio)
    wal = _require_unit_interval("wal_value", wal_value)
    disk = _require_unit_interval("disk_value", disk_value)
    vacuum = _require_unit_interval("autovacuum_value", autovacuum_value)

    pool_status, pool_reason = _pool_status(wait, sat)
    wal_status, wal_reason = _status_from_ratio(
        wal,
        warn_reason=PressureReason.WAL_GROWTH,
        crit_reason=PressureReason.WAL_GROWTH,
    )
    disk_status, disk_reason = _status_from_ratio(
        disk,
        warn_reason=PressureReason.DISK_PRESSURE,
        crit_reason=PressureReason.DISK_PRESSURE,
    )
    vac_status, vac_reason = _status_from_ratio(
        vacuum,
        warn_reason=PressureReason.AUTOVACUUM_LAG,
        crit_reason=PressureReason.AUTOVACUUM_LAG,
    )

    return PressureSnapshot(
        observed_at=observed_at,
        collected_monotonic_ns=collected_monotonic_ns,
        freshness=freshness,
        pool=PoolPressure(
            status=pool_status,
            reason=pool_reason,
            acquisition_wait_seconds=wait,
            saturation_ratio=sat,
        ),
        wal=SignalPressure(status=wal_status, reason=wal_reason, value=wal),
        disk=SignalPressure(status=disk_status, reason=disk_reason, value=disk),
        autovacuum=SignalPressure(
            status=vac_status, reason=vac_reason, value=vacuum
        ),
    )
