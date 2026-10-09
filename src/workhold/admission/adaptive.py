"""Hysteretic PostgreSQL-pressure state machine (OPS-07).

Consumes Plan 01 :class:`~workhold.observability.pressure.PressureSnapshot`
directly — never scrapes metrics exposition. Separate enter/clear ratio
thresholds and consecutive-sample confirmation prevent single-sample flaps.
Claims remain allowed in every overload mode.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from threading import Lock
from typing import Final

from workhold.observability.pressure import Freshness, PressureSnapshot

# Enter thresholds align with Plan 01 pressure ratio bands; clear is lower.
_ENTER_WARN_RATIO: Final[float] = 0.7
_ENTER_CRIT_RATIO: Final[float] = 0.9
_CLEAR_WARN_RATIO: Final[float] = 0.5
_CLEAR_CRIT_RATIO: Final[float] = 0.7

# Pool wait (seconds) enter bands — mirrors pressure.py with hysteresis.
_ENTER_WARN_WAIT: Final[float] = 0.05
_ENTER_CRIT_WAIT: Final[float] = 0.2
_CLEAR_WARN_WAIT: Final[float] = 0.03

TransitionCallback = Callable[["OverloadMode", "OverloadMode", str], None]


class OverloadMode(Enum):
    NORMAL = "normal"
    WARNING = "warning"
    ENQUEUE_THROTTLE = "enqueue_throttle"
    READINESS_FAILURE = "readiness_failure"


@dataclass(frozen=True, slots=True)
class AdaptivePressureConfig:
    """Hysteresis knobs for the pressure state machine."""

    enter_consecutive_samples: int = 2
    clear_consecutive_samples: int = 2
    clear_hold_seconds: float = 1.0
    enter_warn_ratio: float = _ENTER_WARN_RATIO
    enter_crit_ratio: float = _ENTER_CRIT_RATIO
    clear_warn_ratio: float = _CLEAR_WARN_RATIO
    clear_crit_ratio: float = _CLEAR_CRIT_RATIO
    on_transition: TransitionCallback | None = None

    def __post_init__(self) -> None:
        if self.enter_consecutive_samples < 1:
            raise ValueError("enter_consecutive_samples must be >= 1")
        if self.clear_consecutive_samples < 1:
            raise ValueError("clear_consecutive_samples must be >= 1")
        if self.clear_hold_seconds < 0:
            raise ValueError("clear_hold_seconds must be >= 0")
        if not (0.0 < self.clear_warn_ratio < self.enter_warn_ratio):
            raise ValueError("clear_warn_ratio must be below enter_warn_ratio")
        if not (
            self.enter_warn_ratio
            <= self.clear_crit_ratio
            < self.enter_crit_ratio
        ):
            raise ValueError(
                "clear_crit_ratio must sit between enter_warn and enter_crit"
            )


@dataclass
class AdaptivePressureController:
    """Deterministic overload controller driven only by typed pressure snapshots."""

    config: AdaptivePressureConfig = field(default_factory=AdaptivePressureConfig)
    monotonic_clock: Callable[[], float] = field(default=time.monotonic)
    _mode: OverloadMode = field(default=OverloadMode.NORMAL, init=False)
    _enter_streak: int = field(default=0, init=False)
    _clear_streak: int = field(default=0, init=False)
    _clear_hold_started: float | None = field(default=None, init=False)
    _lock: Lock = field(default_factory=Lock, init=False, repr=False)
    _transitions: list[str] = field(default_factory=list, init=False)

    @property
    def mode(self) -> OverloadMode:
        return self._mode

    @property
    def claims_allowed(self) -> bool:
        """Claims always drain backlog — never blocked by soft overload modes."""
        return True

    @property
    def readiness_ok(self) -> bool:
        return self._mode is not OverloadMode.READINESS_FAILURE

    @property
    def transitions(self) -> tuple[str, ...]:
        return tuple(self._transitions)

    def observe(
        self,
        snapshot: PressureSnapshot,
        *,
        now_monotonic: float | None = None,
    ) -> OverloadMode:
        now = self.monotonic_clock() if now_monotonic is None else float(now_monotonic)
        peak, clear_blocked = _peak_pressure(snapshot)
        with self._lock:
            self._apply(peak, clear_blocked=clear_blocked, now=now)
            return self._mode

    def _apply(self, peak: float, *, clear_blocked: bool, now: float) -> None:
        desired = self._desired_mode(peak)
        current = self._mode

        if _mode_rank(desired) > _mode_rank(current):
            self._clear_streak = 0
            self._clear_hold_started = None
            self._enter_streak += 1
            if self._enter_streak >= self.config.enter_consecutive_samples:
                self._set_mode(desired, reason=_enter_reason(desired))
                self._enter_streak = 0
            return

        self._enter_streak = 0

        if clear_blocked or not self._below_clear_threshold(peak):
            self._clear_streak = 0
            self._clear_hold_started = None
            return

        if current is OverloadMode.NORMAL:
            return

        if self._clear_hold_started is None:
            self._clear_hold_started = now
            self._clear_streak = 0
            return

        if (now - self._clear_hold_started) < self.config.clear_hold_seconds:
            # Hold interval must elapse before clear samples count.
            return

        self._clear_streak += 1
        if self._clear_streak >= self.config.clear_consecutive_samples:
            target = _step_down(current)
            self._set_mode(target, reason="clear_hold")
            self._clear_streak = 0
            self._clear_hold_started = None

    def _desired_mode(self, peak: float) -> OverloadMode:
        cfg = self.config
        if peak >= cfg.enter_crit_ratio:
            # Critical pressure: throttle from calm/warn; readiness from throttle.
            if self._mode in (
                OverloadMode.ENQUEUE_THROTTLE,
                OverloadMode.READINESS_FAILURE,
            ):
                return OverloadMode.READINESS_FAILURE
            return OverloadMode.ENQUEUE_THROTTLE
        if peak >= cfg.enter_warn_ratio:
            return OverloadMode.WARNING
        return OverloadMode.NORMAL

    def _below_clear_threshold(self, peak: float) -> bool:
        cfg = self.config
        if self._mode is OverloadMode.WARNING:
            return peak < cfg.clear_warn_ratio
        if self._mode in (
            OverloadMode.ENQUEUE_THROTTLE,
            OverloadMode.READINESS_FAILURE,
        ):
            return peak < cfg.clear_crit_ratio
        return True

    def _set_mode(self, new: OverloadMode, *, reason: str) -> None:
        prev = self._mode
        if prev is new:
            return
        self._mode = new
        self._transitions.append(f"{prev.value}->{new.value}:{reason}")
        if self.config.on_transition is not None:
            self.config.on_transition(prev, new, reason)


def _enter_reason(mode: OverloadMode) -> str:
    if mode is OverloadMode.WARNING:
        return "pressure_warning"
    if mode is OverloadMode.ENQUEUE_THROTTLE:
        return "pressure_critical"
    if mode is OverloadMode.READINESS_FAILURE:
        return "pressure_sustained"
    return "pressure"


def _mode_rank(mode: OverloadMode) -> int:
    order = (
        OverloadMode.NORMAL,
        OverloadMode.WARNING,
        OverloadMode.ENQUEUE_THROTTLE,
        OverloadMode.READINESS_FAILURE,
    )
    return order.index(mode)


def _step_down(mode: OverloadMode) -> OverloadMode:
    rank = _mode_rank(mode)
    if rank <= 0:
        return OverloadMode.NORMAL
    order = (
        OverloadMode.NORMAL,
        OverloadMode.WARNING,
        OverloadMode.ENQUEUE_THROTTLE,
        OverloadMode.READINESS_FAILURE,
    )
    return order[rank - 1]


def _peak_pressure(snapshot: PressureSnapshot) -> tuple[float, bool]:
    """Return (peak_normalized_pressure, clear_blocked).

    Stale/unavailable observations conservatively degrade to critical and never
    authorize clearing overload.
    """
    if snapshot.freshness is Freshness.UNAVAILABLE:
        return 1.0, True
    if snapshot.freshness is Freshness.STALE:
        return 1.0, True

    ratios: list[float] = []
    for signal in (snapshot.wal, snapshot.disk, snapshot.autovacuum):
        if signal.value is not None:
            ratios.append(float(signal.value))
    sat = snapshot.pool.saturation_ratio
    if sat is not None:
        ratios.append(float(sat))
    wait = snapshot.pool.acquisition_wait_seconds
    if wait is not None:
        ratios.append(_wait_as_ratio(float(wait)))
    if not ratios:
        return 1.0, True
    return max(ratios), False


def _wait_as_ratio(wait_seconds: float) -> float:
    """Map pool wait onto the same 0..1 band used by ratio signals."""
    if wait_seconds >= _ENTER_CRIT_WAIT:
        return 0.95
    if wait_seconds >= _ENTER_WARN_WAIT:
        span = _ENTER_CRIT_WAIT - _ENTER_WARN_WAIT
        frac = (wait_seconds - _ENTER_WARN_WAIT) / span
        return _ENTER_WARN_RATIO + frac * (_ENTER_CRIT_RATIO - _ENTER_WARN_RATIO)
    if wait_seconds >= _CLEAR_WARN_WAIT:
        return _CLEAR_WARN_RATIO + 0.05
    return 0.2
