"""Monotonic process lifecycle for graceful API termination (DEP-02).

States advance only forward: startup → running → stopping → stopped.
The first stop request records a monotonic grace deadline; repeated signals
are idempotent and cannot restart acceptance or extend grace.
"""

from __future__ import annotations

import threading
import time
from enum import Enum
from typing import Final


class LifecyclePhase(str, Enum):
    STARTUP = "startup"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"


class Lifecycle:
    """Thread-safe monotonic lifecycle coordinator."""

    __slots__ = (
        "_lock",
        "_phase",
        "_grace_seconds",
        "_stop_deadline_mono",
        "_stop_event",
        "_shutdown_events",
    )

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._phase = LifecyclePhase.STARTUP
        self._grace_seconds: float | None = None
        self._stop_deadline_mono: float | None = None
        self._stop_event = threading.Event()
        self._shutdown_events: list[str] = []

    @property
    def phase(self) -> LifecyclePhase:
        with self._lock:
            return self._phase

    @property
    def grace_seconds(self) -> float | None:
        with self._lock:
            return self._grace_seconds

    @property
    def stop_deadline_mono(self) -> float | None:
        with self._lock:
            return self._stop_deadline_mono

    @property
    def stop_event(self) -> threading.Event:
        return self._stop_event

    @property
    def shutdown_events(self) -> list[str]:
        with self._lock:
            return list(self._shutdown_events)

    def is_ready(self) -> bool:
        with self._lock:
            return self._phase is LifecyclePhase.RUNNING

    def is_accepting(self) -> bool:
        with self._lock:
            return self._phase is LifecyclePhase.RUNNING

    def mark_running(self) -> None:
        with self._lock:
            if self._phase is LifecyclePhase.STARTUP:
                self._phase = LifecyclePhase.RUNNING

    def request_stop(self, grace_seconds: float) -> bool:
        """Enter stopping on first call; later calls are no-ops.

        Returns True iff this call initiated the transition.
        """
        if grace_seconds < 0:
            raise ValueError("grace_seconds must be non-negative")
        with self._lock:
            if self._phase in {LifecyclePhase.STOPPING, LifecyclePhase.STOPPED}:
                return False
            self._phase = LifecyclePhase.STOPPING
            self._grace_seconds = grace_seconds
            self._stop_deadline_mono = time.monotonic() + grace_seconds
            self._record_locked("ready_false")
            self._stop_event.set()
            return True

    def record(self, event: str) -> None:
        with self._lock:
            self._record_locked(event)

    def mark_stopped(self) -> None:
        with self._lock:
            if self._phase is LifecyclePhase.STOPPED:
                return
            self._phase = LifecyclePhase.STOPPED
            self._record_locked("stopped")

    def _record_locked(self, event: str) -> None:
        if not self._shutdown_events or self._shutdown_events[-1] != event:
            self._shutdown_events.append(event)


DEFAULT_SHUTDOWN_GRACE_SECONDS: Final[float] = 30.0
