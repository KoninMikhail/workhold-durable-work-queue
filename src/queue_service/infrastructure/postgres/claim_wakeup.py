"""Dedicated PostgreSQL LISTEN wake substrate for claim long-polling.

Cross-replica wake hints only. Notifications never authorize claims; Plan 03
owns the authoritative claim/reconciliation loop. The listener uses one direct
psycopg autocommit connection and must never borrow the SQLAlchemy role pool.
"""

from __future__ import annotations

import logging
import re
import threading
import time
from collections.abc import Callable, Iterable, Mapping
from enum import Enum
from typing import Final

import psycopg

logger = logging.getLogger(__name__)

# Must match alembic/versions/2001_claim_long_poll_wakeup.py channel literal.
CLAIM_WAKE_CHANNEL: Final[str] = "queue_claim_wakeup"
_QUEUE_NAME_RE: Final[re.Pattern[str]] = re.compile(
    r"^[a-z0-9][a-z0-9._-]{0,127}$"
)
_DEFAULT_INITIAL_BACKOFF_SECONDS: Final[float] = 0.25
_DEFAULT_MAX_BACKOFF_SECONDS: Final[float] = 5.0
_DEFAULT_NOTIFY_POLL_SECONDS: Final[float] = 0.25
_DEFAULT_STOP_JOIN_SECONDS: Final[float] = 5.0
# Matches QUEUE_*_POOL_ACQUISITION_TIMEOUT_SECONDS default and db.psycopg_connect_timeout_seconds.
_DEFAULT_CONNECT_TIMEOUT_SECONDS: Final[int] = 5

ConnectFn = Callable[[], psycopg.Connection]
ClockFn = Callable[[], float]
SleepFn = Callable[[float], None]


class ListenerHealth(str, Enum):
    """Explicit listener connectivity state for outage observation."""

    CONNECTED = "connected"
    DEGRADED = "degraded"


def is_valid_queue_name_payload(payload: str | None) -> bool:
    """Return True when NOTIFY payload is a bounded validated queue name."""
    if payload is None or not isinstance(payload, str):
        return False
    if len(payload) < 1 or len(payload) > 128:
        return False
    return _QUEUE_NAME_RE.fullmatch(payload) is not None


class QueueGenerationCoordinator:
    """Thread-safe per-queue generation counter with global reconnect bumps."""

    __slots__ = ("_cond", "_queue_generations", "_global_generation")

    def __init__(self) -> None:
        self._cond = threading.Condition()
        self._queue_generations: dict[str, int] = {}
        self._global_generation = 0

    def snapshot(self, queue_name: str) -> int:
        with self._cond:
            return self._effective_locked(queue_name)

    def snapshot_many(self, queue_names: Iterable[str]) -> Mapping[str, int]:
        with self._cond:
            return {name: self._effective_locked(name) for name in queue_names}

    def bump_queue(self, queue_name: str) -> int:
        if not is_valid_queue_name_payload(queue_name):
            return self.snapshot(queue_name)
        with self._cond:
            next_value = self._queue_generations.get(queue_name, 0) + 1
            self._queue_generations[queue_name] = next_value
            self._cond.notify_all()
            return next_value + self._global_generation

    def bump_global(self) -> int:
        with self._cond:
            self._global_generation += 1
            self._cond.notify_all()
            return self._global_generation

    def wait(
        self,
        queue_name: str,
        observed: int,
        *,
        timeout: float,
    ) -> bool:
        """Block until generation advances past ``observed`` or timeout elapses.

        Returns True when the generation changed; False on timeout.
        """
        if timeout < 0:
            raise ValueError("timeout must be non-negative")
        deadline = time.monotonic() + timeout
        with self._cond:
            while self._effective_locked(queue_name) == observed:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._cond.wait(timeout=remaining)
            return True

    def wait_any(
        self,
        observed: Mapping[str, int],
        *,
        timeout: float,
    ) -> bool:
        """Block until any observed queue generation advances or timeout elapses.

        Returns True when at least one generation changed; False on timeout.
        """
        if timeout < 0:
            raise ValueError("timeout must be non-negative")
        if not observed:
            return False
        deadline = time.monotonic() + timeout
        with self._cond:
            while all(
                self._effective_locked(name) == value
                for name, value in observed.items()
            ):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._cond.wait(timeout=remaining)
            return True

    def _effective_locked(self, queue_name: str) -> int:
        return self._queue_generations.get(queue_name, 0) + self._global_generation


class ClaimWakeListener:
    """One named daemon thread owning a direct autocommit LISTEN connection."""

    __slots__ = (
        "_dsn",
        "_coordinator",
        "_connect",
        "_clock",
        "_sleep",
        "_connect_timeout_seconds",
        "_initial_backoff_seconds",
        "_max_backoff_seconds",
        "_notify_poll_seconds",
        "_stop",
        "_thread",
        "_health",
        "_health_lock",
        "_connection",
        "_connection_lock",
        "_started",
    )

    def __init__(
        self,
        dsn: str,
        coordinator: QueueGenerationCoordinator,
        *,
        connect: ConnectFn | None = None,
        clock: ClockFn | None = None,
        sleep: SleepFn | None = None,
        connect_timeout_seconds: int = _DEFAULT_CONNECT_TIMEOUT_SECONDS,
        initial_backoff_seconds: float = _DEFAULT_INITIAL_BACKOFF_SECONDS,
        max_backoff_seconds: float = _DEFAULT_MAX_BACKOFF_SECONDS,
        notify_poll_seconds: float = _DEFAULT_NOTIFY_POLL_SECONDS,
    ) -> None:
        if not dsn:
            raise ValueError("dsn is required")
        if connect_timeout_seconds <= 0:
            raise ValueError("connect_timeout_seconds must be positive")
        if initial_backoff_seconds <= 0:
            raise ValueError("initial_backoff_seconds must be positive")
        if max_backoff_seconds < initial_backoff_seconds:
            raise ValueError("max_backoff_seconds must be >= initial_backoff_seconds")
        if notify_poll_seconds <= 0:
            raise ValueError("notify_poll_seconds must be positive")
        self._dsn = dsn
        self._coordinator = coordinator
        self._connect = connect or self._default_connect
        self._clock = clock or time.monotonic
        self._sleep = sleep or time.sleep
        self._connect_timeout_seconds = int(connect_timeout_seconds)
        self._initial_backoff_seconds = float(initial_backoff_seconds)
        self._max_backoff_seconds = float(max_backoff_seconds)
        self._notify_poll_seconds = float(notify_poll_seconds)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._health = ListenerHealth.DEGRADED
        self._health_lock = threading.Lock()
        self._connection: psycopg.Connection | None = None
        self._connection_lock = threading.Lock()
        self._started = False

    def __repr__(self) -> str:
        # Never include DSN or credentials in diagnostics.
        return (
            f"ClaimWakeListener(health={self.health.value!r}, "
            f"started={self._started!r})"
        )

    @property
    def health(self) -> ListenerHealth:
        with self._health_lock:
            return self._health

    @property
    def connection(self) -> psycopg.Connection | None:
        """Test seam: current direct listener connection, if any."""
        with self._connection_lock:
            return self._connection

    def start(self) -> None:
        if self._started:
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="claim-wake-listener",
            daemon=True,
        )
        self._started = True
        self._thread.start()

    def stop(self, *, join_timeout_seconds: float = _DEFAULT_STOP_JOIN_SECONDS) -> None:
        self._stop.set()
        self._close_connection()
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=max(0.0, join_timeout_seconds))
        self._started = False
        with self._health_lock:
            self._health = ListenerHealth.DEGRADED

    def _default_connect(self) -> psycopg.Connection:
        # Autocommit is required for prompt NOTIFY delivery (psycopg guidance).
        # connect_timeout bounds every connect/reconnect so stop cannot wait forever.
        return psycopg.connect(
            self._dsn,
            autocommit=True,
            connect_timeout=self._connect_timeout_seconds,
        )

    def _set_health(self, health: ListenerHealth) -> None:
        with self._health_lock:
            self._health = health

    def _store_connection(self, conn: psycopg.Connection | None) -> None:
        with self._connection_lock:
            self._connection = conn

    def _close_connection(self) -> None:
        with self._connection_lock:
            conn = self._connection
            self._connection = None
        if conn is None:
            return
        try:
            conn.close()
        except Exception:
            logger.debug("claim wake listener connection close failed", exc_info=True)

    def _run(self) -> None:
        attempt = 0
        while not self._stop.is_set():
            conn: psycopg.Connection | None = None
            try:
                conn = self._connect()
                if not conn.autocommit:
                    conn.autocommit = True
                # Channel is a static identifier; never interpolate untrusted input.
                conn.execute(f"LISTEN {CLAIM_WAKE_CHANNEL}")
                self._store_connection(conn)
                if attempt > 0:
                    self._coordinator.bump_global()
                attempt = 0
                self._set_health(ListenerHealth.CONNECTED)
                self._consume(conn)
            except Exception:
                self._set_health(ListenerHealth.DEGRADED)
                logger.warning(
                    "claim wake listener degraded; reconnecting with backoff",
                    exc_info=True,
                )
            finally:
                self._store_connection(None)
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass
            if self._stop.is_set():
                break
            delay = min(
                self._initial_backoff_seconds * (2**attempt),
                self._max_backoff_seconds,
            )
            attempt += 1
            self._interruptible_sleep(delay)

    def _consume(self, conn: psycopg.Connection) -> None:
        while not self._stop.is_set():
            for notify in conn.notifies(timeout=self._notify_poll_seconds):
                if self._stop.is_set():
                    return
                payload = notify.payload
                if not is_valid_queue_name_payload(payload):
                    continue
                self._coordinator.bump_queue(payload)
            # Empty timeout returns; loop to re-check stop.

    def _interruptible_sleep(self, seconds: float) -> None:
        deadline = self._clock() + seconds
        while not self._stop.is_set():
            remaining = deadline - self._clock()
            if remaining <= 0:
                return
            self._sleep(min(0.05, remaining))
