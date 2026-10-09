"""Framework-neutral bridge health snapshots and low-cardinality telemetry (BRDG-02).

Observability is read-only with respect to correctness: metric/log sink failures
must never alter claim, enqueue, or lifecycle outcomes. Metric labels are
allowlisted; payloads, credentials, source identities, and idempotency keys are
never emitted as labels.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Final, Protocol

from workhold_producer.bridge.store import OutboxStore

logger = logging.getLogger(__name__)

ALLOWED_METRIC_LABEL_KEYS: Final[frozenset[str]] = frozenset(
    {"process_role", "queue", "operation", "result"}
)

# Metric names and units (see docs/05-operations/application-outbox-bridge.md).
METRIC_PENDING_DEPTH: Final[str] = "bridge.pending_depth"  # gauge, count
METRIC_OLDEST_LAG: Final[str] = "bridge.oldest_pending_lag_seconds"  # gauge, seconds
METRIC_CLAIMED: Final[str] = "bridge.claimed"  # counter
METRIC_DELIVERED: Final[str] = "bridge.delivered"  # counter, result=new|replay
METRIC_RETRYABLE: Final[str] = "bridge.retryable_error"  # counter
METRIC_CONFLICT: Final[str] = "bridge.permanent_conflict"  # counter
METRIC_MALFORMED: Final[str] = "bridge.malformed_intent"  # counter
METRIC_LEASE_LOSS: Final[str] = "bridge.lease_loss"  # counter
METRIC_LEASE_RECLAIM: Final[str] = "bridge.lease_reclaim"  # counter
METRIC_SHUTDOWN: Final[str] = "bridge.shutdown"  # counter

DEFAULT_DEPTH_CAP: Final[int] = 1000
DEFAULT_LAG_WARN_SECONDS: Final[float] = 300.0
DEFAULT_POLL_STALE_AFTER_SECONDS: Final[float] = 120.0

ClockFn = Callable[[], float]
WallClockFn = Callable[[], datetime]


class BridgeMetricSink(Protocol):
    """Framework-neutral counter/gauge sink (Prometheus/OTel adapter optional)."""

    def emit_counter(
        self,
        name: str,
        *,
        value: float = 1.0,
        labels: Mapping[str, str],
        unit: str = "1",
    ) -> None: ...

    def emit_gauge(
        self,
        name: str,
        *,
        value: float,
        labels: Mapping[str, str],
        unit: str,
    ) -> None: ...


class BridgeLogSink(Protocol):
    """Structured log sink; fields must already be redacted by the caller."""

    def emit(self, event: str, fields: Mapping[str, Any]) -> None: ...


class _NullMetricSink:
    def emit_counter(
        self,
        name: str,
        *,
        value: float = 1.0,
        labels: Mapping[str, str],
        unit: str = "1",
    ) -> None:
        return None

    def emit_gauge(
        self,
        name: str,
        *,
        value: float,
        labels: Mapping[str, str],
        unit: str,
    ) -> None:
        return None


class _LoggerLogSink:
    def __init__(self, log: logging.Logger) -> None:
        self._log = log

    def emit(self, event: str, fields: Mapping[str, Any]) -> None:
        parts = " ".join(f"{k}={v}" for k, v in sorted(fields.items()) if v is not None)
        self._log.info("bridge.%s %s", event, parts)


class BridgeAlertSeverity(Enum):
    NONE = "none"
    WARNING = "warning"


class BridgeAlertKind(Enum):
    BRIDGE_LAG_SUSTAINED = "bridge_lag_sustained"


@dataclass(frozen=True, slots=True)
class BridgeAlert:
    kind: BridgeAlertKind
    severity: BridgeAlertSeverity


@dataclass(frozen=True, slots=True)
class BridgeHealth:
    """Immutable aggregate bridge health with ``as_of`` and freshness.

    Distinguishes process liveness from app-store / Queue readiness. Empty
    backlog is healthy; non-zero lag alone is not data loss or a correctness
    failure.
    """

    as_of: datetime
    freshness_seconds: float
    process_alive: bool
    app_store_reachable: bool
    app_store_query_ok: bool
    queue_reachable: bool
    queue_compatible: bool
    last_successful_poll_at: datetime | None
    last_successful_delivery_at: datetime | None
    pending_count: int
    pending_capped: bool
    pending_approximate: bool
    oldest_pending_created_at: datetime | None
    oldest_pending_lag_seconds: float | None
    poll_stale: bool
    empty_backlog: bool
    correctness_ok: bool
    ready: bool
    status_note: str | None = None


def sanitize_metric_labels(labels: Mapping[str, str]) -> dict[str, str]:
    """Keep only the bounded allowlist; drop forbidden / unbounded keys."""
    return {
        key: value
        for key, value in labels.items()
        if key in ALLOWED_METRIC_LABEL_KEYS and isinstance(value, str)
    }


def evaluate_bridge_alerts(
    *,
    oldest_pending_lag_seconds: float | None,
    pending_count: int,
    pending_capped: bool,
    recent_deliveries: int,
    recent_retries: int,
    recent_conflicts: int,
    lag_warn_seconds: float = DEFAULT_LAG_WARN_SECONDS,
) -> tuple[BridgeAlert, ...]:
    """Alert only when sustained lag combines with depth and stalled/error outcomes.

    The bridge cannot promise zero lag. Empty backlog is never an alert.
    Non-zero lag alone (without depth/progress/error signal) is not alerted.
    """
    del pending_capped  # capped depth still counts as non-empty backlog pressure
    if pending_count <= 0:
        return ()
    if oldest_pending_lag_seconds is None:
        return ()
    if oldest_pending_lag_seconds < lag_warn_seconds:
        return ()
    stalled = recent_deliveries == 0
    errored = recent_retries > 0 or recent_conflicts > 0
    if not (stalled or errored):
        return ()
    return (
        BridgeAlert(
            kind=BridgeAlertKind.BRIDGE_LAG_SUSTAINED,
            severity=BridgeAlertSeverity.WARNING,
        ),
    )


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class BridgeTelemetry:
    """Low-cardinality bridge instrumentation and health snapshot builder.

    Consumes Plan 03 ``get_pending_depth``, ``get_oldest_pending_created_at``,
    and ``get_health_snapshot`` — never issues an unbounded storage scan.
    """

    def __init__(
        self,
        *,
        process_role: str = "bridge",
        depth_cap: int = DEFAULT_DEPTH_CAP,
        metric_sink: BridgeMetricSink | None = None,
        log_sink: BridgeLogSink | None = None,
        clock: ClockFn | None = None,
        wall_clock: WallClockFn | None = None,
        lag_warn_seconds: float = DEFAULT_LAG_WARN_SECONDS,
        poll_stale_after_seconds: float = DEFAULT_POLL_STALE_AFTER_SECONDS,
        logger_: logging.Logger | None = None,
    ) -> None:
        if depth_cap < 1:
            raise ValueError("depth_cap must be >= 1")
        self.process_role = process_role
        self.depth_cap = depth_cap
        self.metric_sink: BridgeMetricSink = metric_sink or _NullMetricSink()
        self.log_sink: BridgeLogSink = log_sink or _LoggerLogSink(logger_ or logger)
        self._clock: ClockFn = clock or time.monotonic
        self._wall_clock: WallClockFn = wall_clock or _utc_now
        self.lag_warn_seconds = lag_warn_seconds
        self.poll_stale_after_seconds = poll_stale_after_seconds

        self._last_successful_poll_at: datetime | None = None
        self._last_successful_delivery_at: datetime | None = None
        self._queue_reachable: bool = True
        self._queue_compatible: bool = True
        self._recent_deliveries: int = 0
        self._recent_retries: int = 0
        self._recent_conflicts: int = 0
        self._last_health: BridgeHealth | None = None

    # ------------------------------------------------------------------
    # Label-safe emit helpers
    # ------------------------------------------------------------------

    def _base_labels(
        self,
        *,
        operation: str,
        result: str,
        queue: str | None = None,
    ) -> dict[str, str]:
        labels: dict[str, str] = {
            "process_role": self.process_role,
            "operation": operation,
            "result": result,
        }
        if queue is not None:
            labels["queue"] = queue
        return sanitize_metric_labels(labels)

    def emit_counter(
        self,
        name: str,
        *,
        value: float = 1.0,
        labels: Mapping[str, str],
        unit: str = "1",
    ) -> None:
        safe = sanitize_metric_labels(labels)
        try:
            self.metric_sink.emit_counter(name, value=value, labels=safe, unit=unit)
        except Exception:
            logger.debug("bridge metric counter sink failed", exc_info=True)

    def emit_gauge(
        self,
        name: str,
        *,
        value: float,
        labels: Mapping[str, str],
        unit: str,
    ) -> None:
        safe = sanitize_metric_labels(labels)
        try:
            self.metric_sink.emit_gauge(name, value=value, labels=safe, unit=unit)
        except Exception:
            logger.debug("bridge metric gauge sink failed", exc_info=True)

    def _log(self, event: str, fields: Mapping[str, Any]) -> None:
        try:
            self.log_sink.emit(event, fields)
        except Exception:
            logger.debug("bridge log sink failed", exc_info=True)

    # ------------------------------------------------------------------
    # Dependency / poll notes
    # ------------------------------------------------------------------

    def note_successful_poll(self, at: datetime | None = None) -> None:
        self._last_successful_poll_at = at or self._wall_clock()

    def note_successful_delivery(self, at: datetime | None = None) -> None:
        self._last_successful_delivery_at = at or self._wall_clock()

    def note_queue_reachable(self) -> None:
        self._queue_reachable = True

    def note_queue_unreachable(self) -> None:
        self._queue_reachable = False

    def note_capability_ok(self) -> None:
        self._queue_compatible = True

    def note_capability_mismatch(self) -> None:
        self._queue_compatible = False

    def project_trace_context(
        self,
        *,
        traceparent: str | None,
        tracestate: str | None = None,
        **_ignored: Any,
    ) -> dict[str, str]:
        """Preserve W3C trace fields only — never copy payload or identities."""
        out: dict[str, str] = {}
        if isinstance(traceparent, str) and traceparent:
            out["traceparent"] = traceparent
        if isinstance(tracestate, str) and tracestate:
            out["tracestate"] = tracestate
        return out

    # ------------------------------------------------------------------
    # Counters
    # ------------------------------------------------------------------

    def record_claimed(self, *, queue: str | None = None, count: int = 1) -> None:
        self.emit_counter(
            METRIC_CLAIMED,
            value=float(count),
            labels=self._base_labels(
                operation="bridge.claim", result="claimed", queue=queue
            ),
        )

    def record_delivered(self, *, queue: str, result: str) -> None:
        # result must be "new" or "replay"
        self._recent_deliveries += 1
        self.note_successful_delivery()
        self.emit_counter(
            METRIC_DELIVERED,
            labels=self._base_labels(
                operation="bridge.deliver", result=result, queue=queue
            ),
        )

    def record_retryable_error(self, *, queue: str, result: str) -> None:
        self._recent_retries += 1
        self.emit_counter(
            METRIC_RETRYABLE,
            labels=self._base_labels(
                operation="bridge.enqueue", result=result, queue=queue
            ),
        )

    def record_permanent_conflict(self, *, queue: str, result: str) -> None:
        self._recent_conflicts += 1
        self.emit_counter(
            METRIC_CONFLICT,
            labels=self._base_labels(
                operation="bridge.enqueue", result=result, queue=queue
            ),
        )

    def record_malformed_intent(self, *, queue: str, result: str) -> None:
        self.note_capability_mismatch()
        self.emit_counter(
            METRIC_MALFORMED,
            labels=self._base_labels(
                operation="bridge.validate", result=result, queue=queue
            ),
        )

    def record_lease_loss(self, *, queue: str) -> None:
        self.emit_counter(
            METRIC_LEASE_LOSS,
            labels=self._base_labels(
                operation="bridge.deliver", result="lease_lost", queue=queue
            ),
        )

    def record_lease_reclaim(self, *, queue: str) -> None:
        self.emit_counter(
            METRIC_LEASE_RECLAIM,
            labels=self._base_labels(
                operation="bridge.claim", result="reclaimed", queue=queue
            ),
        )

    def record_shutdown(self) -> None:
        self.emit_counter(
            METRIC_SHUTDOWN,
            labels=self._base_labels(operation="bridge.shutdown", result="shutdown"),
        )

    def emit_delivery_log(
        self,
        *,
        queue: str,
        task_id: str | None,
        result: str,
        replayed: bool | None = None,
    ) -> None:
        fields: dict[str, Any] = {
            "operation": "bridge.deliver",
            "queue": queue,
            "result": result,
            "process_role": self.process_role,
        }
        if task_id is not None:
            fields["task_id"] = task_id
        if replayed is not None:
            fields["replayed"] = replayed
        self._log("deliver", fields)

    # ------------------------------------------------------------------
    # Health / lag
    # ------------------------------------------------------------------

    def refresh_from_store(self, store: OutboxStore) -> BridgeHealth:
        """Build health from Plan 03 bounded snapshots only (no unbounded scan)."""
        started = self._clock()
        app_reachable = True
        app_query_ok = True
        pending_count = 0
        pending_capped = False
        oldest_created: datetime | None = None
        as_of = self._wall_clock()

        try:
            health_snap = store.get_health_snapshot(self.depth_cap)
            as_of = health_snap.as_of
            app_reachable = health_snap.connected
            app_query_ok = health_snap.query_ok
        except Exception:
            app_reachable = False
            app_query_ok = False

        try:
            depth = store.get_pending_depth(self.depth_cap)
            pending_count = depth.count
            pending_capped = depth.capped
            as_of = depth.as_of
        except Exception:
            app_reachable = False
            app_query_ok = False

        try:
            oldest = store.get_oldest_pending_created_at()
            oldest_created = oldest.created_at
            as_of = oldest.as_of
        except Exception:
            app_reachable = False
            app_query_ok = False

        lag: float | None = None
        if oldest_created is not None:
            lag = max(0.0, (as_of - oldest_created).total_seconds())

        poll_at = self._last_successful_poll_at
        poll_stale = False
        if poll_at is not None:
            age = (as_of - poll_at).total_seconds()
            poll_stale = age > self.poll_stale_after_seconds

        empty = pending_count == 0 and not pending_capped and oldest_created is None
        ready = (
            app_reachable
            and app_query_ok
            and self._queue_reachable
            and self._queue_compatible
            and not poll_stale
        )
        freshness = max(0.0, self._clock() - started)

        depth_result = "capped" if pending_capped else ("empty" if empty else "approx")
        self.emit_gauge(
            METRIC_PENDING_DEPTH,
            value=float(pending_count),
            labels=self._base_labels(
                operation="bridge.snapshot", result=depth_result
            ),
            unit="count",
        )
        if lag is not None:
            self.emit_gauge(
                METRIC_OLDEST_LAG,
                value=float(lag),
                labels=self._base_labels(
                    operation="bridge.snapshot", result="oldest"
                ),
                unit="seconds",
            )

        health = BridgeHealth(
            as_of=as_of,
            freshness_seconds=freshness,
            process_alive=True,
            app_store_reachable=app_reachable,
            app_store_query_ok=app_query_ok,
            queue_reachable=self._queue_reachable,
            queue_compatible=self._queue_compatible,
            last_successful_poll_at=poll_at,
            last_successful_delivery_at=self._last_successful_delivery_at,
            pending_count=pending_count,
            pending_capped=pending_capped,
            pending_approximate=True,
            oldest_pending_created_at=oldest_created,
            oldest_pending_lag_seconds=lag,
            poll_stale=poll_stale,
            empty_backlog=empty,
            correctness_ok=True,
            ready=ready,
            status_note="observational" if not empty else "empty_backlog_healthy",
        )
        self._last_health = health
        return health

    def evaluate_alerts(
        self, health: BridgeHealth | None = None
    ) -> tuple[BridgeAlert, ...]:
        snap = health or self._last_health
        if snap is None:
            return ()
        return evaluate_bridge_alerts(
            oldest_pending_lag_seconds=snap.oldest_pending_lag_seconds,
            pending_count=snap.pending_count,
            pending_capped=snap.pending_capped,
            recent_deliveries=self._recent_deliveries,
            recent_retries=self._recent_retries,
            recent_conflicts=self._recent_conflicts,
            lag_warn_seconds=self.lag_warn_seconds,
        )

    @property
    def last_health(self) -> BridgeHealth | None:
        return self._last_health


def safe_observe(fn: Callable[[], None]) -> None:
    """Run an observational side-effect; never raise into the correctness path."""
    try:
        fn()
    except Exception:
        logger.debug("bridge observability hook failed", exc_info=True)
