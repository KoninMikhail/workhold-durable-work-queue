"""Low-cardinality kernel metric definitions and recording helpers (OPS-02).

Uses an in-process registry compatible with a future Prometheus exposition.
No third-party metrics package is introduced (T-04-01-SC). Recording helpers
are intended for Phase 3.9 service boundaries only after authoritative
outcomes are committed — callers must not count attempted writes as success.
"""

from __future__ import annotations

from dataclasses import dataclass
from threading import Lock
from typing import Final, Iterable, Mapping

# Allowed metric labels per docs/05-operations/03-observability.md.
ALLOWED_LABEL_KEYS: Final[frozenset[str]] = frozenset(
    {
        "queue",
        "operation",
        "result",
        "terminal_outcome",
        "failure_code",
        "process_role",
        # Closed RetentionWindow enum values only (OPS-05).
        "retention_window",
    }
)

# Results that are reported but excluded from service-error ratios.
_NON_ERROR_RATIO_RESULTS: Final[frozenset[str]] = frozenset(
    {
        "empty",
        "paused",
        "draining",
        "invalid_request",
    }
)

# Long-poll lifecycle results that are successful or client/capacity outcomes.
_LONG_POLL_NON_ERROR_RESULTS: Final[frozenset[str]] = frozenset(
    {
        "task",
        "expired",
        "cancelled",
        "shutdown",
        "admission_rejected",
    }
)

_LONG_POLL_RESULTS: Final[frozenset[str]] = frozenset(
    {
        "task",
        "expired",
        "cancelled",
        "shutdown",
        "admission_rejected",
        "error",
    }
)

_LONG_POLL_ATTEMPT_RESULTS: Final[frozenset[str]] = frozenset(
    {"notification", "fallback"}
)

_CLAIM_WAKEUP_RESULTS: Final[frozenset[str]] = frozenset(
    {"notification", "reconnect", "listener_error"}
)

# Histogram buckets derived from Phase 3.9 qualification gates:
# p99 enqueue/claim/heartbeat ≤ 100 ms; p99 baseline complete ≤ 200 ms.
LATENCY_BUCKETS_SECONDS: Final[tuple[float, ...]] = (
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.2,
    0.5,
    1.0,
    2.5,
    5.0,
)

_PROCESS_ROLES: Final[frozenset[str]] = frozenset(
    {"api", "admin", "migrate", "maintain", "relay", "apply"}
)


class MetricLabelError(ValueError):
    """Raised when a caller attempts a forbidden or unknown metric label."""


@dataclass(frozen=True, slots=True)
class MetricSample:
    """One labeled sample from the in-process registry."""

    name: str
    labels: dict[str, str]
    value: float


def _freeze_labels(labels: Mapping[str, str]) -> tuple[tuple[str, str], ...]:
    return tuple(sorted((str(k), str(v)) for k, v in labels.items()))


def _validate_labels(labels: Mapping[str, str]) -> dict[str, str]:
    unknown = set(labels) - ALLOWED_LABEL_KEYS
    if unknown:
        raise MetricLabelError(
            f"forbidden metric label(s): {', '.join(sorted(unknown))}"
        )
    return {str(k): str(v) for k, v in labels.items()}


class KernelMetrics:
    """Process-local kernel SLI registry with allowlisted labels only."""

    def __init__(self, *, process_role: str = "api") -> None:
        if process_role not in _PROCESS_ROLES:
            raise MetricLabelError(f"unknown process_role: {process_role!r}")
        self._process_role = process_role
        self._lock = Lock()
        self._gauges: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}
        self._counters: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}
        self._histograms: dict[
            tuple[str, tuple[tuple[str, str], ...]], list[float]
        ] = {}

    @property
    def process_role(self) -> str:
        return self._process_role

    def set_depth(
        self,
        *,
        queue: str,
        ready: int,
        delayed: int,
        leased: int,
    ) -> None:
        """Set ready/delayed/leased depth gauges keyed by queue only."""
        base = {"queue": queue}
        self._set_gauge("queue_depth_ready", base, float(ready))
        self._set_gauge("queue_depth_delayed", base, float(delayed))
        self._set_gauge("queue_depth_leased", base, float(leased))

    def set_oldest_ready_age_seconds(self, *, queue: str, age_seconds: float) -> None:
        if age_seconds < 0:
            raise MetricLabelError("age_seconds must be >= 0")
        self._set_gauge(
            "queue_oldest_ready_age_seconds",
            {"queue": queue},
            float(age_seconds),
        )

    def set_delivery_depth(self, *, pending: int, publishing: int) -> None:
        """Set Delivery Outbox pending/publishing depth (no destination label)."""
        base = {"process_role": self._process_role}
        self._set_gauge("queue_delivery_depth_pending", base, float(pending))
        self._set_gauge("queue_delivery_depth_publishing", base, float(publishing))

    def set_delivery_oldest_pending_lag_seconds(self, age_seconds: float) -> None:
        """Oldest pending delivery lag from Queue-store event time."""
        if age_seconds < 0:
            raise MetricLabelError("age_seconds must be >= 0")
        self._set_gauge(
            "queue_delivery_oldest_pending_lag_seconds",
            {"process_role": self._process_role},
            float(age_seconds),
        )

    def set_stats_snapshot_age_seconds(self, age_seconds: float) -> None:
        """Record statistics snapshot age (OPS-03 / observability SLI)."""
        if age_seconds < 0:
            raise MetricLabelError("age_seconds must be >= 0")
        self._set_gauge(
            "queue_stats_snapshot_age_seconds",
            {"process_role": self._process_role},
            float(age_seconds),
        )

    def record_operation(
        self,
        *,
        operation: str,
        result: str,
        duration_seconds: float,
        queue: str,
        terminal_outcome: str | None = None,
        failure_code: str | None = None,
        extra_labels: Mapping[str, str] | None = None,
    ) -> None:
        """Record a committed kernel operation outcome and latency.

        Call only after the authoritative Queue-store outcome is known.
        """
        if duration_seconds < 0:
            raise MetricLabelError("duration_seconds must be >= 0")

        labels: dict[str, str] = {
            "queue": queue,
            "operation": operation,
            "result": result,
            "process_role": self._process_role,
        }
        if terminal_outcome is not None:
            labels["terminal_outcome"] = terminal_outcome
        if failure_code is not None:
            labels["failure_code"] = failure_code
        if extra_labels:
            labels.update(dict(extra_labels))

        validated = _validate_labels(labels)
        self._observe_histogram(
            "queue_operation_duration_seconds", validated, duration_seconds
        )
        self._inc_counter("queue_operation_total", validated, 1.0)

        if terminal_outcome is not None:
            term_labels = {
                "queue": queue,
                "terminal_outcome": terminal_outcome,
                "process_role": self._process_role,
            }
            if failure_code is not None:
                term_labels["failure_code"] = failure_code
            self._inc_counter(
                "queue_terminal_outcome_total",
                _validate_labels(term_labels),
                1.0,
            )

    def record_wait_latency(self, *, queue: str, duration_seconds: float) -> None:
        if duration_seconds < 0:
            raise MetricLabelError("duration_seconds must be >= 0")
        labels = _validate_labels(
            {"queue": queue, "process_role": self._process_role}
        )
        self._observe_histogram(
            "queue_wait_duration_seconds", labels, duration_seconds
        )

    def set_long_poll_active(self, active: int) -> None:
        """Gauge of outstanding positive long-poll waiters on this process."""
        if active < 0:
            raise MetricLabelError("active must be >= 0")
        self._set_gauge(
            "queue_long_poll_active",
            {"process_role": self._process_role},
            float(active),
        )

    def set_claim_listener_connected(self, connected: bool) -> None:
        """1 when the dedicated wake listener is connected; 0 when degraded."""
        self._set_gauge(
            "queue_claim_listener_connected",
            {"process_role": self._process_role},
            1.0 if connected else 0.0,
        )

    def record_long_poll(self, *, result: str, duration_seconds: float) -> None:
        """Record a closed-label long-poll lifecycle outcome (no queue label)."""
        if duration_seconds < 0:
            raise MetricLabelError("duration_seconds must be >= 0")
        if result not in _LONG_POLL_RESULTS:
            raise MetricLabelError(f"unknown long-poll result: {result!r}")
        labels = _validate_labels(
            {"result": result, "process_role": self._process_role}
        )
        self._inc_counter("queue_long_poll_total", labels, 1.0)
        self._observe_histogram(
            "queue_long_poll_duration_seconds", labels, duration_seconds
        )

    def record_long_poll_attempt(self, *, result: str) -> None:
        """Count notification-driven vs fallback reconciliation claim attempts."""
        if result not in _LONG_POLL_ATTEMPT_RESULTS:
            raise MetricLabelError(f"unknown long-poll attempt result: {result!r}")
        labels = _validate_labels(
            {"result": result, "process_role": self._process_role}
        )
        self._inc_counter("queue_long_poll_attempts_total", labels, 1.0)

    def record_claim_wakeup(self, *, result: str) -> None:
        """Count wake-substrate events (notification / reconnect / listener_error)."""
        if result not in _CLAIM_WAKEUP_RESULTS:
            raise MetricLabelError(f"unknown claim-wakeup result: {result!r}")
        labels = _validate_labels(
            {"result": result, "process_role": self._process_role}
        )
        self._inc_counter("queue_claim_wakeup_total", labels, 1.0)

    def long_poll_error_ratio(self) -> float:
        """Fraction of long-poll outcomes that are service errors.

        ``expired`` / ``cancelled`` / ``shutdown`` / ``admission_rejected`` /
        ``task`` are excluded from the error numerator (successful or expected).
        """
        with self._lock:
            numerator = 0.0
            denominator = 0.0
            for (name, frozen), value in self._counters.items():
                if name != "queue_long_poll_total":
                    continue
                labels = dict(frozen)
                result = labels.get("result", "")
                denominator += value
                if result == "error":
                    numerator += value
                elif result in _LONG_POLL_NON_ERROR_RESULTS:
                    pass
        if denominator <= 0.0:
            return 0.0
        return numerator / denominator

    def record_processing_latency(
        self, *, queue: str, duration_seconds: float
    ) -> None:
        if duration_seconds < 0:
            raise MetricLabelError("duration_seconds must be >= 0")
        labels = _validate_labels(
            {"queue": queue, "process_role": self._process_role}
        )
        self._observe_histogram(
            "queue_processing_duration_seconds", labels, duration_seconds
        )

    def record_retry(self, *, queue: str, failure_code: str) -> None:
        labels = _validate_labels(
            {
                "queue": queue,
                "failure_code": failure_code,
                "process_role": self._process_role,
            }
        )
        self._inc_counter("queue_retry_total", labels, 1.0)

    def record_lease_expiry(self, *, queue: str) -> None:
        labels = _validate_labels(
            {"queue": queue, "process_role": self._process_role}
        )
        self._inc_counter("queue_lease_expiry_total", labels, 1.0)

    def record_dead_letter(self, *, queue: str, failure_code: str) -> None:
        labels = _validate_labels(
            {
                "queue": queue,
                "failure_code": failure_code,
                "process_role": self._process_role,
            }
        )
        self._inc_counter("queue_dead_letter_total", labels, 1.0)

    def record_break_glass(
        self,
        *,
        operation: str,
        result: str,
        queue: str | None = None,
    ) -> None:
        """Increment low-cardinality break-glass success counter (OPS-09 / D-07).

        Labels are limited to ``operation``, ``result``, and optional ``queue``.
        Never pass reason, incident_reference, tokens, or payloads.
        """
        labels: dict[str, str] = {
            "operation": operation,
            "result": result,
            "process_role": self._process_role,
        }
        if queue is not None:
            labels["queue"] = queue
        self._inc_counter("queue_break_glass_total", _validate_labels(labels), 1.0)

    def set_premake_headroom_days(self, days: float) -> None:
        """Partition premake headroom in UTC days (OPS-05)."""
        self._set_gauge(
            "queue_premake_headroom_days",
            {"process_role": self._process_role},
            float(days),
        )

    def set_maintenance_last_success_age_seconds(self, age_seconds: float) -> None:
        if age_seconds < 0:
            raise MetricLabelError("age_seconds must be >= 0")
        self._set_gauge(
            "queue_maintenance_last_success_age_seconds",
            {"process_role": self._process_role},
            float(age_seconds),
        )

    def record_retention_partition_counts(
        self,
        *,
        result: str,
        created: int,
        detached: int,
        dropped: int,
        purge_deleted: int,
        failure_code: str | None = None,
    ) -> None:
        """Aggregate detach/drop/purge counters — never partition names."""
        labels: dict[str, str] = {
            "process_role": self._process_role,
            "operation": "maintain",
            "result": result,
        }
        if failure_code is not None:
            labels["failure_code"] = failure_code
        validated = _validate_labels(labels)
        self._inc_counter(
            "queue_retention_partitions_created_total", validated, float(created)
        )
        self._inc_counter(
            "queue_retention_partitions_detached_total", validated, float(detached)
        )
        self._inc_counter(
            "queue_retention_partitions_dropped_total", validated, float(dropped)
        )
        self._inc_counter(
            "queue_retention_purge_deleted_total", validated, float(purge_deleted)
        )

    def set_retention_window_bounds(
        self,
        *,
        retention_window: str,
        policy_days: int,
        oldest_retained_age_days: float | None,
        newest_retained_age_days: float | None,
    ) -> None:
        """Per-window policy and age gauges keyed by closed retention_window."""
        labels = _validate_labels(
            {
                "process_role": self._process_role,
                "retention_window": retention_window,
            }
        )
        self._set_gauge(
            "queue_retention_window_policy_days", labels, float(policy_days)
        )
        if oldest_retained_age_days is not None:
            self._set_gauge(
                "queue_retention_window_oldest_age_days",
                labels,
                float(oldest_retained_age_days),
            )
        if newest_retained_age_days is not None:
            self._set_gauge(
                "queue_retention_window_newest_age_days",
                labels,
                float(newest_retained_age_days),
            )

    def service_error_ratio(self, *, operation: str, queue: str) -> float:
        """Service-error ratio excluding empty/pause/drain/invalid outcomes."""
        with self._lock:
            numerator = 0.0
            denominator = 0.0
            for (name, frozen), value in self._counters.items():
                if name != "queue_operation_total":
                    continue
                labels = dict(frozen)
                if labels.get("operation") != operation:
                    continue
                if labels.get("queue") != queue:
                    continue
                result = labels.get("result", "")
                if result in _NON_ERROR_RATIO_RESULTS:
                    continue
                denominator += value
                if result == "service_error":
                    numerator += value
        if denominator <= 0.0:
            return 0.0
        return numerator / denominator

    def snapshot(self) -> list[MetricSample]:
        """Return a stable copy of all samples for tests and exporters."""
        samples: list[MetricSample] = []
        with self._lock:
            for (name, frozen), value in self._gauges.items():
                samples.append(
                    MetricSample(name=name, labels=dict(frozen), value=value)
                )
            for (name, frozen), value in self._counters.items():
                samples.append(
                    MetricSample(name=name, labels=dict(frozen), value=value)
                )
            for (name, frozen), observations in self._histograms.items():
                labels = dict(frozen)
                # Expose count as the histogram series sample for unit tests;
                # bucket/sum export belongs to a later exposition plan.
                samples.append(
                    MetricSample(
                        name=name,
                        labels=labels,
                        value=float(len(observations)),
                    )
                )
        samples.sort(key=lambda s: (s.name, tuple(sorted(s.labels.items()))))
        return samples

    def _set_gauge(
        self, name: str, labels: Mapping[str, str], value: float
    ) -> None:
        validated = _validate_labels(labels)
        key = (name, _freeze_labels(validated))
        with self._lock:
            self._gauges[key] = value

    def _inc_counter(
        self, name: str, labels: Mapping[str, str], delta: float
    ) -> None:
        validated = _validate_labels(labels)
        key = (name, _freeze_labels(validated))
        with self._lock:
            self._counters[key] = self._counters.get(key, 0.0) + delta

    def _observe_histogram(
        self, name: str, labels: Mapping[str, str], observation: float
    ) -> None:
        validated = _validate_labels(labels)
        key = (name, _freeze_labels(validated))
        with self._lock:
            self._histograms.setdefault(key, []).append(observation)


def latency_bucket_le(observation: float) -> Iterable[float]:
    """Yield cumulative histogram ``le`` bounds for ``observation``."""
    for bound in LATENCY_BUCKETS_SECONDS:
        if observation <= bound:
            yield bound
    yield float("inf")
