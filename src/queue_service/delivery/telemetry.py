"""Delivery Outbox telemetry: bounded metrics, correlation, lag alerts (OPS-02).

Wraps Phase 4 :class:`KernelMetrics` and the shared OPS-08 correlation
projector. Metric labels stay inside the observability allowlist; public event
and task IDs appear only in redacted structured logs/traces.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Final

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from queue_service.delivery.models import STATE_PENDING, STATE_PUBLISHING
from queue_service.observability.context import emit_correlation, project_correlation
from queue_service.observability.metrics import KernelMetrics
from queue_service.storage.models import DeliveryEventActive

logger = logging.getLogger("queue_service.delivery.telemetry")

# Synthetic queue label: one configured delivery endpoint per deployment.
_DELIVERY_QUEUE: Final[str] = "_delivery"

DEFAULT_LAG_WARN_SECONDS: Final[float] = 300.0


class DeliveryAlertSeverity(Enum):
    NONE = "none"
    WARNING = "warning"


class DeliveryAlertKind(Enum):
    DELIVERY_LAG_SUSTAINED = "delivery_lag_sustained"


@dataclass(frozen=True, slots=True)
class DeliveryAlert:
    kind: DeliveryAlertKind
    severity: DeliveryAlertSeverity


def project_delivery_correlation(fields: Mapping[str, Any]) -> dict[str, Any]:
    """Project delivery correlation through the shared OPS-08 allowlist."""
    return project_correlation(fields)


def emit_delivery_correlation(
    log: logging.Logger,
    event: str,
    projected: Mapping[str, Any],
) -> dict[str, Any]:
    """Emit allowlisted delivery correlation to logs (and optional spans)."""
    return emit_correlation(log, event, projected)


def evaluate_delivery_alerts(
    *,
    oldest_pending_lag_seconds: float | None,
    pending_depth: int,
    lag_warn_seconds: float = DEFAULT_LAG_WARN_SECONDS,
) -> tuple[DeliveryAlert, ...]:
    """Alert when pending depth is non-zero and oldest lag is sustained."""
    if (
        pending_depth > 0
        and oldest_pending_lag_seconds is not None
        and oldest_pending_lag_seconds >= lag_warn_seconds
    ):
        return (
            DeliveryAlert(
                kind=DeliveryAlertKind.DELIVERY_LAG_SUSTAINED,
                severity=DeliveryAlertSeverity.WARNING,
            ),
        )
    return ()


class DeliveryTelemetry:
    """Instrument relay readiness/claim/publish/outcome transitions."""

    def __init__(
        self,
        *,
        metrics: KernelMetrics,
        log: logging.Logger | None = None,
    ) -> None:
        self._metrics = metrics
        self._log = log or logger
        self._projection_as_of: datetime | None = None

    @property
    def metrics(self) -> KernelMetrics:
        return self._metrics

    @property
    def projection_as_of(self) -> datetime | None:
        return self._projection_as_of

    def record_readiness(
        self,
        *,
        result: str,
        reason_code: str,
        duration_seconds: float,
    ) -> None:
        self._metrics.record_operation(
            operation="delivery.readiness",
            result=result,
            duration_seconds=duration_seconds,
            queue=_DELIVERY_QUEUE,
            failure_code=reason_code if result != "accepting" else None,
        )
        emit_delivery_correlation(
            self._log,
            "delivery.readiness",
            {
                "operation": "delivery.readiness",
                "result": result,
                "code": reason_code,
                "process_role": self._metrics.process_role,
            },
        )

    def record_claim_skip(self, *, reason_code: str, delay_seconds: float) -> None:
        del delay_seconds  # observed via readiness Retry-After; not a metric label
        self._metrics.record_operation(
            operation="delivery.claim_skip",
            result="backpressure",
            duration_seconds=0.0,
            queue=_DELIVERY_QUEUE,
            failure_code=reason_code,
        )
        emit_delivery_correlation(
            self._log,
            "delivery.claim_skip",
            {
                "operation": "delivery.claim_skip",
                "result": "backpressure",
                "code": reason_code,
                "process_role": self._metrics.process_role,
            },
        )

    def record_claim(
        self,
        *,
        result: str,
        duration_seconds: float,
        reclaimed: bool = False,
        event_id: str | None = None,
        source_task_id: str | None = None,
        generation: int | None = None,
    ) -> None:
        self._metrics.record_operation(
            operation="delivery.claim",
            result=result,
            duration_seconds=duration_seconds,
            queue=_DELIVERY_QUEUE,
        )
        if reclaimed:
            self.record_lease_expiry_reclaim()
        fields: dict[str, Any] = {
            "operation": "delivery.claim",
            "result": result,
            "process_role": self._metrics.process_role,
        }
        if event_id is not None:
            fields["event_id"] = event_id
        if source_task_id is not None:
            fields["source_task_id"] = source_task_id
        if generation is not None:
            fields["generation"] = generation
        emit_delivery_correlation(self._log, "delivery.claim", fields)

    def record_lease_expiry_reclaim(self) -> None:
        self._metrics.record_lease_expiry(queue=_DELIVERY_QUEUE)

    def record_publish_start(
        self,
        *,
        event_id: str | None = None,
        source_task_id: str | None = None,
        generation: int | None = None,
    ) -> None:
        fields: dict[str, Any] = {
            "operation": "delivery.publish",
            "result": "start",
            "process_role": self._metrics.process_role,
        }
        if event_id is not None:
            fields["event_id"] = event_id
        if source_task_id is not None:
            fields["source_task_id"] = source_task_id
        if generation is not None:
            fields["generation"] = generation
        emit_delivery_correlation(self._log, "delivery.publish.start", fields)

    def record_publish_result(
        self,
        *,
        result: str,
        duration_seconds: float,
        failure_code: str | None,
        event_id: str | None = None,
        source_task_id: str | None = None,
        generation: int | None = None,
    ) -> None:
        # Adapter disposition is non-terminal: terminal_outcome is recorded only
        # on fenced persistence (ack / dead-letter), never here (avoids double-count).
        self._metrics.record_operation(
            operation="delivery.publish",
            result=result,
            duration_seconds=duration_seconds,
            queue=_DELIVERY_QUEUE,
            failure_code=failure_code,
            terminal_outcome=None,
        )
        fields: dict[str, Any] = {
            "operation": "delivery.publish",
            "result": result,
            "code": failure_code,
            "process_role": self._metrics.process_role,
        }
        if event_id is not None:
            fields["event_id"] = event_id
        if source_task_id is not None:
            fields["source_task_id"] = source_task_id
        if generation is not None:
            fields["generation"] = generation
        emit_delivery_correlation(self._log, "delivery.publish", fields)

    def record_ack(
        self,
        *,
        result: str,
        duration_seconds: float,
        event_id: str | None = None,
        source_task_id: str | None = None,
        generation: int | None = None,
        code: str | None = None,
    ) -> None:
        self._metrics.record_operation(
            operation="delivery.ack",
            result=result,
            duration_seconds=duration_seconds,
            queue=_DELIVERY_QUEUE,
            failure_code=code,
            terminal_outcome="published" if result == "success" else None,
        )
        fields: dict[str, Any] = {
            "operation": "delivery.ack",
            "result": result,
            "code": code,
            "process_role": self._metrics.process_role,
        }
        if event_id is not None:
            fields["event_id"] = event_id
        if source_task_id is not None:
            fields["source_task_id"] = source_task_id
        if generation is not None:
            fields["generation"] = generation
        emit_delivery_correlation(self._log, "delivery.ack", fields)

    def record_retry(
        self,
        *,
        failure_code: str,
        delay_source: str,
        attempt: int,
        event_id: str | None = None,
        source_task_id: str | None = None,
        generation: int | None = None,
        duration_seconds: float = 0.0,
    ) -> None:
        del attempt  # attempt is correlation-only; not a metric label
        self._metrics.record_retry(queue=_DELIVERY_QUEUE, failure_code=failure_code)
        self._metrics.record_operation(
            operation="delivery.retry",
            result=delay_source,
            duration_seconds=duration_seconds,
            queue=_DELIVERY_QUEUE,
            failure_code=failure_code,
        )
        fields: dict[str, Any] = {
            "operation": "delivery.retry",
            "result": delay_source,
            "code": failure_code,
            "process_role": self._metrics.process_role,
        }
        if event_id is not None:
            fields["event_id"] = event_id
        if source_task_id is not None:
            fields["source_task_id"] = source_task_id
        if generation is not None:
            fields["generation"] = generation
        emit_delivery_correlation(self._log, "delivery.retry", fields)

    def record_dead_letter(
        self,
        *,
        failure_code: str,
        event_id: str | None = None,
        source_task_id: str | None = None,
        generation: int | None = None,
        duration_seconds: float = 0.0,
    ) -> None:
        self._metrics.record_dead_letter(
            queue=_DELIVERY_QUEUE, failure_code=failure_code
        )
        self._metrics.record_operation(
            operation="delivery.dead_letter",
            result="dead_lettered",
            duration_seconds=duration_seconds,
            queue=_DELIVERY_QUEUE,
            failure_code=failure_code,
            terminal_outcome="dead_lettered",
        )
        fields: dict[str, Any] = {
            "operation": "delivery.dead_letter",
            "result": "dead_lettered",
            "code": failure_code,
            "process_role": self._metrics.process_role,
        }
        if event_id is not None:
            fields["event_id"] = event_id
        if source_task_id is not None:
            fields["source_task_id"] = source_task_id
        if generation is not None:
            fields["generation"] = generation
        emit_delivery_correlation(self._log, "delivery.dead_letter", fields)

    def record_shutdown(self, *, in_flight: int) -> None:
        result = "clean" if in_flight == 0 else "draining"
        self._metrics.record_operation(
            operation="delivery.shutdown",
            result=result,
            duration_seconds=0.0,
            queue=_DELIVERY_QUEUE,
        )
        emit_delivery_correlation(
            self._log,
            "delivery.shutdown",
            {
                "operation": "delivery.shutdown",
                "result": result,
                "process_role": self._metrics.process_role,
                "code": str(in_flight),
            },
        )

    def set_depths(self, *, pending: int, publishing: int) -> None:
        self._metrics.set_delivery_depth(pending=pending, publishing=publishing)

    def set_oldest_pending_lag_seconds(self, lag_seconds: float) -> None:
        self._metrics.set_delivery_oldest_pending_lag_seconds(lag_seconds)

    def mark_projection_as_of(self, as_of: datetime) -> None:
        if as_of.tzinfo is None:
            as_of = as_of.replace(tzinfo=timezone.utc)
        self._projection_as_of = as_of


def reconcile_delivery_projection(
    session: Session,
    *,
    telemetry: DeliveryTelemetry,
) -> None:
    """Refresh pending/publishing depth and oldest lag from active counters.

    Uses one aggregate + one LIMIT-1 index seek. Never scans terminal history or
    payload JSON.
    """
    store_now = session.scalar(select(func.transaction_timestamp()))
    if store_now is None:
        store_now = datetime.now(timezone.utc)
    elif store_now.tzinfo is None:
        store_now = store_now.replace(tzinfo=timezone.utc)

    row = session.execute(
        text(
            """
            SELECT
                COUNT(*) FILTER (WHERE state_code = :pending) AS pending_count,
                COUNT(*) FILTER (WHERE state_code = :publishing) AS publishing_count
            FROM delivery_events_active
            """
        ),
        {"pending": STATE_PENDING, "publishing": STATE_PUBLISHING},
    ).one()
    pending = int(row[0] or 0)
    publishing = int(row[1] or 0)
    telemetry.set_depths(pending=pending, publishing=publishing)

    oldest = session.execute(
        select(DeliveryEventActive.available_at)
        .where(
            DeliveryEventActive.state_code == STATE_PENDING,
            DeliveryEventActive.available_at <= store_now,
        )
        .order_by(DeliveryEventActive.available_at.asc(), DeliveryEventActive.id.asc())
        .limit(1)
    ).scalar_one_or_none()
    if oldest is None:
        telemetry.set_oldest_pending_lag_seconds(0.0)
    else:
        aware = oldest if oldest.tzinfo else oldest.replace(tzinfo=timezone.utc)
        lag = max(0.0, (store_now - aware).total_seconds())
        telemetry.set_oldest_pending_lag_seconds(lag)
    telemetry.mark_projection_as_of(store_now)


def delivery_stats_from_metrics(
    metrics: KernelMetrics | None,
    *,
    projection_as_of: datetime | None = None,
) -> dict[str, Any]:
    """Bounded delivery section for ``/stats`` (no history/payload scan)."""
    if metrics is None:
        return {
            "pending_depth": None,
            "publishing_depth": None,
            "oldest_pending_lag_seconds": None,
            "as_of": None,
            "availability": "unavailable",
            "source": "process_telemetry",
        }
    pending = _gauge_value(metrics, "queue_delivery_depth_pending")
    publishing = _gauge_value(metrics, "queue_delivery_depth_publishing")
    lag = _gauge_value(metrics, "queue_delivery_oldest_pending_lag_seconds")
    as_of = None
    if projection_as_of is not None:
        aware = (
            projection_as_of
            if projection_as_of.tzinfo
            else projection_as_of.replace(tzinfo=timezone.utc)
        )
        as_of = aware.isoformat().replace("+00:00", "Z")
    availability = (
        "available"
        if pending is not None and publishing is not None
        else "unavailable"
    )
    return {
        "pending_depth": int(pending) if pending is not None else None,
        "publishing_depth": int(publishing) if publishing is not None else None,
        "oldest_pending_lag_seconds": lag,
        "as_of": as_of,
        "availability": availability,
        "source": "process_telemetry",
    }


def _gauge_value(metrics: KernelMetrics, name: str) -> float | None:
    for sample in metrics.snapshot():
        if sample.name == name:
            return float(sample.value)
    return None
