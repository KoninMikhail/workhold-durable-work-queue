"""Bounded freshness-declared operational statistics (OPS-03).

Reads keyed ``queue_counters`` and singleton maintenance metadata plus a
bounded oldest-ready index seek. Never ``COUNT(*)`` active rows and never
scans retained attempts or terminal payloads. Retry/DLQ summaries come from
Plan 01 process telemetry when available; otherwise they are explicit
unavailable (lower severity than correctness-path failure).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Final, Mapping

from sqlalchemy import text
from sqlalchemy.orm import Session

from workhold.delivery.telemetry import (
    DeliveryTelemetry,
    delivery_stats_from_metrics,
    reconcile_delivery_projection,
)
from workhold.observability.metrics import KernelMetrics
from workhold.observability.pressure import Freshness, classify_freshness

# Missing/stale counter material is lower severity than correctness failure
# (docs/05-operations/observability.md).
STATS_MAX_AGE_SECONDS: Final[float] = 30.0
_MAX_QUEUES: Final[int] = 1000
_TASK_READY: Final[int] = 2

_COUNTERS_SQL = text(
    """
    SELECT
        q.name AS queue_name,
        c.queue_id AS queue_id,
        c.ready_count AS ready_count,
        c.delayed_count AS delayed_count,
        c.leased_count AS leased_count,
        c.as_of AS as_of,
        CASE
            WHEN c.ready_count > 0 THEN (
                SELECT t.available_at
                FROM tasks_active AS t
                WHERE t.queue_id = c.queue_id
                  AND t.state_code = :ready_state
                ORDER BY t.available_at ASC, t.id ASC
                LIMIT 1
            )
            ELSE NULL
        END AS oldest_ready_at
    FROM queue_counters AS c
    INNER JOIN queues AS q ON q.id = c.queue_id
    ORDER BY q.name ASC
    LIMIT :max_queues
    """
)

_MAINTENANCE_SQL = text(
    """
    SELECT
        last_started_at,
        last_succeeded_at,
        premade_through,
        retained_from,
        last_error_code,
        updated_at
    FROM partition_maintenance_status
    WHERE singleton_id = 1
    """
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _format_dt(value: datetime | None) -> str | None:
    if value is None:
        return None
    aware = _ensure_aware(value)
    return aware.isoformat().replace("+00:00", "Z")


def _format_date(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _worst_freshness(*values: Freshness) -> Freshness:
    order = (Freshness.UNAVAILABLE, Freshness.STALE, Freshness.FRESH)
    for candidate in order:
        if candidate in values:
            return candidate
    return Freshness.UNAVAILABLE


def _sum_metric_counters(metrics: KernelMetrics, name: str) -> float:
    total = 0.0
    for sample in metrics.snapshot():
        if sample.name == name:
            total += float(sample.value)
    return total


def _telemetry_summary(
    metrics: KernelMetrics | None,
    *,
    counter_name: str,
) -> dict[str, Any]:
    if metrics is None:
        return {
            "total": None,
            "availability": "unavailable",
            "source": "process_telemetry",
        }
    return {
        "total": int(_sum_metric_counters(metrics, counter_name)),
        "availability": "available",
        "source": "process_telemetry",
    }


def build_stats_snapshot(
    session: Session,
    *,
    metrics: KernelMetrics | None = None,
    now: datetime | None = None,
    max_age_seconds: float = STATS_MAX_AGE_SECONDS,
) -> dict[str, Any]:
    """Return a bounded stats snapshot with declared freshness.

    Query budget is independent of retained terminal/attempt history volume:
    one counters join (capped), optional indexed LIMIT-1 oldest-ready seeks,
    and one singleton maintenance read.
    """

    generated_at = _ensure_aware(now) if now is not None else _utc_now()
    rows = session.execute(
        _COUNTERS_SQL,
        {"ready_state": _TASK_READY, "max_queues": _MAX_QUEUES},
    ).mappings().all()

    queues: list[dict[str, Any]] = []
    as_of_candidates: list[datetime] = []
    freshness_values: list[Freshness] = []

    for row in rows:
        as_of = _ensure_aware(row["as_of"])
        as_of_candidates.append(as_of)
        queue_freshness = classify_freshness(
            observed_at=as_of,
            now=generated_at,
            max_age_seconds=max_age_seconds,
        )
        freshness_values.append(queue_freshness)

        oldest_ready_at = row["oldest_ready_at"]
        oldest_age: float | None
        if oldest_ready_at is None:
            oldest_age = None
        else:
            oldest_age = max(
                0.0,
                (generated_at - _ensure_aware(oldest_ready_at)).total_seconds(),
            )
            if metrics is not None:
                metrics.set_oldest_ready_age_seconds(
                    queue=str(row["queue_name"]),
                    age_seconds=oldest_age,
                )
                metrics.set_depth(
                    queue=str(row["queue_name"]),
                    ready=int(row["ready_count"]),
                    delayed=int(row["delayed_count"]),
                    leased=int(row["leased_count"]),
                )

        queues.append(
            {
                "name": str(row["queue_name"]),
                "ready_depth": int(row["ready_count"]),
                "delayed_depth": int(row["delayed_count"]),
                "leased_depth": int(row["leased_count"]),
                "oldest_ready_age_seconds": oldest_age,
                "as_of": _format_dt(as_of),
                "freshness": queue_freshness.value,
            }
        )

    if as_of_candidates:
        # Oldest counter material drives snapshot freshness (conservative).
        snapshot_as_of = min(as_of_candidates)
        snapshot_freshness = _worst_freshness(*freshness_values)
    else:
        snapshot_as_of = generated_at
        snapshot_freshness = Freshness.UNAVAILABLE
        freshness_values.append(Freshness.UNAVAILABLE)

    age_seconds = max(
        0.0, (generated_at - snapshot_as_of).total_seconds()
    )
    if metrics is not None:
        metrics.set_stats_snapshot_age_seconds(age_seconds)

    maintenance_row = session.execute(_MAINTENANCE_SQL).mappings().first()
    if maintenance_row is None:
        maintenance: dict[str, Any] = {
            "availability": "unavailable",
            "last_started_at": None,
            "last_succeeded_at": None,
            "premade_through": None,
            "retained_from": None,
            "last_error_code": None,
            "updated_at": None,
        }
    else:
        maintenance = {
            "availability": "available",
            "last_started_at": _format_dt(maintenance_row["last_started_at"]),
            "last_succeeded_at": _format_dt(maintenance_row["last_succeeded_at"]),
            "premade_through": _format_date(maintenance_row["premade_through"]),
            "retained_from": _format_date(maintenance_row["retained_from"]),
            "last_error_code": maintenance_row["last_error_code"],
            "updated_at": _format_dt(maintenance_row["updated_at"]),
        }

    delivery_projection_as_of: datetime | None = None
    if metrics is not None:
        # Refresh depth/lag gauges from active aggregates so /stats.delivery is
        # available even when this process is not the relay loop.
        delivery_tel = DeliveryTelemetry(metrics=metrics)
        reconcile_delivery_projection(session, telemetry=delivery_tel)
        delivery_projection_as_of = delivery_tel.projection_as_of

    return {
        "as_of": _format_dt(snapshot_as_of),
        "generated_at": _format_dt(generated_at),
        "age_seconds": age_seconds,
        "freshness": snapshot_freshness.value,
        "queues": queues,
        "retry": _telemetry_summary(metrics, counter_name="queue_retry_total"),
        "dead_letter": _telemetry_summary(
            metrics, counter_name="queue_dead_letter_total"
        ),
        "delivery": _delivery_stats_section(
            metrics,
            generated_at=generated_at,
            projection_as_of=delivery_projection_as_of,
        ),
        "maintenance": maintenance,
    }


def _delivery_stats_section(
    metrics: KernelMetrics | None,
    *,
    generated_at: datetime,
    projection_as_of: datetime | None = None,
) -> dict[str, Any]:
    section = delivery_stats_from_metrics(
        metrics, projection_as_of=projection_as_of
    )
    if section["availability"] == "available" and section["as_of"] is None:
        section = dict(section)
        section["as_of"] = _format_dt(generated_at)
    return section


def snapshot_as_mapping(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    """Return a JSON-safe copy of a stats snapshot."""
    return dict(snapshot)
