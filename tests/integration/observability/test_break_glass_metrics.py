"""Low-cardinality break-glass success metrics (D-07..D-08 / OPS-09).

Metrics are additive to OPS-08 correlation; labels limited to
``operation``, ``result``, and optional allowlisted ``queue``.
"""

from __future__ import annotations

import pytest

from workhold.observability.metrics import (
    ALLOWED_LABEL_KEYS,
    KernelMetrics,
)
from workhold.operations.break_glass import record_break_glass_success


_BREAK_GLASS_METRIC_NAMES = frozenset(
    {
        "queue_break_glass_total",
        "queue_break_glass_ops_total",
    }
)


def _require_break_glass_metric_helper() -> None:
    if not hasattr(KernelMetrics, "record_break_glass"):
        registry = KernelMetrics(process_role="admin")
        names = {s.name for s in registry.snapshot()}
        if not (_BREAK_GLASS_METRIC_NAMES & names):
            pytest.fail("record_break_glass helper missing after 14-04 wiring")


def _counter_samples(metrics: KernelMetrics) -> list:
    return [
        s
        for s in metrics.snapshot()
        if s.name in _BREAK_GLASS_METRIC_NAMES
    ]


def test_break_glass_metric_labels_are_allowlisted_only() -> None:
    """D-07: no reason/incident/payload/token labels on break-glass counters."""
    assert "operation" in ALLOWED_LABEL_KEYS
    assert "result" in ALLOWED_LABEL_KEYS
    assert "queue" in ALLOWED_LABEL_KEYS
    for forbidden in ("reason", "incident_reference", "payload", "claim_token", "token"):
        assert forbidden not in ALLOWED_LABEL_KEYS


def test_successful_force_lease_expiry_increments_break_glass_counter() -> None:
    """Successful forceLeaseExpiry (existing op) must increment allowlisted counter."""
    _require_break_glass_metric_helper()
    metrics = KernelMetrics(process_role="admin")
    metrics.record_break_glass(
        operation="forceLeaseExpiry",
        result="retry_scheduled",
        queue="orders",
    )
    hits = _counter_samples(metrics)
    assert len(hits) == 1
    assert hits[0].name == "queue_break_glass_total"
    assert hits[0].value == 1.0
    assert hits[0].labels["operation"] == "forceLeaseExpiry"
    assert hits[0].labels["result"] == "retry_scheduled"
    assert hits[0].labels["queue"] == "orders"
    assert "reason" not in hits[0].labels
    assert "incident_reference" not in hits[0].labels


def test_successful_delivery_break_glass_op_increments_counter_placeholder() -> None:
    """forceDeliveryReclaim success increments same counter family (14-03 ops)."""
    _require_break_glass_metric_helper()
    metrics = KernelMetrics(process_role="admin")
    record_break_glass_success(
        metrics,
        operation="forceDeliveryReclaim",
        result="reclaimed",
        queue="orders",
    )
    hits = _counter_samples(metrics)
    assert len(hits) == 1
    assert hits[0].name == "queue_break_glass_total"
    assert hits[0].value == 1.0
    assert hits[0].labels["operation"] == "forceDeliveryReclaim"
    assert hits[0].labels["result"] == "reclaimed"
    assert hits[0].labels["queue"] == "orders"


def test_record_break_glass_success_noop_without_metrics() -> None:
    """Helper is safe when metrics registry was not injected."""
    record_break_glass_success(
        None,
        operation="raiseReplayLimit",
        result="raised",
        queue="orders",
    )
