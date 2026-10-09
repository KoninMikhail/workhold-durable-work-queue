"""Label-cardinality and secret-redaction regression coverage (OPS-02, OPS-08)."""

from __future__ import annotations

import pytest

from queue_service.observability import context as obs_context
from queue_service.observability import metrics


def test_depth_gauges_are_keyed_by_queue_only() -> None:
    registry = metrics.KernelMetrics()
    registry.set_depth(queue="orders", ready=3, delayed=1, leased=2)
    registry.set_oldest_ready_age_seconds(queue="orders", age_seconds=12.5)

    series = registry.snapshot()
    depth = {s.name: s for s in series if s.name.startswith("queue_depth_")}
    assert set(depth) == {
        "queue_depth_ready",
        "queue_depth_delayed",
        "queue_depth_leased",
    }
    for sample in depth.values():
        assert sample.labels == {"queue": "orders"}
        assert "task_id" not in sample.labels
        assert "worker_id" not in sample.labels

    age = next(s for s in series if s.name == "queue_oldest_ready_age_seconds")
    assert age.labels == {"queue": "orders"}
    assert age.value == pytest.approx(12.5)


def test_operation_histograms_and_outcome_counters_cover_kernel_surface() -> None:
    registry = metrics.KernelMetrics(process_role="api")

    registry.record_operation(
        operation="enqueue",
        result="success",
        duration_seconds=0.042,
        queue="orders",
    )
    registry.record_operation(
        operation="claim",
        result="empty",
        duration_seconds=0.008,
        queue="orders",
    )
    registry.record_operation(
        operation="claim",
        result="success",
        duration_seconds=0.015,
        queue="orders",
    )
    registry.record_operation(
        operation="heartbeat",
        result="success",
        duration_seconds=0.011,
        queue="orders",
    )
    registry.record_operation(
        operation="complete",
        result="success",
        duration_seconds=0.09,
        queue="orders",
        terminal_outcome="succeeded",
    )
    registry.record_operation(
        operation="fail",
        result="success",
        duration_seconds=0.05,
        queue="orders",
        terminal_outcome="dead_lettered",
        failure_code="handler.timeout",
    )
    registry.record_wait_latency(queue="orders", duration_seconds=1.2)
    registry.record_processing_latency(queue="orders", duration_seconds=0.7)
    registry.record_retry(queue="orders", failure_code="handler.timeout")
    registry.record_lease_expiry(queue="orders")
    registry.record_dead_letter(queue="orders", failure_code="handler.timeout")

    names = {s.name for s in registry.snapshot()}
    assert "queue_operation_duration_seconds" in names
    assert "queue_operation_total" in names
    assert "queue_wait_duration_seconds" in names
    assert "queue_processing_duration_seconds" in names
    assert "queue_retry_total" in names
    assert "queue_lease_expiry_total" in names
    assert "queue_dead_letter_total" in names
    assert "queue_terminal_outcome_total" in names

    buckets = metrics.LATENCY_BUCKETS_SECONDS
    # Phase 3.9 gate: p99 enqueue/claim/heartbeat ≤100ms, complete ≤200ms.
    assert 0.1 in buckets
    assert 0.2 in buckets
    assert buckets == tuple(sorted(buckets))


def test_empty_claim_is_success_not_service_error() -> None:
    registry = metrics.KernelMetrics(process_role="api")
    registry.record_operation(
        operation="claim",
        result="empty",
        duration_seconds=0.01,
        queue="orders",
    )
    registry.record_operation(
        operation="claim",
        result="paused",
        duration_seconds=0.01,
        queue="orders",
    )
    registry.record_operation(
        operation="enqueue",
        result="draining",
        duration_seconds=0.01,
        queue="orders",
    )
    registry.record_operation(
        operation="enqueue",
        result="invalid_request",
        duration_seconds=0.01,
        queue="orders",
    )
    registry.record_operation(
        operation="enqueue",
        result="service_error",
        duration_seconds=0.5,
        queue="orders",
    )

    counters = [
        s
        for s in registry.snapshot()
        if s.name == "queue_operation_total" and s.labels.get("operation") == "claim"
    ]
    empty = next(s for s in counters if s.labels["result"] == "empty")
    paused = next(s for s in counters if s.labels["result"] == "paused")
    assert empty.value == 1
    assert paused.value == 1

    error_ratio = registry.service_error_ratio(operation="enqueue", queue="orders")
    # invalid_request / draining / paused must not inflate the service-error ratio.
    assert error_ratio == pytest.approx(1.0)  # 1 service_error / 1 counted


def test_long_poll_metrics_closed_labels_and_expiry_not_error() -> None:
    registry = metrics.KernelMetrics(process_role="api")
    registry.set_long_poll_active(3)
    registry.record_long_poll(result="expired", duration_seconds=1.5)
    registry.record_long_poll(result="task", duration_seconds=0.2)
    registry.record_long_poll(result="error", duration_seconds=0.1)
    registry.record_long_poll_attempt(result="notification")
    registry.record_long_poll_attempt(result="fallback")
    registry.record_claim_wakeup(result="notification")
    registry.set_claim_listener_connected(True)

    series = registry.snapshot()
    long_poll = [s for s in series if s.name.startswith("queue_long_poll")]
    for sample in long_poll:
        assert set(sample.labels) <= {"result", "process_role"}
        assert "queue" not in sample.labels
        assert "worker_id" not in sample.labels
        assert "claim_id" not in sample.labels

    expired = next(
        s
        for s in series
        if s.name == "queue_long_poll_total" and s.labels.get("result") == "expired"
    )
    assert expired.value == 1
    assert registry.long_poll_error_ratio() == pytest.approx(1.0 / 3.0)

    with pytest.raises(metrics.MetricLabelError):
        registry.record_long_poll(result="unknown", duration_seconds=0.1)


def test_metric_labels_are_strictly_allowlisted() -> None:
    registry = metrics.KernelMetrics(process_role="api")
    with pytest.raises(metrics.MetricLabelError):
        registry.record_operation(
            operation="enqueue",
            result="success",
            duration_seconds=0.01,
            queue="orders",
            extra_labels={"task_id": "t-1"},
        )
    with pytest.raises(metrics.MetricLabelError):
        registry.record_operation(
            operation="enqueue",
            result="success",
            duration_seconds=0.01,
            queue="orders",
            extra_labels={"worker_id": "w-1"},
        )
    with pytest.raises(metrics.MetricLabelError):
        registry.record_operation(
            operation="enqueue",
            result="success",
            duration_seconds=0.01,
            queue="orders",
            extra_labels={"request_id": "r-1"},
        )
    with pytest.raises(metrics.MetricLabelError):
        registry.record_operation(
            operation="enqueue",
            result="success",
            duration_seconds=0.01,
            queue="orders",
            extra_labels={"idempotency_key": "k-1"},
        )

    registry.record_operation(
        operation="fail",
        result="success",
        duration_seconds=0.02,
        queue="orders",
        terminal_outcome="retried",
        failure_code="handler.timeout",
    )
    sample = next(
        s
        for s in registry.snapshot()
        if s.name == "queue_operation_total" and s.labels.get("failure_code")
    )
    assert set(sample.labels) <= metrics.ALLOWED_LABEL_KEYS
    assert sample.labels["process_role"] == "api"


def test_correlation_context_excludes_secrets_by_construction() -> None:
    sentry_sentinel = "SENTRY_DSN_SENTINEL_9z8y"
    projected = obs_context.project_correlation(
        {
            "request_id": "req-1",
            "trace_id": "trace-1",
            "queue": "orders",
            "operation": "claim",
            "task_id": "11111111-1111-1111-1111-111111111111",
            "claim_id": "22222222-2222-2222-2222-222222222222",
            "event_id": "33333333-3333-3333-3333-333333333333",
            "generation": 3,
            "worker_id": "worker-a",
            "config_version": 7,
            "policy_version": 2,
            "result": "success",
            "code": "ok",
            # Forbidden — must never appear:
            "payload": {"secret": "value"},
            "claim_token": "super-secret-token",
            "idempotency_key": "idem-xyz",
            "failure_detail": "stack trace with secrets",
            "authorization": "Bearer leak",
            "X-Queue-Claim-Token": "header-token",
            "sentry_dsn": sentry_sentinel,
        }
    )

    assert projected == {
        "request_id": "req-1",
        "trace_id": "trace-1",
        "queue": "orders",
        "operation": "claim",
        "task_id": "11111111-1111-1111-1111-111111111111",
        "claim_id": "22222222-2222-2222-2222-222222222222",
        "event_id": "33333333-3333-3333-3333-333333333333",
        "generation": 3,
        "worker_id": "worker-a",
        "config_version": 7,
        "policy_version": 2,
        "result": "success",
        "code": "ok",
    }
    serialized = str(projected).lower()
    assert "super-secret-token" not in serialized
    assert "idem-xyz" not in serialized
    assert "stack trace" not in serialized
    assert "bearer leak" not in serialized
    assert "header-token" not in serialized
    assert sentry_sentinel not in serialized
    assert "sentry_dsn" not in projected
    assert "secret" not in serialized or "secret" not in projected.values()


def test_correlation_projector_is_shared_for_logs_and_traces() -> None:
    fields = {
        "request_id": "req-2",
        "queue": "billing",
        "operation": "complete",
        "generation": 1,
        "claim_token": "must-not-leak",
        "payload": {"x": 1},
    }
    log_attrs = obs_context.project_correlation(fields)
    trace_attrs = obs_context.project_correlation(fields)
    assert log_attrs == trace_attrs
    assert "claim_token" not in log_attrs
    assert "payload" not in trace_attrs
    assert set(log_attrs) <= obs_context.CORRELATION_ALLOWLIST
