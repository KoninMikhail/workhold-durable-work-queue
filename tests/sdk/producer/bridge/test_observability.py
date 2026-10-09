"""Bridge lag/health observability, redaction, and cardinality (06-06 / BRDG-02)."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest

from workhold_producer.bridge.store import (
    AppStoreHealthSnapshot,
    BoundedPendingDepth,
    OldestPendingSnapshot,
    OutboxIntent,
)
from _workhold_client_core.errors import (
    ProtocolError,
    TimeoutError as ClientTimeoutError,
    TransportError,
)
from _workhold_client_core.models import (
    EnqueueResponse,
    ErrorCode,
    ProtocolErrorBody,
    Task,
    TaskState,
)


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


def _utc(ts: str = "2026-09-19T12:00:00Z") -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def _task(*, task_id: str = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", queue: str = "orders") -> Task:
    return Task(
        task_id=task_id,
        queue_name=queue,
        producer_id="bridge-principal",
        state=TaskState("ready"),
        priority=0,
        available_at="2026-09-19T12:00:00Z",
        retry_policy_version=1,
        created_at="2026-09-19T12:00:00Z",
        spawned_task_ids=[],
        delivery_event_ids=[],
    )


def _intent(
    *,
    row_id: str = "row-1",
    namespace: str = "orders.checkout",
    schema_version: int = 1,
    queue: str = "orders",
    payload: Mapping[str, Any] | None = None,
    created_at: datetime | None = None,
    token: str = "lease-token-1",
    generation: int = 1,
    traceparent: str | None = None,
) -> OutboxIntent:
    return OutboxIntent(
        source_namespace=namespace,
        source_row_id=row_id,
        schema_version=schema_version,
        target_queue=queue,
        enqueue_request={"payload": dict(payload or {"order_id": 42}), "priority": 0},
        created_at=created_at or _utc(),
        ownership_token=token,
        generation=generation,
        lease_expires_at=_utc() + timedelta(seconds=30),
        traceparent=traceparent,
    )


def _protocol_error(*, code: str, retryable: bool) -> ProtocolError:
    body = ProtocolErrorBody(
        code=ErrorCode.parse(code),
        message="conflict",
        retryable=retryable,
        request_id="22222222-2222-4222-8222-222222222222",
        details={},
        retry_after_ms=None,
    )
    return ProtocolError(status_code=409 if not retryable else 503, body=body)


@dataclass
class _Row:
    intent: OutboxIntent
    state: str = "pending"


@dataclass
class ObservabilityStore:
    """OutboxStore fake that records which snapshot methods were called."""

    rows: dict[tuple[str, str], _Row] = field(default_factory=dict)
    connected: bool = True
    query_ok: bool = True
    as_of: datetime = field(default_factory=_utc)
    depth_calls: list[int] = field(default_factory=list)
    oldest_calls: int = 0
    health_calls: list[int] = field(default_factory=list)
    unbounded_count_called: bool = False
    claim_batches: list[list[OutboxIntent]] = field(default_factory=list)
    claim_calls: int = 0
    stale_tokens: set[str] = field(default_factory=set)
    mark_delivered_calls: list[dict[str, Any]] = field(default_factory=list)
    schedule_retry_calls: list[dict[str, Any]] = field(default_factory=list)
    terminal_calls: list[dict[str, Any]] = field(default_factory=list)

    def seed(self, intent: OutboxIntent, *, state: str = "pending") -> None:
        self.rows[(intent.source_namespace, intent.source_row_id)] = _Row(
            intent=intent, state=state
        )

    def claim(self, *, limit: int, lease_seconds: int) -> Sequence[OutboxIntent]:
        if not self.connected:
            raise OSError("app store unreachable")
        self.claim_calls += 1
        claimed: list[OutboxIntent] = []
        for row in list(self.rows.values()):
            if len(claimed) >= limit:
                break
            if row.state in {"pending", "retryable_failure"} or (
                row.state == "leased" and row.intent.ownership_token in self.stale_tokens
            ):
                token = f"tok-{self.claim_calls}-{len(claimed)}"
                gen = (
                    row.intent.generation + 1
                    if row.state != "pending"
                    else max(1, row.intent.generation)
                )
                updated = replace(
                    row.intent,
                    ownership_token=token,
                    generation=gen,
                    lease_expires_at=_utc() + timedelta(seconds=lease_seconds),
                )
                row.intent = updated
                row.state = "leased"
                claimed.append(updated)
        self.claim_batches.append(list(claimed))
        return list(claimed)

    def mark_delivered(
        self,
        *,
        source_namespace: str,
        source_row_id: str,
        ownership_token: str,
        queue_task_id: str | None = None,
    ) -> bool:
        self.mark_delivered_calls.append(
            {
                "source_namespace": source_namespace,
                "source_row_id": source_row_id,
                "ownership_token": ownership_token,
                "queue_task_id": queue_task_id,
            }
        )
        row = self.rows.get((source_namespace, source_row_id))
        if row is None:
            return False
        if ownership_token in self.stale_tokens:
            return False
        if row.intent.ownership_token != ownership_token or row.state != "leased":
            return False
        row.state = "delivered"
        return True

    def schedule_retry(
        self,
        *,
        source_namespace: str,
        source_row_id: str,
        ownership_token: str,
        available_at_delay_seconds: float,
        failure_code: str | None = None,
    ) -> bool:
        self.schedule_retry_calls.append(
            {
                "source_namespace": source_namespace,
                "source_row_id": source_row_id,
                "ownership_token": ownership_token,
                "available_at_delay_seconds": available_at_delay_seconds,
                "failure_code": failure_code,
            }
        )
        row = self.rows.get((source_namespace, source_row_id))
        if row is None or ownership_token in self.stale_tokens:
            return False
        if row.intent.ownership_token != ownership_token or row.state != "leased":
            return False
        row.state = "retryable_failure"
        return True

    def mark_terminal_operator_action(
        self,
        *,
        source_namespace: str,
        source_row_id: str,
        ownership_token: str,
        reason: str,
    ) -> bool:
        self.terminal_calls.append(
            {
                "source_namespace": source_namespace,
                "source_row_id": source_row_id,
                "ownership_token": ownership_token,
                "reason": reason,
            }
        )
        row = self.rows.get((source_namespace, source_row_id))
        if row is None or ownership_token in self.stale_tokens:
            return False
        if row.intent.ownership_token != ownership_token or row.state != "leased":
            return False
        row.state = "terminal_operator_action"
        return True

    def get_pending_depth(self, depth_cap: int) -> BoundedPendingDepth:
        self.depth_calls.append(depth_cap)
        if not self.connected:
            raise OSError("app store unreachable")
        if not self.query_ok:
            raise RuntimeError("query failed")
        pending = sum(
            1
            for r in self.rows.values()
            if r.state not in {"delivered", "terminal_operator_action"}
        )
        capped = pending > depth_cap
        return BoundedPendingDepth(
            count=min(pending, depth_cap),
            depth_cap=depth_cap,
            capped=capped,
            as_of=self.as_of,
        )

    def get_oldest_pending_created_at(self) -> OldestPendingSnapshot:
        self.oldest_calls += 1
        if not self.connected:
            raise OSError("app store unreachable")
        if not self.query_ok:
            raise RuntimeError("query failed")
        pending = [
            r.intent.created_at
            for r in self.rows.values()
            if r.state not in {"delivered", "terminal_operator_action"}
        ]
        return OldestPendingSnapshot(
            created_at=min(pending) if pending else None,
            as_of=self.as_of,
        )

    def get_health_snapshot(self, depth_cap: int) -> AppStoreHealthSnapshot:
        """Health snapshot without invoking the other two Plan-03 methods.

        BridgeTelemetry must still call ``get_pending_depth`` and
        ``get_oldest_pending_created_at`` explicitly (Plan 06-06).
        """
        self.health_calls.append(depth_cap)
        if not self.connected:
            return AppStoreHealthSnapshot(
                as_of=self.as_of,
                connected=False,
                query_ok=False,
                pending_count=0,
                pending_capped=False,
                oldest_pending_created_at=None,
            )
        pending = sum(
            1
            for r in self.rows.values()
            if r.state not in {"delivered", "terminal_operator_action"}
        )
        capped = pending > depth_cap
        oldest_times = [
            r.intent.created_at
            for r in self.rows.values()
            if r.state not in {"delivered", "terminal_operator_action"}
        ]
        return AppStoreHealthSnapshot(
            as_of=self.as_of,
            connected=True,
            query_ok=self.query_ok,
            pending_count=min(pending, depth_cap),
            pending_capped=capped,
            oldest_pending_created_at=min(oldest_times) if oldest_times else None,
        )

    def unbounded_count(self) -> int:
        """Must never be called by observability code."""
        self.unbounded_count_called = True
        return len(self.rows)


class FakeMetricSink:
    def __init__(self) -> None:
        self.counters: list[dict[str, Any]] = []
        self.gauges: list[dict[str, Any]] = []

    def emit_counter(
        self,
        name: str,
        *,
        value: float = 1.0,
        labels: Mapping[str, str],
        unit: str = "1",
    ) -> None:
        self.counters.append(
            {"name": name, "value": value, "labels": dict(labels), "unit": unit}
        )

    def emit_gauge(
        self,
        name: str,
        *,
        value: float,
        labels: Mapping[str, str],
        unit: str,
    ) -> None:
        self.gauges.append(
            {"name": name, "value": value, "labels": dict(labels), "unit": unit}
        )


class FakeLogSink:
    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []

    def emit(self, event: str, fields: Mapping[str, Any]) -> None:
        self.events.append({"event": event, "fields": dict(fields)})


class FakeProducer:
    def __init__(
        self, outcomes: list[EnqueueResponse | BaseException] | None = None
    ) -> None:
        self.outcomes = list(outcomes or [])
        self.calls: list[dict[str, Any]] = []

    def _enqueue_with_available_at_raw(
        self,
        queue_name: str,
        *,
        idempotency_key: str,
        payload: Any,
        priority: int = 0,
        available_at: str | None | object = ...,
    ) -> EnqueueResponse:
        self.calls.append(
            {
                "queue_name": queue_name,
                "idempotency_key": idempotency_key,
                "payload": payload,
                "priority": priority,
                "available_at": available_at,
            }
        )
        if not self.outcomes:
            return EnqueueResponse(task=_task(queue=queue_name), replayed=False)
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


FORBIDDEN_LABEL_KEYS = frozenset(
    {
        "source_namespace",
        "source_row_id",
        "idempotency_key",
        "task_id",
        "request_id",
        "payload",
        "credentials",
        "password",
        "token",
        "claim_token",
        "dsn",
        "sql",
        "message",
        "detail",
        "free_text",
    }
)

ALLOWED_LABEL_KEYS = frozenset({"process_role", "queue", "operation", "result"})


def _all_emitted_labels(metrics: FakeMetricSink) -> list[dict[str, str]]:
    return [e["labels"] for e in metrics.counters + metrics.gauges]


# ---------------------------------------------------------------------------
# Imports under test (RED until GREEN)
# ---------------------------------------------------------------------------


def test_bridge_health_and_telemetry_exports_exist() -> None:
    from workhold_producer.bridge.observability import BridgeHealth, BridgeTelemetry
    from workhold_producer.bridge import BridgeHealth as ExportedHealth
    from workhold_producer.bridge import BridgeTelemetry as ExportedTelemetry

    assert BridgeHealth is ExportedHealth
    assert BridgeTelemetry is ExportedTelemetry


# ---------------------------------------------------------------------------
# Health / lag / depth
# ---------------------------------------------------------------------------


def test_empty_backlog_is_healthy_with_zero_lag() -> None:
    from workhold_producer.bridge.observability import BridgeTelemetry

    store = ObservabilityStore()
    metrics = FakeMetricSink()
    clock = MagicMock(side_effect=[1000.0, 1000.5])
    tel = BridgeTelemetry(
        process_role="bridge",
        depth_cap=10,
        metric_sink=metrics,
        clock=clock,
        wall_clock=lambda: _utc("2026-09-19T12:00:00Z"),
    )

    health = tel.refresh_from_store(store)

    assert health.process_alive is True
    assert health.app_store_reachable is True
    assert health.app_store_query_ok is True
    assert health.pending_count == 0
    assert health.pending_capped is False
    assert health.oldest_pending_created_at is None
    assert health.oldest_pending_lag_seconds is None
    assert health.ready is True
    assert health.empty_backlog is True
    assert "data_loss" not in (health.status_note or "")
    assert health.as_of == _utc("2026-09-19T12:00:00Z")
    assert health.freshness_seconds == pytest.approx(0.5)


def test_growing_oldest_lag_uses_app_db_as_of() -> None:
    from workhold_producer.bridge.observability import BridgeTelemetry

    store = ObservabilityStore(as_of=_utc("2026-09-19T12:10:00Z"))
    store.seed(_intent(created_at=_utc("2026-09-19T12:00:00Z")))
    tel = BridgeTelemetry(
        process_role="bridge",
        depth_cap=100,
        metric_sink=FakeMetricSink(),
        wall_clock=lambda: _utc("2026-09-19T12:10:00Z"),
        clock=lambda: 0.0,
    )

    health = tel.refresh_from_store(store)

    assert health.oldest_pending_lag_seconds == pytest.approx(600.0)
    assert health.empty_backlog is False
    # Lag alone must not be framed as correctness failure / data loss.
    assert health.correctness_ok is True
    lag_gauges = [
        g for g in tel.metric_sink.gauges if g["name"] == "bridge.oldest_pending_lag_seconds"
    ]
    assert lag_gauges
    assert lag_gauges[-1]["unit"] == "seconds"
    assert lag_gauges[-1]["value"] == pytest.approx(600.0)


def test_pending_depth_is_capped_and_declared_approximate() -> None:
    from workhold_producer.bridge.observability import BridgeTelemetry

    store = ObservabilityStore()
    for i in range(15):
        store.seed(_intent(row_id=f"r{i}"))
    metrics = FakeMetricSink()
    tel = BridgeTelemetry(
        process_role="bridge",
        depth_cap=10,
        metric_sink=metrics,
        wall_clock=lambda: _utc(),
        clock=lambda: 0.0,
    )

    health = tel.refresh_from_store(store)

    assert health.pending_count == 10
    assert health.pending_capped is True
    assert health.pending_approximate is True
    depth_gauges = [g for g in metrics.gauges if g["name"] == "bridge.pending_depth"]
    assert depth_gauges[-1]["value"] == 10.0
    assert depth_gauges[-1]["unit"] == "count"
    assert depth_gauges[-1]["labels"]["result"] == "capped"


def test_refresh_consumes_all_three_plan03_snapshot_methods() -> None:
    from workhold_producer.bridge.observability import BridgeTelemetry

    store = ObservabilityStore()
    store.seed(_intent())
    tel = BridgeTelemetry(
        process_role="bridge",
        depth_cap=5,
        metric_sink=FakeMetricSink(),
        wall_clock=lambda: _utc(),
        clock=lambda: 0.0,
    )

    tel.refresh_from_store(store)

    assert store.depth_calls  # get_pending_depth called
    assert store.oldest_calls >= 1
    assert store.health_calls == [5]
    # Explicit depth + oldest in addition to whatever health calls internally.
    assert 5 in store.depth_calls
    assert store.unbounded_count_called is False


def test_app_store_unavailable_separates_liveness_from_readiness() -> None:
    from workhold_producer.bridge.observability import BridgeTelemetry

    store = ObservabilityStore(connected=False)
    tel = BridgeTelemetry(
        process_role="bridge",
        depth_cap=10,
        metric_sink=FakeMetricSink(),
        wall_clock=lambda: _utc(),
        clock=lambda: 1.0,
    )

    health = tel.refresh_from_store(store)

    assert health.process_alive is True
    assert health.app_store_reachable is False
    assert health.ready is False


def test_stale_poll_marks_degraded_not_data_loss() -> None:
    from workhold_producer.bridge.observability import BridgeTelemetry

    tel = BridgeTelemetry(
        process_role="bridge",
        depth_cap=10,
        metric_sink=FakeMetricSink(),
        wall_clock=lambda: _utc("2026-09-19T12:30:00Z"),
        clock=lambda: 0.0,
        poll_stale_after_seconds=60.0,
    )
    tel.note_successful_poll(at=_utc("2026-09-19T12:00:00Z"))
    store = ObservabilityStore(as_of=_utc("2026-09-19T12:30:00Z"))
    store.seed(_intent(created_at=_utc("2026-09-19T11:00:00Z")))

    health = tel.refresh_from_store(store)

    assert health.last_successful_poll_at == _utc("2026-09-19T12:00:00Z")
    assert health.poll_stale is True
    assert health.correctness_ok is True
    assert health.ready is False


def test_queue_unavailable_and_capability_mismatch_are_distinct() -> None:
    from workhold_producer.bridge.observability import BridgeTelemetry

    tel = BridgeTelemetry(
        process_role="bridge",
        depth_cap=10,
        metric_sink=FakeMetricSink(),
        wall_clock=lambda: _utc(),
        clock=lambda: 0.0,
    )
    tel.note_queue_unreachable()
    store = ObservabilityStore()
    health_unreachable = tel.refresh_from_store(store)
    assert health_unreachable.queue_reachable is False
    assert health_unreachable.queue_compatible is True
    assert health_unreachable.ready is False

    tel.note_queue_reachable()
    tel.note_capability_mismatch()
    health_mismatch = tel.refresh_from_store(store)
    assert health_mismatch.queue_reachable is True
    assert health_mismatch.queue_compatible is False
    assert health_mismatch.ready is False


# ---------------------------------------------------------------------------
# Counters / runner integration
# ---------------------------------------------------------------------------


def test_runner_emits_claim_delivery_retry_conflict_lease_and_shutdown() -> None:
    from workhold_producer.bridge.observability import BridgeTelemetry
    from workhold_producer.bridge.runner import BridgeRunner

    class LeaseLossStore(ObservabilityStore):
        def claim(self, *, limit: int, lease_seconds: int) -> Sequence[OutboxIntent]:
            claimed = list(super().claim(limit=limit, lease_seconds=lease_seconds))
            for intent in claimed:
                if intent.source_row_id == "lease-loss":
                    self.stale_tokens.add(intent.ownership_token)
            return claimed

    store = LeaseLossStore()
    for row_id, kwargs in [
        ("new", {}),
        ("replay", {}),
        ("retry", {}),
        ("conflict", {}),
        ("malformed", {"schema_version": 99}),
        ("lease-loss", {}),
    ]:
        store.seed(_intent(row_id=row_id, **kwargs))

    producer = FakeProducer(
        [
            EnqueueResponse(task=_task(), replayed=False),
            EnqueueResponse(
                task=_task(task_id="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"), replayed=True
            ),
            ClientTimeoutError(timeout_s=1.0),
            _protocol_error(code="idempotency_conflict", retryable=False),
            # malformed skips enqueue; lease-loss still enqueues then loses ack
            EnqueueResponse(task=_task(), replayed=False),
        ]
    )
    metrics = FakeMetricSink()
    tel = BridgeTelemetry(
        process_role="bridge",
        depth_cap=50,
        metric_sink=metrics,
        log_sink=FakeLogSink(),
        wall_clock=lambda: _utc(),
        clock=lambda: 0.0,
    )
    runner = BridgeRunner.for_tests(
        store=store,
        producer=producer,
        batch_size=10,
        max_in_flight=1,
        telemetry=tel,
        initial_backoff_seconds=1.0,
        max_backoff_seconds=1.0,
        backoff_jitter_ratio=0.0,
    )
    runner.poll_once()
    runner.request_shutdown()
    tel.record_shutdown()

    names = [c["name"] for c in metrics.counters]
    assert "bridge.claimed" in names
    assert "bridge.delivered" in names
    assert any(
        c["name"] == "bridge.delivered" and c["labels"]["result"] == "new"
        for c in metrics.counters
    )
    assert any(
        c["name"] == "bridge.delivered" and c["labels"]["result"] == "replay"
        for c in metrics.counters
    )
    assert "bridge.retryable_error" in names
    assert "bridge.permanent_conflict" in names
    assert "bridge.malformed_intent" in names
    assert "bridge.lease_loss" in names
    assert "bridge.shutdown" in names


def test_lease_reclaim_counter_on_reclaimed_generation() -> None:
    from workhold_producer.bridge.observability import BridgeTelemetry
    from workhold_producer.bridge.runner import BridgeRunner

    store = ObservabilityStore()
    # Already leased with stale token → reclaim bumps generation.
    intent = _intent(row_id="reclaim-me", generation=2, token="old-token")
    store.seed(intent, state="leased")
    store.stale_tokens.add("old-token")

    metrics = FakeMetricSink()
    tel = BridgeTelemetry(
        process_role="bridge",
        depth_cap=10,
        metric_sink=metrics,
        wall_clock=lambda: _utc(),
        clock=lambda: 0.0,
    )
    runner = BridgeRunner.for_tests(
        store=store,
        producer=FakeProducer([EnqueueResponse(task=_task(), replayed=False)]),
        telemetry=tel,
        batch_size=1,
        max_in_flight=1,
        backoff_jitter_ratio=0.0,
    )
    runner.poll_once()

    assert any(c["name"] == "bridge.lease_reclaim" for c in metrics.counters)


def test_transport_error_marks_queue_unreachable() -> None:
    from workhold_producer.bridge.observability import BridgeTelemetry
    from workhold_producer.bridge.runner import BridgeRunner

    store = ObservabilityStore()
    store.seed(_intent())
    tel = BridgeTelemetry(
        process_role="bridge",
        depth_cap=10,
        metric_sink=FakeMetricSink(),
        wall_clock=lambda: _utc(),
        clock=lambda: 0.0,
    )
    runner = BridgeRunner.for_tests(
        store=store,
        producer=FakeProducer([TransportError(reason="down")]),
        telemetry=tel,
        batch_size=1,
        max_in_flight=1,
        backoff_jitter_ratio=0.0,
        initial_backoff_seconds=0.0,
        max_backoff_seconds=0.0,
    )
    runner.poll_once()
    health = tel.refresh_from_store(store)
    assert health.queue_reachable is False


# ---------------------------------------------------------------------------
# Alerts
# ---------------------------------------------------------------------------


def test_alerts_combine_sustained_lag_with_depth_and_outcomes() -> None:
    from workhold_producer.bridge.observability import (
        BridgeAlertKind,
        BridgeTelemetry,
        evaluate_bridge_alerts,
    )

    # Lag alone — no alert.
    alone = evaluate_bridge_alerts(
        oldest_pending_lag_seconds=900.0,
        pending_count=0,
        pending_capped=False,
        recent_deliveries=0,
        recent_retries=0,
        recent_conflicts=0,
        lag_warn_seconds=300.0,
    )
    assert alone == ()

    # Empty backlog — healthy, no alert even if lag None.
    empty = evaluate_bridge_alerts(
        oldest_pending_lag_seconds=None,
        pending_count=0,
        pending_capped=False,
        recent_deliveries=0,
        recent_retries=0,
        recent_conflicts=0,
        lag_warn_seconds=300.0,
    )
    assert empty == ()

    # Sustained lag + depth + no progress / errors → warning.
    stuck = evaluate_bridge_alerts(
        oldest_pending_lag_seconds=900.0,
        pending_count=5,
        pending_capped=False,
        recent_deliveries=0,
        recent_retries=3,
        recent_conflicts=0,
        lag_warn_seconds=300.0,
    )
    assert any(a.kind == BridgeAlertKind.BRIDGE_LAG_SUSTAINED for a in stuck)

    tel = BridgeTelemetry(
        process_role="bridge",
        depth_cap=10,
        metric_sink=FakeMetricSink(),
        wall_clock=lambda: _utc("2026-09-19T12:20:00Z"),
        clock=lambda: 0.0,
        lag_warn_seconds=300.0,
    )
    store = ObservabilityStore(as_of=_utc("2026-09-19T12:20:00Z"))
    store.seed(_intent(created_at=_utc("2026-09-19T12:00:00Z")))
    for _ in range(3):
        tel.record_retryable_error(queue="orders", result="timeout")
    health = tel.refresh_from_store(store)
    alerts = tel.evaluate_alerts(health)
    assert any(a.kind == BridgeAlertKind.BRIDGE_LAG_SUSTAINED for a in alerts)


# ---------------------------------------------------------------------------
# Redaction / cardinality / W3C
# ---------------------------------------------------------------------------


def test_forbidden_keys_never_appear_as_metric_labels() -> None:
    from workhold_producer.bridge.observability import BridgeTelemetry

    metrics = FakeMetricSink()
    tel = BridgeTelemetry(
        process_role="bridge",
        depth_cap=10,
        metric_sink=metrics,
        wall_clock=lambda: _utc(),
        clock=lambda: 0.0,
    )
    # Attempt to inject forbidden labels through public record APIs and raw emit.
    tel.record_claimed(queue="orders", count=1)
    tel.record_delivered(queue="orders", result="new")
    tel.record_retryable_error(queue="orders", result="timeout")
    tel.record_permanent_conflict(queue="orders", result="idempotency_conflict")
    tel.record_malformed_intent(queue="orders", result="unsupported_schema_version")
    tel.record_lease_loss(queue="orders")
    tel.record_lease_reclaim(queue="orders")
    tel.record_shutdown()
    tel.refresh_from_store(ObservabilityStore())

    # Raw path must sanitize.
    tel.emit_counter(
        "bridge.probe",
        labels={
            "process_role": "bridge",
            "queue": "orders",
            "operation": "bridge.probe",
            "result": "ok",
            "source_row_id": "secret-row",
            "idempotency_key": "bridge:v1:abc",
            "task_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            "request_id": "22222222-2222-4222-8222-222222222222",
            "payload": '{"x":1}',
            "credentials": "Bearer xyz",
        },
    )

    for labels in _all_emitted_labels(metrics):
        assert set(labels.keys()) <= ALLOWED_LABEL_KEYS
        assert FORBIDDEN_LABEL_KEYS.isdisjoint(labels.keys())
        for key, value in labels.items():
            assert key in ALLOWED_LABEL_KEYS
            assert isinstance(value, str)
            assert "Bearer" not in value
            assert "bridge:v1:" not in value
            assert "secret-row" not in value


def test_logs_may_include_public_task_id_but_not_payload_or_source_ids() -> None:
    from workhold_producer.bridge.observability import BridgeTelemetry
    from workhold_producer.bridge.runner import BridgeRunner

    store = ObservabilityStore()
    store.seed(
        _intent(
            row_id="secret-source",
            namespace="secret.ns",
            payload={"ssn": "000-00-0000"},
        )
    )
    logs = FakeLogSink()
    metrics = FakeMetricSink()
    tel = BridgeTelemetry(
        process_role="bridge",
        depth_cap=10,
        metric_sink=metrics,
        log_sink=logs,
        wall_clock=lambda: _utc(),
        clock=lambda: 0.0,
    )
    runner = BridgeRunner.for_tests(
        store=store,
        producer=FakeProducer(
            [EnqueueResponse(task=_task(task_id="cccccccc-cccc-4ccc-8ccc-cccccccccccc"), replayed=False)]
        ),
        telemetry=tel,
        batch_size=1,
        max_in_flight=1,
        backoff_jitter_ratio=0.0,
    )
    runner.poll_once()

    blob = repr(logs.events) + repr(metrics.counters) + repr(metrics.gauges)
    assert "secret-source" not in blob
    assert "secret.ns" not in blob
    assert "000-00-0000" not in blob
    assert "ssn" not in blob
    # Public task id may appear in log correlation fields only.
    assert any(
        e["fields"].get("task_id") == "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
        for e in logs.events
    )
    for labels in _all_emitted_labels(metrics):
        assert "task_id" not in labels


def test_w3c_trace_context_preserved_without_payload_fields() -> None:
    from workhold_producer.bridge.observability import BridgeTelemetry

    tel = BridgeTelemetry(
        process_role="bridge",
        depth_cap=10,
        metric_sink=FakeMetricSink(),
        log_sink=FakeLogSink(),
        wall_clock=lambda: _utc(),
        clock=lambda: 0.0,
    )
    ctx = tel.project_trace_context(
        traceparent="00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        tracestate="rojo=00f067aa0ba902b7",
        payload={"secret": True},
        source_row_id="row-9",
        idempotency_key="bridge:v1:abc",
    )
    assert ctx["traceparent"].startswith("00-")
    assert ctx["tracestate"] == "rojo=00f067aa0ba902b7"
    assert "payload" not in ctx
    assert "source_row_id" not in ctx
    assert "idempotency_key" not in ctx


def test_health_snapshot_is_immutable() -> None:
    from workhold_producer.bridge.observability import BridgeTelemetry

    tel = BridgeTelemetry(
        process_role="bridge",
        depth_cap=10,
        metric_sink=FakeMetricSink(),
        wall_clock=lambda: _utc(),
        clock=lambda: 0.0,
    )
    health = tel.refresh_from_store(ObservabilityStore())
    with pytest.raises(Exception):
        health.pending_count = 99  # type: ignore[misc]


def test_telemetry_does_not_alter_correctness_path() -> None:
    from workhold_producer.bridge.observability import BridgeTelemetry
    from workhold_producer.bridge.runner import BridgeRunner

    store = ObservabilityStore()
    store.seed(_intent(row_id="ok"))
    class BoomSink(FakeMetricSink):
        def emit_counter(self, *args: Any, **kwargs: Any) -> None:
            raise RuntimeError("telemetry backend down")

    tel = BridgeTelemetry(
        process_role="bridge",
        depth_cap=10,
        metric_sink=BoomSink(),
        wall_clock=lambda: _utc(),
        clock=lambda: 0.0,
    )
    runner = BridgeRunner.for_tests(
        store=store,
        producer=FakeProducer([EnqueueResponse(task=_task(), replayed=False)]),
        telemetry=tel,
        batch_size=1,
        max_in_flight=1,
        backoff_jitter_ratio=0.0,
    )
    # Correctness path must still deliver despite telemetry failures.
    processed = runner.poll_once()
    assert processed == 1
    assert store.rows[("orders.checkout", "ok")].state == "delivered"
