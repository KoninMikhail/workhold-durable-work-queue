"""BridgeRunner outcome classification, replay, backoff, and shutdown (06-04 / BRDG-02)."""

from __future__ import annotations

import json
import logging
import threading
from collections.abc import Callable, Mapping, Sequence
from http.server import BaseHTTPRequestHandler, HTTPServer
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest

from queue_service_producer.bridge.idempotency import bridge_idempotency_key
from queue_service_producer.bridge.store import (
    AppStoreHealthSnapshot,
    BoundedPendingDepth,
    OldestPendingSnapshot,
    OutboxIntent,
)
from _queue_service_client_core.errors import (
    AuthenticationError,
    MalformedResponseError,
    ProtocolError,
    TimeoutError as ClientTimeoutError,
    TransportError,
)
from _queue_service_client_core.models import (
    EnqueueResponse,
    ErrorCode,
    ProtocolErrorBody,
    Task,
    TaskState,
)
from queue_service_producer.client import ProducerClient, _AVAILABLE_AT_OMITTED
from _queue_service_client_core.transport import HttpJsonTransport


# ---------------------------------------------------------------------------
# Recording HTTP server (wire-body assertions)
# ---------------------------------------------------------------------------


class _RecordingHandler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return

    def _dispatch(self) -> None:
        length = int(self.headers.get("Content-Length", "0") or "0")
        body = self.rfile.read(length) if length else b""
        self.server.recorded.append(  # type: ignore[attr-defined]
            {"method": self.command, "path": self.path.split("?", 1)[0], "body": body}
        )
        key = (self.command, self.path.split("?", 1)[0])
        responder = self.server.routes.get(key)  # type: ignore[attr-defined]
        if responder is None:
            self.send_response(404)
            self.end_headers()
            return
        status, payload = responder(body, {})
        raw = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_POST(self) -> None:  # noqa: N802
        self._dispatch()


@pytest.fixture
def recording_server() -> Any:
    server = HTTPServer(("127.0.0.1", 0), _RecordingHandler)
    server.recorded = []  # type: ignore[attr-defined]
    server.routes = {}  # type: ignore[attr-defined]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address[:2]
    base_url = f"http://{host}:{port}"
    try:
        yield server, base_url
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


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
    priority: int = 0,
    available_at: str | None = None,
    token: str = "lease-token-1",
    generation: int = 1,
    enqueue_request: Mapping[str, Any] | None = None,
) -> OutboxIntent:
    body: dict[str, Any]
    if enqueue_request is not None:
        body = dict(enqueue_request)
    else:
        body = {"payload": dict(payload or {"order_id": 42}), "priority": priority}
        if available_at is not None:
            body["available_at"] = available_at
    return OutboxIntent(
        source_namespace=namespace,
        source_row_id=row_id,
        schema_version=schema_version,
        target_queue=queue,
        enqueue_request=body,
        created_at=_utc(),
        ownership_token=token,
        generation=generation,
        lease_expires_at=_utc() + timedelta(seconds=30),
    )


def _protocol_error(
    *,
    code: str,
    retryable: bool,
    status_code: int = 409,
    message: str = "conflict",
) -> ProtocolError:
    body = ProtocolErrorBody(
        code=ErrorCode.parse(code),
        message=message,
        retryable=retryable,
        request_id="22222222-2222-4222-8222-222222222222",
        details={},
        retry_after_ms=None,
    )
    return ProtocolError(status_code=status_code, body=body)


@dataclass
class _Row:
    intent: OutboxIntent
    state: str = "leased"
    available_at_delay: float | None = None
    failure_code: str | None = None
    queue_task_id: str | None = None
    terminal_reason: str | None = None


@dataclass
class FakeStore:
    """In-memory OutboxStore that never holds a transaction across enqueue."""

    rows: dict[tuple[str, str], _Row] = field(default_factory=dict)
    claim_batches: list[list[OutboxIntent]] = field(default_factory=list)
    transaction_open: bool = False
    claim_calls: int = 0
    mark_delivered_calls: list[dict[str, Any]] = field(default_factory=list)
    schedule_retry_calls: list[dict[str, Any]] = field(default_factory=list)
    terminal_calls: list[dict[str, Any]] = field(default_factory=list)
    stale_tokens: set[str] = field(default_factory=set)
    claim_limit_seen: list[int] = field(default_factory=list)

    def seed(self, intent: OutboxIntent, *, state: str = "pending") -> None:
        self.rows[(intent.source_namespace, intent.source_row_id)] = _Row(
            intent=intent, state=state
        )

    def claim(self, *, limit: int, lease_seconds: int) -> Sequence[OutboxIntent]:
        self.claim_calls += 1
        self.claim_limit_seen.append(limit)
        self.transaction_open = True
        try:
            claimed: list[OutboxIntent] = []
            for key, row in list(self.rows.items()):
                if len(claimed) >= limit:
                    break
                if row.state in {"pending", "retryable_failure"} or (
                    row.state == "leased" and row.intent.ownership_token in self.stale_tokens
                ):
                    token = f"tok-{self.claim_calls}-{len(claimed)}"
                    gen = row.intent.generation + 1 if row.state != "pending" else max(1, row.intent.generation)
                    if row.state == "pending":
                        gen = max(1, row.intent.generation)
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
        finally:
            self.transaction_open = False

    def mark_delivered(
        self,
        *,
        source_namespace: str,
        source_row_id: str,
        ownership_token: str,
        queue_task_id: str | None = None,
    ) -> bool:
        self.transaction_open = True
        try:
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
            row.queue_task_id = queue_task_id
            return True
        finally:
            self.transaction_open = False

    def schedule_retry(
        self,
        *,
        source_namespace: str,
        source_row_id: str,
        ownership_token: str,
        available_at_delay_seconds: float,
        failure_code: str | None = None,
    ) -> bool:
        self.transaction_open = True
        try:
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
            if row is None:
                return False
            if ownership_token in self.stale_tokens:
                return False
            if row.intent.ownership_token != ownership_token or row.state != "leased":
                return False
            row.state = "retryable_failure"
            row.available_at_delay = available_at_delay_seconds
            row.failure_code = failure_code
            return True
        finally:
            self.transaction_open = False

    def mark_terminal_operator_action(
        self,
        *,
        source_namespace: str,
        source_row_id: str,
        ownership_token: str,
        reason: str,
    ) -> bool:
        self.transaction_open = True
        try:
            self.terminal_calls.append(
                {
                    "source_namespace": source_namespace,
                    "source_row_id": source_row_id,
                    "ownership_token": ownership_token,
                    "reason": reason,
                }
            )
            row = self.rows.get((source_namespace, source_row_id))
            if row is None:
                return False
            if ownership_token in self.stale_tokens:
                return False
            if row.intent.ownership_token != ownership_token or row.state != "leased":
                return False
            row.state = "terminal_operator_action"
            row.terminal_reason = reason
            return True
        finally:
            self.transaction_open = False

    def get_pending_depth(self, depth_cap: int) -> BoundedPendingDepth:
        pending = sum(1 for r in self.rows.values() if r.state != "delivered")
        capped = pending > depth_cap
        return BoundedPendingDepth(
            count=min(pending, depth_cap),
            depth_cap=depth_cap,
            capped=capped,
            as_of=_utc(),
        )

    def get_oldest_pending_created_at(self) -> OldestPendingSnapshot:
        pending = [r.intent.created_at for r in self.rows.values() if r.state != "delivered"]
        return OldestPendingSnapshot(
            created_at=min(pending) if pending else None,
            as_of=_utc(),
        )

    def get_health_snapshot(self, depth_cap: int) -> AppStoreHealthSnapshot:
        depth = self.get_pending_depth(depth_cap)
        oldest = self.get_oldest_pending_created_at()
        return AppStoreHealthSnapshot(
            as_of=_utc(),
            connected=True,
            query_ok=True,
            pending_count=depth.count,
            pending_capped=depth.capped,
            oldest_pending_created_at=oldest.created_at,
        )


class FakeProducer:
    """Records raw enqueue calls; scripted outcomes via ``outcomes`` queue."""

    def __init__(
        self,
        outcomes: list[EnqueueResponse | BaseException] | None = None,
        *,
        store: FakeStore | None = None,
    ) -> None:
        self.outcomes = list(outcomes or [])
        self.calls: list[dict[str, Any]] = []
        self.store = store
        self.in_flight = 0
        self.max_observed_in_flight = 0

    def _enqueue_with_available_at_raw(
        self,
        queue_name: str,
        *,
        idempotency_key: str,
        payload: Any,
        priority: int = 0,
        available_at: str | None | object = _AVAILABLE_AT_OMITTED,
    ) -> EnqueueResponse:
        if self.store is not None and self.store.transaction_open:
            raise AssertionError("app-DB transaction must be closed before Queue enqueue")
        self.in_flight += 1
        self.max_observed_in_flight = max(self.max_observed_in_flight, self.in_flight)
        try:
            call = {
                "queue_name": queue_name,
                "idempotency_key": idempotency_key,
                "payload": payload,
                "priority": priority,
                "available_at": available_at,
            }
            self.calls.append(call)
            if not self.outcomes:
                raise AssertionError("FakeProducer exhausted outcomes")
            outcome = self.outcomes.pop(0)
            if isinstance(outcome, BaseException):
                raise outcome
            return outcome
        finally:
            self.in_flight -= 1


@dataclass
class FakeClock:
    now: float = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class RecordingSleep:
    def __init__(self, clock: FakeClock) -> None:
        self.clock = clock
        self.delays: list[float] = []

    def __call__(self, seconds: float) -> None:
        self.delays.append(seconds)
        self.clock.advance(seconds)


# ---------------------------------------------------------------------------
# Helpers that import the module under test (fail RED until implemented)
# ---------------------------------------------------------------------------


def _runner_cls():
    from queue_service_producer.bridge.runner import BridgeRunner

    return BridgeRunner


def _make_runner(
    store: FakeStore,
    producer: FakeProducer,
    *,
    batch_size: int = 8,
    lease_seconds: int = 30,
    max_in_flight: int = 1,
    idle_poll_seconds: float = 0.0,
    initial_backoff_seconds: float = 1.0,
    max_backoff_seconds: float = 60.0,
    backoff_jitter_ratio: float = 0.0,
    clock: Callable[[], float] | None = None,
    sleep: Callable[[float], None] | None = None,
    jitter: Callable[[float, float], float] | None = None,
    logger: logging.Logger | None = None,
):
    BridgeRunner = _runner_cls()
    return BridgeRunner.for_tests(
        store=store,
        producer=producer,
        batch_size=batch_size,
        lease_seconds=lease_seconds,
        max_in_flight=max_in_flight,
        idle_poll_seconds=idle_poll_seconds,
        initial_backoff_seconds=initial_backoff_seconds,
        max_backoff_seconds=max_backoff_seconds,
        backoff_jitter_ratio=backoff_jitter_ratio,
        clock=clock,
        sleep=sleep,
        jitter=jitter,
        logger=logger,
    )


# ---------------------------------------------------------------------------
# Capability discovery fail-closed defaults
# ---------------------------------------------------------------------------


def test_default_without_live_fetcher_blocks_poll() -> None:
    store = FakeStore()
    store.seed(_intent())
    producer = FakeProducer(
        [EnqueueResponse(task=_task(task_id="task-new"), replayed=False)],
        store=store,
    )
    BridgeRunner = _runner_cls()
    runner = BridgeRunner(store=store, producer=producer)

    assert runner.poll_once() == 0
    assert store.claim_calls == 0
    assert producer.calls == []


def test_for_tests_static_snapshot_allows_poll() -> None:
    from queue_service_producer.bridge.compatibility import SUPPORTED_CAPABILITIES

    store = FakeStore()
    store.seed(_intent())
    producer = FakeProducer(
        [EnqueueResponse(task=_task(task_id="task-new"), replayed=False)],
        store=store,
    )
    BridgeRunner = _runner_cls()
    incompatible = dict(SUPPORTED_CAPABILITIES)
    incompatible["enqueue_dedup_ttl_seconds"] = 0
    incompatible["durable_idempotent_enqueue"] = False
    runner = BridgeRunner.for_tests(
        store=store,
        producer=producer,
        capabilities=incompatible,
    )

    assert runner.poll_once() == 0
    assert store.claim_calls == 0


# ---------------------------------------------------------------------------
# Outcome classification
# ---------------------------------------------------------------------------


def test_new_enqueue_marks_delivered_with_public_task_id() -> None:
    store = FakeStore()
    intent = _intent()
    store.seed(intent)
    producer = FakeProducer(
        [EnqueueResponse(task=_task(task_id="task-new"), replayed=False)],
        store=store,
    )
    runner = _make_runner(store, producer)

    processed = runner.poll_once()

    assert processed == 1
    row = store.rows[("orders.checkout", "row-1")]
    assert row.state == "delivered"
    assert row.queue_task_id == "task-new"
    expected_key = bridge_idempotency_key("orders.checkout", "row-1")
    assert producer.calls[0]["idempotency_key"] == expected_key
    assert producer.calls[0]["payload"] == {"order_id": 42}
    assert producer.calls[0]["priority"] == 0
    assert producer.calls[0]["queue_name"] == "orders"


def test_matching_replay_marks_delivered_same_task_identity() -> None:
    store = FakeStore()
    store.seed(_intent())
    producer = FakeProducer(
        [EnqueueResponse(task=_task(task_id="task-orig"), replayed=True)],
        store=store,
    )
    runner = _make_runner(store, producer)

    assert runner.poll_once() == 1
    row = store.rows[("orders.checkout", "row-1")]
    assert row.state == "delivered"
    assert row.queue_task_id == "task-orig"
    assert producer.calls[0]["idempotency_key"] == bridge_idempotency_key(
        "orders.checkout", "row-1"
    )


def test_uncertain_response_replays_same_key_100_times() -> None:
    """Exactly 100 timeout/uncertain cycles must keep the same Idempotency-Key."""
    store = FakeStore()
    store.seed(_intent())
    expected_key = bridge_idempotency_key("orders.checkout", "row-1")
    outcomes: list[EnqueueResponse | BaseException] = [
        ClientTimeoutError(timeout_s=1.0) for _ in range(100)
    ]
    outcomes.append(EnqueueResponse(task=_task(task_id="task-final"), replayed=True))
    producer = FakeProducer(outcomes, store=store)
    clock = FakeClock()
    sleep = RecordingSleep(clock)
    runner = _make_runner(
        store,
        producer,
        initial_backoff_seconds=0.01,
        max_backoff_seconds=0.01,
        backoff_jitter_ratio=0.0,
        clock=clock,
        sleep=sleep,
        jitter=lambda a, b: a,
    )

    for i in range(100):
        processed = runner.poll_once()
        assert processed == 1, f"cycle {i}"
        row = store.rows[("orders.checkout", "row-1")]
        assert row.state == "retryable_failure", f"cycle {i}"
        # Make reclaimable for next poll without waiting real time.
        row.state = "pending"

    assert runner.poll_once() == 1
    assert len(producer.calls) == 101
    assert all(c["idempotency_key"] == expected_key for c in producer.calls)
    assert store.rows[("orders.checkout", "row-1")].state == "delivered"
    assert store.rows[("orders.checkout", "row-1")].queue_task_id == "task-final"


def test_transport_error_schedules_retry() -> None:
    store = FakeStore()
    store.seed(_intent())
    producer = FakeProducer([TransportError(reason="connection reset")], store=store)
    runner = _make_runner(store, producer, backoff_jitter_ratio=0.0)

    assert runner.poll_once() == 1
    assert store.rows[("orders.checkout", "row-1")].state == "retryable_failure"
    assert len(store.schedule_retry_calls) == 1
    assert store.schedule_retry_calls[0]["failure_code"] in {
        "transport_error",
        "uncertain",
        "retryable",
    }


def test_retryable_protocol_error_schedules_retry() -> None:
    store = FakeStore()
    store.seed(_intent())
    producer = FakeProducer(
        [_protocol_error(code="resource_exhausted", retryable=True, status_code=429)],
        store=store,
    )
    runner = _make_runner(store, producer, backoff_jitter_ratio=0.0)

    assert runner.poll_once() == 1
    assert store.rows[("orders.checkout", "row-1")].state == "retryable_failure"
    assert store.schedule_retry_calls[0]["failure_code"] == "resource_exhausted"


def test_idempotency_conflict_is_terminal_never_rewritten() -> None:
    store = FakeStore()
    original = _intent(payload={"order_id": 1})
    store.seed(original)
    producer = FakeProducer(
        [_protocol_error(code="idempotency_conflict", retryable=False, status_code=409)],
        store=store,
    )
    runner = _make_runner(store, producer)

    assert runner.poll_once() == 1
    row = store.rows[("orders.checkout", "row-1")]
    assert row.state == "terminal_operator_action"
    assert row.terminal_reason == "idempotency_conflict"
    assert len(store.schedule_retry_calls) == 0
    # Intent body unchanged — never rewritten into a second task request.
    assert dict(row.intent.enqueue_request) == dict(original.enqueue_request)
    assert len(producer.calls) == 1


def test_unsupported_schema_major_is_terminal_without_enqueue() -> None:
    store = FakeStore()
    store.seed(_intent(schema_version=99))
    producer = FakeProducer(store=store)
    runner = _make_runner(store, producer)

    assert runner.poll_once() == 1
    assert store.rows[("orders.checkout", "row-1")].state == "terminal_operator_action"
    assert store.terminal_calls[0]["reason"] == "unsupported_schema_version"
    assert producer.calls == []


def test_malformed_immutable_intent_is_terminal_without_enqueue() -> None:
    store = FakeStore()
    store.seed(_intent(enqueue_request={"priority": 0}))  # missing payload
    producer = FakeProducer(store=store)
    runner = _make_runner(store, producer)

    assert runner.poll_once() == 1
    assert store.rows[("orders.checkout", "row-1")].state == "terminal_operator_action"
    assert store.terminal_calls[0]["reason"] == "malformed_enqueue_request"
    assert producer.calls == []


def test_forbidden_queue_is_terminal() -> None:
    store = FakeStore()
    store.seed(_intent())
    producer = FakeProducer(
        [_protocol_error(code="permission_denied", retryable=False, status_code=403)],
        store=store,
    )
    runner = _make_runner(store, producer)

    assert runner.poll_once() == 1
    assert store.rows[("orders.checkout", "row-1")].state == "terminal_operator_action"
    assert store.terminal_calls[0]["reason"] == "permission_denied"


def test_queue_not_found_is_terminal() -> None:
    store = FakeStore()
    store.seed(_intent())
    producer = FakeProducer(
        [_protocol_error(code="queue_not_found", retryable=False, status_code=404)],
        store=store,
    )
    runner = _make_runner(store, producer)

    assert runner.poll_once() == 1
    assert store.rows[("orders.checkout", "row-1")].state == "terminal_operator_action"
    assert store.terminal_calls[0]["reason"] == "queue_not_found"


def test_authentication_error_is_terminal() -> None:
    store = FakeStore()
    store.seed(_intent())
    body = ProtocolErrorBody(
        code=ErrorCode.parse("unauthenticated"),
        message="bad token",
        retryable=False,
        request_id="22222222-2222-4222-8222-222222222222",
        details={},
        retry_after_ms=None,
    )
    producer = FakeProducer(
        [AuthenticationError(status_code=401, body=body)],
        store=store,
    )
    runner = _make_runner(store, producer)

    assert runner.poll_once() == 1
    assert store.rows[("orders.checkout", "row-1")].state == "terminal_operator_action"


def test_malformed_response_remains_replayable() -> None:
    store = FakeStore()
    store.seed(_intent())
    producer = FakeProducer(
        [MalformedResponseError(status_code=200, reason="empty success response body")],
        store=store,
    )
    runner = _make_runner(store, producer, backoff_jitter_ratio=0.0)

    assert runner.poll_once() == 1
    assert store.rows[("orders.checkout", "row-1")].state == "retryable_failure"


# ---------------------------------------------------------------------------
# Lease / transaction / backoff / bounds
# ---------------------------------------------------------------------------


def test_stale_app_row_lease_does_not_mark_delivered() -> None:
    store = FakeStore()
    intent = _intent(token="stale-token")
    store.seed(intent, state="leased")
    # Force claim to return this leased row once, then mark token stale before ack.
    store.rows[("orders.checkout", "row-1")].state = "pending"
    producer = FakeProducer(
        [EnqueueResponse(task=_task(task_id="task-x"), replayed=False)],
        store=store,
    )
    runner = _make_runner(store, producer)

    # After claim, invalidate the lease token before mark_delivered.
    original_mark = store.mark_delivered

    def mark_then_stale(**kwargs: Any) -> bool:
        store.stale_tokens.add(kwargs["ownership_token"])
        return original_mark(**kwargs)

    store.mark_delivered = mark_then_stale  # type: ignore[method-assign]

    assert runner.poll_once() == 1
    # mark_delivered was attempted but returned False — row must not be delivered.
    assert store.mark_delivered_calls
    # FakeStore with stale token returns False; state stays leased from claim.
    row = store.rows[("orders.checkout", "row-1")]
    assert row.state != "delivered"


def test_app_transaction_closed_before_each_enqueue() -> None:
    store = FakeStore()
    store.seed(_intent(row_id="a"))
    store.seed(_intent(row_id="b", namespace="orders.checkout"))
    producer = FakeProducer(
        [
            EnqueueResponse(task=_task(task_id="t1"), replayed=False),
            EnqueueResponse(task=_task(task_id="t2"), replayed=False),
        ],
        store=store,
    )
    runner = _make_runner(store, producer, batch_size=2)

    assert runner.poll_once() == 2
    assert len(producer.calls) == 2
    assert all(r.state == "delivered" for r in store.rows.values())


def test_bounded_exponential_backoff_with_jitter() -> None:
    store = FakeStore()
    # generation=4 → attempt index 3 → base * 2^3 = 8, capped at max=10, jitter mid.
    store.seed(_intent(generation=4))
    producer = FakeProducer([ClientTimeoutError(timeout_s=0.5)], store=store)
    delays: list[float] = []

    def capture_jitter(low: float, high: float) -> float:
        delays.append((low, high))  # type: ignore[arg-type]
        return (low + high) / 2

    runner = _make_runner(
        store,
        producer,
        initial_backoff_seconds=1.0,
        max_backoff_seconds=10.0,
        backoff_jitter_ratio=0.1,
        jitter=capture_jitter,
    )

    assert runner.poll_once() == 1
    delay = store.schedule_retry_calls[0]["available_at_delay_seconds"]
    # generation 4 → exponent max(0, generation-1)=3 → 1*8=8, jitter ±10% → [7.2, 8.8]
    assert 7.2 <= delay <= 8.8
    assert delays  # jitter consulted


def test_batch_size_limit() -> None:
    store = FakeStore()
    for i in range(5):
        store.seed(_intent(row_id=f"row-{i}"))
    producer = FakeProducer(
        [EnqueueResponse(task=_task(task_id=f"t-{i}"), replayed=False) for i in range(2)],
        store=store,
    )
    runner = _make_runner(store, producer, batch_size=2)

    assert runner.poll_once() == 2
    assert store.claim_limit_seen == [2]
    assert len(producer.calls) == 2
    pending = sum(1 for r in store.rows.values() if r.state != "delivered")
    assert pending == 3


def test_max_in_flight_limit() -> None:
    store = FakeStore()
    for i in range(4):
        store.seed(_intent(row_id=f"p-{i}"))

    barrier_hits: list[int] = []

    class SlowProducer(FakeProducer):
        def _enqueue_with_available_at_raw(self, *args: Any, **kwargs: Any) -> EnqueueResponse:
            self.in_flight += 1
            barrier_hits.append(self.in_flight)
            self.max_observed_in_flight = max(self.max_observed_in_flight, self.in_flight)
            try:
                if self.store is not None and self.store.transaction_open:
                    raise AssertionError("transaction open during enqueue")
                call = {
                    "queue_name": args[0] if args else kwargs.get("queue_name"),
                    "idempotency_key": kwargs["idempotency_key"],
                    "payload": kwargs["payload"],
                    "priority": kwargs.get("priority", 0),
                    "available_at": kwargs.get("available_at", _AVAILABLE_AT_OMITTED),
                }
                self.calls.append(call)
                return EnqueueResponse(task=_task(task_id=f"t-{len(self.calls)}"), replayed=False)
            finally:
                self.in_flight -= 1

    producer = SlowProducer(store=store)
    runner = _make_runner(store, producer, batch_size=4, max_in_flight=2)

    assert runner.poll_once() == 4
    assert producer.max_observed_in_flight <= 2
    assert max(barrier_hits) <= 2


def test_shutdown_stops_new_claims() -> None:
    store = FakeStore()
    store.seed(_intent())
    producer = FakeProducer(store=store)
    clock = FakeClock()
    sleep = RecordingSleep(clock)
    runner = _make_runner(store, producer, idle_poll_seconds=0.01, clock=clock, sleep=sleep)

    runner.request_shutdown()
    runner.run()

    assert store.claim_calls == 0
    assert producer.calls == []


def test_shutdown_releases_unfinished_batch() -> None:
    """Shutdown finishes the in-flight item, does not claim further, leaves others reclaimable."""
    store = FakeStore()
    for i in range(3):
        store.seed(_intent(row_id=f"s-{i}"))

    claim_count = {"n": 0}

    class ControlledStore(FakeStore):
        def claim(self, *, limit: int, lease_seconds: int) -> Sequence[OutboxIntent]:
            claim_count["n"] += 1
            if claim_count["n"] > 1:
                raise AssertionError("must not claim after shutdown begins mid-batch")
            return super().claim(limit=limit, lease_seconds=lease_seconds)

    cstore = ControlledStore()
    for i in range(3):
        cstore.seed(_intent(row_id=f"s-{i}"))

    enqueues = {"n": 0}

    class ShutdownProducer(FakeProducer):
        def _enqueue_with_available_at_raw(self, *args: Any, **kwargs: Any) -> EnqueueResponse:
            enqueues["n"] += 1
            if enqueues["n"] == 1:
                # Signal shutdown after first enqueue starts (batch already claimed).
                runner.request_shutdown()
            return EnqueueResponse(
                task=_task(task_id=f"done-{enqueues['n']}"),
                replayed=False,
            )

    producer = ShutdownProducer(store=cstore)
    runner = _make_runner(cstore, producer, batch_size=3, max_in_flight=1)

    runner.run()

    # First claim happened; no second claim.
    assert claim_count["n"] == 1
    # At least the first intent completed; remaining leased rows are left for reclaim
    # (not forced to delivered / not rewritten).
    delivered = [r for r in cstore.rows.values() if r.state == "delivered"]
    unfinished = [r for r in cstore.rows.values() if r.state == "leased"]
    assert len(delivered) >= 1
    assert len(delivered) + len(unfinished) == 3
    # Unfinished leases were abandoned (not marked terminal or delivered).
    assert all(r.queue_task_id is None for r in unfinished)


def test_cancellation_cooperates_with_shutdown() -> None:
    store = FakeStore()
    store.seed(_intent())
    producer = FakeProducer(
        [EnqueueResponse(task=_task(task_id="t"), replayed=False)],
        store=store,
    )
    runner = _make_runner(store, producer)
    runner.request_shutdown()
    assert runner.poll_once() == 0
    assert store.claim_calls == 0


def test_exact_stored_enqueue_request_passed_through() -> None:
    store = FakeStore()
    store.seed(
        _intent(
            enqueue_request={
                "payload": {"nested": {"x": 1}, "secret_should_not_log": "x"},
                "priority": 0,
                "available_at": "2026-09-20T00:00:00Z",
            }
        )
    )
    producer = FakeProducer(
        [EnqueueResponse(task=_task(task_id="t"), replayed=False)],
        store=store,
    )
    runner = _make_runner(store, producer)
    assert runner.poll_once() == 1
    call = producer.calls[0]
    assert call["payload"] == {"nested": {"x": 1}, "secret_should_not_log": "x"}
    assert call["priority"] == 0
    assert call["available_at"] == "2026-09-20T00:00:00Z"


def test_logs_omit_payload_credentials_source_id_and_idempotency_key(
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = FakeStore()
    store.seed(
        _intent(
            namespace="secret-namespace",
            row_id="secret-row-id",
            payload={"credit_card": "4111"},
        )
    )
    producer = FakeProducer(
        [EnqueueResponse(task=_task(task_id="public-task"), replayed=False)],
        store=store,
    )
    logger = logging.getLogger("queue_service_producer.bridge.runner.test")
    runner = _make_runner(store, producer, logger=logger)

    with caplog.at_level(logging.DEBUG, logger=logger.name):
        runner.poll_once()

    blob = " ".join(r.getMessage() for r in caplog.records).lower()
    assert "secret-namespace" not in blob
    assert "secret-row-id" not in blob
    assert "4111" not in blob
    assert "credit_card" not in blob
    key = bridge_idempotency_key("secret-namespace", "secret-row-id")
    assert key.lower() not in blob
    assert "bearer" not in blob


def test_bridge_runner_exported_from_bridge_package() -> None:
    from queue_service_producer.bridge import BridgeRunner

    assert BridgeRunner is _runner_cls()


def test_bridge_omits_available_at_when_key_absent() -> None:
    store = FakeStore()
    store.seed(_intent(enqueue_request={"payload": {"x": 1}, "priority": 0}))
    producer = FakeProducer(
        [EnqueueResponse(task=_task(task_id="t"), replayed=False)],
        store=store,
    )
    runner = _make_runner(store, producer)
    assert runner.poll_once() == 1
    assert producer.calls[0]["available_at"] is _AVAILABLE_AT_OMITTED


def test_bridge_explicit_null_passes_none_not_sentinel() -> None:
    store = FakeStore()
    store.seed(
        _intent(
            enqueue_request={
                "payload": {"x": 1},
                "priority": 0,
                "available_at": None,
            }
        )
    )
    producer = FakeProducer(
        [EnqueueResponse(task=_task(task_id="t"), replayed=False)],
        store=store,
    )
    runner = _make_runner(store, producer)
    assert runner.poll_once() == 1
    assert producer.calls[0]["available_at"] is None


def test_bridge_explicit_null_emits_json_null_on_wire(recording_server: Any) -> None:
    server, base_url = recording_server
    server.routes[("POST", "/v1/queues/orders/tasks")] = (
        lambda body, headers: (
            201,
            {
                "task": {
                    "task_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                    "queue_name": "orders",
                    "producer_id": "p",
                    "state": "ready",
                    "priority": 0,
                    "available_at": "2026-09-19T00:00:00Z",
                    "retry_policy_version": 1,
                    "created_at": "2026-09-19T00:00:00Z",
                    "spawned_task_ids": [],
                    "delivery_event_ids": [],
                },
                "replayed": False,
            },
        )
    )
    producer = ProducerClient(
        HttpJsonTransport(base_url, timeout_s=2.0),
        bearer_token="secret-token-value",
    )
    store = FakeStore()
    store.seed(
        _intent(
            queue="orders",
            enqueue_request={
                "payload": {"x": 1},
                "priority": 0,
                "available_at": None,
            },
        )
    )
    runner = _make_runner(store, producer)
    assert runner.poll_once() == 1
    wire = json.loads(server.recorded[0]["body"].decode("utf-8"))
    assert wire == {"payload": {"x": 1}, "priority": 0, "available_at": None}


# ---------------------------------------------------------------------------
# Phase 12 Wave 0 scaffolds (WORK-16 bounded priority) — Plan 08 removes skips
# ---------------------------------------------------------------------------

PRIORITY_MIN = -32768
PRIORITY_MAX = 32767


def test_zero_priority_major_1_intent_delivers_while_capability_false() -> None:
    """Rolling compatibility: existing zero-priority intents remain valid."""
    store = FakeStore()
    store.seed(_intent(priority=0))
    producer = FakeProducer(
        [EnqueueResponse(task=_task(task_id="t-zero"), replayed=False)],
        store=store,
    )
    runner = _make_runner(store, producer)
    assert runner.poll_once() == 1
    assert producer.calls[0]["priority"] == 0
    assert store.rows[("orders.checkout", "row-1")].state == "delivered"


def test_non_zero_priority_intent_delivers_with_exact_pass_through() -> None:
    """Bounded non-zero immutable intents reach Queue without coercion."""
    store = FakeStore()
    store.seed(_intent(priority=100, enqueue_request={"payload": {"x": 1}, "priority": 100}))
    producer = FakeProducer(
        [EnqueueResponse(task=_task(task_id="t-nonzero"), replayed=False)],
        store=store,
    )
    runner = _make_runner(store, producer)
    assert runner.poll_once() == 1
    assert producer.calls[0]["priority"] == 100
    assert type(producer.calls[0]["priority"]) is int
    assert store.rows[("orders.checkout", "row-1")].state == "delivered"


@pytest.mark.parametrize("priority", [PRIORITY_MIN, PRIORITY_MAX, 42, -100])
def test_bridge_passes_exact_accepted_priority_to_raw_enqueue(priority: int) -> None:
    store = FakeStore()
    store.seed(
        _intent(
            enqueue_request={"payload": {"order_id": 42}, "priority": priority},
        )
    )
    producer = FakeProducer(
        [EnqueueResponse(task=_task(task_id="t-priority"), replayed=False)],
        store=store,
    )
    runner = _make_runner(store, producer)
    assert runner.poll_once() == 1
    assert producer.calls[0]["priority"] == priority
    assert type(producer.calls[0]["priority"]) is int


@pytest.mark.parametrize(
    "priority",
    [
        PRIORITY_MIN - 1,
        PRIORITY_MAX + 1,
        True,
        False,
        "100",
        1.5,
        None,
    ],
)
def test_out_of_range_or_non_integer_intent_is_malformed_without_rewrite(
    priority: object,
) -> None:
    store = FakeStore()
    store.seed(
        _intent(
            enqueue_request={"payload": {"order_id": 42}, "priority": priority},
        )
    )
    producer = FakeProducer([], store=store)
    runner = _make_runner(store, producer)
    assert runner.poll_once() == 1
    assert producer.calls == []
    row = store.rows[("orders.checkout", "row-1")]
    assert row.state == "terminal_operator_action"
    assert store.terminal_calls[0]["reason"] == "malformed_enqueue_request"
    assert row.intent.enqueue_request["priority"] == priority


def test_bridge_never_coerces_string_priority_with_int() -> None:
    store = FakeStore()
    store.seed(
        _intent(
            enqueue_request={"payload": {"x": 1}, "priority": "32767"},
        )
    )
    producer = FakeProducer([], store=store)
    runner = _make_runner(store, producer)
    assert runner.poll_once() == 1
    assert producer.calls == []
    assert store.terminal_calls[0]["reason"] == "malformed_enqueue_request"
