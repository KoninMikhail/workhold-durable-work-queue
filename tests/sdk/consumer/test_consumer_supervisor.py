"""Deterministic ConsumerSupervisor tests (SDK-06).

Ports WorkerSupervisor behavioral fixtures onto ConsumerClient/Claim and adds
race coverage for terminal-vs-heartbeat, full-capacity shutdown, lease loss,
and grace expiry.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

import pytest

from _workhold_client_core.errors import LeaseLostError, ProtocolError
from _workhold_client_core.models import (
    ClaimSummary,
    ErrorCode,
    HeartbeatResult,
    ProtocolErrorBody,
    Task,
    TaskState,
)
from _workhold_client_core.transport import TransportResponse
from workhold_consumer.client import Claim, ConsumerClient
from workhold_consumer.supervisor import (
    HEARTBEAT_JITTER_RATIO,
    CancellationToken,
    ConsumerSupervisor,
    HandlerErrorEvent,
    LeaseLostEvent,
)


def _task(*, task_id: str = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa") -> Task:
    return Task(
        task_id=task_id,
        queue_name="orders",
        producer_id="producer-1",
        state=TaskState.parse("leased"),
        priority=0,
        available_at="2026-09-19T00:00:00Z",
        retry_policy_version=1,
        created_at="2026-09-19T00:00:00Z",
        spawned_task_ids=(),
        delivery_event_ids=(),
        payload={"order_id": 1},
    )


@dataclass
class FakeClaim:
    claim_id: str
    recommended_heartbeat_seconds: int = 10
    task: Task = field(default_factory=_task)
    generation: int = 1
    claimed_at: str = "2026-09-19T00:00:00Z"
    lease_expires_at: str = "2026-09-19T00:01:00Z"
    worker_id: str = "worker-1"
    cancel_requested: bool = False
    server_time: str = "2026-09-19T00:00:00Z"
    heartbeat_side_effect: Callable[[FakeClaim], HeartbeatResult | None] | None = None
    _lease_lost: bool = field(default=False, init=False, repr=False)
    _terminal: bool = field(default=False, init=False, repr=False)
    mutations: list[str] = field(default_factory=list, init=False, repr=False)
    heartbeat_count: int = field(default=0, init=False, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    @property
    def lease_lost(self) -> bool:
        return self._lease_lost

    @property
    def is_terminal(self) -> bool:
        return self._terminal

    def mark_lease_lost(self) -> None:
        self._lease_lost = True

    def heartbeat(self, *, lease_seconds: int) -> HeartbeatResult:
        with self._lock:
            if self._lease_lost:
                raise LeaseLostError(claim_id=self.claim_id)
            # Terminal claims must not advance heartbeat accounting (race window).
            if self._terminal:
                return HeartbeatResult(
                    claim=ClaimSummary(
                        claim_id=self.claim_id,
                        generation=self.generation,
                        claimed_at=self.claimed_at,
                        lease_expires_at=self.lease_expires_at,
                        worker_id=self.worker_id,
                        cancel_requested=self.cancel_requested,
                    ),
                    server_time=self.server_time,
                    recommended_heartbeat_seconds=self.recommended_heartbeat_seconds,
                )
            self.mutations.append(f"heartbeat:{lease_seconds}")
            self.heartbeat_count += 1
            if self.heartbeat_side_effect is not None:
                override = self.heartbeat_side_effect(self)
                if override is not None:
                    return override
            return HeartbeatResult(
                claim=ClaimSummary(
                    claim_id=self.claim_id,
                    generation=self.generation,
                    claimed_at=self.claimed_at,
                    lease_expires_at=self.lease_expires_at,
                    worker_id=self.worker_id,
                    cancel_requested=self.cancel_requested,
                ),
                server_time=self.server_time,
                recommended_heartbeat_seconds=self.recommended_heartbeat_seconds,
            )

    def complete(self, *, spawn: Sequence[Any] | None = None) -> None:
        with self._lock:
            if self._lease_lost:
                raise LeaseLostError(claim_id=self.claim_id)
            self.mutations.append("complete")
            self._terminal = True

    def fail(
        self,
        *,
        retryable: bool,
        failure_code: str,
        failure_detail: str | None = None,
    ) -> None:
        with self._lock:
            if self._lease_lost:
                raise LeaseLostError(claim_id=self.claim_id)
            self.mutations.append(f"fail:{failure_code}")
            self._terminal = True

    def ack_cancel(self) -> None:
        with self._lock:
            if self._lease_lost:
                raise LeaseLostError(claim_id=self.claim_id)
            self.mutations.append("ack_cancel")
            self._terminal = True


class FakeConsumerClient:
    """Behavioral stand-in for ConsumerClient.claim (historical FakeWorkerClient)."""

    def __init__(
        self,
        *,
        capabilities: Any | None = None,
        capabilities_error: BaseException | None = None,
    ) -> None:
        self._lock = threading.Lock()
        self._pending: list[list[FakeClaim]] = []
        self.claim_calls: list[dict[str, Any]] = []
        self.capabilities_calls = 0
        self._capabilities = capabilities
        self._capabilities_error = capabilities_error
        self._claim_block: threading.Event | None = None
        self._claim_release: threading.Event | None = None

    def enqueue_claims(self, batch: list[FakeClaim]) -> None:
        with self._lock:
            self._pending.append(batch)

    def set_claim_hold(
        self,
        *,
        entered: threading.Event,
        release: threading.Event,
    ) -> None:
        self._claim_block = entered
        self._claim_release = release

    def get_capabilities(self) -> Any:
        self.capabilities_calls += 1
        if self._capabilities_error is not None:
            raise self._capabilities_error
        if self._capabilities is None:
            raise AssertionError("get_capabilities called without fixture capabilities")
        return self._capabilities

    def claim(
        self,
        *,
        queues: Sequence[str],
        worker_id: str,
        lease_seconds: int,
        max_tasks: int = 1,
        wait_seconds: int = 0,
        capabilities: Any | None = None,
        cancellation: object | None = None,
    ) -> list[FakeClaim]:
        # Mirror ConsumerClient: positive wait re-fetches when capabilities omitted.
        if wait_seconds > 0 or max_tasks != 1:
            if capabilities is None:
                self.get_capabilities()
        if self._claim_block is not None:
            self._claim_block.set()
        if self._claim_release is not None:
            # Wait until cancelled or released (no sleep oracle).
            while not self._claim_release.is_set():
                if cancellation is not None:
                    is_set = getattr(cancellation, "is_set", None)
                    if callable(is_set) and is_set():
                        from _workhold_client_core.errors import RequestCancelledError

                        raise RequestCancelledError()
                time.sleep(0.01)
        with self._lock:
            self.claim_calls.append(
                {
                    "queues": list(queues),
                    "worker_id": worker_id,
                    "lease_seconds": lease_seconds,
                    "max_tasks": max_tasks,
                    "wait_seconds": wait_seconds,
                    "capabilities": capabilities,
                    "cancellation": cancellation,
                }
            )
            if not self._pending:
                return []
            return self._pending.pop(0)


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []
        self._cond = threading.Condition()

    def __call__(self) -> float:
        with self._cond:
            return self.now

    def sleep(self, seconds: float) -> None:
        with self._cond:
            self.sleeps.append(seconds)
            self.now += max(0.0, seconds)
            self._cond.notify_all()


def _lease_lost_protocol_error(claim_id: str) -> ProtocolError:
    body = ProtocolErrorBody(
        code=ErrorCode.parse("lease_lost"),
        message="fence lost",
        retryable=False,
        request_id="00000000-0000-4000-8000-000000000001",
        details={"claim_id": claim_id},
    )
    return ProtocolError(status_code=409, body=body)


def test_heartbeat_intervals_stay_within_documented_jitter_bounds() -> None:
    consumer = FakeConsumerClient()
    claim = FakeClaim(claim_id="c-hb", recommended_heartbeat_seconds=10)
    consumer.enqueue_claims([claim])

    clock = FakeClock()
    values = iter([9.0, 11.0, 10.0, 9.5])

    def jitter(lo: float, hi: float) -> float:
        assert lo == pytest.approx(10 * (1 - HEARTBEAT_JITTER_RATIO))
        assert hi == pytest.approx(10 * (1 + HEARTBEAT_JITTER_RATIO))
        try:
            return next(values)
        except StopIteration:
            return 10.0

    ready = threading.Event()

    def handler(c: FakeClaim, token: CancellationToken) -> None:
        while c.heartbeat_count < 3 and not token.is_cancelled():
            time.sleep(0.01)
        ready.set()
        c.complete()

    supervisor = ConsumerSupervisor(
        consumer,  # type: ignore[arg-type]
        queues=["orders"],
        worker_id="worker-1",
        lease_seconds=60,
        handler=handler,  # type: ignore[arg-type]
        wait_seconds=0,
        handler_capacity=1,
        shutdown_grace_seconds=2.0,
        clock=clock,
        sleep=clock.sleep,
        jitter=jitter,
        idle_poll_seconds=0.01,
    )

    thread = threading.Thread(target=supervisor.run, daemon=True)
    thread.start()
    assert ready.wait(timeout=3.0)
    supervisor.request_shutdown()
    thread.join(timeout=3.0)
    assert not thread.is_alive()

    lo = 10 * (1 - HEARTBEAT_JITTER_RATIO)
    hi = 10 * (1 + HEARTBEAT_JITTER_RATIO)
    hb_sleeps = [s for s in clock.sleeps if lo - 0.001 <= s <= hi + 0.001]
    assert len(hb_sleeps) >= 3
    for delay in hb_sleeps:
        assert lo <= delay <= hi


def test_active_handlers_never_exceed_configured_capacity() -> None:
    consumer = FakeConsumerClient()
    for i in range(5):
        consumer.enqueue_claims([FakeClaim(claim_id=f"c-{i}")])

    active = 0
    max_active = 0
    lock = threading.Lock()
    release = threading.Event()
    started = threading.Barrier(3)

    def handler(c: FakeClaim, token: CancellationToken) -> None:
        nonlocal active, max_active
        with lock:
            active += 1
            max_active = max(max_active, active)
        try:
            started.wait(timeout=2.0)
            assert release.wait(timeout=2.0)
            c.complete()
        finally:
            with lock:
                active -= 1

    supervisor = ConsumerSupervisor(
        consumer,  # type: ignore[arg-type]
        queues=["orders"],
        worker_id="worker-1",
        lease_seconds=60,
        handler=handler,  # type: ignore[arg-type]
        wait_seconds=0,
        handler_capacity=2,
        shutdown_grace_seconds=2.0,
        idle_poll_seconds=0.01,
        heartbeat_jitter_ratio=0.0,
    )

    thread = threading.Thread(target=supervisor.run, daemon=True)
    thread.start()
    started.wait(timeout=2.0)
    with lock:
        assert active == 2
        assert max_active == 2
    time.sleep(0.15)
    with lock:
        assert active == 2
        assert max_active == 2
    release.set()
    supervisor.request_shutdown()
    thread.join(timeout=3.0)
    assert not thread.is_alive()
    assert max_active == 2


def test_lease_loss_reaches_callback_and_blocks_later_mutations() -> None:
    consumer = FakeConsumerClient()

    def side_effect(claim: FakeClaim) -> HeartbeatResult | None:
        if claim.heartbeat_count >= 1:
            claim.mark_lease_lost()
            raise _lease_lost_protocol_error(claim.claim_id)
        return None

    claim = FakeClaim(
        claim_id="c-lost",
        recommended_heartbeat_seconds=1,
        heartbeat_side_effect=side_effect,
    )
    consumer.enqueue_claims([claim])

    lost_events: list[LeaseLostEvent] = []
    lost = threading.Event()
    entered = threading.Event()
    allow_finish = threading.Event()

    def on_lease_lost(event: LeaseLostEvent) -> None:
        lost_events.append(event)
        lost.set()

    def handler(c: FakeClaim, token: CancellationToken) -> None:
        entered.set()
        assert allow_finish.wait(timeout=3.0)
        with pytest.raises(LeaseLostError):
            c.complete()

    clock = FakeClock()
    supervisor = ConsumerSupervisor(
        consumer,  # type: ignore[arg-type]
        queues=["orders"],
        worker_id="worker-1",
        lease_seconds=60,
        handler=handler,  # type: ignore[arg-type]
        wait_seconds=0,
        handler_capacity=1,
        shutdown_grace_seconds=2.0,
        on_lease_lost=on_lease_lost,
        clock=clock,
        sleep=clock.sleep,
        jitter=lambda lo, hi: lo,
        idle_poll_seconds=0.01,
    )

    thread = threading.Thread(target=supervisor.run, daemon=True)
    thread.start()
    assert entered.wait(timeout=2.0)
    assert lost.wait(timeout=2.0)
    assert len(lost_events) == 1
    assert lost_events[0].claim_id == "c-lost"
    assert lost_events[0].task_id == claim.task.task_id
    assert "token" not in repr(lost_events[0]).lower()

    mutations_at_loss = list(claim.mutations)
    allow_finish.set()
    supervisor.request_shutdown()
    thread.join(timeout=3.0)
    assert not thread.is_alive()
    assert claim.mutations == mutations_at_loss
    assert "complete" not in claim.mutations


def test_shutdown_stops_claims_and_exits_at_grace() -> None:
    consumer = FakeConsumerClient()
    claim = FakeClaim(claim_id="c-slow", recommended_heartbeat_seconds=60)
    consumer.enqueue_claims([claim])
    for _ in range(30):
        consumer.enqueue_claims([])

    started = threading.Event()
    hold = threading.Event()

    def handler(c: FakeClaim, token: CancellationToken) -> None:
        started.set()
        hold.wait(timeout=5.0)

    supervisor = ConsumerSupervisor(
        consumer,  # type: ignore[arg-type]
        queues=["orders"],
        worker_id="worker-1",
        lease_seconds=60,
        handler=handler,  # type: ignore[arg-type]
        wait_seconds=0,
        handler_capacity=1,
        shutdown_grace_seconds=0.2,
        idle_poll_seconds=0.02,
        heartbeat_jitter_ratio=0.0,
    )

    thread = threading.Thread(target=supervisor.run, daemon=True)
    thread.start()
    assert started.wait(timeout=2.0)
    claims_before = len(consumer.claim_calls)
    supervisor.request_shutdown()
    thread.join(timeout=2.0)
    assert not thread.is_alive()
    assert len(consumer.claim_calls) == claims_before
    assert "complete" not in claim.mutations
    assert "fail" not in claim.mutations
    assert "ack_cancel" not in claim.mutations
    hold.set()


def test_cooperative_cancellation_observable_and_ack_cancel_by_handler() -> None:
    consumer = FakeConsumerClient()

    def side_effect(claim: FakeClaim) -> HeartbeatResult | None:
        claim.cancel_requested = True
        return HeartbeatResult(
            claim=ClaimSummary(
                claim_id=claim.claim_id,
                generation=claim.generation,
                claimed_at=claim.claimed_at,
                lease_expires_at=claim.lease_expires_at,
                worker_id=claim.worker_id,
                cancel_requested=True,
            ),
            server_time=claim.server_time,
            recommended_heartbeat_seconds=claim.recommended_heartbeat_seconds,
        )

    claim = FakeClaim(
        claim_id="c-cancel",
        recommended_heartbeat_seconds=1,
        heartbeat_side_effect=side_effect,
    )
    consumer.enqueue_claims([claim])
    saw_cancel = threading.Event()

    def handler(c: FakeClaim, token: CancellationToken) -> None:
        assert token.wait(timeout=2.0)
        saw_cancel.set()
        c.ack_cancel()

    clock = FakeClock()
    supervisor = ConsumerSupervisor(
        consumer,  # type: ignore[arg-type]
        queues=["orders"],
        worker_id="worker-1",
        lease_seconds=60,
        handler=handler,  # type: ignore[arg-type]
        wait_seconds=0,
        handler_capacity=1,
        shutdown_grace_seconds=2.0,
        clock=clock,
        sleep=clock.sleep,
        jitter=lambda lo, hi: lo,
        idle_poll_seconds=0.01,
    )

    thread = threading.Thread(target=supervisor.run, daemon=True)
    thread.start()
    assert saw_cancel.wait(timeout=3.0)
    supervisor.request_shutdown()
    thread.join(timeout=3.0)
    assert not thread.is_alive()
    assert claim.mutations.count("ack_cancel") == 1


def test_handler_error_reaches_typed_callback() -> None:
    consumer = FakeConsumerClient()
    claim = FakeClaim(claim_id="c-err", recommended_heartbeat_seconds=60)
    consumer.enqueue_claims([claim])

    errors: list[HandlerErrorEvent] = []
    done = threading.Event()

    def on_handler_error(event: HandlerErrorEvent) -> None:
        errors.append(event)
        done.set()

    def handler(c: FakeClaim, token: CancellationToken) -> None:
        raise RuntimeError("handler boom")

    supervisor = ConsumerSupervisor(
        consumer,  # type: ignore[arg-type]
        queues=["orders"],
        worker_id="worker-1",
        lease_seconds=60,
        handler=handler,  # type: ignore[arg-type]
        wait_seconds=0,
        handler_capacity=1,
        shutdown_grace_seconds=1.0,
        on_handler_error=on_handler_error,
        idle_poll_seconds=0.01,
        heartbeat_jitter_ratio=0.0,
    )

    thread = threading.Thread(target=supervisor.run, daemon=True)
    thread.start()
    assert done.wait(timeout=2.0)
    supervisor.request_shutdown()
    thread.join(timeout=2.0)
    assert len(errors) == 1
    assert errors[0].claim_id == "c-err"
    assert errors[0].exc_type == "RuntimeError"
    assert "claim-token" not in repr(errors[0]).lower()


class TerminalRaceTransport:
    """Hold a real Claim heartbeat until complete has removed its fence."""

    def __init__(self) -> None:
        self.heartbeat_entered = threading.Event()
        self.release_heartbeat = threading.Event()

    def request(self, method: str, path: str, **kwargs: Any) -> TransportResponse:
        if path.endswith(":heartbeat"):
            self.heartbeat_entered.set()
            assert self.release_heartbeat.wait(timeout=2.0)
            raise _lease_lost_protocol_error("c-race")
        if path.endswith(":complete"):
            return TransportResponse(
                status_code=200,
                headers={},
                body={
                    "task_id": _task().task_id,
                    "state": "succeeded",
                    "spawned_task_ids": [],
                    "replayed": False,
                },
                raw_body=b"",
            )
        raise AssertionError(f"unexpected request: {method} {path}")


def test_terminal_complete_suppresses_spurious_lease_lost() -> None:
    """A real in-flight heartbeat losing its post-complete fence is benign."""

    consumer = FakeConsumerClient()
    transport = TerminalRaceTransport()
    client = ConsumerClient(transport, bearer_token="worker-secret")  # type: ignore[arg-type]
    claim = Claim(
        client,
        task=_task(),
        claim_id="c-race",
        generation=1,
        claimed_at="2026-09-19T00:00:00Z",
        lease_expires_at="2026-09-19T00:01:00Z",
        worker_id="worker-1",
        cancel_requested=False,
        claim_token="claim-secret",
        server_time="2026-09-19T00:00:00Z",
        recommended_heartbeat_seconds=1,
    )
    consumer.enqueue_claims([claim])  # type: ignore[list-item]

    lost_events: list[LeaseLostEvent] = []
    terminal_done = threading.Event()

    def on_lease_lost(event: LeaseLostEvent) -> None:
        lost_events.append(event)

    def handler(c: Claim, token: CancellationToken) -> None:
        assert transport.heartbeat_entered.wait(timeout=2.0)
        try:
            c.complete(spawn=[])
            terminal_done.set()
        finally:
            transport.release_heartbeat.set()
        time.sleep(0.05)

    supervisor = ConsumerSupervisor(
        consumer,  # type: ignore[arg-type]
        queues=["orders"],
        worker_id="worker-1",
        lease_seconds=60,
        handler=handler,  # type: ignore[arg-type]
        wait_seconds=0,
        handler_capacity=1,
        shutdown_grace_seconds=2.0,
        on_lease_lost=on_lease_lost,
        jitter=lambda lo, hi: 0.01,
        idle_poll_seconds=0.05,
    )

    thread = threading.Thread(target=supervisor.run, daemon=True)
    thread.start()
    assert terminal_done.wait(timeout=5.0)
    time.sleep(0.2)
    supervisor.request_shutdown()
    thread.join(timeout=3.0)
    assert not thread.is_alive()
    assert claim.is_terminal is True
    assert claim.lease_lost is False
    assert lost_events == []


def test_supervisor_rejects_surplus_claims() -> None:
    consumer = FakeConsumerClient()
    consumer.enqueue_claims(
        [FakeClaim(claim_id="c-0"), FakeClaim(claim_id="c-1")]
    )

    errors: list[BaseException] = []

    def handler(c: FakeClaim, token: CancellationToken) -> None:
        c.complete()

    supervisor = ConsumerSupervisor(
        consumer,  # type: ignore[arg-type]
        queues=["orders"],
        worker_id="worker-1",
        lease_seconds=60,
        handler=handler,  # type: ignore[arg-type]
        wait_seconds=0,
        handler_capacity=1,
        shutdown_grace_seconds=1.0,
        idle_poll_seconds=0.01,
    )

    def _run() -> None:
        try:
            supervisor.run()
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    thread.join(timeout=2.0)
    assert len(errors) == 1
    assert isinstance(errors[0], ValueError)
    assert "expected exactly 1 claim" in str(errors[0])


def test_handler_terminal_stops_heartbeats_race() -> None:
    """Handler complete must stop the heartbeat loop (terminal vs heartbeat race)."""

    consumer = FakeConsumerClient()
    claim = FakeClaim(claim_id="c-term", recommended_heartbeat_seconds=1)
    consumer.enqueue_claims([claim])

    clock = FakeClock()
    terminal_done = threading.Event()
    saw_hb = threading.Event()

    def handler(c: FakeClaim, token: CancellationToken) -> None:
        deadline = time.monotonic() + 2.0
        while c.heartbeat_count < 1 and time.monotonic() < deadline:
            time.sleep(0.005)
        assert c.heartbeat_count >= 1
        saw_hb.set()
        c.complete()
        terminal_done.set()
        # Hold briefly so a racy heartbeat loop would still have time to fire.
        time.sleep(0.05)

    supervisor = ConsumerSupervisor(
        consumer,  # type: ignore[arg-type]
        queues=["orders"],
        worker_id="worker-1",
        lease_seconds=60,
        handler=handler,  # type: ignore[arg-type]
        wait_seconds=0,
        handler_capacity=1,
        shutdown_grace_seconds=2.0,
        clock=clock,
        sleep=clock.sleep,
        jitter=lambda lo, hi: lo,
        idle_poll_seconds=0.01,
    )

    thread = threading.Thread(target=supervisor.run, daemon=True)
    thread.start()
    assert terminal_done.wait(timeout=3.0)
    assert saw_hb.is_set()
    hb_at_terminal = claim.heartbeat_count
    time.sleep(0.08)
    assert claim.heartbeat_count == hb_at_terminal
    assert claim.mutations.count("complete") == 1
    assert all(m.startswith("heartbeat:") or m == "complete" for m in claim.mutations)
    supervisor.request_shutdown()
    thread.join(timeout=3.0)
    assert not thread.is_alive()
    assert claim.heartbeat_count == hb_at_terminal


def test_shutdown_at_full_capacity_blocks_new_claims() -> None:
    """Shutdown while capacity is saturated must not begin new claims."""

    consumer = FakeConsumerClient()
    for i in range(4):
        consumer.enqueue_claims([FakeClaim(claim_id=f"c-full-{i}")])

    hold = threading.Event()
    started = threading.Barrier(3)

    def handler(c: FakeClaim, token: CancellationToken) -> None:
        started.wait(timeout=2.0)
        assert hold.wait(timeout=3.0)
        c.complete()

    supervisor = ConsumerSupervisor(
        consumer,  # type: ignore[arg-type]
        queues=["orders"],
        worker_id="worker-1",
        lease_seconds=60,
        handler=handler,  # type: ignore[arg-type]
        wait_seconds=0,
        handler_capacity=2,
        shutdown_grace_seconds=1.0,
        idle_poll_seconds=0.01,
        heartbeat_jitter_ratio=0.0,
    )

    thread = threading.Thread(target=supervisor.run, daemon=True)
    thread.start()
    started.wait(timeout=2.0)
    assert supervisor.active_handler_count == 2
    claims_at_full = len(consumer.claim_calls)
    supervisor.request_shutdown()
    thread.join(timeout=2.0)
    assert not thread.is_alive()
    assert len(consumer.claim_calls) == claims_at_full
    hold.set()


def test_grace_expiry_stops_heartbeats_without_forced_terminals() -> None:
    """Grace expiry stops heartbeats and leaves leases for server expiry.

    Uses wall-clock sleep (not FakeClock) so heartbeat intervals are real and
    grace expiry can stop the loop without the injectable clock racing ahead.
    """

    consumer = FakeConsumerClient()
    claim = FakeClaim(claim_id="c-grace", recommended_heartbeat_seconds=1)
    consumer.enqueue_claims([claim])

    entered = threading.Event()
    hold = threading.Event()

    def handler(c: FakeClaim, token: CancellationToken) -> None:
        entered.set()
        hold.wait(timeout=5.0)

    supervisor = ConsumerSupervisor(
        consumer,  # type: ignore[arg-type]
        queues=["orders"],
        worker_id="worker-1",
        lease_seconds=60,
        handler=handler,  # type: ignore[arg-type]
        wait_seconds=0,
        handler_capacity=1,
        shutdown_grace_seconds=0.15,
        idle_poll_seconds=0.02,
        heartbeat_jitter_ratio=0.0,
    )

    thread = threading.Thread(target=supervisor.run, daemon=True)
    thread.start()
    assert entered.wait(timeout=2.0)
    # Allow at least one heartbeat before shutdown/grace.
    deadline = time.monotonic() + 3.0
    while claim.heartbeat_count < 1 and time.monotonic() < deadline:
        time.sleep(0.02)
    assert claim.heartbeat_count >= 1
    supervisor.request_shutdown()
    thread.join(timeout=2.0)
    assert not thread.is_alive()
    hb_after_join = claim.heartbeat_count
    time.sleep(0.25)
    assert claim.heartbeat_count == hb_after_join
    assert "complete" not in claim.mutations
    assert "fail" not in claim.mutations
    assert "ack_cancel" not in claim.mutations
    hold.set()

def _enabled_caps(**overrides: object) -> Any:
    from _workhold_client_core.capabilities import Capabilities

    body: dict[str, object] = {
        "protocol_major": 1,
        "protocol_version": "1.0",
        "schema_revision": "0001",
        "scheduling": True,
        "priority": True,
        "delivery_events": False,
        "batch_claim": False,
        "long_polling": True,
        "max_claim_tasks": 1,
        "max_wait_seconds": 20,
        "payload_runtime_max_bytes": 262144,
        "payload_hard_max_bytes": 1048576,
        "enqueue_dedup_ttl_seconds": 7776000,
        "enqueue_dedup_ttl_min_seconds": 2592000,
        "enqueue_dedup_ttl_max_seconds": 31536000,
        "terminal_replay_ttl_seconds": 604800,
        "terminal_replay_ttl_min_seconds": 86400,
        "terminal_replay_ttl_max_seconds": 2592000,
        "admin_replay_ttl_seconds": 2592000,
        "admin_replay_ttl_min_seconds": 604800,
        "admin_replay_ttl_max_seconds": 7776000,
    }
    body.update(overrides)
    return Capabilities.parse(body)


def test_supervisor_default_wait_capped_when_max_wait_lower() -> None:
    caps = _enabled_caps(max_wait_seconds=10)
    consumer = FakeConsumerClient(capabilities=caps)
    consumer.enqueue_claims([FakeClaim(claim_id="c-cap")])
    done = threading.Event()

    def handler(c: FakeClaim, token: CancellationToken) -> None:
        c.complete()
        done.set()

    supervisor = ConsumerSupervisor(
        consumer,  # type: ignore[arg-type]
        queues=["orders"],
        worker_id="worker-1",
        lease_seconds=60,
        handler=handler,  # type: ignore[arg-type]
        handler_capacity=1,
        shutdown_grace_seconds=1.0,
        idle_poll_seconds=0.01,
        heartbeat_jitter_ratio=0.0,
    )
    thread = threading.Thread(target=supervisor.run, daemon=True)
    thread.start()
    assert done.wait(timeout=2.0)
    supervisor.request_shutdown()
    thread.join(timeout=2.0)
    assert consumer.claim_calls[0]["wait_seconds"] == 10


def test_supervisor_defaults_to_15_after_capability_preflight() -> None:
    from workhold_consumer.supervisor import DEFAULT_WAIT_SECONDS

    caps = _enabled_caps()
    consumer = FakeConsumerClient(capabilities=caps)
    consumer.enqueue_claims([FakeClaim(claim_id="c-default")])
    done = threading.Event()

    def handler(c: FakeClaim, token: CancellationToken) -> None:
        c.complete()
        done.set()

    supervisor = ConsumerSupervisor(
        consumer,  # type: ignore[arg-type]
        queues=["orders"],
        worker_id="worker-1",
        lease_seconds=60,
        handler=handler,  # type: ignore[arg-type]
        handler_capacity=1,
        shutdown_grace_seconds=1.0,
        idle_poll_seconds=0.01,
        heartbeat_jitter_ratio=0.0,
    )
    thread = threading.Thread(target=supervisor.run, daemon=True)
    thread.start()
    assert done.wait(timeout=2.0)
    supervisor.request_shutdown()
    thread.join(timeout=2.0)
    assert consumer.capabilities_calls == 1
    assert consumer.claim_calls[0]["wait_seconds"] == DEFAULT_WAIT_SECONDS
    assert consumer.claim_calls[0]["capabilities"] is caps
    assert DEFAULT_WAIT_SECONDS == 15


def test_supervisor_reuses_preflight_capabilities_across_empty_polls() -> None:
    """Preflight caps must be passed into claim so real Client does not re-fetch."""

    caps = _enabled_caps()
    consumer = FakeConsumerClient(capabilities=caps)
    # Leave pending empty: each claim returns [] (empty long-poll expiry).

    def handler(c: FakeClaim, token: CancellationToken) -> None:
        c.complete()

    supervisor = ConsumerSupervisor(
        consumer,  # type: ignore[arg-type]
        queues=["orders"],
        worker_id="worker-1",
        lease_seconds=60,
        handler=handler,  # type: ignore[arg-type]
        wait_seconds=5,
        handler_capacity=1,
        shutdown_grace_seconds=1.0,
        idle_poll_seconds=0.01,
        heartbeat_jitter_ratio=0.0,
    )
    thread = threading.Thread(target=supervisor.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 2.0
    while len(consumer.claim_calls) < 3 and time.monotonic() < deadline:
        time.sleep(0.01)
    supervisor.request_shutdown()
    thread.join(timeout=2.0)
    assert not thread.is_alive()
    assert len(consumer.claim_calls) >= 3
    assert consumer.capabilities_calls == 1
    assert all(call["capabilities"] is caps for call in consumer.claim_calls)
    assert all(call["wait_seconds"] == 5 for call in consumer.claim_calls)


def test_supervisor_zero_disables_long_poll_without_capability_fetch() -> None:
    consumer = FakeConsumerClient()
    consumer.enqueue_claims([FakeClaim(claim_id="c-zero")])
    done = threading.Event()

    def handler(c: FakeClaim, token: CancellationToken) -> None:
        c.complete()
        done.set()

    supervisor = ConsumerSupervisor(
        consumer,  # type: ignore[arg-type]
        queues=["orders"],
        worker_id="worker-1",
        lease_seconds=60,
        handler=handler,  # type: ignore[arg-type]
        wait_seconds=0,
        handler_capacity=1,
        shutdown_grace_seconds=1.0,
        idle_poll_seconds=0.01,
        heartbeat_jitter_ratio=0.0,
    )
    thread = threading.Thread(target=supervisor.run, daemon=True)
    thread.start()
    assert done.wait(timeout=2.0)
    supervisor.request_shutdown()
    thread.join(timeout=2.0)
    assert consumer.capabilities_calls == 0
    assert consumer.claim_calls[0]["wait_seconds"] == 0


def test_supervisor_capability_false_fails_closed_before_claim() -> None:
    caps = _enabled_caps(long_polling=False, max_wait_seconds=0)
    consumer = FakeConsumerClient(capabilities=caps)

    def handler(c: FakeClaim, token: CancellationToken) -> None:
        c.complete()

    supervisor = ConsumerSupervisor(
        consumer,  # type: ignore[arg-type]
        queues=["orders"],
        worker_id="worker-1",
        lease_seconds=60,
        handler=handler,  # type: ignore[arg-type]
        handler_capacity=1,
        shutdown_grace_seconds=0.5,
    )
    with pytest.raises(ValueError, match="long_polling"):
        supervisor.run()
    assert consumer.claim_calls == []


def test_supervisor_no_poll_without_handler_capacity() -> None:
    caps = _enabled_caps()
    consumer = FakeConsumerClient(capabilities=caps)
    for i in range(3):
        consumer.enqueue_claims([FakeClaim(claim_id=f"c-cap-{i}")])

    hold = threading.Event()
    started = threading.Event()

    def handler(c: FakeClaim, token: CancellationToken) -> None:
        started.set()
        assert hold.wait(timeout=3.0)
        c.complete()

    supervisor = ConsumerSupervisor(
        consumer,  # type: ignore[arg-type]
        queues=["orders"],
        worker_id="worker-1",
        lease_seconds=60,
        handler=handler,  # type: ignore[arg-type]
        wait_seconds=10,
        handler_capacity=1,
        shutdown_grace_seconds=1.0,
        idle_poll_seconds=0.01,
        heartbeat_jitter_ratio=0.0,
    )
    thread = threading.Thread(target=supervisor.run, daemon=True)
    thread.start()
    assert started.wait(timeout=2.0)
    time.sleep(0.05)
    assert len(consumer.claim_calls) == 1
    supervisor.request_shutdown()
    hold.set()
    thread.join(timeout=2.0)
    assert not thread.is_alive()
    assert len(consumer.claim_calls) == 1


def test_supervisor_shutdown_cancels_outstanding_wait() -> None:
    caps = _enabled_caps()
    consumer = FakeConsumerClient(capabilities=caps)
    entered = threading.Event()
    release = threading.Event()
    consumer.set_claim_hold(entered=entered, release=release)

    def handler(c: FakeClaim, token: CancellationToken) -> None:
        c.complete()

    supervisor = ConsumerSupervisor(
        consumer,  # type: ignore[arg-type]
        queues=["orders"],
        worker_id="worker-1",
        lease_seconds=60,
        handler=handler,  # type: ignore[arg-type]
        wait_seconds=15,
        handler_capacity=1,
        shutdown_grace_seconds=1.0,
        idle_poll_seconds=0.01,
        heartbeat_jitter_ratio=0.0,
    )
    thread = threading.Thread(target=supervisor.run, daemon=True)
    thread.start()
    assert entered.wait(timeout=2.0)
    supervisor.request_shutdown()
    thread.join(timeout=2.0)
    assert not thread.is_alive()
    release.set()
    assert consumer.claim_calls == [] or (
        consumer.claim_calls and consumer.claim_calls[0]["wait_seconds"] == 15
    )


def test_lease_loss_never_substitutes_replacement_work() -> None:
    """Lease loss must not trigger an extra claim while capacity is occupied."""

    consumer = FakeConsumerClient()

    def side_effect(claim: FakeClaim) -> HeartbeatResult | None:
        claim.mark_lease_lost()
        raise LeaseLostError(claim_id=claim.claim_id)

    claim = FakeClaim(
        claim_id="c-lease-sub",
        recommended_heartbeat_seconds=1,
        heartbeat_side_effect=side_effect,
    )
    consumer.enqueue_claims([claim])
    consumer.enqueue_claims([FakeClaim(claim_id="c-should-not-run")])

    lost = threading.Event()
    hold = threading.Event()
    entered = threading.Event()

    def on_lost(event: LeaseLostEvent) -> None:
        lost.set()

    def handler(c: FakeClaim, token: CancellationToken) -> None:
        entered.set()
        assert hold.wait(timeout=3.0)

    clock = FakeClock()
    supervisor = ConsumerSupervisor(
        consumer,  # type: ignore[arg-type]
        queues=["orders"],
        worker_id="worker-1",
        lease_seconds=60,
        handler=handler,  # type: ignore[arg-type]
        wait_seconds=0,
        handler_capacity=1,
        shutdown_grace_seconds=1.0,
        on_lease_lost=on_lost,
        clock=clock,
        sleep=clock.sleep,
        jitter=lambda lo, hi: lo,
        idle_poll_seconds=0.01,
        heartbeat_jitter_ratio=0.0,
    )

    thread = threading.Thread(target=supervisor.run, daemon=True)
    thread.start()
    assert entered.wait(timeout=2.0)
    assert lost.wait(timeout=2.0)
    time.sleep(0.05)
    assert len(consumer.claim_calls) == 1
    hold.set()
    supervisor.request_shutdown()
    thread.join(timeout=2.0)
