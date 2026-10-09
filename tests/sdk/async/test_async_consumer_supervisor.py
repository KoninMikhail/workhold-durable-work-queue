"""Deterministic AsyncConsumerSupervisor tests (SDK-06 / SDK-09).

Parameterizes the synchronous ConsumerSupervisor lifecycle truth table for
async parity and adds races for heartbeat-vs-terminal, task cancellation,
full capacity, and grace expiry. Handler cancellation must not auto
ack/fail/complete.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

import pytest

from _queue_service_client_core.errors import LeaseLostError, ProtocolError
from _queue_service_client_core.models import (
    ClaimSummary,
    ErrorCode,
    HeartbeatResult,
    ProtocolErrorBody,
    Task,
    TaskState,
)
from _queue_service_client_core.transport import TransportResponse
from queue_service_consumer.async_client import AsyncClaim, AsyncConsumerClient
from queue_service_consumer.async_supervisor import (
    AsyncCancellationToken,
    AsyncConsumerSupervisor,
)
from queue_service_consumer.supervisor import (
    HEARTBEAT_JITTER_RATIO,
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
class FakeAsyncClaim:
    claim_id: str
    recommended_heartbeat_seconds: int = 10
    task: Task = field(default_factory=_task)
    generation: int = 1
    claimed_at: str = "2026-09-19T00:00:00Z"
    lease_expires_at: str = "2026-09-19T00:01:00Z"
    worker_id: str = "worker-1"
    cancel_requested: bool = False
    server_time: str = "2026-09-19T00:00:00Z"
    heartbeat_side_effect: (
        Callable[[FakeAsyncClaim], Awaitable[HeartbeatResult | None]] | None
    ) = None
    _lease_lost: bool = field(default=False, init=False, repr=False)
    _terminal: bool = field(default=False, init=False, repr=False)
    mutations: list[str] = field(default_factory=list, init=False, repr=False)
    heartbeat_count: int = field(default=0, init=False, repr=False)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False, repr=False)

    @property
    def lease_lost(self) -> bool:
        return self._lease_lost

    @property
    def is_terminal(self) -> bool:
        return self._terminal

    def mark_lease_lost(self) -> None:
        self._lease_lost = True

    async def heartbeat(self, *, lease_seconds: int) -> HeartbeatResult:
        async with self._lock:
            if self._lease_lost:
                raise LeaseLostError(claim_id=self.claim_id)
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
                override = await self.heartbeat_side_effect(self)
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

    async def complete(self, *, spawn: Sequence[Any] | None = None) -> None:
        async with self._lock:
            if self._lease_lost:
                raise LeaseLostError(claim_id=self.claim_id)
            self.mutations.append("complete")
            self._terminal = True

    async def fail(
        self,
        *,
        retryable: bool,
        failure_code: str,
        failure_detail: str | None = None,
    ) -> None:
        async with self._lock:
            if self._lease_lost:
                raise LeaseLostError(claim_id=self.claim_id)
            self.mutations.append(f"fail:{failure_code}")
            self._terminal = True

    async def ack_cancel(self) -> None:
        async with self._lock:
            if self._lease_lost:
                raise LeaseLostError(claim_id=self.claim_id)
            self.mutations.append("ack_cancel")
            self._terminal = True


class FakeAsyncConsumerClient:
    """Behavioral stand-in for AsyncConsumerClient.claim."""

    def __init__(
        self,
        *,
        capabilities: Any | None = None,
        capabilities_error: BaseException | None = None,
    ) -> None:
        self._lock = asyncio.Lock()
        self._pending: list[list[FakeAsyncClaim]] = []
        self.claim_calls: list[dict[str, Any]] = []
        self.capabilities_calls = 0
        self._capabilities = capabilities
        self._capabilities_error = capabilities_error
        self._claim_block: asyncio.Event | None = None
        self._claim_release: asyncio.Event | None = None

    def enqueue_claims(self, batch: list[FakeAsyncClaim]) -> None:
        self._pending.append(batch)

    def set_claim_hold(
        self,
        *,
        entered: asyncio.Event,
        release: asyncio.Event,
    ) -> None:
        self._claim_block = entered
        self._claim_release = release

    async def get_capabilities(self) -> Any:
        self.capabilities_calls += 1
        if self._capabilities_error is not None:
            raise self._capabilities_error
        if self._capabilities is None:
            raise AssertionError("get_capabilities called without fixture capabilities")
        return self._capabilities

    async def claim(
        self,
        *,
        queues: Sequence[str],
        worker_id: str,
        lease_seconds: int,
        max_tasks: int = 1,
        wait_seconds: int = 0,
        capabilities: Any | None = None,
        cancellation: object | None = None,
    ) -> list[FakeAsyncClaim]:
        # Mirror AsyncConsumerClient: positive wait re-fetches when capabilities omitted.
        if wait_seconds > 0 or max_tasks != 1:
            if capabilities is None:
                await self.get_capabilities()
        if self._claim_block is not None:
            self._claim_block.set()
        if self._claim_release is not None:
            while not self._claim_release.is_set():
                if cancellation is not None:
                    is_set = getattr(cancellation, "is_set", None)
                    if callable(is_set) and is_set():
                        raise asyncio.CancelledError()
                await asyncio.sleep(0.01)
        async with self._lock:
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

    def __call__(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += max(0.0, seconds)
        await asyncio.sleep(0)


def _lease_lost_protocol_error(claim_id: str) -> ProtocolError:
    body = ProtocolErrorBody(
        code=ErrorCode.parse("lease_lost"),
        message="fence lost",
        retryable=False,
        request_id="00000000-0000-4000-8000-000000000001",
        details={"claim_id": claim_id},
    )
    return ProtocolError(status_code=409, body=body)


class AsyncTerminalRaceTransport:
    """Hold a real AsyncClaim heartbeat until complete removes its fence."""

    def __init__(self) -> None:
        self.heartbeat_entered = asyncio.Event()
        self.release_heartbeat = asyncio.Event()

    async def request(
        self, method: str, path: str, **kwargs: Any
    ) -> TransportResponse:
        if path.endswith(":heartbeat"):
            self.heartbeat_entered.set()
            await asyncio.wait_for(self.release_heartbeat.wait(), timeout=2.0)
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


# ---------------------------------------------------------------------------
# Lifecycle truth table (sync parity)
# ---------------------------------------------------------------------------

LIFECYCLE_CASES = [
    "capacity_bound",
    "shutdown_stops_claims",
    "grace_no_forced_terminal",
    "handler_cancel_no_auto_terminal",
]


@pytest.mark.parametrize("scenario", LIFECYCLE_CASES)
async def test_lifecycle_truth_table_async_parity(scenario: str) -> None:
    if scenario == "capacity_bound":
        await _assert_capacity_bound()
    elif scenario == "shutdown_stops_claims":
        await _assert_shutdown_stops_claims()
    elif scenario == "grace_no_forced_terminal":
        await _assert_grace_no_forced_terminal()
    elif scenario == "handler_cancel_no_auto_terminal":
        await _assert_handler_cancel_no_auto_terminal()
    else:
        raise AssertionError(scenario)


async def _assert_capacity_bound() -> None:
    consumer = FakeAsyncConsumerClient()
    for i in range(5):
        consumer.enqueue_claims([FakeAsyncClaim(claim_id=f"c-{i}")])

    active = 0
    max_active = 0
    release = asyncio.Event()
    started = asyncio.Barrier(3)

    async def handler(c: FakeAsyncClaim, token: AsyncCancellationToken) -> None:
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        try:
            await started.wait()
            assert await asyncio.wait_for(release.wait(), timeout=2.0) or True
            await c.complete()
        finally:
            active -= 1

    supervisor = AsyncConsumerSupervisor(
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
    run_task = asyncio.create_task(supervisor.run())
    await asyncio.wait_for(started.wait(), timeout=2.0)
    assert active == 2
    assert max_active == 2
    await asyncio.sleep(0.05)
    assert active == 2
    release.set()
    supervisor.request_shutdown()
    await asyncio.wait_for(run_task, timeout=3.0)
    assert max_active == 2


async def _assert_shutdown_stops_claims() -> None:
    consumer = FakeAsyncConsumerClient()
    claim = FakeAsyncClaim(claim_id="c-slow", recommended_heartbeat_seconds=60)
    consumer.enqueue_claims([claim])
    for _ in range(30):
        consumer.enqueue_claims([])

    started = asyncio.Event()
    hold = asyncio.Event()

    async def handler(c: FakeAsyncClaim, token: AsyncCancellationToken) -> None:
        started.set()
        try:
            await asyncio.wait_for(hold.wait(), timeout=5.0)
        except TimeoutError:
            pass

    supervisor = AsyncConsumerSupervisor(
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
    run_task = asyncio.create_task(supervisor.run())
    await asyncio.wait_for(started.wait(), timeout=2.0)
    claims_before = len(consumer.claim_calls)
    supervisor.request_shutdown()
    await asyncio.wait_for(run_task, timeout=2.0)
    assert len(consumer.claim_calls) == claims_before
    assert "complete" not in claim.mutations
    assert "fail" not in claim.mutations
    assert "ack_cancel" not in claim.mutations
    hold.set()


async def _assert_grace_no_forced_terminal() -> None:
    consumer = FakeAsyncConsumerClient()
    claim = FakeAsyncClaim(claim_id="c-grace", recommended_heartbeat_seconds=1)
    consumer.enqueue_claims([claim])

    entered = asyncio.Event()
    hold = asyncio.Event()

    async def handler(c: FakeAsyncClaim, token: AsyncCancellationToken) -> None:
        entered.set()
        try:
            await asyncio.wait_for(hold.wait(), timeout=5.0)
        except TimeoutError:
            pass

    supervisor = AsyncConsumerSupervisor(
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
    run_task = asyncio.create_task(supervisor.run())
    await asyncio.wait_for(entered.wait(), timeout=2.0)
    deadline = asyncio.get_running_loop().time() + 3.0
    while claim.heartbeat_count < 1 and asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(0.02)
    assert claim.heartbeat_count >= 1
    supervisor.request_shutdown()
    await asyncio.wait_for(run_task, timeout=2.0)
    hb_after = claim.heartbeat_count
    await asyncio.sleep(0.25)
    assert claim.heartbeat_count == hb_after
    assert "complete" not in claim.mutations
    assert "fail" not in claim.mutations
    assert "ack_cancel" not in claim.mutations
    hold.set()


async def _assert_handler_cancel_no_auto_terminal() -> None:
    consumer = FakeAsyncConsumerClient()
    claim = FakeAsyncClaim(claim_id="c-cancel-task", recommended_heartbeat_seconds=60)
    consumer.enqueue_claims([claim])

    entered = asyncio.Event()

    async def handler(c: FakeAsyncClaim, token: AsyncCancellationToken) -> None:
        entered.set()
        await asyncio.Event().wait()

    supervisor = AsyncConsumerSupervisor(
        consumer,  # type: ignore[arg-type]
        queues=["orders"],
        worker_id="worker-1",
        lease_seconds=60,
        handler=handler,  # type: ignore[arg-type]
        wait_seconds=0,
        handler_capacity=1,
        shutdown_grace_seconds=0.05,
        idle_poll_seconds=0.01,
        heartbeat_jitter_ratio=0.0,
    )
    run_task = asyncio.create_task(supervisor.run())
    await asyncio.wait_for(entered.wait(), timeout=2.0)
    supervisor.request_shutdown()
    await asyncio.wait_for(run_task, timeout=2.0)
    assert "complete" not in claim.mutations
    assert "fail" not in claim.mutations
    assert "ack_cancel" not in claim.mutations


async def test_heartbeat_intervals_stay_within_documented_jitter_bounds() -> None:
    consumer = FakeAsyncConsumerClient()
    claim = FakeAsyncClaim(claim_id="c-hb", recommended_heartbeat_seconds=10)
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

    ready = asyncio.Event()

    async def handler(c: FakeAsyncClaim, token: AsyncCancellationToken) -> None:
        while c.heartbeat_count < 3 and not token.is_cancelled():
            await asyncio.sleep(0.01)
        ready.set()
        await c.complete()

    supervisor = AsyncConsumerSupervisor(
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
    run_task = asyncio.create_task(supervisor.run())
    await asyncio.wait_for(ready.wait(), timeout=3.0)
    supervisor.request_shutdown()
    await asyncio.wait_for(run_task, timeout=3.0)

    lo = 10 * (1 - HEARTBEAT_JITTER_RATIO)
    hi = 10 * (1 + HEARTBEAT_JITTER_RATIO)
    hb_sleeps = [s for s in clock.sleeps if lo - 1e-9 <= s <= hi + 1e-9]
    assert len(hb_sleeps) >= 3
    for delay in hb_sleeps:
        assert lo - 1e-9 <= delay <= hi + 1e-9


async def test_lease_loss_reaches_callback_and_blocks_later_mutations() -> None:
    consumer = FakeAsyncConsumerClient()

    async def side_effect(claim: FakeAsyncClaim) -> HeartbeatResult | None:
        if claim.heartbeat_count >= 1:
            claim.mark_lease_lost()
            raise _lease_lost_protocol_error(claim.claim_id)
        return None

    claim = FakeAsyncClaim(
        claim_id="c-lost",
        recommended_heartbeat_seconds=1,
        heartbeat_side_effect=side_effect,
    )
    consumer.enqueue_claims([claim])

    lost_events: list[LeaseLostEvent] = []
    lost = asyncio.Event()
    entered = asyncio.Event()
    allow_finish = asyncio.Event()

    def on_lease_lost(event: LeaseLostEvent) -> None:
        lost_events.append(event)
        lost.set()

    async def handler(c: FakeAsyncClaim, token: AsyncCancellationToken) -> None:
        entered.set()
        await asyncio.wait_for(allow_finish.wait(), timeout=3.0)
        with pytest.raises(LeaseLostError):
            await c.complete()

    clock = FakeClock()
    supervisor = AsyncConsumerSupervisor(
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
    run_task = asyncio.create_task(supervisor.run())
    await asyncio.wait_for(entered.wait(), timeout=2.0)
    await asyncio.wait_for(lost.wait(), timeout=2.0)
    assert len(lost_events) == 1
    assert lost_events[0].claim_id == "c-lost"
    assert lost_events[0].task_id == claim.task.task_id
    assert "token" not in repr(lost_events[0]).lower()

    mutations_at_loss = list(claim.mutations)
    allow_finish.set()
    supervisor.request_shutdown()
    await asyncio.wait_for(run_task, timeout=3.0)
    assert claim.mutations == mutations_at_loss
    assert "complete" not in claim.mutations


async def test_cooperative_cancellation_observable_and_ack_cancel_by_handler() -> None:
    consumer = FakeAsyncConsumerClient()

    async def side_effect(claim: FakeAsyncClaim) -> HeartbeatResult | None:
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

    claim = FakeAsyncClaim(
        claim_id="c-cancel",
        recommended_heartbeat_seconds=1,
        heartbeat_side_effect=side_effect,
    )
    consumer.enqueue_claims([claim])
    saw_cancel = asyncio.Event()

    async def handler(c: FakeAsyncClaim, token: AsyncCancellationToken) -> None:
        assert await token.wait(timeout=2.0)
        saw_cancel.set()
        await c.ack_cancel()

    clock = FakeClock()
    supervisor = AsyncConsumerSupervisor(
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
    run_task = asyncio.create_task(supervisor.run())
    await asyncio.wait_for(saw_cancel.wait(), timeout=3.0)
    supervisor.request_shutdown()
    await asyncio.wait_for(run_task, timeout=3.0)
    assert claim.mutations.count("ack_cancel") == 1


async def test_handler_error_reaches_typed_callback() -> None:
    consumer = FakeAsyncConsumerClient()
    claim = FakeAsyncClaim(claim_id="c-err", recommended_heartbeat_seconds=60)
    consumer.enqueue_claims([claim])

    errors: list[HandlerErrorEvent] = []
    done = asyncio.Event()

    def on_handler_error(event: HandlerErrorEvent) -> None:
        errors.append(event)
        done.set()

    async def handler(c: FakeAsyncClaim, token: AsyncCancellationToken) -> None:
        raise RuntimeError("handler boom")

    supervisor = AsyncConsumerSupervisor(
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
    run_task = asyncio.create_task(supervisor.run())
    await asyncio.wait_for(done.wait(), timeout=2.0)
    supervisor.request_shutdown()
    await asyncio.wait_for(run_task, timeout=2.0)
    assert len(errors) == 1
    assert errors[0].claim_id == "c-err"
    assert errors[0].exc_type == "RuntimeError"
    assert "claim-token" not in repr(errors[0]).lower()


async def test_terminal_complete_suppresses_spurious_lease_lost() -> None:
    """A real in-flight heartbeat losing its post-complete fence is benign."""

    consumer = FakeAsyncConsumerClient()
    transport = AsyncTerminalRaceTransport()
    client = AsyncConsumerClient(transport, bearer_token="worker-secret")
    claim = AsyncClaim(
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
    terminal_done = asyncio.Event()

    async def handler(c: AsyncClaim, token: AsyncCancellationToken) -> None:
        await asyncio.wait_for(transport.heartbeat_entered.wait(), timeout=2.0)
        try:
            await c.complete(spawn=[])
            terminal_done.set()
        finally:
            transport.release_heartbeat.set()
        await asyncio.sleep(0.05)

    supervisor = AsyncConsumerSupervisor(
        consumer,  # type: ignore[arg-type]
        queues=["orders"],
        worker_id="worker-1",
        lease_seconds=60,
        handler=handler,  # type: ignore[arg-type]
        wait_seconds=0,
        handler_capacity=1,
        shutdown_grace_seconds=2.0,
        on_lease_lost=lost_events.append,
        jitter=lambda lo, hi: 0.01,
        idle_poll_seconds=0.05,
    )
    run_task = asyncio.create_task(supervisor.run())
    await asyncio.wait_for(terminal_done.wait(), timeout=3.0)
    await asyncio.sleep(0.1)
    supervisor.request_shutdown()
    await asyncio.wait_for(run_task, timeout=3.0)
    assert claim.is_terminal is True
    assert claim.lease_lost is False
    assert lost_events == []


async def test_supervisor_rejects_surplus_claims() -> None:
    consumer = FakeAsyncConsumerClient()
    consumer.enqueue_claims(
        [FakeAsyncClaim(claim_id="c-0"), FakeAsyncClaim(claim_id="c-1")]
    )

    async def handler(
        c: FakeAsyncClaim, token: AsyncCancellationToken
    ) -> None:
        await c.complete()

    supervisor = AsyncConsumerSupervisor(
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
    with pytest.raises(ValueError, match="expected exactly 1 claim"):
        await supervisor.run()
    assert supervisor.active_handler_count == 0


async def test_handler_terminal_stops_heartbeats_race() -> None:
    """Handler complete must stop the heartbeat loop (terminal vs heartbeat race)."""

    consumer = FakeAsyncConsumerClient()
    claim = FakeAsyncClaim(claim_id="c-term", recommended_heartbeat_seconds=1)
    consumer.enqueue_claims([claim])

    clock = FakeClock()
    terminal_done = asyncio.Event()
    saw_hb = asyncio.Event()

    async def handler(c: FakeAsyncClaim, token: AsyncCancellationToken) -> None:
        deadline = asyncio.get_running_loop().time() + 2.0
        while c.heartbeat_count < 1 and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.005)
        assert c.heartbeat_count >= 1
        saw_hb.set()
        await c.complete()
        terminal_done.set()
        await asyncio.sleep(0.05)

    supervisor = AsyncConsumerSupervisor(
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
    run_task = asyncio.create_task(supervisor.run())
    await asyncio.wait_for(terminal_done.wait(), timeout=3.0)
    assert saw_hb.is_set()
    hb_at_terminal = claim.heartbeat_count
    await asyncio.sleep(0.08)
    assert claim.heartbeat_count == hb_at_terminal
    assert claim.mutations.count("complete") == 1
    assert all(m.startswith("heartbeat:") or m == "complete" for m in claim.mutations)
    supervisor.request_shutdown()
    await asyncio.wait_for(run_task, timeout=3.0)
    assert claim.heartbeat_count == hb_at_terminal


async def test_shutdown_at_full_capacity_blocks_new_claims() -> None:
    """Shutdown while capacity is saturated must not begin new claims."""

    consumer = FakeAsyncConsumerClient()
    for i in range(4):
        consumer.enqueue_claims([FakeAsyncClaim(claim_id=f"c-full-{i}")])

    hold = asyncio.Event()
    started = asyncio.Barrier(3)

    async def handler(c: FakeAsyncClaim, token: AsyncCancellationToken) -> None:
        await started.wait()
        await asyncio.wait_for(hold.wait(), timeout=3.0)
        await c.complete()

    supervisor = AsyncConsumerSupervisor(
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
    run_task = asyncio.create_task(supervisor.run())
    await asyncio.wait_for(started.wait(), timeout=2.0)
    assert supervisor.active_handler_count == 2
    claims_at_full = len(consumer.claim_calls)
    supervisor.request_shutdown()
    await asyncio.wait_for(run_task, timeout=2.0)
    assert len(consumer.claim_calls) == claims_at_full
    hold.set()


async def test_callbacks_contain_no_claim_token_or_payload() -> None:
    consumer = FakeAsyncConsumerClient()
    claim = FakeAsyncClaim(claim_id="c-safe", recommended_heartbeat_seconds=60)
    consumer.enqueue_claims([claim])

    errors: list[HandlerErrorEvent] = []
    done = asyncio.Event()

    def on_handler_error(event: HandlerErrorEvent) -> None:
        errors.append(event)
        done.set()

    async def handler(c: FakeAsyncClaim, token: AsyncCancellationToken) -> None:
        raise RuntimeError("boom")

    supervisor = AsyncConsumerSupervisor(
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
    run_task = asyncio.create_task(supervisor.run())
    await asyncio.wait_for(done.wait(), timeout=2.0)
    supervisor.request_shutdown()
    await asyncio.wait_for(run_task, timeout=2.0)
    dumped = repr(errors[0]).lower()
    assert "token" not in dumped
    assert "payload" not in dumped
    assert "order_id" not in dumped


def _enabled_caps(**overrides: object) -> Any:
    from _queue_service_client_core.capabilities import Capabilities

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


async def test_supervisor_default_wait_capped_when_max_wait_lower() -> None:
    caps = _enabled_caps(max_wait_seconds=10)
    consumer = FakeAsyncConsumerClient(capabilities=caps)
    consumer.enqueue_claims([FakeAsyncClaim(claim_id="c-cap")])
    done = asyncio.Event()

    async def handler(
        c: FakeAsyncClaim, token: AsyncCancellationToken
    ) -> None:
        await c.complete()
        done.set()

    supervisor = AsyncConsumerSupervisor(
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
    run_task = asyncio.create_task(supervisor.run())
    await asyncio.wait_for(done.wait(), timeout=2.0)
    supervisor.request_shutdown()
    await asyncio.wait_for(run_task, timeout=2.0)
    assert consumer.claim_calls[0]["wait_seconds"] == 10


async def test_supervisor_defaults_to_15_after_capability_preflight() -> None:
    from queue_service_consumer.supervisor import DEFAULT_WAIT_SECONDS

    caps = _enabled_caps()
    consumer = FakeAsyncConsumerClient(capabilities=caps)
    consumer.enqueue_claims([FakeAsyncClaim(claim_id="c-default")])
    done = asyncio.Event()

    async def handler(c: FakeAsyncClaim, token: AsyncCancellationToken) -> None:
        await c.complete()
        done.set()

    supervisor = AsyncConsumerSupervisor(
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
    run_task = asyncio.create_task(supervisor.run())
    await asyncio.wait_for(done.wait(), timeout=2.0)
    supervisor.request_shutdown()
    await asyncio.wait_for(run_task, timeout=2.0)
    assert consumer.capabilities_calls == 1
    assert consumer.claim_calls[0]["wait_seconds"] == DEFAULT_WAIT_SECONDS
    assert consumer.claim_calls[0]["capabilities"] is caps
    assert DEFAULT_WAIT_SECONDS == 15


async def test_supervisor_reuses_preflight_capabilities_across_empty_polls() -> None:
    """Preflight caps must be passed into claim so real Client does not re-fetch."""

    caps = _enabled_caps()
    consumer = FakeAsyncConsumerClient(capabilities=caps)

    async def handler(c: FakeAsyncClaim, token: AsyncCancellationToken) -> None:
        await c.complete()

    supervisor = AsyncConsumerSupervisor(
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
    run_task = asyncio.create_task(supervisor.run())
    deadline = asyncio.get_running_loop().time() + 2.0
    while len(consumer.claim_calls) < 3 and asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(0.01)
    supervisor.request_shutdown()
    await asyncio.wait_for(run_task, timeout=2.0)
    assert len(consumer.claim_calls) >= 3
    assert consumer.capabilities_calls == 1
    assert all(call["capabilities"] is caps for call in consumer.claim_calls)
    assert all(call["wait_seconds"] == 5 for call in consumer.claim_calls)


async def test_supervisor_zero_disables_long_poll_without_capability_fetch() -> None:
    consumer = FakeAsyncConsumerClient()
    consumer.enqueue_claims([FakeAsyncClaim(claim_id="c-zero")])
    done = asyncio.Event()

    async def handler(c: FakeAsyncClaim, token: AsyncCancellationToken) -> None:
        await c.complete()
        done.set()

    supervisor = AsyncConsumerSupervisor(
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
    run_task = asyncio.create_task(supervisor.run())
    await asyncio.wait_for(done.wait(), timeout=2.0)
    supervisor.request_shutdown()
    await asyncio.wait_for(run_task, timeout=2.0)
    assert consumer.capabilities_calls == 0
    assert consumer.claim_calls[0]["wait_seconds"] == 0


async def test_supervisor_capability_false_fails_closed_before_claim() -> None:
    caps = _enabled_caps(long_polling=False, max_wait_seconds=0)
    consumer = FakeAsyncConsumerClient(capabilities=caps)

    async def handler(c: FakeAsyncClaim, token: AsyncCancellationToken) -> None:
        await c.complete()

    supervisor = AsyncConsumerSupervisor(
        consumer,  # type: ignore[arg-type]
        queues=["orders"],
        worker_id="worker-1",
        lease_seconds=60,
        handler=handler,  # type: ignore[arg-type]
        handler_capacity=1,
        shutdown_grace_seconds=0.5,
    )
    with pytest.raises(ValueError, match="long_polling"):
        await supervisor.run()
    assert consumer.claim_calls == []


async def test_supervisor_no_poll_without_handler_capacity() -> None:
    caps = _enabled_caps()
    consumer = FakeAsyncConsumerClient(capabilities=caps)
    for i in range(3):
        consumer.enqueue_claims([FakeAsyncClaim(claim_id=f"c-cap-{i}")])

    hold = asyncio.Event()
    started = asyncio.Event()

    async def handler(c: FakeAsyncClaim, token: AsyncCancellationToken) -> None:
        started.set()
        await asyncio.wait_for(hold.wait(), timeout=3.0)
        await c.complete()

    supervisor = AsyncConsumerSupervisor(
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
    run_task = asyncio.create_task(supervisor.run())
    await asyncio.wait_for(started.wait(), timeout=2.0)
    await asyncio.sleep(0.05)
    assert len(consumer.claim_calls) == 1
    supervisor.request_shutdown()
    hold.set()
    await asyncio.wait_for(run_task, timeout=2.0)
    assert len(consumer.claim_calls) == 1


async def test_supervisor_shutdown_cancels_outstanding_wait() -> None:
    caps = _enabled_caps()
    consumer = FakeAsyncConsumerClient(capabilities=caps)
    entered = asyncio.Event()
    release = asyncio.Event()
    consumer.set_claim_hold(entered=entered, release=release)

    async def handler(c: FakeAsyncClaim, token: AsyncCancellationToken) -> None:
        await c.complete()

    supervisor = AsyncConsumerSupervisor(
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
    run_task = asyncio.create_task(supervisor.run())
    await asyncio.wait_for(entered.wait(), timeout=2.0)
    supervisor.request_shutdown()
    await asyncio.wait_for(run_task, timeout=2.0)
    release.set()
    assert consumer.claim_calls == [] or (
        consumer.claim_calls and consumer.claim_calls[0]["wait_seconds"] == 15
    )


async def test_lease_loss_never_substitutes_replacement_work() -> None:
    """Lease loss must not trigger an extra claim while capacity is occupied."""

    consumer = FakeAsyncConsumerClient()

    async def side_effect(claim: FakeAsyncClaim) -> HeartbeatResult | None:
        claim.mark_lease_lost()
        raise LeaseLostError(claim_id=claim.claim_id)

    claim = FakeAsyncClaim(
        claim_id="c-lease-sub",
        recommended_heartbeat_seconds=1,
        heartbeat_side_effect=side_effect,
    )
    consumer.enqueue_claims([claim])
    consumer.enqueue_claims([FakeAsyncClaim(claim_id="c-should-not-run")])

    lost = asyncio.Event()
    hold = asyncio.Event()
    entered = asyncio.Event()

    def on_lost(event: LeaseLostEvent) -> None:
        lost.set()

    async def handler(c: FakeAsyncClaim, token: AsyncCancellationToken) -> None:
        entered.set()
        await asyncio.wait_for(hold.wait(), timeout=3.0)

    clock = FakeClock()
    supervisor = AsyncConsumerSupervisor(
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

    run_task = asyncio.create_task(supervisor.run())
    await asyncio.wait_for(entered.wait(), timeout=2.0)
    await asyncio.wait_for(lost.wait(), timeout=2.0)
    await asyncio.sleep(0.05)
    assert len(consumer.claim_calls) == 1
    supervisor.request_shutdown()
    hold.set()
    await asyncio.wait_for(run_task, timeout=2.0)
