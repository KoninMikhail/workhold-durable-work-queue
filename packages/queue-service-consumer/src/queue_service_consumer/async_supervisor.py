"""Optional async supervised consumer loop over AsyncConsumerClient.

Mirrors :class:`~queue_service_consumer.supervisor.ConsumerSupervisor`
lifecycle guarantees with structured asyncio tasks: capacity-bounded claims,
jittered heartbeats, cooperative cancellation, lease-loss callbacks, and
grace-bounded shutdown. Handler task cancellation never implies Queue
ack/fail/complete — lease expiry remains authoritative after grace.

Long-poll wait defaults to :data:`DEFAULT_WAIT_SECONDS` (15) after a one-time
authenticated capability preflight. Pass ``wait_seconds=0`` to disable.
``asyncio.CancelledError`` on outstanding polls propagates unchanged and never
triggers a replacement claim or terminal mutation.
"""

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, Protocol

from _queue_service_client_core.capabilities import Capabilities
from _queue_service_client_core.capability_guard import require_long_polling
from _queue_service_client_core.errors import LeaseLostError, ProtocolError
from queue_service_consumer.async_client import AsyncClaim, AsyncConsumerClient
from queue_service_consumer.supervisor import (
    DEFAULT_WAIT_SECONDS,
    HEARTBEAT_JITTER_RATIO,
    HandlerErrorEvent,
    LeaseLostEvent,
)


class AsyncCancellationToken:
    """Cooperative cancellation signal for async application handlers."""

    def __init__(self) -> None:
        self._event = asyncio.Event()

    def is_cancelled(self) -> bool:
        return self._event.is_set()

    async def wait(self, timeout: float | None = None) -> bool:
        if timeout is None:
            await self._event.wait()
            return True
        try:
            await asyncio.wait_for(self._event.wait(), timeout=timeout)
            return True
        except TimeoutError:
            return False

    def _cancel(self) -> None:
        self._event.set()


class _AsyncClaimLike(Protocol):
    claim_id: str
    recommended_heartbeat_seconds: int
    cancel_requested: bool

    @property
    def lease_lost(self) -> bool: ...

    @property
    def is_terminal(self) -> bool: ...

    @property
    def task(self) -> Any: ...

    async def heartbeat(self, *, lease_seconds: int) -> Any: ...


def _claim_finished(claim: _AsyncClaimLike) -> bool:
    return bool(claim.lease_lost or getattr(claim, "is_terminal", False))


AsyncHandler = Callable[[AsyncClaim, AsyncCancellationToken], Awaitable[None]]
LeaseLostCallback = Callable[[LeaseLostEvent], None]
HandlerErrorCallback = Callable[[HandlerErrorEvent], None]
ClockFn = Callable[[], float]
AsyncSleepFn = Callable[[float], Awaitable[None]]
JitterFn = Callable[[float, float], float]


class AsyncConsumerSupervisor:
    """Bounded async claim/heartbeat/handler loop over :class:`AsyncConsumerClient`.

    Never exceeds ``handler_capacity``, never claims after ``request_shutdown``,
    never auto-retries terminal bodies, never acknowledges cancellation before
    the handler observes the token, never substitutes a different claim after
    lease loss, and never maps handler :exc:`asyncio.CancelledError` to a
    Queue terminal mutation.

    ``wait_seconds``:
    - ``None`` (default): capability-preflight once, then use
      :data:`DEFAULT_WAIT_SECONDS` (15) capped by advertised max.
    - ``0``: disable long polling (immediate claims).
    - positive: explicit bounded wait after the same capability gate.
    """

    def __init__(
        self,
        consumer: AsyncConsumerClient,
        *,
        queues: Sequence[str],
        worker_id: str,
        lease_seconds: int,
        handler: AsyncHandler,
        handler_capacity: int = 1,
        shutdown_grace_seconds: float = 30.0,
        wait_seconds: int | None = None,
        on_lease_lost: LeaseLostCallback | None = None,
        on_handler_error: HandlerErrorCallback | None = None,
        clock: ClockFn | None = None,
        sleep: AsyncSleepFn | None = None,
        jitter: JitterFn | None = None,
        idle_poll_seconds: float = 0.05,
        heartbeat_jitter_ratio: float = HEARTBEAT_JITTER_RATIO,
    ) -> None:
        if handler_capacity < 1:
            raise ValueError("handler_capacity must be >= 1")
        if shutdown_grace_seconds < 0:
            raise ValueError("shutdown_grace_seconds must be >= 0")
        if not queues:
            raise ValueError("queues must be non-empty")
        if not worker_id:
            raise ValueError("worker_id is required")
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be >= 1")
        if not 0.0 <= heartbeat_jitter_ratio < 1.0:
            raise ValueError("heartbeat_jitter_ratio must be in [0, 1)")
        if wait_seconds is not None:
            if isinstance(wait_seconds, bool) or type(wait_seconds) is not int:
                raise ValueError("wait_seconds must be an integer or None")
            if wait_seconds < 0:
                raise ValueError("wait_seconds must be >= 0")

        self._consumer = consumer
        self._queues = list(queues)
        self._worker_id = worker_id
        self._lease_seconds = lease_seconds
        self._handler = handler
        self._handler_capacity = handler_capacity
        self._shutdown_grace_seconds = shutdown_grace_seconds
        self._wait_seconds_arg = wait_seconds
        self._on_lease_lost = on_lease_lost
        self._on_handler_error = on_handler_error
        self._clock: ClockFn = clock or time.monotonic
        self._sleep: AsyncSleepFn = sleep or asyncio.sleep
        self._jitter: JitterFn = jitter or random.uniform
        self._idle_poll_seconds = idle_poll_seconds
        self._heartbeat_jitter_ratio = heartbeat_jitter_ratio

        self._shutdown = asyncio.Event()
        self._claim_cancel = asyncio.Event()
        self._resolved_wait: int | None = None
        self._capabilities: Capabilities | None = None
        self._outstanding_poll: asyncio.Task[list[Any]] | None = None
        self._active = 0
        self._in_use = 0
        self._cap_lock = asyncio.Lock()
        self._stop_heartbeats: dict[str, asyncio.Event] = {}
        self._hb_tasks: dict[str, asyncio.Task[None]] = {}
        self._handler_tasks: set[asyncio.Task[None]] = set()

    @property
    def active_handler_count(self) -> int:
        return self._active

    @property
    def wait_seconds(self) -> int | None:
        """Resolved wait after preflight, or constructor value before ``run``."""

        if self._resolved_wait is not None:
            return self._resolved_wait
        return self._wait_seconds_arg

    def request_shutdown(self) -> None:
        """Cancel outstanding wait first, then stop claiming and drain handlers."""

        self._shutdown.set()
        self._claim_cancel.set()
        poll = self._outstanding_poll
        if poll is not None and not poll.done():
            poll.cancel()

    async def run(self) -> None:
        """Supervised loop until shutdown completes or grace expires."""

        self._resolved_wait = await self._preflight_wait_seconds()

        try:
            while True:
                if self._shutdown.is_set():
                    await self._await_shutdown()
                    return

                if not await self._try_acquire_capacity():
                    await self._sleep(self._idle_poll_seconds)
                    continue

                if self._shutdown.is_set():
                    await self._release_capacity()
                    await self._await_shutdown()
                    return

                assert self._resolved_wait is not None
                poll_task = asyncio.create_task(
                    self._consumer.claim(
                        queues=self._queues,
                        worker_id=self._worker_id,
                        lease_seconds=self._lease_seconds,
                        max_tasks=1,
                        wait_seconds=self._resolved_wait,
                        capabilities=self._capabilities,
                        cancellation=self._claim_cancel,
                    )
                )
                self._outstanding_poll = poll_task
                try:
                    claims = await poll_task
                except asyncio.CancelledError:
                    await self._release_capacity()
                    if self._shutdown.is_set():
                        await self._await_shutdown()
                        return
                    raise
                except Exception:
                    await self._release_capacity()
                    raise
                finally:
                    if self._outstanding_poll is poll_task:
                        self._outstanding_poll = None

                if not claims:
                    await self._release_capacity()
                    if self._resolved_wait == 0:
                        await self._sleep(self._idle_poll_seconds)
                    continue

                if len(claims) != 1:
                    await self._release_capacity()
                    raise ValueError(
                        f"expected exactly 1 claim for max_tasks=1, got {len(claims)}"
                    )

                claim = claims[0]
                stop_hb = asyncio.Event()
                self._stop_heartbeats[claim.claim_id] = stop_hb
                self._active += 1

                token = AsyncCancellationToken()
                hb_task = asyncio.create_task(
                    self._heartbeat_loop(claim, token, stop_hb),
                    name=f"queue-async-hb-{claim.claim_id}",
                )
                self._hb_tasks[claim.claim_id] = hb_task
                handler_task = asyncio.create_task(
                    self._run_handler(claim, token, stop_hb),
                    name=f"queue-async-handler-{claim.claim_id}",
                )
                self._handler_tasks.add(handler_task)
                handler_task.add_done_callback(self._handler_tasks.discard)
        finally:
            await self._stop_all_heartbeats()

    async def _preflight_wait_seconds(self) -> int:
        requested = self._wait_seconds_arg
        if requested == 0:
            return 0
        caps = await self._consumer.get_capabilities()
        self._capabilities = caps
        if requested is None:
            # Cap default 15 by advertisement, but never let resolved==0
            # short-circuit require_long_polling (wait 0 bypasses the gate).
            resolved = min(DEFAULT_WAIT_SECONDS, caps.max_wait_seconds)
            require_long_polling(
                caps, resolved if resolved > 0 else DEFAULT_WAIT_SECONDS
            )
            return resolved
        require_long_polling(caps, requested)
        return requested

    async def _try_acquire_capacity(self) -> bool:
        async with self._cap_lock:
            if self._in_use >= self._handler_capacity:
                return False
            self._in_use += 1
            return True

    async def _release_capacity(self) -> None:
        async with self._cap_lock:
            self._in_use = max(0, self._in_use - 1)

    async def _await_shutdown(self) -> None:
        deadline = self._clock() + self._shutdown_grace_seconds
        while True:
            if self._active == 0:
                await self._stop_all_heartbeats()
                return
            remaining = deadline - self._clock()
            if remaining <= 0:
                # Grace expired: stop heartbeats; leave outstanding leases for
                # server expiry. Cancel handler tasks without Queue terminals.
                await self._stop_all_heartbeats()
                for task in list(self._handler_tasks):
                    task.cancel()
                return
            await self._sleep(min(self._idle_poll_seconds, remaining))

    async def _stop_all_heartbeats(self) -> None:
        for stop in list(self._stop_heartbeats.values()):
            stop.set()
        pending = [task for task in self._hb_tasks.values() if not task.done()]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        self._hb_tasks.clear()

    async def _run_handler(
        self,
        claim: _AsyncClaimLike,
        token: AsyncCancellationToken,
        stop_hb: asyncio.Event,
    ) -> None:
        try:
            try:
                await self._handler(claim, token)  # type: ignore[arg-type]
            except asyncio.CancelledError:
                # Application/task cancellation must not imply Queue terminals.
                raise
            except Exception as exc:
                if self._on_handler_error is not None:
                    self._on_handler_error(
                        HandlerErrorEvent(
                            claim_id=claim.claim_id,
                            task_id=str(getattr(claim.task, "task_id", "")),
                            exc_type=type(exc).__name__,
                        )
                    )
        finally:
            stop_hb.set()
            self._stop_heartbeats.pop(claim.claim_id, None)
            hb_task = self._hb_tasks.pop(claim.claim_id, None)
            if hb_task is not None and not hb_task.done():
                hb_task.cancel()
                try:
                    await hb_task
                except (asyncio.CancelledError, Exception):
                    pass
            self._active = max(0, self._active - 1)
            await self._release_capacity()

    async def _heartbeat_loop(
        self,
        claim: _AsyncClaimLike,
        token: AsyncCancellationToken,
        stop_hb: asyncio.Event,
    ) -> None:
        try:
            while not stop_hb.is_set() and not _claim_finished(claim):
                recommended = max(1, int(claim.recommended_heartbeat_seconds))
                ratio = self._heartbeat_jitter_ratio
                lo = recommended * (1.0 - ratio)
                hi = recommended * (1.0 + ratio)
                delay = self._jitter(lo, hi)
                await self._interruptible_sleep(delay, stop_hb, claim)
                if stop_hb.is_set() or _claim_finished(claim):
                    return
                try:
                    result = await claim.heartbeat(lease_seconds=self._lease_seconds)
                except asyncio.CancelledError:
                    raise
                except LeaseLostError:
                    # Only suppress after a successful terminal; lease_lost alone
                    # is the signal we must report (do not use _claim_finished).
                    if getattr(claim, "is_terminal", False):
                        return
                    self._notify_lease_lost(claim)
                    token._cancel()
                    return
                except ProtocolError as exc:
                    if (
                        getattr(exc, "code", None) is not None
                        and exc.code.value == "lease_lost"
                    ):
                        if getattr(claim, "is_terminal", False):
                            return
                        self._notify_lease_lost(claim)
                        token._cancel()
                        return
                    stop_hb.set()
                    return
                except Exception:
                    stop_hb.set()
                    return

                if _claim_finished(claim):
                    return

                cancel_requested = bool(
                    getattr(result.claim, "cancel_requested", False)
                    or claim.cancel_requested
                )
                if cancel_requested:
                    claim.cancel_requested = True
                    token._cancel()
        except asyncio.CancelledError:
            return

    async def _interruptible_sleep(
        self,
        seconds: float,
        stop_hb: asyncio.Event,
        claim: _AsyncClaimLike | None = None,
    ) -> None:
        if seconds <= 0:
            return
        # Real asyncio.sleep: Event.wait is interruptible via wait_for.
        if self._sleep is asyncio.sleep:
            try:
                await asyncio.wait_for(stop_hb.wait(), timeout=seconds)
            except TimeoutError:
                return
            return
        # Injectable clock: advance in one step, then yield so sibling tasks
        # (handler terminals) can publish before the next heartbeat.
        deadline = self._clock() + seconds
        while True:
            if stop_hb.is_set() or (claim is not None and _claim_finished(claim)):
                return
            remaining = deadline - self._clock()
            if remaining <= 0:
                await self._sleep(0.0)
                return
            await self._sleep(remaining)
            await asyncio.sleep(0)
            if stop_hb.is_set() or (claim is not None and _claim_finished(claim)):
                return
            return

    def _notify_lease_lost(self, claim: _AsyncClaimLike) -> None:
        if self._on_lease_lost is None:
            return
        self._on_lease_lost(
            LeaseLostEvent(
                claim_id=claim.claim_id,
                task_id=str(getattr(claim.task, "task_id", "")),
            )
        )


__all__ = [
    "DEFAULT_WAIT_SECONDS",
    "AsyncCancellationToken",
    "AsyncConsumerSupervisor",
    "HandlerErrorEvent",
    "LeaseLostEvent",
]
