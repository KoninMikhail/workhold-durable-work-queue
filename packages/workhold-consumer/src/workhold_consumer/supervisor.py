"""Optional supervised consumer loop composed from ConsumerClient primitives.

Heartbeat timing uses the documented safe jitter band
``recommended * (1 ± HEARTBEAT_JITTER_RATIO)`` (default ±10%).

Long-poll wait defaults to :data:`DEFAULT_WAIT_SECONDS` (15) after a one-time
authenticated capability preflight. Pass ``wait_seconds=0`` to disable.
"""

from __future__ import annotations

import random
import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from _workhold_client_core.capabilities import Capabilities
from _workhold_client_core.capability_guard import require_long_polling
from _workhold_client_core.errors import (
    LeaseLostError,
    ProtocolError,
    RequestCancelledError,
)
from workhold_consumer.client import Claim, ConsumerClient

# Documented safe bounds around the server-recommended heartbeat interval.
HEARTBEAT_JITTER_RATIO = 0.1
# Capability-validated supervisor default (Phase 20.1 D-05).
DEFAULT_WAIT_SECONDS = 15


class CancellationToken:
    """Cooperative cancellation signal observed by application handlers."""

    def __init__(self) -> None:
        self._event = threading.Event()

    def is_cancelled(self) -> bool:
        return self._event.is_set()

    def wait(self, timeout: float | None = None) -> bool:
        return self._event.wait(timeout)

    def _cancel(self) -> None:
        self._event.set()


@dataclass(frozen=True, slots=True)
class LeaseLostEvent:
    """Safe lease-loss metadata (never includes claim tokens)."""

    claim_id: str
    task_id: str


@dataclass(frozen=True, slots=True)
class HandlerErrorEvent:
    """Safe handler-failure metadata (never includes claim tokens)."""

    claim_id: str
    task_id: str
    exc_type: str


class _ClaimLike(Protocol):
    claim_id: str
    recommended_heartbeat_seconds: int
    cancel_requested: bool

    @property
    def lease_lost(self) -> bool: ...

    @property
    def is_terminal(self) -> bool: ...

    @property
    def task(self) -> Any: ...

    def heartbeat(self, *, lease_seconds: int) -> Any: ...


def _claim_finished(claim: _ClaimLike) -> bool:
    return bool(claim.lease_lost or getattr(claim, "is_terminal", False))


Handler = Callable[[Claim, CancellationToken], None]
LeaseLostCallback = Callable[[LeaseLostEvent], None]
HandlerErrorCallback = Callable[[HandlerErrorEvent], None]
ClockFn = Callable[[], float]
SleepFn = Callable[[float], None]
JitterFn = Callable[[float, float], float]


class ConsumerSupervisor:
    """Bounded claim/heartbeat/handler loop over :class:`ConsumerClient`.

    The supervisor never exceeds ``handler_capacity``, never claims after
    ``request_shutdown``, never auto-retries terminal bodies, never
    acknowledges cancellation before the handler observes the token, and never
    substitutes a different claim after lease loss.

    ``wait_seconds``:
    - ``None`` (default): capability-preflight once, then use
      :data:`DEFAULT_WAIT_SECONDS` (15) capped by advertised max.
    - ``0``: disable long polling (immediate claims).
    - positive: explicit bounded wait after the same capability gate.
    """

    def __init__(
        self,
        consumer: ConsumerClient,
        *,
        queues: Sequence[str],
        worker_id: str,
        lease_seconds: int,
        handler: Handler,
        handler_capacity: int = 1,
        shutdown_grace_seconds: float = 30.0,
        wait_seconds: int | None = None,
        on_lease_lost: LeaseLostCallback | None = None,
        on_handler_error: HandlerErrorCallback | None = None,
        clock: ClockFn | None = None,
        sleep: SleepFn | None = None,
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
        self._sleep: SleepFn = sleep or time.sleep
        self._jitter: JitterFn = jitter or random.uniform
        self._idle_poll_seconds = idle_poll_seconds
        self._heartbeat_jitter_ratio = heartbeat_jitter_ratio

        self._shutdown = threading.Event()
        self._claim_cancel = threading.Event()
        self._resolved_wait: int | None = None
        self._capabilities: Capabilities | None = None
        self._active = 0
        self._active_lock = threading.Lock()
        self._slots = threading.BoundedSemaphore(handler_capacity)
        self._stop_heartbeats: dict[str, threading.Event] = {}
        self._hb_lock = threading.Lock()

    @property
    def active_handler_count(self) -> int:
        with self._active_lock:
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
        transport = getattr(self._consumer, "_transport", None)
        cancel_active = getattr(transport, "cancel_active", None) if transport is not None else None
        if callable(cancel_active):
            cancel_active()

    def run(self) -> None:
        """Blocking supervised loop until shutdown completes or grace expires."""

        self._resolved_wait = self._preflight_wait_seconds()

        while True:
            if self._shutdown.is_set():
                self._await_shutdown()
                return

            if not self._slots.acquire(blocking=False):
                self._sleep(self._idle_poll_seconds)
                continue

            if self._shutdown.is_set():
                self._slots.release()
                self._await_shutdown()
                return

            assert self._resolved_wait is not None
            try:
                claims = self._consumer.claim(
                    queues=self._queues,
                    worker_id=self._worker_id,
                    lease_seconds=self._lease_seconds,
                    max_tasks=1,
                    wait_seconds=self._resolved_wait,
                    capabilities=self._capabilities,
                    cancellation=self._claim_cancel,
                )
            except RequestCancelledError:
                self._slots.release()
                if self._shutdown.is_set():
                    self._await_shutdown()
                    return
                raise
            except Exception:
                self._slots.release()
                raise

            if not claims:
                self._slots.release()
                if self._resolved_wait == 0:
                    self._sleep(self._idle_poll_seconds)
                continue

            if len(claims) != 1:
                self._slots.release()
                raise ValueError(
                    f"expected exactly 1 claim for max_tasks=1, got {len(claims)}"
                )

            claim = claims[0]
            stop_hb = threading.Event()
            with self._hb_lock:
                self._stop_heartbeats[claim.claim_id] = stop_hb
            with self._active_lock:
                self._active += 1

            token = CancellationToken()
            hb_thread = threading.Thread(
                target=self._heartbeat_loop,
                args=(claim, token, stop_hb),
                name=f"queue-hb-{claim.claim_id}",
                daemon=True,
            )
            handler_thread = threading.Thread(
                target=self._run_handler,
                args=(claim, token, stop_hb),
                name=f"queue-handler-{claim.claim_id}",
                daemon=True,
            )
            hb_thread.start()
            handler_thread.start()

    def _preflight_wait_seconds(self) -> int:
        requested = self._wait_seconds_arg
        if requested == 0:
            return 0
        caps = self._consumer.get_capabilities()
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

    def _await_shutdown(self) -> None:
        deadline = self._clock() + self._shutdown_grace_seconds
        while True:
            with self._active_lock:
                if self._active == 0:
                    self._stop_all_heartbeats()
                    return
            remaining = deadline - self._clock()
            if remaining <= 0:
                # Grace expired: leave outstanding leases for server expiry.
                self._stop_all_heartbeats()
                return
            self._sleep(min(self._idle_poll_seconds, remaining))

    def _stop_all_heartbeats(self) -> None:
        with self._hb_lock:
            for stop in self._stop_heartbeats.values():
                stop.set()

    def _run_handler(
        self,
        claim: _ClaimLike,
        token: CancellationToken,
        stop_hb: threading.Event,
    ) -> None:
        try:
            try:
                self._handler(claim, token)  # type: ignore[arg-type]
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
            with self._hb_lock:
                self._stop_heartbeats.pop(claim.claim_id, None)
            with self._active_lock:
                self._active -= 1
            self._slots.release()

    def _heartbeat_loop(
        self,
        claim: _ClaimLike,
        token: CancellationToken,
        stop_hb: threading.Event,
    ) -> None:
        while not stop_hb.is_set() and not _claim_finished(claim):
            recommended = max(1, int(claim.recommended_heartbeat_seconds))
            ratio = self._heartbeat_jitter_ratio
            lo = recommended * (1.0 - ratio)
            hi = recommended * (1.0 + ratio)
            delay = self._jitter(lo, hi)
            # Interruptible sleep so shutdown/handler completion wakes promptly.
            self._interruptible_sleep(delay, stop_hb, claim)
            if stop_hb.is_set() or _claim_finished(claim):
                return
            try:
                result = claim.heartbeat(lease_seconds=self._lease_seconds)
            except LeaseLostError:
                # Only suppress after a successful terminal; lease_lost alone
                # is the signal we must report (do not use _claim_finished).
                if getattr(claim, "is_terminal", False):
                    return
                self._notify_lease_lost(claim)
                token._cancel()
                return
            except ProtocolError as exc:
                if getattr(exc, "code", None) is not None and exc.code.value == "lease_lost":
                    if getattr(claim, "is_terminal", False):
                        return
                    self._notify_lease_lost(claim)
                    token._cancel()
                    return
                # Non-lease protocol errors: stop heartbeats for this claim.
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

    def _interruptible_sleep(
        self,
        seconds: float,
        stop_hb: threading.Event,
        claim: _ClaimLike | None = None,
    ) -> None:
        if seconds <= 0:
            return
        # Wall clock: Event.wait is interruptible. Injectable clocks often advance
        # instantly, so poll stop/terminal with a tiny real wait to avoid a tight
        # spin that races past handler terminals.
        if self._sleep is time.sleep:
            stop_hb.wait(timeout=seconds)
            return
        deadline = self._clock() + seconds
        while True:
            if stop_hb.is_set() or (claim is not None and _claim_finished(claim)):
                return
            remaining = deadline - self._clock()
            if remaining <= 0:
                self._sleep(0.0)
                return
            # Advance injectable time in one step, then yield wall time so other
            # threads (handler terminal) can publish before the next heartbeat.
            self._sleep(remaining)
            if stop_hb.wait(timeout=0.001):
                return
            if claim is not None and _claim_finished(claim):
                return
            return

    def _notify_lease_lost(self, claim: _ClaimLike) -> None:
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
    "HEARTBEAT_JITTER_RATIO",
    "CancellationToken",
    "ConsumerSupervisor",
    "HandlerErrorEvent",
    "LeaseLostEvent",
]
