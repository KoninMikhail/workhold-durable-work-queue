"""Bounded generation-safe claim long-poll loop (WORK-17 / API-09).

Wraps short :class:`ClaimService` attempts. Never holds a Session, transaction,
or pooled connection between attempts. Wake notifications and reconnect bumps
are hints only — eligibility remains authoritative in each claim attempt.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final, Literal

from workhold.infrastructure.postgres.claim_wakeup import QueueGenerationCoordinator
from workhold.intake.contracts import IntakeValidationError
from workhold.observability.metrics import KernelMetrics

ClockFn = Callable[[], float]
CancelProbeFn = Callable[[], bool]
ClaimAttemptFn = Callable[[], "ClaimAttemptBatch"]
WaitFn = Callable[[Mapping[str, int], float], bool]

LongPollResultCode = Literal[
    "task",
    "expired",
    "cancelled",
    "shutdown",
    "admission_rejected",
    "error",
]
AttemptReason = Literal["notification", "fallback"]

_DEFAULT_FALLBACK_SECONDS: Final[float] = 1.0
_DEFAULT_PROBE_SECONDS: Final[float] = 0.25
_ADMISSION_RETRY_AFTER_MS: Final[int] = 250


class ClaimWaitAborted(Exception):
    """Outstanding wait ended without a claim batch response body."""

    def __init__(self, reason: Literal["cancelled", "shutdown"]) -> None:
        self.reason = reason
        super().__init__(reason)


@dataclass(frozen=True, slots=True)
class ClaimAttemptBatch:
    """One authoritative multi-queue claim attempt outcome."""

    tasks: list[dict[str, Any]]
    queue_states: dict[str, str]
    server_time: datetime | None


class WaiterAdmission:
    """Bounded semaphore for authenticated positive ``wait_seconds`` waits."""

    __slots__ = ("_cond", "_slots", "_active", "_accepting")

    def __init__(self, max_outstanding: int) -> None:
        if max_outstanding < 1:
            raise ValueError("max_outstanding must be >= 1")
        self._cond = threading.Condition()
        self._slots = int(max_outstanding)
        self._active = 0
        self._accepting = True

    @property
    def active(self) -> int:
        with self._cond:
            return self._active

    @property
    def max_slots(self) -> int:
        return self._slots

    def stop_accepting(self) -> None:
        with self._cond:
            self._accepting = False
            self._cond.notify_all()

    def try_acquire(self) -> bool:
        with self._cond:
            if not self._accepting:
                return False
            if self._active >= self._slots:
                return False
            self._active += 1
            return True

    def release(self) -> None:
        with self._cond:
            if self._active > 0:
                self._active -= 1
            self._cond.notify_all()


class ClaimLongPollService:
    """Observe → claim → wait loop with 1s reconciliation and cancel probes."""

    __slots__ = (
        "_coordinator",
        "_admission",
        "_metrics",
        "_fallback_seconds",
        "_probe_seconds",
        "_clock",
        "_wait_fn",
    )

    def __init__(
        self,
        *,
        coordinator: QueueGenerationCoordinator,
        admission: WaiterAdmission,
        metrics: KernelMetrics | None = None,
        fallback_seconds: float = _DEFAULT_FALLBACK_SECONDS,
        probe_seconds: float = _DEFAULT_PROBE_SECONDS,
        clock: ClockFn | None = None,
        wait_fn: WaitFn | None = None,
    ) -> None:
        if fallback_seconds <= 0:
            raise ValueError("fallback_seconds must be positive")
        if probe_seconds <= 0:
            raise ValueError("probe_seconds must be positive")
        self._coordinator = coordinator
        self._admission = admission
        self._metrics = metrics
        self._fallback_seconds = float(fallback_seconds)
        self._probe_seconds = float(probe_seconds)
        self._clock = clock or time.monotonic
        self._wait_fn = wait_fn or self._default_wait

    def _default_wait(self, observed: Mapping[str, int], timeout: float) -> bool:
        return self._coordinator.wait_any(observed, timeout=timeout)

    def run(
        self,
        *,
        queues: Sequence[str],
        wait_seconds: int,
        attempt: ClaimAttemptFn,
        is_cancelled: CancelProbeFn,
        is_shutdown: CancelProbeFn,
    ) -> ClaimAttemptBatch:
        """Execute one claim request lifecycle.

        ``wait_seconds == 0`` performs a single attempt and never acquires a
        waiter slot. Positive waits acquire admission, then loop until task,
        empty expiry, cancel, or shutdown.
        """
        started = self._clock()
        charged = wait_seconds > 0
        if charged and not self._admission.try_acquire():
            self._record_outcome("admission_rejected", self._clock() - started)
            raise IntakeValidationError(
                "resource_exhausted",
                "claim long-poll waiter capacity exhausted",
                retryable=True,
                retry_after_ms=_ADMISSION_RETRY_AFTER_MS,
                details={
                    "limit": self._admission.max_slots,
                    "observed": self._admission.active,
                },
            )

        if self._metrics is not None and charged:
            self._metrics.set_long_poll_active(self._admission.active)

        outcome: LongPollResultCode = "error"
        last_batch = ClaimAttemptBatch(tasks=[], queue_states={}, server_time=None)
        try:
            if is_shutdown():
                outcome = "shutdown"
                raise ClaimWaitAborted("shutdown")
            if is_cancelled():
                outcome = "cancelled"
                raise ClaimWaitAborted("cancelled")

            if wait_seconds == 0:
                batch = attempt()
                outcome = "task" if batch.tasks else "expired"
                return batch

            deadline = started + float(wait_seconds)
            queue_names = tuple(queues)
            first = True
            while True:
                if is_shutdown():
                    outcome = "shutdown"
                    raise ClaimWaitAborted("shutdown")
                if is_cancelled():
                    outcome = "cancelled"
                    raise ClaimWaitAborted("cancelled")

                now = self._clock()
                if not first and now >= deadline:
                    outcome = "expired"
                    return ClaimAttemptBatch(
                        tasks=[],
                        queue_states=dict(last_batch.queue_states),
                        server_time=last_batch.server_time,
                    )

                observed = self._coordinator.snapshot_many(queue_names)
                batch = attempt()
                last_batch = batch
                first = False
                if batch.tasks:
                    outcome = "task"
                    return batch

                next_reconcile_at = self._clock() + self._fallback_seconds
                while True:
                    if is_shutdown():
                        outcome = "shutdown"
                        raise ClaimWaitAborted("shutdown")
                    if is_cancelled():
                        outcome = "cancelled"
                        raise ClaimWaitAborted("cancelled")

                    now = self._clock()
                    remaining = deadline - now
                    if remaining <= 0:
                        outcome = "expired"
                        return ClaimAttemptBatch(
                            tasks=[],
                            queue_states=dict(last_batch.queue_states),
                            server_time=last_batch.server_time,
                        )

                    until_reconcile = next_reconcile_at - now
                    if until_reconcile <= 0:
                        self._record_attempt("fallback")
                        break

                    slice_timeout = min(
                        remaining, self._probe_seconds, until_reconcile
                    )
                    changed = self._wait_fn(observed, slice_timeout)
                    if changed:
                        self._record_attempt("notification")
                        break
                    # Probe timeout with no generation change: re-check cancel only.
        except ClaimWaitAborted:
            raise
        except IntakeValidationError:
            raise
        except Exception:
            outcome = "error"
            raise
        finally:
            if charged:
                self._admission.release()
                if self._metrics is not None:
                    self._metrics.set_long_poll_active(self._admission.active)
                self._record_outcome(outcome, self._clock() - started)

    def _record_outcome(self, result: LongPollResultCode, duration: float) -> None:
        if self._metrics is None:
            return
        self._metrics.record_long_poll(
            result=result, duration_seconds=max(0.0, duration)
        )

    def _record_attempt(self, result: AttemptReason) -> None:
        if self._metrics is None:
            return
        self._metrics.record_long_poll_attempt(result=result)
