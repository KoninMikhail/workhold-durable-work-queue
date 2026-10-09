"""Enqueue-path adaptive rate gates and hard deployment ceilings (OPS-07).

Soft token buckets may only tighten hard ceilings. Idempotent replay must be
resolved before invoking the gate so clients can recover original task identity
under overload.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from threading import Lock
from typing import Final

from workhold.admission.adaptive import (
    AdaptivePressureController,
    OverloadMode,
)
from workhold.intake.contracts import IntakeValidationError

# Soft defaults stay at or below Phase 3.9 qualification envelope.
HARD_QUEUE_ENQUEUE_RPS_CEILING: Final[float] = 100.0
HARD_INSTANCE_ENQUEUE_RPS_CEILING: Final[float] = 500.0

_HINT_THROTTLE: Final[str] = "enqueue_throttled_pressure"
_HINT_READINESS: Final[str] = "readiness_pressure"


@dataclass(frozen=True, slots=True)
class AdaptiveEnqueueConfig:
    """Queue/instance enqueue rates; soft throttle rates may only tighten."""

    queue_enqueue_rps: float = HARD_QUEUE_ENQUEUE_RPS_CEILING
    instance_enqueue_rps: float = HARD_INSTANCE_ENQUEUE_RPS_CEILING
    throttle_queue_enqueue_rps: float = 10.0
    throttle_instance_enqueue_rps: float = 50.0
    retry_after_ms: int = 250
    readiness_retry_after_ms: int = 1000

    def __post_init__(self) -> None:
        if self.queue_enqueue_rps > HARD_QUEUE_ENQUEUE_RPS_CEILING:
            raise ValueError(
                "queue_enqueue_rps cannot raise the hard deployment ceiling"
            )
        if self.instance_enqueue_rps > HARD_INSTANCE_ENQUEUE_RPS_CEILING:
            raise ValueError(
                "instance_enqueue_rps cannot raise the hard deployment ceiling"
            )
        if self.throttle_queue_enqueue_rps > self.queue_enqueue_rps:
            raise ValueError(
                "throttle_queue_enqueue_rps cannot exceed queue_enqueue_rps"
            )
        if self.throttle_instance_enqueue_rps > self.instance_enqueue_rps:
            raise ValueError(
                "throttle_instance_enqueue_rps cannot exceed instance_enqueue_rps"
            )
        if self.queue_enqueue_rps < 0 or self.instance_enqueue_rps < 0:
            raise ValueError("enqueue rps must be >= 0")
        if self.throttle_queue_enqueue_rps < 0 or self.throttle_instance_enqueue_rps < 0:
            raise ValueError("throttle enqueue rps must be >= 0")
        if self.retry_after_ms < 0 or self.readiness_retry_after_ms < 0:
            raise ValueError("retry_after_ms must be >= 0")


@dataclass
class _TokenBucket:
    rate_per_second: float
    capacity: float
    tokens: float
    updated_monotonic: float

    def refill(self, now: float) -> None:
        elapsed = max(0.0, now - self.updated_monotonic)
        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate_per_second)
        self.updated_monotonic = now

    def try_consume(self, now: float, amount: float = 1.0) -> bool:
        self.refill(now)
        if self.tokens >= amount:
            self.tokens -= amount
            return True
        return False


@dataclass
class AdaptiveEnqueueGate:
    """Apply pressure mode + token buckets to *new* producer enqueues."""

    controller: AdaptivePressureController
    config: AdaptiveEnqueueConfig = field(default_factory=AdaptiveEnqueueConfig)
    monotonic_clock: Callable[[], float] = field(default=time.monotonic)
    _lock: Lock = field(default_factory=Lock, init=False, repr=False)
    _queue_buckets: dict[str, _TokenBucket] = field(
        default_factory=dict, init=False, repr=False
    )
    _instance_bucket: _TokenBucket | None = field(default=None, init=False, repr=False)
    _rejection_counts: dict[str, int] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        cfg = self.config
        now = self.monotonic_clock()
        self._instance_bucket = _TokenBucket(
            rate_per_second=cfg.instance_enqueue_rps,
            capacity=max(cfg.instance_enqueue_rps, 1.0),
            tokens=max(cfg.instance_enqueue_rps, 1.0),
            updated_monotonic=now,
        )

    def check_new_enqueue(self, *, queue_name: str) -> None:
        """Reject new enqueue under readiness failure or exhausted throttle buckets."""
        now = self.monotonic_clock()
        mode = self.controller.mode
        with self._lock:
            if mode is OverloadMode.READINESS_FAILURE:
                self._inc("readiness_pressure")
                raise IntakeValidationError(
                    "dependency_unavailable",
                    "PostgreSQL pressure readiness failure",
                    retryable=True,
                    retry_after_ms=self.config.readiness_retry_after_ms,
                    details={"hint": _HINT_READINESS},
                )
            if mode is OverloadMode.ENQUEUE_THROTTLE:
                if not self._try_throttle_tokens(queue_name=queue_name, now=now):
                    self._inc("enqueue_throttled_pressure")
                    raise IntakeValidationError(
                        "resource_exhausted",
                        "enqueue rate limited under PostgreSQL pressure",
                        retryable=True,
                        retry_after_ms=self.config.retry_after_ms,
                        details={"hint": _HINT_THROTTLE},
                    )

    def _try_throttle_tokens(self, *, queue_name: str, now: float) -> bool:
        assert self._instance_bucket is not None
        cfg = self.config
        queue_bucket = self._queue_buckets.get(queue_name)
        if queue_bucket is None:
            capacity = max(cfg.throttle_queue_enqueue_rps, 0.0)
            queue_bucket = _TokenBucket(
                rate_per_second=cfg.throttle_queue_enqueue_rps,
                capacity=max(capacity, 0.0),
                tokens=capacity,
                updated_monotonic=now,
            )
            self._queue_buckets[queue_name] = queue_bucket
        else:
            queue_bucket.rate_per_second = cfg.throttle_queue_enqueue_rps
            queue_bucket.capacity = max(cfg.throttle_queue_enqueue_rps, 0.0)

        self._instance_bucket.rate_per_second = cfg.throttle_instance_enqueue_rps
        self._instance_bucket.capacity = max(cfg.throttle_instance_enqueue_rps, 0.0)

        if not queue_bucket.try_consume(now):
            return False
        if not self._instance_bucket.try_consume(now):
            queue_bucket.tokens = min(
                queue_bucket.capacity, queue_bucket.tokens + 1.0
            )
            return False
        return True

    def _inc(self, name: str) -> None:
        self._rejection_counts[name] = self._rejection_counts.get(name, 0) + 1


def check_adaptive_new_enqueue(
    gate: AdaptiveEnqueueGate,
    *,
    queue_name: str,
    is_idempotent_replay: bool,
) -> None:
    """Gate a new enqueue after idempotency resolution; replay bypasses throttle."""
    if is_idempotent_replay:
        return
    gate.check_new_enqueue(queue_name=queue_name)
