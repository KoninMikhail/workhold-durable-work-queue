"""Deterministic clock, sleep, and jitter helpers for client tests."""

from __future__ import annotations

import asyncio
import random
from collections.abc import Callable
from dataclasses import dataclass, field


@dataclass
class DeterministicClock:
    """Monotonic clock with injectable sleep for retry/backoff tests."""

    start_s: float = 0.0
    _now_s: float = field(init=False)
    _sleep_log: list[float] = field(default_factory=list)
    _rng: random.Random = field(default_factory=lambda: random.Random(0))

    def __post_init__(self) -> None:
        self._now_s = float(self.start_s)

    def time(self) -> float:
        return self._now_s

    def advance(self, seconds: float) -> float:
        if seconds < 0:
            raise ValueError("seconds must be >= 0")
        self._now_s += float(seconds)
        return self._now_s

    def sleep(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError("seconds must be >= 0")
        self._sleep_log.append(float(seconds))
        self.advance(seconds)

    async def async_sleep(self, seconds: float) -> None:
        self.sleep(seconds)
        await asyncio.sleep(0)

    def jitter(self, span: float) -> float:
        if span < 0:
            raise ValueError("span must be >= 0")
        if span == 0:
            return 0.0
        return self._rng.uniform(-span, span)

    def reseed(self, seed: int) -> None:
        self._rng.seed(seed)

    @property
    def sleep_log(self) -> tuple[float, ...]:
        return tuple(self._sleep_log)

    def as_time_fn(self) -> Callable[[], float]:
        return self.time

    def as_sleep_fn(self) -> Callable[[float], None]:
        return self.sleep

    def as_async_sleep_fn(self) -> Callable[[float], object]:
        return self.async_sleep
