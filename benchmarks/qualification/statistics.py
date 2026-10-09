"""Deterministic nearest-rank percentiles and success-ratio helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from math import ceil
from typing import Iterable, Mapping, Sequence


class Outcome(str, Enum):
    SUCCESS = "success"
    EMPTY = "empty"
    ERROR = "error"
    PLANNED_PAUSE = "planned_pause"
    PLANNED_DRAIN = "planned_drain"
    INVALID_REQUEST = "invalid_request"


@dataclass(frozen=True, slots=True)
class LatencySample:
    operation: str
    scenario: str
    latency_ns: int
    outcome: Outcome
    fan_out: int = 0


@dataclass(slots=True)
class OperationStats:
    operation: str
    scenario: str
    count: int = 0
    valid_attempts: int = 0
    successes: int = 0
    errors: int = 0
    planned_pause: int = 0
    planned_drain: int = 0
    invalid_requests: int = 0
    latency_samples_ns: list[int] = field(default_factory=list)
    p50_ns: int | None = None
    p99_ns: int | None = None

    def finalize(self) -> None:
        if self.latency_samples_ns:
            self.p50_ns = nearest_rank_percentile(self.latency_samples_ns, 50)
            self.p99_ns = nearest_rank_percentile(self.latency_samples_ns, 99)


def nearest_rank_percentile(samples: Sequence[int], percentile: int) -> int:
    """Nearest-rank percentile over integer nanosecond samples.

    rank = ceil(percentile/100 * n), 1-indexed into the ascending sample list.
    """
    if not samples:
        raise ValueError("empty sample set")
    if percentile <= 0 or percentile > 100:
        raise ValueError("percentile must be in (0, 100]")
    ordered = sorted(int(v) for v in samples)
    rank = max(1, ceil(percentile / 100 * len(ordered)))
    return ordered[rank - 1]


def success_ratio(successes: int, valid_attempts: int) -> float:
    if valid_attempts <= 0:
        raise ValueError("valid_attempts must be positive")
    return successes / valid_attempts


def aggregate_operation_stats(
    samples: Iterable[LatencySample],
) -> Mapping[tuple[str, str], OperationStats]:
    """Aggregate per (operation, scenario).

    Empty claims are successful valid operations. Planned pause/drain and
    intentionally invalid requests are excluded from the success denominator.
    Max-fanout complete remains on scenario=max_fanout and is never merged into
    baseline complete percentiles.
    """
    buckets: dict[tuple[str, str], OperationStats] = {}
    for sample in samples:
        key = (sample.operation, sample.scenario)
        stats = buckets.get(key)
        if stats is None:
            stats = OperationStats(operation=sample.operation, scenario=sample.scenario)
            buckets[key] = stats
        stats.count += 1
        if sample.outcome is Outcome.PLANNED_PAUSE:
            stats.planned_pause += 1
            continue
        if sample.outcome is Outcome.PLANNED_DRAIN:
            stats.planned_drain += 1
            continue
        if sample.outcome is Outcome.INVALID_REQUEST:
            stats.invalid_requests += 1
            continue
        stats.valid_attempts += 1
        stats.latency_samples_ns.append(sample.latency_ns)
        if sample.outcome is Outcome.ERROR:
            stats.errors += 1
        elif sample.outcome in (Outcome.SUCCESS, Outcome.EMPTY):
            stats.successes += 1
        else:
            raise ValueError(f"unsupported outcome: {sample.outcome!r}")
    for stats in buckets.values():
        stats.finalize()
    return buckets
