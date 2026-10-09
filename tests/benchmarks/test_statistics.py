"""Deterministic nearest-rank percentiles and success-ratio semantics (QUAL-03/05)."""

from __future__ import annotations

import pytest

from benchmarks.qualification.statistics import (
    LatencySample,
    Outcome,
    aggregate_operation_stats,
    nearest_rank_percentile,
    success_ratio,
)


def test_nearest_rank_p50_p99_from_integer_nanosecond_samples() -> None:
    samples = list(range(1, 101))
    assert nearest_rank_percentile(samples, 50) == 50
    assert nearest_rank_percentile(samples, 99) == 99


def test_nearest_rank_is_deterministic_and_order_independent() -> None:
    samples = [10, 30, 20, 50, 40]
    assert nearest_rank_percentile(samples, 50) == nearest_rank_percentile(
        sorted(samples, reverse=True), 50
    )
    assert nearest_rank_percentile(samples, 50) == 30


def test_nearest_rank_rejects_empty_samples() -> None:
    with pytest.raises(ValueError, match="empty"):
        nearest_rank_percentile([], 50)


def test_empty_claims_count_as_successful_valid_operations() -> None:
    samples = [
        LatencySample("claim", "baseline", 1_000_000, Outcome.SUCCESS),
        LatencySample("claim", "baseline", 2_000_000, Outcome.EMPTY),
        LatencySample("claim", "baseline", 3_000_000, Outcome.ERROR),
    ]
    stats = aggregate_operation_stats(samples)[("claim", "baseline")]
    assert stats.valid_attempts == 3
    assert stats.successes == 2
    assert stats.errors == 1
    assert success_ratio(stats.successes, stats.valid_attempts) == pytest.approx(2 / 3)


def test_planned_pause_drain_and_invalid_excluded_from_denominator() -> None:
    samples = [
        LatencySample("claim", "baseline", 1_000_000, Outcome.SUCCESS),
        LatencySample("claim", "baseline", 2_000_000, Outcome.EMPTY),
        LatencySample("claim", "pause", 3_000_000, Outcome.PLANNED_PAUSE),
        LatencySample("claim", "drain", 4_000_000, Outcome.PLANNED_DRAIN),
        LatencySample("enqueue", "invalid", 5_000_000, Outcome.INVALID_REQUEST),
    ]
    by_key = aggregate_operation_stats(samples)
    claim = by_key[("claim", "baseline")]
    assert claim.valid_attempts == 2
    assert claim.successes == 2
    assert by_key[("claim", "pause")].planned_pause == 1
    assert by_key[("claim", "pause")].valid_attempts == 0
    assert by_key[("claim", "drain")].planned_drain == 1
    assert by_key[("enqueue", "invalid")].invalid_requests == 1
    assert by_key[("enqueue", "invalid")].valid_attempts == 0


def test_max_fanout_complete_not_merged_into_baseline_complete_p99() -> None:
    samples = [
        LatencySample("complete", "baseline", 10_000_000, Outcome.SUCCESS, fan_out=0),
        LatencySample("complete", "baseline", 20_000_000, Outcome.SUCCESS, fan_out=8),
        LatencySample(
            "complete", "max_fanout", 900_000_000, Outcome.SUCCESS, fan_out=64
        ),
    ]
    by_key = aggregate_operation_stats(samples)
    assert by_key[("complete", "baseline")].p99_ns == 20_000_000
    assert by_key[("complete", "max_fanout")].p99_ns == 900_000_000
