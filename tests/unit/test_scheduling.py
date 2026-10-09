"""Unit scaffolds for shared Queue-store scheduling policy (Phase 11 Plan 08 / WORK-15).

Plan 01 removes every temporary skip and implements SchedulingPolicy.resolve.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

_STORE_NOW = datetime(2026, 9, 19, 12, 0, 0, tzinfo=UTC)
_HORIZON_SECONDS = 86400


def test_null_available_at_resolves_to_store_now_immediate() -> None:
    from queue_service.scheduling import SchedulingPolicy

    policy = SchedulingPolicy(horizon_seconds=_HORIZON_SECONDS)
    decision = policy.resolve(None, store_now=_STORE_NOW)
    assert decision.available_at == _STORE_NOW
    assert decision.is_delayed is False


def test_past_available_at_persisted_unchanged_immediate() -> None:
    from queue_service.scheduling import SchedulingPolicy

    past = _STORE_NOW - timedelta(seconds=30)
    policy = SchedulingPolicy(horizon_seconds=_HORIZON_SECONDS)
    decision = policy.resolve(past, store_now=_STORE_NOW)
    assert decision.available_at == past
    assert decision.is_delayed is False


def test_current_available_at_persisted_unchanged_immediate() -> None:
    from queue_service.scheduling import SchedulingPolicy

    policy = SchedulingPolicy(horizon_seconds=_HORIZON_SECONDS)
    decision = policy.resolve(_STORE_NOW, store_now=_STORE_NOW)
    assert decision.available_at == _STORE_NOW
    assert decision.is_delayed is False


def test_in_horizon_future_available_at_persisted_unchanged_delayed() -> None:
    from queue_service.scheduling import SchedulingPolicy

    future = _STORE_NOW + timedelta(hours=6)
    policy = SchedulingPolicy(horizon_seconds=_HORIZON_SECONDS)
    decision = policy.resolve(future, store_now=_STORE_NOW)
    assert decision.available_at == future
    assert decision.is_delayed is True


def test_exact_86400_second_horizon_boundary_accepted_delayed() -> None:
    from queue_service.scheduling import SchedulingPolicy

    boundary = _STORE_NOW + timedelta(seconds=_HORIZON_SECONDS)
    policy = SchedulingPolicy(horizon_seconds=_HORIZON_SECONDS)
    decision = policy.resolve(boundary, store_now=_STORE_NOW)
    assert decision.available_at == boundary
    assert decision.is_delayed is True


def test_one_microsecond_over_horizon_rejects_with_field_limit_metadata() -> None:
    from queue_service.scheduling import SchedulingPolicy, SchedulingValidationError

    over = _STORE_NOW + timedelta(seconds=_HORIZON_SECONDS, microseconds=1)
    with pytest.raises(SchedulingValidationError) as exc_info:
        SchedulingPolicy(horizon_seconds=_HORIZON_SECONDS).resolve(
            over,
            store_now=_STORE_NOW,
        )
    err = exc_info.value
    assert err.code == "validation_failed"
    assert err.retryable is False
    assert "field" in err.details or "limit" in err.details
    assert str(_STORE_NOW) not in err.message


@pytest.mark.parametrize(
    "naive",
    [
        datetime(2026, 9, 20, 12, 0, 0),
        datetime(2026, 9, 20, 12, 0, 0, tzinfo=UTC).replace(tzinfo=None),
    ],
)
def test_naive_available_at_rejects_with_stable_metadata(naive: datetime) -> None:
    from queue_service.scheduling import SchedulingPolicy, SchedulingValidationError

    with pytest.raises(SchedulingValidationError) as exc_info:
        SchedulingPolicy(horizon_seconds=_HORIZON_SECONDS).resolve(
            naive,
            store_now=_STORE_NOW,
        )
    err = exc_info.value
    assert err.code == "validation_failed"
    assert err.retryable is False


def test_horizon_zero_rejects_every_future_instant() -> None:
    from queue_service.scheduling import SchedulingPolicy, SchedulingValidationError

    future = _STORE_NOW + timedelta(microseconds=1)
    with pytest.raises(SchedulingValidationError):
        SchedulingPolicy(horizon_seconds=0).resolve(future, store_now=_STORE_NOW)

    policy = SchedulingPolicy(horizon_seconds=0)
    assert policy.resolve(None, store_now=_STORE_NOW).is_delayed is False
    assert policy.resolve(_STORE_NOW, store_now=_STORE_NOW).is_delayed is False
