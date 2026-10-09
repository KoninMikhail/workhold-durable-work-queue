"""Unit tests for enqueue-time retry / dead-letter decisions (WORK-05, WORK-06)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from queue_service.domain import retry as retry_mod
from queue_service.domain.queue_control import BackoffStrategy, DomainValidationError, PolicyVersion


def _policy(
    *,
    version: int = 1,
    enabled: bool = True,
    max_attempts: int = 3,
    backoff: BackoffStrategy = BackoffStrategy.FIXED,
    delay: int = 30,
) -> retry_mod.EnqueuedRetryPolicy:
    return retry_mod.EnqueuedRetryPolicy(
        policy_version=PolicyVersion(value=version),
        enabled=enabled,
        max_attempts=max_attempts,
        backoff_strategy=backoff,
        retry_delay_seconds=delay,
    )


def test_decision_uses_enqueue_policy_not_current_queue_policy() -> None:
    """A changed current queue policy cannot affect a task holding an older snapshot."""
    enqueue_snapshot = _policy(version=1, enabled=True, max_attempts=2, delay=10)
    # "Current" queue policy after activation — never passed into the decision.
    _current_queue_policy = _policy(version=2, enabled=False, max_attempts=1, delay=999)

    queue_now = datetime(2026, 9, 19, 12, 0, 0, tzinfo=UTC)
    decision = retry_mod.decide_retry_or_dead_letter(
        policy=enqueue_snapshot,
        completed_attempt_count=1,
        queue_now=queue_now,
        cause=retry_mod.RetryCause.WORKER_FAILURE,
    )

    assert isinstance(decision, retry_mod.RetryScheduled)
    assert decision.kind is retry_mod.RetryOutcomeKind.RETRY_SCHEDULED
    assert decision.available_at == queue_now + timedelta(seconds=10)
    assert decision.policy_version == PolicyVersion(value=1)
    # Current disabled policy was never consulted; retry still scheduled.
    assert _current_queue_policy.enabled is False


def test_enabled_fixed_delay_schedules_while_below_max_attempts() -> None:
    policy = _policy(enabled=True, max_attempts=3, delay=45)
    queue_now = datetime(2026, 9, 19, 8, 0, 0, tzinfo=UTC)

    for completed in (1, 2):
        decision = retry_mod.decide_retry_or_dead_letter(
            policy=policy,
            completed_attempt_count=completed,
            queue_now=queue_now,
            cause=retry_mod.RetryCause.WORKER_FAILURE,
        )
        assert isinstance(decision, retry_mod.RetryScheduled)
        assert decision.available_at == queue_now + timedelta(seconds=45)


def test_enabled_exhaustion_dead_letters() -> None:
    policy = _policy(enabled=True, max_attempts=3, delay=45)
    queue_now = datetime(2026, 9, 19, 8, 0, 0, tzinfo=UTC)

    decision = retry_mod.decide_retry_or_dead_letter(
        policy=policy,
        completed_attempt_count=3,
        queue_now=queue_now,
        cause=retry_mod.RetryCause.LEASE_EXPIRY,
    )
    assert isinstance(decision, retry_mod.DeadLettered)
    assert decision.kind is retry_mod.RetryOutcomeKind.DEAD_LETTERED
    assert decision.reason == retry_mod.REASON_ATTEMPTS_EXHAUSTED
    assert decision.cause is retry_mod.RetryCause.LEASE_EXPIRY


def test_disabled_retry_dead_letters_first_worker_failure() -> None:
    policy = _policy(enabled=False, max_attempts=5, delay=60)
    queue_now = datetime(2026, 9, 19, 9, 0, 0, tzinfo=UTC)

    decision = retry_mod.decide_retry_or_dead_letter(
        policy=policy,
        completed_attempt_count=1,
        queue_now=queue_now,
        cause=retry_mod.RetryCause.WORKER_FAILURE,
    )
    assert isinstance(decision, retry_mod.DeadLettered)
    assert decision.reason == retry_mod.REASON_RETRY_DISABLED
    assert decision.cause is retry_mod.RetryCause.WORKER_FAILURE


def test_disabled_retry_dead_letters_first_lease_expiry() -> None:
    policy = _policy(enabled=False, max_attempts=5, delay=60)
    queue_now = datetime(2026, 9, 19, 9, 0, 0, tzinfo=UTC)

    decision = retry_mod.decide_retry_or_dead_letter(
        policy=policy,
        completed_attempt_count=1,
        queue_now=queue_now,
        cause=retry_mod.RetryCause.LEASE_EXPIRY,
    )
    assert isinstance(decision, retry_mod.DeadLettered)
    assert decision.reason == retry_mod.REASON_RETRY_DISABLED
    assert decision.cause is retry_mod.RetryCause.LEASE_EXPIRY


def test_available_at_uses_queue_store_time_and_bounded_delay() -> None:
    policy = _policy(enabled=True, max_attempts=4, delay=0)
    queue_now = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)

    decision = retry_mod.decide_retry_or_dead_letter(
        policy=policy,
        completed_attempt_count=1,
        queue_now=queue_now,
        cause=retry_mod.RetryCause.WORKER_FAILURE,
    )
    assert isinstance(decision, retry_mod.RetryScheduled)
    assert decision.available_at == queue_now


def test_rejects_unsupported_backoff_strategy() -> None:
    with pytest.raises(DomainValidationError) as exc:
        retry_mod.EnqueuedRetryPolicy(
            policy_version=PolicyVersion(value=1),
            enabled=True,
            max_attempts=2,
            backoff_strategy="exponential",  # type: ignore[arg-type]
            retry_delay_seconds=10,
        )
    assert exc.value.code == "validation_failed"


def test_rejects_unbounded_retry_delay() -> None:
    with pytest.raises(DomainValidationError) as exc:
        retry_mod.EnqueuedRetryPolicy(
            policy_version=PolicyVersion(value=1),
            enabled=True,
            max_attempts=2,
            backoff_strategy=BackoffStrategy.FIXED,
            retry_delay_seconds=86401,
        )
    assert exc.value.code == "validation_failed"


def test_rejects_completed_attempt_count_below_one() -> None:
    policy = _policy()
    with pytest.raises(DomainValidationError) as exc:
        retry_mod.decide_retry_or_dead_letter(
            policy=policy,
            completed_attempt_count=0,
            queue_now=datetime(2026, 9, 19, tzinfo=UTC),
            cause=retry_mod.RetryCause.WORKER_FAILURE,
        )
    assert exc.value.code == "validation_failed"


def test_enabled_max_attempts_one_dead_letters_first_failure() -> None:
    policy = _policy(enabled=True, max_attempts=1, delay=15)
    decision = retry_mod.decide_retry_or_dead_letter(
        policy=policy,
        completed_attempt_count=1,
        queue_now=datetime(2026, 9, 19, tzinfo=UTC),
        cause=retry_mod.RetryCause.WORKER_FAILURE,
    )
    assert isinstance(decision, retry_mod.DeadLettered)
    assert decision.reason == retry_mod.REASON_ATTEMPTS_EXHAUSTED
