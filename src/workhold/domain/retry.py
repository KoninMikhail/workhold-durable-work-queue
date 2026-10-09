"""Pure retry / dead-letter decision from an enqueue-time policy snapshot.

Callers supply the immutable policy version referenced by the task, the
completed attempt count (including the just-ended attempt), Queue-store
transition time, and cause. The decision never reads the queue's current
policy, worker clocks, or persistence.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Final

from workhold.domain.queue_control import (
    RETRY_DELAY_SECONDS_ABSOLUTE_MAX,
    BackoffStrategy,
    DomainValidationError,
    PolicyVersion,
)

REASON_RETRY_DISABLED: Final[str] = "retry_disabled"
REASON_ATTEMPTS_EXHAUSTED: Final[str] = "attempts_exhausted"

# Server-assigned diagnostic for lease-expiry dead letters (Phase 3.1 failure_code
# contract: 1..128 lowercase ASCII matching ^[a-z][a-z0-9._-]{0,127}$).
LEASE_EXPIRY_FAILURE_CODE: Final[str] = "lease.expired"


class RetryCause(str, Enum):
    """Failure transition causes that close a processing attempt."""

    WORKER_FAILURE = "worker_failure"
    LEASE_EXPIRY = "lease_expiry"


class RetryOutcomeKind(str, Enum):
    """Deterministic outcomes of a failure / lease-expiry decision."""

    RETRY_SCHEDULED = "retry_scheduled"
    DEAD_LETTERED = "dead_lettered"


@dataclass(frozen=True, slots=True)
class EnqueuedRetryPolicy:
    """Immutable retry-policy snapshot selected for a task at enqueue."""

    policy_version: PolicyVersion
    enabled: bool
    max_attempts: int
    backoff_strategy: BackoffStrategy
    retry_delay_seconds: int

    def __post_init__(self) -> None:
        if not isinstance(self.policy_version, PolicyVersion):
            raise DomainValidationError(
                "validation_failed",
                "policy_version must be a PolicyVersion",
            )
        if not isinstance(self.enabled, bool):
            raise DomainValidationError("validation_failed", "enabled must be a boolean")
        if (
            isinstance(self.max_attempts, bool)
            or not isinstance(self.max_attempts, int)
            or self.max_attempts < 1
        ):
            raise DomainValidationError(
                "validation_failed",
                "max_attempts must be an integer >= 1",
            )
        if not isinstance(self.backoff_strategy, BackoffStrategy):
            raise DomainValidationError(
                "validation_failed",
                "backoff_strategy must be BackoffStrategy.FIXED",
            )
        if self.backoff_strategy is not BackoffStrategy.FIXED:
            raise DomainValidationError(
                "validation_failed",
                "backoff_strategy must be 'fixed'",
            )
        if (
            isinstance(self.retry_delay_seconds, bool)
            or not isinstance(self.retry_delay_seconds, int)
            or self.retry_delay_seconds < 0
            or self.retry_delay_seconds > RETRY_DELAY_SECONDS_ABSOLUTE_MAX
        ):
            raise DomainValidationError(
                "validation_failed",
                "retry_delay_seconds must be >= 0 and "
                f"<= {RETRY_DELAY_SECONDS_ABSOLUTE_MAX}",
            )

    @property
    def allowed_processing_attempts(self) -> int:
        """Disabled retry yields exactly one processing attempt (ADR 008)."""
        if not self.enabled:
            return 1
        return self.max_attempts


@dataclass(frozen=True, slots=True)
class RetryScheduled:
    """Schedule another claim after a bounded fixed delay."""

    kind: RetryOutcomeKind
    available_at: datetime
    policy_version: PolicyVersion


@dataclass(frozen=True, slots=True)
class DeadLettered:
    """Terminal dead-letter outcome with a stable diagnostic reason."""

    kind: RetryOutcomeKind
    reason: str
    policy_version: PolicyVersion
    cause: RetryCause


def decide_retry_or_dead_letter(
    *,
    policy: EnqueuedRetryPolicy,
    completed_attempt_count: int,
    queue_now: datetime,
    cause: RetryCause,
) -> RetryScheduled | DeadLettered:
    """Decide retry vs dead-letter using only the task's enqueue-time policy.

    ``completed_attempt_count`` must include the attempt that just ended.
    ``available_at`` is always ``queue_now + retry_delay_seconds`` when retrying.
    """
    if not isinstance(policy, EnqueuedRetryPolicy):
        raise DomainValidationError(
            "validation_failed",
            "policy must be an EnqueuedRetryPolicy snapshot",
        )
    if (
        isinstance(completed_attempt_count, bool)
        or not isinstance(completed_attempt_count, int)
        or completed_attempt_count < 1
    ):
        raise DomainValidationError(
            "validation_failed",
            "completed_attempt_count must be an integer >= 1 "
            "(include the just-ended attempt)",
        )
    if not isinstance(queue_now, datetime):
        raise DomainValidationError(
            "validation_failed",
            "queue_now must be a datetime from Queue-store time",
        )
    if not isinstance(cause, RetryCause):
        raise DomainValidationError(
            "validation_failed",
            "cause must be RetryCause.WORKER_FAILURE or RetryCause.LEASE_EXPIRY",
        )

    allowed = policy.allowed_processing_attempts
    if completed_attempt_count >= allowed:
        reason = (
            REASON_RETRY_DISABLED if not policy.enabled else REASON_ATTEMPTS_EXHAUSTED
        )
        return DeadLettered(
            kind=RetryOutcomeKind.DEAD_LETTERED,
            reason=reason,
            policy_version=policy.policy_version,
            cause=cause,
        )

    return RetryScheduled(
        kind=RetryOutcomeKind.RETRY_SCHEDULED,
        available_at=queue_now + timedelta(seconds=policy.retry_delay_seconds),
        policy_version=policy.policy_version,
    )
