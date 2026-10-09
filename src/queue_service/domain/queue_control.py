"""Named-queue control-plane domain contracts (pure; no HTTP/PostgreSQL).

Encodes accepted queue states, retry-policy validation, optimistic config
versions, and the runtime operation gate from ``runtime-semantics.md``.
Deployment ceilings are injected by callers; deployment-owned settings are
intentionally unrepresentable on mutation models.
"""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Final

# OpenAPI / storage absolute bound for retry_delay_seconds.
RETRY_DELAY_SECONDS_ABSOLUTE_MAX: Final[int] = 86400
_QUEUE_NAME_RE: Final[re.Pattern[str]] = re.compile(r"^[a-z0-9][a-z0-9._-]*$")
_QUEUE_NAME_MAX_LEN: Final[int] = 128
_ACTOR_ID_MAX_LEN: Final[int] = 128
_IDEMPOTENCY_KEY_MAX_LEN: Final[int] = 256


class DomainValidationError(ValueError):
    """Phase 3.1 validation/conflict failure projected from the domain layer."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(message)


class QueueState(str, Enum):
    """Persisted named-queue runtime state (CTRL-01 / ADR 010)."""

    ACTIVE = "active"
    PAUSED = "paused"
    DRAINING = "draining"


class BackoffStrategy(str, Enum):
    """Accepted retry backoff strategies (OpenAPI const / storage code 1)."""

    FIXED = "fixed"


class QueueOperation(str, Enum):
    """Runtime operations gated by queue state."""

    EXTERNAL_ENQUEUE = "external_enqueue"
    INTERNAL_SPAWN = "internal_spawn"
    CLAIM = "claim"
    LEASE_MUTATION = "lease_mutation"
    CANCEL = "cancel"
    DELIVERY_RELAY = "delivery_relay"


class OperationGateOutcome(str, Enum):
    """Result of evaluating a queue-state gate.

    ``PAUSED_EMPTY`` is a successful empty claim outcome, not an error.
    """

    ALLOWED = "allowed"
    REJECTED = "rejected"
    PAUSED_EMPTY = "paused_empty"


@dataclass(frozen=True, slots=True)
class ConfigVersion:
    """Optimistic concurrency token for queue configuration (CTRL-02)."""

    value: int


@dataclass(frozen=True, slots=True)
class PolicyVersion:
    """Immutable retry-policy version identity within a named queue."""

    value: int


@dataclass(frozen=True, slots=True)
class RetryPolicyDraft:
    """Validated retry-policy content (OpenAPI CreatePolicyRequest fields)."""

    enabled: bool
    max_attempts: int
    backoff_strategy: BackoffStrategy
    retry_delay_seconds: int

    @property
    def allowed_processing_attempts(self) -> int:
        """Disabled retry yields exactly one processing attempt (WORK-12)."""
        if not self.enabled:
            return 1
        return self.max_attempts


@dataclass(frozen=True, slots=True)
class AdminRequestMetadata:
    """Request metadata carried by admin mutations (Phase 3.1 audit/idempotency)."""

    actor_id: str
    request_id: str
    idempotency_key: str

    def __post_init__(self) -> None:
        if not isinstance(self.actor_id, str) or not (1 <= len(self.actor_id) <= _ACTOR_ID_MAX_LEN):
            raise DomainValidationError(
                "validation_failed",
                "actor_id must be a non-empty string up to 128 characters",
            )
        try:
            uuid.UUID(self.request_id)
        except (TypeError, ValueError) as exc:
            raise DomainValidationError(
                "validation_failed",
                "request_id must be a UUID string",
            ) from exc
        if not isinstance(self.idempotency_key, str) or not (
            1 <= len(self.idempotency_key) <= _IDEMPOTENCY_KEY_MAX_LEN
        ):
            raise DomainValidationError(
                "validation_failed",
                "idempotency_key must be 1..256 characters",
            )


@dataclass(frozen=True, slots=True)
class SetQueueStateMutation:
    """Runtime set-queue-state command (OpenAPI SetQueueStateRequest + metadata)."""

    expected_config_version: ConfigVersion
    state: QueueState
    metadata: AdminRequestMetadata


@dataclass(frozen=True, slots=True)
class CreatePolicyMutation:
    """Create an immutable retry-policy version (OpenAPI CreatePolicyRequest)."""

    policy: RetryPolicyDraft
    metadata: AdminRequestMetadata


@dataclass(frozen=True, slots=True)
class ActivatePolicyMutation:
    """Activate a policy version under optimistic concurrency."""

    expected_config_version: ConfigVersion
    policy_version: PolicyVersion
    metadata: AdminRequestMetadata


@dataclass(frozen=True, slots=True)
class CreateQueueMutation:
    """Explicit named-queue creation with initial policy."""

    name: str
    initial_policy: RetryPolicyDraft
    metadata: AdminRequestMetadata

    def __post_init__(self) -> None:
        if (
            not isinstance(self.name, str)
            or not (1 <= len(self.name) <= _QUEUE_NAME_MAX_LEN)
            or _QUEUE_NAME_RE.fullmatch(self.name) is None
        ):
            raise DomainValidationError(
                "validation_failed",
                "queue name must match OpenAPI pattern ^[a-z0-9][a-z0-9._-]*$ (1..128)",
            )


_OPERATION_GATE: Final[dict[tuple[QueueState, QueueOperation], OperationGateOutcome]] = {
    (QueueState.ACTIVE, QueueOperation.EXTERNAL_ENQUEUE): OperationGateOutcome.ALLOWED,
    (QueueState.PAUSED, QueueOperation.EXTERNAL_ENQUEUE): OperationGateOutcome.ALLOWED,
    (QueueState.DRAINING, QueueOperation.EXTERNAL_ENQUEUE): OperationGateOutcome.REJECTED,
    (QueueState.ACTIVE, QueueOperation.INTERNAL_SPAWN): OperationGateOutcome.ALLOWED,
    (QueueState.PAUSED, QueueOperation.INTERNAL_SPAWN): OperationGateOutcome.ALLOWED,
    (QueueState.DRAINING, QueueOperation.INTERNAL_SPAWN): OperationGateOutcome.ALLOWED,
    (QueueState.ACTIVE, QueueOperation.CLAIM): OperationGateOutcome.ALLOWED,
    (QueueState.PAUSED, QueueOperation.CLAIM): OperationGateOutcome.PAUSED_EMPTY,
    (QueueState.DRAINING, QueueOperation.CLAIM): OperationGateOutcome.ALLOWED,
    (QueueState.ACTIVE, QueueOperation.LEASE_MUTATION): OperationGateOutcome.ALLOWED,
    (QueueState.PAUSED, QueueOperation.LEASE_MUTATION): OperationGateOutcome.ALLOWED,
    (QueueState.DRAINING, QueueOperation.LEASE_MUTATION): OperationGateOutcome.ALLOWED,
    (QueueState.ACTIVE, QueueOperation.CANCEL): OperationGateOutcome.ALLOWED,
    (QueueState.PAUSED, QueueOperation.CANCEL): OperationGateOutcome.ALLOWED,
    (QueueState.DRAINING, QueueOperation.CANCEL): OperationGateOutcome.ALLOWED,
    (QueueState.ACTIVE, QueueOperation.DELIVERY_RELAY): OperationGateOutcome.ALLOWED,
    (QueueState.PAUSED, QueueOperation.DELIVERY_RELAY): OperationGateOutcome.ALLOWED,
    (QueueState.DRAINING, QueueOperation.DELIVERY_RELAY): OperationGateOutcome.ALLOWED,
}


def parse_queue_state(value: object) -> QueueState:
    """Parse a queue state string; reject unknown values with validation_failed."""
    if isinstance(value, QueueState):
        return value
    if not isinstance(value, str):
        raise DomainValidationError("validation_failed", "queue state must be a string")
    try:
        return QueueState(value)
    except ValueError as exc:
        raise DomainValidationError(
            "validation_failed",
            "queue state must be one of: active, paused, draining",
        ) from exc


def parse_config_version(value: object) -> ConfigVersion:
    """Parse optimistic config_version (int64 minimum 1)."""
    if isinstance(value, ConfigVersion):
        return value
    if isinstance(value, bool) or not isinstance(value, int):
        raise DomainValidationError(
            "validation_failed",
            "config_version must be an integer >= 1",
        )
    if value < 1:
        raise DomainValidationError(
            "validation_failed",
            "config_version must be an integer >= 1",
        )
    return ConfigVersion(value=value)


def parse_policy_version(value: object) -> PolicyVersion:
    """Parse policy version identity (int32 minimum 1)."""
    if isinstance(value, PolicyVersion):
        return value
    if isinstance(value, bool) or not isinstance(value, int):
        raise DomainValidationError(
            "validation_failed",
            "policy_version must be an integer >= 1",
        )
    if value < 1:
        raise DomainValidationError(
            "validation_failed",
            "policy_version must be an integer >= 1",
        )
    return PolicyVersion(value=value)


def assert_expected_config_version(
    *,
    current: ConfigVersion,
    expected: ConfigVersion,
) -> None:
    """Fail with config_version_conflict when optimistic versions diverge."""
    if current.value != expected.value:
        raise DomainValidationError(
            "config_version_conflict",
            "expected_config_version does not match current config_version",
        )


def validate_retry_policy_draft(
    *,
    enabled: bool,
    max_attempts: int,
    backoff_strategy: object,
    retry_delay_seconds: int,
    deployment_retry_delay_ceiling_seconds: int,
) -> RetryPolicyDraft:
    """Validate retry-policy draft against OpenAPI/storage bounds and deployment ceiling."""
    if not isinstance(enabled, bool):
        raise DomainValidationError("validation_failed", "enabled must be a boolean")

    if isinstance(max_attempts, bool) or not isinstance(max_attempts, int) or max_attempts < 1:
        raise DomainValidationError(
            "validation_failed",
            "max_attempts must be an integer >= 1",
        )

    if isinstance(backoff_strategy, BackoffStrategy):
        strategy = backoff_strategy
    elif isinstance(backoff_strategy, str):
        try:
            strategy = BackoffStrategy(backoff_strategy)
        except ValueError as exc:
            raise DomainValidationError(
                "validation_failed",
                "backoff_strategy must be 'fixed'",
            ) from exc
    else:
        raise DomainValidationError(
            "validation_failed",
            "backoff_strategy must be 'fixed'",
        )

    if (
        isinstance(deployment_retry_delay_ceiling_seconds, bool)
        or not isinstance(deployment_retry_delay_ceiling_seconds, int)
        or deployment_retry_delay_ceiling_seconds < 0
    ):
        raise DomainValidationError(
            "validation_failed",
            "deployment_retry_delay_ceiling_seconds must be an integer >= 0",
        )

    effective_ceiling = min(
        deployment_retry_delay_ceiling_seconds,
        RETRY_DELAY_SECONDS_ABSOLUTE_MAX,
    )

    if (
        isinstance(retry_delay_seconds, bool)
        or not isinstance(retry_delay_seconds, int)
        or retry_delay_seconds < 0
        or retry_delay_seconds > effective_ceiling
        or retry_delay_seconds > RETRY_DELAY_SECONDS_ABSOLUTE_MAX
    ):
        raise DomainValidationError(
            "validation_failed",
            "retry_delay_seconds must be >= 0 and <= deployment ceiling "
            f"(absolute max {RETRY_DELAY_SECONDS_ABSOLUTE_MAX})",
        )

    return RetryPolicyDraft(
        enabled=enabled,
        max_attempts=max_attempts,
        backoff_strategy=strategy,
        retry_delay_seconds=retry_delay_seconds,
    )


def evaluate_operation_gate(
    state: QueueState,
    operation: QueueOperation,
) -> OperationGateOutcome:
    """Return the runtime-semantics gate outcome for ``state`` × ``operation``."""
    if not isinstance(state, QueueState):
        raise DomainValidationError("validation_failed", "state must be a QueueState")
    if not isinstance(operation, QueueOperation):
        raise DomainValidationError(
            "validation_failed",
            "operation must be a QueueOperation",
        )
    return _OPERATION_GATE[(state, operation)]
