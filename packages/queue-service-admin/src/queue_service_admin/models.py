"""Admin/observer domain models with OpenAPI bounds and tolerant parsing."""

from __future__ import annotations

import re
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Final

from _queue_service_client_core.models import Task

PAGE_LIMIT_MIN: Final[int] = 1
PAGE_LIMIT_MAX: Final[int] = 100
PAGE_LIMIT_DEFAULT: Final[int] = 50
CURSOR_MAX_LENGTH: Final[int] = 512
QUEUE_NAME_MAX_LENGTH: Final[int] = 128
IDEMPOTENCY_KEY_MIN_LENGTH: Final[int] = 1
IDEMPOTENCY_KEY_MAX_LENGTH: Final[int] = 256
REASON_MAX_LENGTH: Final[int] = 512
CONFIRMATION_TOKEN_MAX_LENGTH: Final[int] = 24576
BULK_CANDIDATE_MAX: Final[int] = 100
BULK_BATCH_MAX: Final[int] = 25
BULK_SAMPLE_MAX: Final[int] = 5
POLICY_VERSION_MIN: Final[int] = 1
CONFIG_VERSION_MIN: Final[int] = 1
RETRY_DELAY_SECONDS_MAX: Final[int] = 86400
MAX_ATTEMPTS_MIN: Final[int] = 1
BREAK_GLASS_REASON_MAX_LENGTH: Final[int] = 512
INCIDENT_REFERENCE_MAX_LENGTH: Final[int] = 128
PARTITION_NAME_MAX_LENGTH: Final[int] = 128
REPLAY_FACTOR_MIN: Final[float] = 1.0
REPLAY_FACTOR_MAX: Final[float] = 10.0
REPLAY_TTL_SECONDS_MIN: Final[int] = 1
REPLAY_TTL_SECONDS_MAX: Final[int] = 3600
REGISTRY_ENTRY_ID_MIN: Final[int] = 1
EXTEND_SECONDS_MIN: Final[int] = 1
EXTEND_SECONDS_MAX: Final[int] = 2592000
FAILURE_CODE_MAX_LENGTH: Final[int] = 128

_QUEUE_NAME_RE: Final[re.Pattern[str]] = re.compile(r"^[a-z0-9][a-z0-9._-]*$")
_PARTITION_NAME_RE: Final[re.Pattern[str]] = re.compile(r"^[a-z][a-z0-9_]*_[0-9]{8}$")

KNOWN_QUEUE_STATES: Final[frozenset[str]] = frozenset({"active", "paused", "draining"})
KNOWN_BACKOFF_STRATEGIES: Final[frozenset[str]] = frozenset({"fixed"})
KNOWN_ATTEMPT_OUTCOMES: Final[frozenset[str]] = frozenset(
    {
        "active",
        "succeeded",
        "retry_scheduled",
        "dead_lettered",
        "expired",
        "cancelled",
    }
)
KNOWN_MAINTENANCE_OUTCOMES: Final[frozenset[str]] = frozenset(
    {"succeeded", "failed", "skipped_lock"}
)
KNOWN_STATS_FRESHNESS: Final[frozenset[str]] = frozenset(
    {"fresh", "stale", "unavailable"}
)
KNOWN_STATS_AVAILABILITY: Final[frozenset[str]] = frozenset({"available", "unavailable"})
KNOWN_STATS_TELEMETRY_SOURCE: Final[frozenset[str]] = frozenset({"process_telemetry"})

KNOWN_AUDIT_OPERATIONS: Final[frozenset[str]] = frozenset(
    {
        "create_queue",
        "create_policy",
        "activate_policy",
        "set_queue_state",
        "run_maintenance",
        "replay_dead_letter",
        "bulk_replay",
        "bulk_cancel",
    }
)
KNOWN_BULK_OPERATIONS: Final[frozenset[str]] = frozenset({"bulk_replay", "bulk_cancel"})
_FORBIDDEN_BULK_FILTER_KEYS: Final[frozenset[str]] = frozenset(
    {
        "q",
        "query",
        "search",
        "text",
        "payload",
        "payload_field",
        "business_key",
        "worker_id",
        "sql",
        "offset",
    }
)
_ALLOWED_BULK_FILTER_KEYS: Final[frozenset[str]] = frozenset(
    {
        "from",
        "to",
        "failure_code",
        "state",
        "states",
    }
)
_FORBIDDEN_BREAK_GLASS_SECRET_KEY_RE: Final[re.Pattern[str]] = re.compile(
    r"claim.*token|lease_token",
    re.IGNORECASE,
)
KNOWN_BULK_ITEM_OUTCOMES: Final[frozenset[str]] = frozenset(
    {
        "succeeded",
        "replayed",
        "skipped",
        "failed",
        "cancelled",
        "cancel_requested",
    }
)


@dataclass(frozen=True, slots=True)
class QueueState:
    value: str

    @property
    def is_unknown(self) -> bool:
        return self.value not in KNOWN_QUEUE_STATES

    @classmethod
    def parse(cls, raw: object) -> QueueState:
        if not isinstance(raw, str) or not raw:
            raise ValueError("queue.state must be a non-empty string")
        return cls(raw)


@dataclass(frozen=True, slots=True)
class BackoffStrategy:
    value: str

    @property
    def is_unknown(self) -> bool:
        return self.value not in KNOWN_BACKOFF_STRATEGIES

    @classmethod
    def parse(cls, raw: object) -> BackoffStrategy:
        if not isinstance(raw, str) or not raw:
            raise ValueError("backoff_strategy must be a non-empty string")
        return cls(raw)


@dataclass(frozen=True, slots=True)
class AttemptOutcome:
    value: str

    @property
    def is_unknown(self) -> bool:
        return self.value not in KNOWN_ATTEMPT_OUTCOMES

    @classmethod
    def parse(cls, raw: object) -> AttemptOutcome:
        if not isinstance(raw, str) or not raw:
            raise ValueError("attempt.outcome must be a non-empty string")
        return cls(raw)


@dataclass(frozen=True, slots=True)
class AuditOperation:
    value: str

    @property
    def is_unknown(self) -> bool:
        return self.value not in KNOWN_AUDIT_OPERATIONS

    @classmethod
    def parse(cls, raw: object) -> AuditOperation:
        if not isinstance(raw, str) or not raw:
            raise ValueError("audit.operation must be a non-empty string")
        return cls(raw)


@dataclass(frozen=True, slots=True)
class MaintenanceOutcome:
    value: str

    @property
    def is_unknown(self) -> bool:
        return self.value not in KNOWN_MAINTENANCE_OUTCOMES

    @classmethod
    def parse(cls, raw: object) -> MaintenanceOutcome:
        if not isinstance(raw, str) or not raw:
            raise ValueError("maintenance.outcome must be a non-empty string")
        return cls(raw)


@dataclass(frozen=True, slots=True)
class StatsFreshness:
    value: str

    @property
    def is_unknown(self) -> bool:
        return self.value not in KNOWN_STATS_FRESHNESS

    @classmethod
    def parse(cls, raw: object) -> StatsFreshness:
        if not isinstance(raw, str) or not raw:
            raise ValueError("freshness must be a non-empty string")
        return cls(raw)


@dataclass(frozen=True, slots=True)
class StatsAvailability:
    value: str

    @property
    def is_unknown(self) -> bool:
        return self.value not in KNOWN_STATS_AVAILABILITY

    @classmethod
    def parse(cls, raw: object) -> StatsAvailability:
        if not isinstance(raw, str) or not raw:
            raise ValueError("availability must be a non-empty string")
        return cls(raw)


@dataclass(frozen=True, slots=True)
class StatsTelemetrySource:
    value: str

    @property
    def is_unknown(self) -> bool:
        return self.value not in KNOWN_STATS_TELEMETRY_SOURCE

    @classmethod
    def parse(cls, raw: object) -> StatsTelemetrySource:
        if not isinstance(raw, str) or not raw:
            raise ValueError("telemetry.source must be a non-empty string")
        return cls(raw)


@dataclass(frozen=True, slots=True)
class ConfigVersion:
    """Optimistic concurrency pin for admin mutations (OpenAPI ``expected_config_version``)."""

    value: int

    def to_wire(self) -> int:
        return self.value

    @classmethod
    def parse(cls, raw: object) -> ConfigVersion:
        return cls(value=_require_config_version(raw, "expected_config_version"))


@dataclass(frozen=True, slots=True)
class PolicyVersion:
    """Immutable retry-policy version number (OpenAPI path ``policy_version``)."""

    value: int

    def to_wire(self) -> int:
        return self.value

    @classmethod
    def parse(cls, raw: object) -> PolicyVersion:
        return cls(value=_require_policy_version(raw, "policy_version"))


@dataclass(frozen=True, slots=True)
class RetryPolicyDraft:
    """Retry-policy content for create queue / create policy (OpenAPI draft schemas)."""

    enabled: bool
    max_attempts: int
    backoff_strategy: BackoffStrategy
    retry_delay_seconds: int

    def to_wire(self) -> dict[str, Any]:
        if self.backoff_strategy.value != "fixed":
            raise ValueError("backoff_strategy must be fixed for draft mutations")
        return {
            "enabled": self.enabled,
            "max_attempts": _require_max_attempts(self.max_attempts),
            "backoff_strategy": self.backoff_strategy.value,
            "retry_delay_seconds": _require_retry_delay_seconds(self.retry_delay_seconds),
        }

    @classmethod
    def parse(cls, raw: object) -> RetryPolicyDraft:
        if not isinstance(raw, Mapping):
            raise ValueError("retry policy draft must be an object")
        required = (
            "enabled",
            "max_attempts",
            "backoff_strategy",
            "retry_delay_seconds",
        )
        missing = [key for key in required if key not in raw]
        if missing:
            raise ValueError(f"retry policy draft missing fields: {missing}")
        strategy = BackoffStrategy.parse(raw["backoff_strategy"])
        if strategy.value != "fixed":
            raise ValueError("backoff_strategy must be fixed for draft mutations")
        return cls(
            enabled=_require_bool(raw["enabled"], "enabled"),
            max_attempts=_require_max_attempts(raw["max_attempts"]),
            backoff_strategy=strategy,
            retry_delay_seconds=_require_retry_delay_seconds(raw["retry_delay_seconds"]),
        )


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    version: int
    enabled: bool
    max_attempts: int
    backoff_strategy: BackoffStrategy
    retry_delay_seconds: int
    created_at: str
    extra: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def parse(cls, raw: object) -> RetryPolicy:
        if not isinstance(raw, Mapping):
            raise ValueError("active_policy must be an object")
        required = (
            "version",
            "enabled",
            "max_attempts",
            "backoff_strategy",
            "retry_delay_seconds",
            "created_at",
        )
        missing = [key for key in required if key not in raw]
        if missing:
            raise ValueError(f"active_policy missing fields: {missing}")
        known = set(required)
        extra = {k: v for k, v in raw.items() if k not in known}
        return cls(
            version=_require_int(raw["version"], "active_policy.version"),
            enabled=_require_bool(raw["enabled"], "active_policy.enabled"),
            max_attempts=_require_int(raw["max_attempts"], "active_policy.max_attempts"),
            backoff_strategy=BackoffStrategy.parse(raw["backoff_strategy"]),
            retry_delay_seconds=_require_int(
                raw["retry_delay_seconds"], "active_policy.retry_delay_seconds"
            ),
            created_at=_require_str(raw["created_at"], "active_policy.created_at"),
            extra=dict(extra),
        )


@dataclass(frozen=True, slots=True)
class Queue:
    queue_id: str
    name: str
    state: QueueState
    config_version: int
    active_policy: RetryPolicy
    created_at: str
    updated_at: str
    active_depth: int | None = None
    ready_count: int | None = None
    delayed_count: int | None = None
    leased_count: int | None = None
    drain_complete: bool | None = None
    extra: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def parse(cls, raw: object) -> Queue:
        if not isinstance(raw, Mapping):
            raise ValueError("queue must be an object")
        required = (
            "queue_id",
            "name",
            "state",
            "config_version",
            "active_policy",
            "created_at",
            "updated_at",
        )
        missing = [key for key in required if key not in raw]
        if missing:
            raise ValueError(f"queue missing required fields: {missing}")
        known = set(required) | {
            "active_depth",
            "ready_count",
            "delayed_count",
            "leased_count",
            "drain_complete",
        }
        extra = {k: v for k, v in raw.items() if k not in known}
        return cls(
            queue_id=_require_str(raw["queue_id"], "queue_id"),
            name=_require_str(raw["name"], "name"),
            state=QueueState.parse(raw["state"]),
            config_version=_require_int(raw["config_version"], "config_version"),
            active_policy=RetryPolicy.parse(raw["active_policy"]),
            created_at=_require_str(raw["created_at"], "created_at"),
            updated_at=_require_str(raw["updated_at"], "updated_at"),
            active_depth=_optional_int(raw.get("active_depth"), "active_depth"),
            ready_count=_optional_int(raw.get("ready_count"), "ready_count"),
            delayed_count=_optional_int(raw.get("delayed_count"), "delayed_count"),
            leased_count=_optional_int(raw.get("leased_count"), "leased_count"),
            drain_complete=_optional_bool(raw.get("drain_complete"), "drain_complete"),
            extra=dict(extra),
        )


@dataclass(frozen=True, slots=True)
class Attempt:
    attempt_id: int
    task_id: str
    claim_id: str
    generation: int
    claimed_at: str
    worker_id: str
    lease_expires_at: str
    outcome: AttemptOutcome
    ended_at: str | None = None
    failure_code: str | None = None
    failure_detail: str | None = None
    extra: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def parse(cls, raw: object) -> Attempt:
        if not isinstance(raw, Mapping):
            raise ValueError("attempt must be an object")
        required = (
            "attempt_id",
            "task_id",
            "claim_id",
            "generation",
            "claimed_at",
            "worker_id",
            "lease_expires_at",
            "outcome",
        )
        missing = [key for key in required if key not in raw]
        if missing:
            raise ValueError(f"attempt missing required fields: {missing}")
        known = set(required) | {"ended_at", "failure_code", "failure_detail"}
        extra = {k: v for k, v in raw.items() if k not in known}
        return cls(
            attempt_id=_require_int(raw["attempt_id"], "attempt_id"),
            task_id=_require_str(raw["task_id"], "task_id"),
            claim_id=_require_str(raw["claim_id"], "claim_id"),
            generation=_require_int(raw["generation"], "generation"),
            claimed_at=_require_str(raw["claimed_at"], "claimed_at"),
            worker_id=_require_str(raw["worker_id"], "worker_id"),
            lease_expires_at=_require_str(raw["lease_expires_at"], "lease_expires_at"),
            outcome=AttemptOutcome.parse(raw["outcome"]),
            ended_at=_optional_str(raw.get("ended_at"), "ended_at"),
            failure_code=_optional_str(raw.get("failure_code"), "failure_code"),
            failure_detail=_optional_str(raw.get("failure_detail"), "failure_detail"),
            extra=dict(extra),
        )


@dataclass(frozen=True, slots=True)
class QueuePage:
    items: tuple[Queue, ...]
    next_cursor: str | None = None
    extra: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def parse(cls, raw: object) -> QueuePage:
        if not isinstance(raw, Mapping):
            raise ValueError("queue page must be an object")
        if "items" not in raw:
            raise ValueError("queue page missing items")
        items_raw = raw["items"]
        if not isinstance(items_raw, list):
            raise ValueError("queue page items must be an array")
        known = {"items", "next_cursor"}
        extra = {k: v for k, v in raw.items() if k not in known}
        return cls(
            items=tuple(Queue.parse(item) for item in items_raw),
            next_cursor=_optional_str(raw.get("next_cursor"), "next_cursor"),
            extra=dict(extra),
        )


@dataclass(frozen=True, slots=True)
class AdminMutationResult:
    queue: Queue
    replayed: bool
    admin_replay_expires_at: str
    extra: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def parse(cls, raw: object) -> AdminMutationResult:
        if not isinstance(raw, Mapping):
            raise ValueError("admin mutation result must be an object")
        required = ("queue", "replayed", "admin_replay_expires_at")
        missing = [key for key in required if key not in raw]
        if missing:
            raise ValueError(f"admin mutation result missing fields: {missing}")
        known = set(required)
        extra = {k: v for k, v in raw.items() if k not in known}
        return cls(
            queue=Queue.parse(raw["queue"]),
            replayed=_require_bool(raw["replayed"], "replayed"),
            admin_replay_expires_at=_require_str(
                raw["admin_replay_expires_at"], "admin_replay_expires_at"
            ),
            extra=dict(extra),
        )


@dataclass(frozen=True, slots=True)
class AttemptPage:
    items: tuple[Attempt, ...]
    next_cursor: str | None = None
    extra: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def parse(cls, raw: object) -> AttemptPage:
        if not isinstance(raw, Mapping):
            raise ValueError("attempt page must be an object")
        if "items" not in raw:
            raise ValueError("attempt page missing items")
        items_raw = raw["items"]
        if not isinstance(items_raw, list):
            raise ValueError("attempt page items must be an array")
        known = {"items", "next_cursor"}
        extra = {k: v for k, v in raw.items() if k not in known}
        return cls(
            items=tuple(Attempt.parse(item) for item in items_raw),
            next_cursor=_optional_str(raw.get("next_cursor"), "next_cursor"),
            extra=dict(extra),
        )


@dataclass(frozen=True, slots=True)
class TaskPage:
    items: tuple[Task, ...]
    next_cursor: str | None = None
    extra: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def parse(cls, raw: object) -> TaskPage:
        if not isinstance(raw, Mapping):
            raise ValueError("task page must be an object")
        if "items" not in raw:
            raise ValueError("task page missing items")
        items_raw = raw["items"]
        if not isinstance(items_raw, list):
            raise ValueError("task page items must be an array")
        known = {"items", "next_cursor"}
        extra = {k: v for k, v in raw.items() if k not in known}
        return cls(
            items=tuple(Task.parse(item) for item in items_raw),
            next_cursor=_optional_str(raw.get("next_cursor"), "next_cursor"),
            extra=dict(extra),
        )


@dataclass(frozen=True, slots=True)
class DeadLetterPage:
    items: tuple[Task, ...]
    next_cursor: str | None = None
    extra: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def parse(cls, raw: object) -> DeadLetterPage:
        if not isinstance(raw, Mapping):
            raise ValueError("dead letter page must be an object")
        if "items" not in raw:
            raise ValueError("dead letter page missing items")
        items_raw = raw["items"]
        if not isinstance(items_raw, list):
            raise ValueError("dead letter page items must be an array")
        known = {"items", "next_cursor"}
        extra = {k: v for k, v in raw.items() if k not in known}
        return cls(
            items=tuple(Task.parse(item) for item in items_raw),
            next_cursor=_optional_str(raw.get("next_cursor"), "next_cursor"),
            extra=dict(extra),
        )


@dataclass(frozen=True, slots=True)
class AuditRecord:
    audit_id: int
    audit_at: str
    actor_id: str
    operation: AuditOperation
    request_id: str
    queue_id: str | None = None
    previous_config_version: int | None = None
    new_config_version: int | None = None
    extra: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def parse(cls, raw: object) -> AuditRecord:
        if not isinstance(raw, Mapping):
            raise ValueError("audit record must be an object")
        required = ("audit_id", "audit_at", "actor_id", "operation", "request_id")
        missing = [key for key in required if key not in raw]
        if missing:
            raise ValueError(f"audit record missing required fields: {missing}")
        known = set(required) | {
            "queue_id",
            "previous_config_version",
            "new_config_version",
        }
        extra = {k: v for k, v in raw.items() if k not in known}
        return cls(
            audit_id=_require_int(raw["audit_id"], "audit_id"),
            audit_at=_require_str(raw["audit_at"], "audit_at"),
            actor_id=_require_str(raw["actor_id"], "actor_id"),
            operation=AuditOperation.parse(raw["operation"]),
            request_id=_require_str(raw["request_id"], "request_id"),
            queue_id=_optional_str(raw.get("queue_id"), "queue_id"),
            previous_config_version=_optional_int(
                raw.get("previous_config_version"), "previous_config_version"
            ),
            new_config_version=_optional_int(
                raw.get("new_config_version"), "new_config_version"
            ),
            extra=dict(extra),
        )


@dataclass(frozen=True, slots=True)
class AuditPage:
    items: tuple[AuditRecord, ...]
    next_cursor: str | None = None
    extra: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def parse(cls, raw: object) -> AuditPage:
        if not isinstance(raw, Mapping):
            raise ValueError("audit page must be an object")
        if "items" not in raw:
            raise ValueError("audit page missing items")
        items_raw = raw["items"]
        if not isinstance(items_raw, list):
            raise ValueError("audit page items must be an array")
        known = {"items", "next_cursor"}
        extra = {k: v for k, v in raw.items() if k not in known}
        return cls(
            items=tuple(AuditRecord.parse(item) for item in items_raw),
            next_cursor=_optional_str(raw.get("next_cursor"), "next_cursor"),
            extra=dict(extra),
        )


@dataclass(frozen=True, slots=True)
class MaintenanceStatus:
    updated_at: str
    last_started_at: str | None = None
    last_succeeded_at: str | None = None
    premade_through: str | None = None
    retained_from: str | None = None
    last_error_code: str | None = None
    outcome: MaintenanceOutcome | None = None
    maintenance_run_id: str | None = None
    extra: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def parse(cls, raw: object) -> MaintenanceStatus:
        if not isinstance(raw, Mapping):
            raise ValueError("maintenance status must be an object")
        if "updated_at" not in raw:
            raise ValueError("maintenance status missing updated_at")
        known = {
            "updated_at",
            "last_started_at",
            "last_succeeded_at",
            "premade_through",
            "retained_from",
            "last_error_code",
            "outcome",
            "maintenance_run_id",
        }
        extra = {k: v for k, v in raw.items() if k not in known}
        outcome_raw = raw.get("outcome")
        return cls(
            updated_at=_require_str(raw["updated_at"], "updated_at"),
            last_started_at=_optional_str(raw.get("last_started_at"), "last_started_at"),
            last_succeeded_at=_optional_str(
                raw.get("last_succeeded_at"), "last_succeeded_at"
            ),
            premade_through=_optional_str(raw.get("premade_through"), "premade_through"),
            retained_from=_optional_str(raw.get("retained_from"), "retained_from"),
            last_error_code=_optional_str(raw.get("last_error_code"), "last_error_code"),
            outcome=(
                None if outcome_raw is None else MaintenanceOutcome.parse(outcome_raw)
            ),
            maintenance_run_id=_optional_str(
                raw.get("maintenance_run_id"), "maintenance_run_id"
            ),
            extra=dict(extra),
        )


@dataclass(frozen=True, slots=True)
class MaintenanceRunResult:
    status: MaintenanceStatus
    replayed: bool
    admin_replay_expires_at: str
    extra: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def parse(cls, raw: object) -> MaintenanceRunResult:
        if not isinstance(raw, Mapping):
            raise ValueError("maintenance run result must be an object")
        required = ("status", "replayed", "admin_replay_expires_at")
        missing = [key for key in required if key not in raw]
        if missing:
            raise ValueError(f"maintenance run result missing fields: {missing}")
        known = set(required)
        extra = {k: v for k, v in raw.items() if k not in known}
        return cls(
            status=MaintenanceStatus.parse(raw["status"]),
            replayed=_require_bool(raw["replayed"], "replayed"),
            admin_replay_expires_at=_require_str(
                raw["admin_replay_expires_at"], "admin_replay_expires_at"
            ),
            extra=dict(extra),
        )


@dataclass(frozen=True, slots=True)
class StatsQueueAggregate:
    name: str
    ready_depth: int
    delayed_depth: int
    leased_depth: int
    as_of: str
    freshness: StatsFreshness
    oldest_ready_age_seconds: float | None = None
    extra: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def parse(cls, raw: object) -> StatsQueueAggregate:
        if not isinstance(raw, Mapping):
            raise ValueError("stats queue aggregate must be an object")
        required = (
            "name",
            "ready_depth",
            "delayed_depth",
            "leased_depth",
            "as_of",
            "freshness",
        )
        missing = [key for key in required if key not in raw]
        if missing:
            raise ValueError(f"stats queue aggregate missing fields: {missing}")
        known = set(required) | {"oldest_ready_age_seconds"}
        extra = {k: v for k, v in raw.items() if k not in known}
        age = raw.get("oldest_ready_age_seconds")
        if age is not None and not isinstance(age, (int, float)):
            raise ValueError("oldest_ready_age_seconds must be a number or null")
        return cls(
            name=_require_str(raw["name"], "name"),
            ready_depth=_require_int(raw["ready_depth"], "ready_depth"),
            delayed_depth=_require_int(raw["delayed_depth"], "delayed_depth"),
            leased_depth=_require_int(raw["leased_depth"], "leased_depth"),
            as_of=_require_str(raw["as_of"], "as_of"),
            freshness=StatsFreshness.parse(raw["freshness"]),
            oldest_ready_age_seconds=float(age) if age is not None else None,
            extra=dict(extra),
        )


@dataclass(frozen=True, slots=True)
class StatsTelemetrySummary:
    availability: StatsAvailability
    source: StatsTelemetrySource
    total: int | None = None
    extra: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def parse(cls, raw: object) -> StatsTelemetrySummary:
        if not isinstance(raw, Mapping):
            raise ValueError("stats telemetry summary must be an object")
        if "availability" not in raw or "source" not in raw:
            raise ValueError("stats telemetry summary requires availability and source")
        known = {"availability", "source", "total"}
        extra = {k: v for k, v in raw.items() if k not in known}
        total = raw.get("total")
        if total is not None:
            total = _require_int(total, "total")
        return cls(
            availability=StatsAvailability.parse(raw["availability"]),
            source=StatsTelemetrySource.parse(raw["source"]),
            total=total,
            extra=dict(extra),
        )


@dataclass(frozen=True, slots=True)
class StatsMaintenanceSummary:
    availability: StatsAvailability
    last_started_at: str | None = None
    last_succeeded_at: str | None = None
    premade_through: str | None = None
    retained_from: str | None = None
    last_error_code: str | None = None
    updated_at: str | None = None
    extra: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def parse(cls, raw: object) -> StatsMaintenanceSummary:
        if not isinstance(raw, Mapping):
            raise ValueError("stats maintenance summary must be an object")
        if "availability" not in raw:
            raise ValueError("stats maintenance summary missing availability")
        known = {
            "availability",
            "last_started_at",
            "last_succeeded_at",
            "premade_through",
            "retained_from",
            "last_error_code",
            "updated_at",
        }
        extra = {k: v for k, v in raw.items() if k not in known}
        return cls(
            availability=StatsAvailability.parse(raw["availability"]),
            last_started_at=_optional_str(raw.get("last_started_at"), "last_started_at"),
            last_succeeded_at=_optional_str(
                raw.get("last_succeeded_at"), "last_succeeded_at"
            ),
            premade_through=_optional_str(raw.get("premade_through"), "premade_through"),
            retained_from=_optional_str(raw.get("retained_from"), "retained_from"),
            last_error_code=_optional_str(raw.get("last_error_code"), "last_error_code"),
            updated_at=_optional_str(raw.get("updated_at"), "updated_at"),
            extra=dict(extra),
        )


@dataclass(frozen=True, slots=True)
class StatsSnapshot:
    as_of: str
    generated_at: str
    age_seconds: float
    freshness: StatsFreshness
    queues: tuple[StatsQueueAggregate, ...]
    retry: StatsTelemetrySummary
    dead_letter: StatsTelemetrySummary
    maintenance: StatsMaintenanceSummary
    extra: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def parse(cls, raw: object) -> StatsSnapshot:
        if not isinstance(raw, Mapping):
            raise ValueError("stats snapshot must be an object")
        required = (
            "as_of",
            "generated_at",
            "age_seconds",
            "freshness",
            "queues",
            "retry",
            "dead_letter",
            "maintenance",
        )
        missing = [key for key in required if key not in raw]
        if missing:
            raise ValueError(f"stats snapshot missing fields: {missing}")
        queues_raw = raw["queues"]
        if not isinstance(queues_raw, list):
            raise ValueError("stats snapshot queues must be an array")
        age = raw["age_seconds"]
        if not isinstance(age, (int, float)):
            raise ValueError("age_seconds must be a number")
        known = set(required)
        extra = {k: v for k, v in raw.items() if k not in known}
        return cls(
            as_of=_require_str(raw["as_of"], "as_of"),
            generated_at=_require_str(raw["generated_at"], "generated_at"),
            age_seconds=float(age),
            freshness=StatsFreshness.parse(raw["freshness"]),
            queues=tuple(StatsQueueAggregate.parse(item) for item in queues_raw),
            retry=StatsTelemetrySummary.parse(raw["retry"]),
            dead_letter=StatsTelemetrySummary.parse(raw["dead_letter"]),
            maintenance=StatsMaintenanceSummary.parse(raw["maintenance"]),
            extra=dict(extra),
        )


@dataclass(frozen=True, slots=True)
class BulkOperation:
    value: str

    @property
    def is_unknown(self) -> bool:
        return self.value not in KNOWN_BULK_OPERATIONS

    @classmethod
    def parse(cls, raw: object) -> BulkOperation:
        if not isinstance(raw, str) or not raw:
            raise ValueError("operation must be a non-empty string")
        return cls(raw)


@dataclass(frozen=True, slots=True)
class BulkItemOutcomeKind:
    value: str

    @property
    def is_unknown(self) -> bool:
        return self.value not in KNOWN_BULK_ITEM_OUTCOMES

    @classmethod
    def parse(cls, raw: object) -> BulkItemOutcomeKind:
        if not isinstance(raw, str) or not raw:
            raise ValueError("outcome must be a non-empty string")
        return cls(raw)


@dataclass(frozen=True, slots=True)
class DeadLetterReplayResult:
    """Single dead-letter replay outcome.

    Replay is at-least-once: ``replayed`` may be true on idempotent retry and
    external side effects may repeat. ``source_task_id`` preserves immutable
    terminal lineage; the new ``task_id`` is a distinct ready task.
    """

    task_id: str
    source_task_id: str
    queue: str
    policy_version: int
    replayed: bool
    warning: str
    admin_replay_expires_at: str | None = None
    extra: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def parse(cls, raw: object) -> DeadLetterReplayResult:
        if not isinstance(raw, Mapping):
            raise ValueError("dead letter replay result must be an object")
        required = (
            "task_id",
            "source_task_id",
            "queue",
            "policy_version",
            "replayed",
            "warning",
        )
        missing = [key for key in required if key not in raw]
        if missing:
            raise ValueError(f"dead letter replay result missing fields: {missing}")
        known = set(required) | {"admin_replay_expires_at"}
        extra = {k: v for k, v in raw.items() if k not in known}
        return cls(
            task_id=_require_str(raw["task_id"], "task_id"),
            source_task_id=_require_str(raw["source_task_id"], "source_task_id"),
            queue=_require_str(raw["queue"], "queue"),
            policy_version=_require_policy_version(raw["policy_version"], "policy_version"),
            replayed=_require_bool(raw["replayed"], "replayed"),
            warning=_require_str(raw["warning"], "warning"),
            admin_replay_expires_at=_optional_str(
                raw.get("admin_replay_expires_at"), "admin_replay_expires_at"
            ),
            extra=dict(extra),
        )


@dataclass(frozen=True, slots=True)
class BulkPreviewResult:
    """Dry-run bulk command preview with principal-bound confirmation token."""

    operation: BulkOperation
    queue: str
    candidate_count: int
    truncated: bool
    sample_task_ids: tuple[str, ...]
    confirmation_token: str
    confirmation_expires_at: str
    max_batch: int
    extra: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def parse(cls, raw: object) -> BulkPreviewResult:
        if not isinstance(raw, Mapping):
            raise ValueError("bulk preview result must be an object")
        required = (
            "operation",
            "queue",
            "candidate_count",
            "truncated",
            "sample_task_ids",
            "confirmation_token",
            "confirmation_expires_at",
            "max_batch",
        )
        missing = [key for key in required if key not in raw]
        if missing:
            raise ValueError(f"bulk preview result missing fields: {missing}")
        sample_raw = raw["sample_task_ids"]
        if not isinstance(sample_raw, list):
            raise ValueError("sample_task_ids must be an array")
        if len(sample_raw) > BULK_SAMPLE_MAX:
            raise ValueError("sample_task_ids exceeds 5 items")
        candidate_count = _require_int(raw["candidate_count"], "candidate_count")
        if candidate_count < 0 or candidate_count > BULK_CANDIDATE_MAX:
            raise ValueError("candidate_count must be between 0 and 100 inclusive")
        max_batch = _require_int(raw["max_batch"], "max_batch")
        if max_batch < 1 or max_batch > BULK_BATCH_MAX:
            raise ValueError("max_batch must be between 1 and 25 inclusive")
        known = set(required)
        extra = {k: v for k, v in raw.items() if k not in known}
        return cls(
            operation=BulkOperation.parse(raw["operation"]),
            queue=_require_str(raw["queue"], "queue"),
            candidate_count=candidate_count,
            truncated=_require_bool(raw["truncated"], "truncated"),
            sample_task_ids=tuple(_require_str(item, "sample_task_ids[]") for item in sample_raw),
            confirmation_token=validate_confirmation_token(
                _require_str(raw["confirmation_token"], "confirmation_token")
            ),
            confirmation_expires_at=_require_str(
                raw["confirmation_expires_at"], "confirmation_expires_at"
            ),
            max_batch=max_batch,
            extra=dict(extra),
        )

    def __repr__(self) -> str:
        return (
            f"BulkPreviewResult(operation={self.operation!r}, queue={self.queue!r}, "
            f"candidate_count={self.candidate_count}, truncated={self.truncated}, "
            f"sample_task_ids={self.sample_task_ids!r}, "
            f"confirmation_token=<redacted>, "
            f"confirmation_expires_at={self.confirmation_expires_at!r}, "
            f"max_batch={self.max_batch})"
        )

    def __str__(self) -> str:
        return self.__repr__()


@dataclass(frozen=True, slots=True)
class BulkItemOutcome:
    task_id: str
    outcome: BulkItemOutcomeKind
    code: str | None = None
    extra: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def parse(cls, raw: object) -> BulkItemOutcome:
        if not isinstance(raw, Mapping):
            raise ValueError("bulk item outcome must be an object")
        if "task_id" not in raw or "outcome" not in raw:
            raise ValueError("bulk item outcome requires task_id and outcome")
        known = {"task_id", "outcome", "code"}
        extra = {k: v for k, v in raw.items() if k not in known}
        return cls(
            task_id=_require_str(raw["task_id"], "task_id"),
            outcome=BulkItemOutcomeKind.parse(raw["outcome"]),
            code=_optional_str(raw.get("code"), "code"),
            extra=dict(extra),
        )


@dataclass(frozen=True, slots=True)
class BulkExecuteResult:
    """Bounded bulk execute summary with explicit partial/retry semantics."""

    operation: BulkOperation
    queue: str
    candidate_count: int
    start_index: int
    processed: int
    succeeded: int
    skipped: int
    failed: int
    partial: bool
    outcomes: tuple[BulkItemOutcome, ...]
    next_start_index: int | None = None
    extra: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def parse(cls, raw: object) -> BulkExecuteResult:
        if not isinstance(raw, Mapping):
            raise ValueError("bulk execute result must be an object")
        required = (
            "operation",
            "queue",
            "candidate_count",
            "start_index",
            "processed",
            "succeeded",
            "skipped",
            "failed",
            "partial",
            "outcomes",
        )
        missing = [key for key in required if key not in raw]
        if missing:
            raise ValueError(f"bulk execute result missing fields: {missing}")
        outcomes_raw = raw["outcomes"]
        if not isinstance(outcomes_raw, list):
            raise ValueError("outcomes must be an array")
        if len(outcomes_raw) > BULK_BATCH_MAX:
            raise ValueError("outcomes exceeds 25 items")
        candidate_count = _require_int(raw["candidate_count"], "candidate_count")
        if candidate_count < 0 or candidate_count > BULK_CANDIDATE_MAX:
            raise ValueError("candidate_count must be between 0 and 100 inclusive")
        processed = _require_int(raw["processed"], "processed")
        if processed < 0 or processed > BULK_BATCH_MAX:
            raise ValueError("processed must be between 0 and 25 inclusive")
        start_index = _require_int(raw["start_index"], "start_index")
        if start_index < 0:
            raise ValueError("start_index must be non-negative")
        known = set(required) | {"next_start_index"}
        extra = {k: v for k, v in raw.items() if k not in known}
        return cls(
            operation=BulkOperation.parse(raw["operation"]),
            queue=_require_str(raw["queue"], "queue"),
            candidate_count=candidate_count,
            start_index=start_index,
            processed=processed,
            succeeded=_require_int(raw["succeeded"], "succeeded"),
            skipped=_require_int(raw["skipped"], "skipped"),
            failed=_require_int(raw["failed"], "failed"),
            partial=_require_bool(raw["partial"], "partial"),
            outcomes=tuple(BulkItemOutcome.parse(item) for item in outcomes_raw),
            next_start_index=_optional_int(raw.get("next_start_index"), "next_start_index"),
            extra=dict(extra),
        )


@dataclass(frozen=True, slots=True)
class BreakGlassMutationResult:
    operation: str
    target_id: str
    outcome: str
    queue: str | None = None
    generation: int | None = None
    extra: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def parse(cls, raw: object) -> BreakGlassMutationResult:
        if not isinstance(raw, Mapping):
            raise ValueError("break-glass mutation result must be an object")
        _reject_break_glass_secret_keys(raw)
        required = ("operation", "target_id", "outcome")
        missing = [key for key in required if key not in raw]
        if missing:
            raise ValueError(f"break-glass mutation result missing fields: {missing}")
        known = set(required) | {"queue", "generation"}
        extra = {k: v for k, v in raw.items() if k not in known}
        generation = raw.get("generation")
        if generation is not None:
            generation = _require_int(generation, "generation")
            if generation < 0:
                raise ValueError("generation must be at least 0")
        return cls(
            operation=_require_str(raw["operation"], "operation"),
            target_id=_require_str(raw["target_id"], "target_id"),
            outcome=_require_str(raw["outcome"], "outcome"),
            queue=_optional_str(raw.get("queue"), "queue"),
            generation=generation,
            extra=dict(extra),
        )


@dataclass(frozen=True, slots=True)
class BreakGlassCounterResult:
    operation: str
    queue: str
    target_id: str
    outcome: str
    delayed_count: int
    ready_count: int
    leased_count: int
    extra: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def parse(cls, raw: object) -> BreakGlassCounterResult:
        if not isinstance(raw, Mapping):
            raise ValueError("break-glass counter result must be an object")
        _reject_break_glass_secret_keys(raw)
        required = (
            "operation",
            "queue",
            "target_id",
            "outcome",
            "delayed_count",
            "ready_count",
            "leased_count",
        )
        missing = [key for key in required if key not in raw]
        if missing:
            raise ValueError(f"break-glass counter result missing fields: {missing}")
        known = set(required)
        extra = {k: v for k, v in raw.items() if k not in known}
        return cls(
            operation=_require_str(raw["operation"], "operation"),
            queue=_require_str(raw["queue"], "queue"),
            target_id=_require_str(raw["target_id"], "target_id"),
            outcome=_require_str(raw["outcome"], "outcome"),
            delayed_count=_require_int(raw["delayed_count"], "delayed_count"),
            ready_count=_require_int(raw["ready_count"], "ready_count"),
            leased_count=_require_int(raw["leased_count"], "leased_count"),
            extra=dict(extra),
        )


@dataclass(frozen=True, slots=True)
class BreakGlassReplayLimitResult:
    operation: str
    queue: str
    target_id: str
    outcome: str
    effective_rps: float
    extra: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def parse(cls, raw: object) -> BreakGlassReplayLimitResult:
        if not isinstance(raw, Mapping):
            raise ValueError("break-glass replay limit result must be an object")
        _reject_break_glass_secret_keys(raw)
        required = ("operation", "queue", "target_id", "outcome", "effective_rps")
        missing = [key for key in required if key not in raw]
        if missing:
            raise ValueError(f"break-glass replay limit result missing fields: {missing}")
        effective = raw["effective_rps"]
        if not isinstance(effective, (int, float)):
            raise ValueError("effective_rps must be a number")
        if effective < 0:
            raise ValueError("effective_rps must be at least 0")
        known = set(required)
        extra = {k: v for k, v in raw.items() if k not in known}
        return cls(
            operation=_require_str(raw["operation"], "operation"),
            queue=_require_str(raw["queue"], "queue"),
            target_id=_require_str(raw["target_id"], "target_id"),
            outcome=_require_str(raw["outcome"], "outcome"),
            effective_rps=float(effective),
            extra=dict(extra),
        )


def validate_break_glass_reason(reason: str) -> str:
    if not isinstance(reason, str) or not reason:
        raise ValueError("reason must be a non-empty string")
    if len(reason) > BREAK_GLASS_REASON_MAX_LENGTH:
        raise ValueError("reason exceeds 512 characters")
    return reason


def validate_incident_reference(incident_reference: str) -> str:
    if not isinstance(incident_reference, str) or not incident_reference:
        raise ValueError("incident_reference must be a non-empty string")
    if len(incident_reference) > INCIDENT_REFERENCE_MAX_LENGTH:
        raise ValueError("incident_reference exceeds 128 characters")
    return incident_reference


def validate_risk_acknowledged(value: bool) -> bool:
    if value is not True:
        raise ValueError("risk_acknowledged must be true")
    return True


def validate_acknowledge_duplicate_window(value: bool) -> bool:
    if value is not True:
        raise ValueError("acknowledge_duplicate_window must be true")
    return True


def validate_event_id(event_id: str) -> str:
    return validate_task_id(event_id)


def validate_partition_name(partition_name: str) -> str:
    if not isinstance(partition_name, str) or not partition_name:
        raise ValueError("partition_name must be a non-empty string")
    if len(partition_name) > PARTITION_NAME_MAX_LENGTH:
        raise ValueError("partition_name exceeds 128 characters")
    if not _PARTITION_NAME_RE.match(partition_name):
        raise ValueError("partition_name must match OpenAPI pattern")
    return partition_name


def validate_replay_factor(factor: float) -> float:
    if isinstance(factor, bool) or not isinstance(factor, (int, float)):
        raise ValueError("factor must be a number")
    numeric = float(factor)
    if numeric < REPLAY_FACTOR_MIN or numeric > REPLAY_FACTOR_MAX:
        raise ValueError("factor must be between 1.0 and 10.0 inclusive")
    return numeric


def validate_replay_ttl_seconds(ttl_seconds: int) -> int:
    if isinstance(ttl_seconds, bool) or not isinstance(ttl_seconds, int):
        raise ValueError("ttl_seconds must be an integer")
    if ttl_seconds < REPLAY_TTL_SECONDS_MIN or ttl_seconds > REPLAY_TTL_SECONDS_MAX:
        raise ValueError("ttl_seconds must be between 1 and 3600 inclusive")
    return ttl_seconds


def validate_registry_entry_id(entry_id: int) -> int:
    if isinstance(entry_id, bool) or not isinstance(entry_id, int):
        raise ValueError("entry_id must be an integer")
    if entry_id < REGISTRY_ENTRY_ID_MIN:
        raise ValueError("entry_id must be at least 1")
    return entry_id


def validate_extend_seconds(extend_seconds: int) -> int:
    if isinstance(extend_seconds, bool) or not isinstance(extend_seconds, int):
        raise ValueError("extend_seconds must be an integer")
    if extend_seconds < EXTEND_SECONDS_MIN or extend_seconds > EXTEND_SECONDS_MAX:
        raise ValueError("extend_seconds must be between 1 and 2592000 inclusive")
    return extend_seconds


def validate_failure_code(failure_code: str) -> str:
    if not isinstance(failure_code, str) or not failure_code:
        raise ValueError("failure_code must be a non-empty string")
    if len(failure_code) > FAILURE_CODE_MAX_LENGTH:
        raise ValueError("failure_code exceeds 128 characters")
    return failure_code


def validate_idempotency_key(key: str) -> str:
    if not isinstance(key, str) or not key:
        raise ValueError("idempotency_key must be a non-empty string")
    if len(key) < IDEMPOTENCY_KEY_MIN_LENGTH or len(key) > IDEMPOTENCY_KEY_MAX_LENGTH:
        raise ValueError("idempotency_key must be between 1 and 256 characters inclusive")
    return key


def validate_reason(reason: str) -> str:
    if not isinstance(reason, str) or not reason:
        raise ValueError("reason must be a non-empty string")
    if len(reason) > REASON_MAX_LENGTH:
        raise ValueError("reason exceeds 512 characters")
    return reason


def validate_confirmation_token(token: str) -> str:
    if not isinstance(token, str) or not token:
        raise ValueError("confirmation_token must be a non-empty string")
    if len(token) > CONFIRMATION_TOKEN_MAX_LENGTH:
        raise ValueError("confirmation_token exceeds 24576 characters")
    return token


def _reject_break_glass_secret_keys(raw: Mapping[str, Any]) -> None:
    for key in raw:
        key_str = str(key)
        if _FORBIDDEN_BREAK_GLASS_SECRET_KEY_RE.search(key_str):
            raise ValueError(f"break-glass result must not contain {key_str!r}")


def validate_bulk_filters(filters: Mapping[str, str]) -> dict[str, str]:
    if not isinstance(filters, Mapping):
        raise ValueError("filters must be a mapping")
    normalized: dict[str, str] = {}
    for raw_key, raw_val in filters.items():
        if not isinstance(raw_key, str) or not raw_key.strip():
            raise ValueError("filter keys must be non-empty strings")
        key = raw_key.strip().lower()
        if key in _FORBIDDEN_BULK_FILTER_KEYS:
            raise ValueError(f"filter {key!r} is not allowed")
        if key not in _ALLOWED_BULK_FILTER_KEYS:
            raise ValueError(f"unknown filter {key!r}")
        if not isinstance(raw_val, str) or not raw_val.strip():
            raise ValueError("filter values must be non-empty strings")
        stripped = raw_val.strip()
        if len(stripped) > 128:
            raise ValueError("filter values must be at most 128 characters")
        normalized[key] = stripped
    return dict(sorted(normalized.items()))


def validate_batch_limit(batch_limit: int | None) -> int | None:
    if batch_limit is None:
        return None
    if isinstance(batch_limit, bool) or not isinstance(batch_limit, int):
        raise ValueError("batch_limit must be an integer or None")
    if batch_limit < 1 or batch_limit > BULK_BATCH_MAX:
        raise ValueError("batch_limit must be between 1 and 25 inclusive")
    return batch_limit


def validate_start_index(start_index: int) -> int:
    if isinstance(start_index, bool) or not isinstance(start_index, int):
        raise ValueError("start_index must be an integer")
    if start_index < 0:
        raise ValueError("start_index must be non-negative")
    return start_index


def validate_known_queue_state(state: QueueState) -> QueueState:
    if state.is_unknown:
        raise ValueError("state must be active, paused, or draining")
    return state


def validate_queue_name(name: str) -> str:
    if not isinstance(name, str) or not name:
        raise ValueError("queue_name must be a non-empty string")
    if len(name) > QUEUE_NAME_MAX_LENGTH:
        raise ValueError("queue_name exceeds 128 characters")
    if not _QUEUE_NAME_RE.match(name):
        raise ValueError("queue_name must match OpenAPI pattern")
    return name


def validate_task_id(task_id: str) -> str:
    if not isinstance(task_id, str) or not task_id:
        raise ValueError("task_id must be a non-empty string")
    try:
        uuid.UUID(task_id)
    except ValueError as exc:
        raise ValueError("task_id must be a UUID") from exc
    return task_id


def validate_page_limit(limit: int) -> int:
    if isinstance(limit, bool) or not isinstance(limit, int):
        raise ValueError("limit must be an integer")
    if limit < PAGE_LIMIT_MIN or limit > PAGE_LIMIT_MAX:
        raise ValueError("limit must be between 1 and 100 inclusive")
    return limit


def validate_cursor(cursor: str | None) -> str | None:
    if cursor is None:
        return None
    if not isinstance(cursor, str):
        raise ValueError("cursor must be a string or None")
    if len(cursor) > CURSOR_MAX_LENGTH:
        raise ValueError("cursor exceeds 512 characters")
    return cursor


def validate_time_range(time_from: datetime, time_to: datetime) -> tuple[datetime, datetime]:
    if time_from.tzinfo is None or time_from.utcoffset() is None:
        raise ValueError("from must be timezone-aware")
    if time_to.tzinfo is None or time_to.utcoffset() is None:
        raise ValueError("to must be timezone-aware")
    if time_from > time_to:
        raise ValueError("from must be no later than to")
    return time_from, time_to


def format_datetime(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("datetime must be timezone-aware")
    utc = value.astimezone(UTC)
    text = utc.isoformat()
    if text.endswith("+00:00"):
        return text[:-6] + "Z"
    return text


def _require_str(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a string")
    return value


def _optional_str(value: object, name: str) -> str | None:
    if value is None:
        return None
    return _require_str(value, name)


def _require_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    return value


def _optional_int(value: object, name: str) -> int | None:
    if value is None:
        return None
    return _require_int(value, name)


def _require_bool(value: object, name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{name} must be a boolean")
    return value


def _optional_bool(value: object, name: str) -> bool | None:
    if value is None:
        return None
    return _require_bool(value, name)


def _require_config_version(value: object, name: str) -> int:
    parsed = _require_int(value, name)
    if parsed < CONFIG_VERSION_MIN:
        raise ValueError(f"{name} must be at least 1")
    return parsed


def _require_policy_version(value: object, name: str) -> int:
    parsed = _require_int(value, name)
    if parsed < POLICY_VERSION_MIN:
        raise ValueError(f"{name} must be at least 1")
    return parsed


def _require_max_attempts(value: object) -> int:
    parsed = _require_int(value, "max_attempts")
    if parsed < MAX_ATTEMPTS_MIN:
        raise ValueError("max_attempts must be at least 1")
    return parsed


def _require_retry_delay_seconds(value: object) -> int:
    parsed = _require_int(value, "retry_delay_seconds")
    if parsed < 0 or parsed > RETRY_DELAY_SECONDS_MAX:
        raise ValueError("retry_delay_seconds must be between 0 and 86400 inclusive")
    return parsed
