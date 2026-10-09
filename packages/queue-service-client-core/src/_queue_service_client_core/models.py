"""Tolerant protocol value objects for the Queue HTTP/JSON client."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Final

KNOWN_TASK_STATES: Final[frozenset[str]] = frozenset(
    {
        "delayed",
        "ready",
        "leased",
        "retry_scheduled",
        "succeeded",
        "dead_lettered",
        "cancelled",
    }
)

KNOWN_ERROR_CODES: Final[frozenset[str]] = frozenset(
    {
        "validation_failed",
        "payload_too_large",
        "idempotency_key_required",
        "idempotency_conflict",
        "queue_not_found",
        "queue_draining",
        "task_not_found",
        "claim_not_found",
        "lease_lost",
        "task_already_terminal",
        "cancel_race_lost",
        "config_version_conflict",
        "permission_denied",
        "unauthenticated",
        "resource_exhausted",
        "dependency_unavailable",
        "internal_error",
    }
)

_TASK_REQUIRED: Final[frozenset[str]] = frozenset(
    {
        "task_id",
        "queue_name",
        "producer_id",
        "state",
        "priority",
        "available_at",
        "retry_policy_version",
        "created_at",
        "spawned_task_ids",
        "delivery_event_ids",
    }
)

_TASK_KNOWN_OPTIONAL: Final[frozenset[str]] = frozenset(
    {
        "payload",
        "current_claim",
        "terminal_at",
        "failure_code",
        "failure_detail",
        "source_task_id",
        "spawn_ordinal",
    }
)


@dataclass(frozen=True, slots=True)
class TaskState:
    """Task lifecycle state; unknown wire values are preserved."""

    value: str

    @property
    def is_unknown(self) -> bool:
        return self.value not in KNOWN_TASK_STATES

    @classmethod
    def parse(cls, raw: object) -> TaskState:
        if not isinstance(raw, str) or not raw:
            raise ValueError("task.state must be a non-empty string")
        return cls(raw)


@dataclass(frozen=True, slots=True)
class ErrorCode:
    """Protocol error code; unknown wire values are preserved."""

    value: str

    @property
    def is_unknown(self) -> bool:
        return self.value not in KNOWN_ERROR_CODES

    @classmethod
    def parse(cls, raw: object) -> ErrorCode:
        if not isinstance(raw, str) or not raw:
            raise ValueError("error.code must be a non-empty string")
        return cls(raw)


@dataclass(frozen=True, slots=True)
class ClaimSummary:
    """Public claim summary embedded on Task (no secret token)."""

    claim_id: str
    generation: int
    claimed_at: str
    lease_expires_at: str
    worker_id: str
    cancel_requested: bool
    extra: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def parse(cls, raw: object) -> ClaimSummary:
        if not isinstance(raw, Mapping):
            raise ValueError("current_claim must be an object")
        required = (
            "claim_id",
            "generation",
            "claimed_at",
            "lease_expires_at",
            "worker_id",
            "cancel_requested",
        )
        missing = [key for key in required if key not in raw]
        if missing:
            raise ValueError(f"current_claim missing fields: {missing}")
        known = set(required)
        extra = {k: v for k, v in raw.items() if k not in known}
        return cls(
            claim_id=_require_str(raw["claim_id"], "current_claim.claim_id"),
            generation=_require_int(raw["generation"], "current_claim.generation"),
            claimed_at=_require_str(raw["claimed_at"], "current_claim.claimed_at"),
            lease_expires_at=_require_str(
                raw["lease_expires_at"], "current_claim.lease_expires_at"
            ),
            worker_id=_require_str(raw["worker_id"], "current_claim.worker_id"),
            cancel_requested=_require_bool(
                raw["cancel_requested"], "current_claim.cancel_requested"
            ),
            extra=dict(extra),
        )


@dataclass(frozen=True, slots=True)
class Task:
    """Task resource from OpenAPI ``Task`` (additive fields tolerated)."""

    task_id: str
    queue_name: str
    producer_id: str
    state: TaskState
    priority: int
    available_at: str
    retry_policy_version: int
    created_at: str
    spawned_task_ids: tuple[str, ...]
    delivery_event_ids: tuple[str, ...]
    payload: Any = None
    current_claim: ClaimSummary | None = None
    terminal_at: str | None = None
    failure_code: str | None = None
    failure_detail: str | None = None
    source_task_id: str | None = None
    spawn_ordinal: int | None = None
    extra: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def parse(cls, raw: object) -> Task:
        if not isinstance(raw, Mapping):
            raise ValueError("task must be an object")
        missing = sorted(_TASK_REQUIRED - set(raw))
        if missing:
            raise ValueError(f"task missing required fields: {missing}")

        spawned = raw["spawned_task_ids"]
        delivery = raw["delivery_event_ids"]
        if not isinstance(spawned, list) or not all(isinstance(x, str) for x in spawned):
            raise ValueError("spawned_task_ids must be an array of strings")
        if not isinstance(delivery, list) or not all(isinstance(x, str) for x in delivery):
            raise ValueError("delivery_event_ids must be an array of strings")

        current_claim_raw = raw.get("current_claim", None)
        current_claim = (
            None if current_claim_raw is None else ClaimSummary.parse(current_claim_raw)
        )

        known = _TASK_REQUIRED | _TASK_KNOWN_OPTIONAL
        extra = {k: v for k, v in raw.items() if k not in known}

        return cls(
            task_id=_require_str(raw["task_id"], "task_id"),
            queue_name=_require_str(raw["queue_name"], "queue_name"),
            producer_id=_require_str(raw["producer_id"], "producer_id"),
            state=TaskState.parse(raw["state"]),
            priority=_require_int(raw["priority"], "priority"),
            available_at=_require_str(raw["available_at"], "available_at"),
            retry_policy_version=_require_int(
                raw["retry_policy_version"], "retry_policy_version"
            ),
            created_at=_require_str(raw["created_at"], "created_at"),
            spawned_task_ids=tuple(spawned),
            delivery_event_ids=tuple(delivery),
            payload=raw.get("payload"),
            current_claim=current_claim,
            terminal_at=_optional_str(raw.get("terminal_at"), "terminal_at"),
            failure_code=_optional_str(raw.get("failure_code"), "failure_code"),
            failure_detail=_optional_str(raw.get("failure_detail"), "failure_detail"),
            source_task_id=_optional_str(raw.get("source_task_id"), "source_task_id"),
            spawn_ordinal=_optional_int(raw.get("spawn_ordinal"), "spawn_ordinal"),
            extra=dict(extra),
        )


@dataclass(frozen=True, slots=True)
class EnqueueResponse:
    task: Task
    replayed: bool
    extra: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def parse(cls, raw: object) -> EnqueueResponse:
        if not isinstance(raw, Mapping):
            raise ValueError("enqueue response must be an object")
        if "task" not in raw or "replayed" not in raw:
            raise ValueError("enqueue response requires task and replayed")
        known = {"task", "replayed"}
        extra = {k: v for k, v in raw.items() if k not in known}
        return cls(
            task=Task.parse(raw["task"]),
            replayed=_require_bool(raw["replayed"], "replayed"),
            extra=dict(extra),
        )


@dataclass(frozen=True, slots=True)
class ResolveSubmissionResponse:
    task: Task
    dedup_expires_at: str
    extra: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def parse(cls, raw: object) -> ResolveSubmissionResponse:
        if not isinstance(raw, Mapping):
            raise ValueError("resolve response must be an object")
        if "task" not in raw or "dedup_expires_at" not in raw:
            raise ValueError("resolve response requires task and dedup_expires_at")
        known = {"task", "dedup_expires_at"}
        extra = {k: v for k, v in raw.items() if k not in known}
        return cls(
            task=Task.parse(raw["task"]),
            dedup_expires_at=_require_str(raw["dedup_expires_at"], "dedup_expires_at"),
            extra=dict(extra),
        )


@dataclass(frozen=True, slots=True)
class CancelResponse:
    task: Task
    extra: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def parse(cls, raw: object) -> CancelResponse:
        if not isinstance(raw, Mapping):
            raise ValueError("cancel response must be an object")
        if "task" not in raw:
            raise ValueError("cancel response requires task")
        known = {"task"}
        extra = {k: v for k, v in raw.items() if k not in known}
        return cls(task=Task.parse(raw["task"]), extra=dict(extra))


@dataclass(frozen=True, slots=True)
class HeartbeatResult:
    claim: ClaimSummary
    server_time: str
    recommended_heartbeat_seconds: int
    extra: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def parse(cls, raw: object) -> HeartbeatResult:
        if not isinstance(raw, Mapping):
            raise ValueError("heartbeat response must be an object")
        required = ("claim", "server_time", "recommended_heartbeat_seconds")
        missing = [key for key in required if key not in raw]
        if missing:
            raise ValueError(f"heartbeat response missing fields: {missing}")
        known = set(required)
        extra = {k: v for k, v in raw.items() if k not in known}
        return cls(
            claim=ClaimSummary.parse(raw["claim"]),
            server_time=_require_str(raw["server_time"], "server_time"),
            recommended_heartbeat_seconds=_require_int(
                raw["recommended_heartbeat_seconds"], "recommended_heartbeat_seconds"
            ),
            extra=dict(extra),
        )


@dataclass(frozen=True, slots=True)
class CompleteResult:
    task_id: str
    state: TaskState
    spawned_task_ids: tuple[str, ...]
    replayed: bool
    extra: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def parse(cls, raw: object) -> CompleteResult:
        if not isinstance(raw, Mapping):
            raise ValueError("complete response must be an object")
        required = ("task_id", "state", "spawned_task_ids", "replayed")
        missing = [key for key in required if key not in raw]
        if missing:
            raise ValueError(f"complete response missing fields: {missing}")
        spawned = raw["spawned_task_ids"]
        if not isinstance(spawned, list) or not all(isinstance(x, str) for x in spawned):
            raise ValueError("spawned_task_ids must be an array of strings")
        known = set(required)
        extra = {k: v for k, v in raw.items() if k not in known}
        return cls(
            task_id=_require_str(raw["task_id"], "task_id"),
            state=TaskState.parse(raw["state"]),
            spawned_task_ids=tuple(spawned),
            replayed=_require_bool(raw["replayed"], "replayed"),
            extra=dict(extra),
        )


@dataclass(frozen=True, slots=True)
class FailResult:
    task_id: str
    state: TaskState
    replayed: bool
    available_at: str | None = None
    terminal_at: str | None = None
    extra: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def parse(cls, raw: object) -> FailResult:
        if not isinstance(raw, Mapping):
            raise ValueError("fail response must be an object")
        for key in ("task_id", "state", "replayed"):
            if key not in raw:
                raise ValueError(f"fail response missing field: {key}")
        known = {"task_id", "state", "replayed", "available_at", "terminal_at"}
        extra = {k: v for k, v in raw.items() if k not in known}
        return cls(
            task_id=_require_str(raw["task_id"], "task_id"),
            state=TaskState.parse(raw["state"]),
            replayed=_require_bool(raw["replayed"], "replayed"),
            available_at=_optional_str(raw.get("available_at"), "available_at"),
            terminal_at=_optional_str(raw.get("terminal_at"), "terminal_at"),
            extra=dict(extra),
        )


@dataclass(frozen=True, slots=True)
class AckCancelResult:
    task_id: str
    state: TaskState
    terminal_at: str
    replayed: bool
    extra: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def parse(cls, raw: object) -> AckCancelResult:
        if not isinstance(raw, Mapping):
            raise ValueError("ack_cancel response must be an object")
        required = ("task_id", "state", "terminal_at", "replayed")
        missing = [key for key in required if key not in raw]
        if missing:
            raise ValueError(f"ack_cancel response missing fields: {missing}")
        known = set(required)
        extra = {k: v for k, v in raw.items() if k not in known}
        return cls(
            task_id=_require_str(raw["task_id"], "task_id"),
            state=TaskState.parse(raw["state"]),
            terminal_at=_require_str(raw["terminal_at"], "terminal_at"),
            replayed=_require_bool(raw["replayed"], "replayed"),
            extra=dict(extra),
        )


@dataclass(frozen=True, slots=True)
class ProtocolErrorBody:
    code: ErrorCode
    message: str
    retryable: bool
    request_id: str
    details: Mapping[str, Any]
    retry_after_ms: int | None = None

    @classmethod
    def parse(cls, raw: object) -> ProtocolErrorBody:
        if not isinstance(raw, Mapping):
            raise ValueError("error body must be an object")
        required = ("code", "message", "retryable", "request_id", "details")
        missing = [key for key in required if key not in raw]
        if missing:
            raise ValueError(f"error body missing fields: {missing}")
        details = raw["details"]
        if not isinstance(details, Mapping):
            raise ValueError("error.details must be an object")
        retry_after = raw.get("retry_after_ms", None)
        if retry_after is not None and (
            isinstance(retry_after, bool) or type(retry_after) is not int
        ):
            raise ValueError("error.retry_after_ms must be an integer or null")
        return cls(
            code=ErrorCode.parse(raw["code"]),
            message=_require_str(raw["message"], "message"),
            retryable=_require_bool(raw["retryable"], "retryable"),
            request_id=_require_str(raw["request_id"], "request_id"),
            details=dict(details),
            retry_after_ms=retry_after,
        )


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
