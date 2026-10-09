"""Tolerant OpenAPI ``Capabilities`` wire parsing."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Final

_REQUIRED: Final[frozenset[str]] = frozenset(
    {
        "protocol_major",
        "protocol_version",
        "schema_revision",
        "scheduling",
        "priority",
        "delivery_events",
        "batch_claim",
        "long_polling",
        "max_claim_tasks",
        "max_wait_seconds",
        "payload_runtime_max_bytes",
        "payload_hard_max_bytes",
        "enqueue_dedup_ttl_seconds",
        "enqueue_dedup_ttl_min_seconds",
        "enqueue_dedup_ttl_max_seconds",
        "terminal_replay_ttl_seconds",
        "terminal_replay_ttl_min_seconds",
        "terminal_replay_ttl_max_seconds",
        "admin_replay_ttl_seconds",
        "admin_replay_ttl_min_seconds",
        "admin_replay_ttl_max_seconds",
    }
)


@dataclass(frozen=True, slots=True)
class Capabilities:
    """Capabilities resource; additive fields land in ``extra``."""

    protocol_major: int
    protocol_version: str
    schema_revision: str
    scheduling: bool
    priority: bool
    delivery_events: bool
    batch_claim: bool
    long_polling: bool
    max_claim_tasks: int
    max_wait_seconds: int
    payload_runtime_max_bytes: int
    payload_hard_max_bytes: int
    enqueue_dedup_ttl_seconds: int
    enqueue_dedup_ttl_min_seconds: int
    enqueue_dedup_ttl_max_seconds: int
    terminal_replay_ttl_seconds: int
    terminal_replay_ttl_min_seconds: int
    terminal_replay_ttl_max_seconds: int
    admin_replay_ttl_seconds: int
    admin_replay_ttl_min_seconds: int
    admin_replay_ttl_max_seconds: int
    extra: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def parse(cls, raw: object) -> Capabilities:
        if not isinstance(raw, Mapping):
            raise ValueError("capabilities must be an object")
        missing = sorted(_REQUIRED - set(raw))
        if missing:
            raise ValueError(f"capabilities missing required fields: {missing}")

        extra = {k: v for k, v in raw.items() if k not in _REQUIRED}
        return cls(
            protocol_major=_require_int(raw["protocol_major"], "protocol_major"),
            protocol_version=_require_str(raw["protocol_version"], "protocol_version"),
            schema_revision=_require_str(raw["schema_revision"], "schema_revision"),
            scheduling=_require_bool(raw["scheduling"], "scheduling"),
            priority=_require_bool(raw["priority"], "priority"),
            delivery_events=_require_bool(raw["delivery_events"], "delivery_events"),
            batch_claim=_require_bool(raw["batch_claim"], "batch_claim"),
            long_polling=_require_bool(raw["long_polling"], "long_polling"),
            max_claim_tasks=_require_int(raw["max_claim_tasks"], "max_claim_tasks"),
            max_wait_seconds=_require_int(raw["max_wait_seconds"], "max_wait_seconds"),
            payload_runtime_max_bytes=_require_int(
                raw["payload_runtime_max_bytes"], "payload_runtime_max_bytes"
            ),
            payload_hard_max_bytes=_require_int(
                raw["payload_hard_max_bytes"], "payload_hard_max_bytes"
            ),
            enqueue_dedup_ttl_seconds=_require_int(
                raw["enqueue_dedup_ttl_seconds"], "enqueue_dedup_ttl_seconds"
            ),
            enqueue_dedup_ttl_min_seconds=_require_int(
                raw["enqueue_dedup_ttl_min_seconds"], "enqueue_dedup_ttl_min_seconds"
            ),
            enqueue_dedup_ttl_max_seconds=_require_int(
                raw["enqueue_dedup_ttl_max_seconds"], "enqueue_dedup_ttl_max_seconds"
            ),
            terminal_replay_ttl_seconds=_require_int(
                raw["terminal_replay_ttl_seconds"], "terminal_replay_ttl_seconds"
            ),
            terminal_replay_ttl_min_seconds=_require_int(
                raw["terminal_replay_ttl_min_seconds"], "terminal_replay_ttl_min_seconds"
            ),
            terminal_replay_ttl_max_seconds=_require_int(
                raw["terminal_replay_ttl_max_seconds"], "terminal_replay_ttl_max_seconds"
            ),
            admin_replay_ttl_seconds=_require_int(
                raw["admin_replay_ttl_seconds"], "admin_replay_ttl_seconds"
            ),
            admin_replay_ttl_min_seconds=_require_int(
                raw["admin_replay_ttl_min_seconds"], "admin_replay_ttl_min_seconds"
            ),
            admin_replay_ttl_max_seconds=_require_int(
                raw["admin_replay_ttl_max_seconds"], "admin_replay_ttl_max_seconds"
            ),
            extra=dict(extra),
        )


def _require_str(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a string")
    return value


def _require_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    return value


def _require_bool(value: object, name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{name} must be a boolean")
    return value
