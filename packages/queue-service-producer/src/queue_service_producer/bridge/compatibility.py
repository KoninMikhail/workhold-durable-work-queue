"""Queue protocol/capability compatibility for the application-outbox bridge.

Derives support from Phase 3.1 OpenAPI ``Capabilities`` consts and Phase 6 intent
schema major 1. Fail-closed for protocol-major mismatch, missing durable
idempotent-enqueue semantics, and unavailable/malformed discovery. Tolerant of
additive intent extension fields and unknown extensible Queue error codes
(explicit UNKNOWN — never silent success).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from _queue_service_client_core.models import ErrorCode

# Authoritative Phase 3.1 OpenAPI Capabilities consts.
SUPPORTED_PROTOCOL_MAJOR = 1
SUPPORTED_INTENT_SCHEMA_MAJOR = 1
SUPPORTED_SCHEMA_REVISION = "0001"

SUPPORTED_CAPABILITIES: dict[str, Any] = {
    "protocol_major": SUPPORTED_PROTOCOL_MAJOR,
    "protocol_version": "1.0",
    "schema_revision": SUPPORTED_SCHEMA_REVISION,
    "scheduling": True,
    "priority": True,
    "delivery_events": False,
    "batch_claim": False,
    "long_polling": False,
    "max_claim_tasks": 1,
    "max_wait_seconds": 0,
    "payload_runtime_max_bytes": 262144,
    "payload_hard_max_bytes": 1048576,
    "enqueue_dedup_ttl_seconds": 7776000,
    "enqueue_dedup_ttl_min_seconds": 2592000,
    "enqueue_dedup_ttl_max_seconds": 31536000,
    "terminal_replay_ttl_seconds": 604800,
    "terminal_replay_ttl_min_seconds": 86400,
    "terminal_replay_ttl_max_seconds": 2592000,
    "admin_replay_ttl_seconds": 2592000,
    "admin_replay_ttl_min_seconds": 604800,
    "admin_replay_ttl_max_seconds": 7776000,
    "durable_idempotent_enqueue": True,
}

_REQUIRED_KEYS = frozenset({"protocol_major", "protocol_version", "schema_revision"})


class CompatibilityStatus(Enum):
    SUPPORTED = "supported"
    INCOMPATIBLE = "incompatible"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class CompatibilityResult:
    status: CompatibilityStatus
    reason: str
    allows_poll: bool
    preserves_pending_intents: bool = True
    mutates_persisted_intent: bool = False
    treat_as_success: bool = False
    ignored_extension_keys: tuple[str, ...] = ()
    protocol_major: int | None = None
    durable_idempotent_enqueue: bool | None = None
    details: Mapping[str, Any] = field(default_factory=dict)


def _as_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def has_durable_idempotent_enqueue(capabilities: Mapping[str, Any]) -> bool | None:
    """True/False when known; None when the durable-enqueue signal is absent."""
    flag = capabilities.get("durable_idempotent_enqueue")
    if isinstance(flag, bool):
        return flag
    ttl = capabilities.get("enqueue_dedup_ttl_seconds")
    if isinstance(ttl, bool):
        return None
    if isinstance(ttl, int):
        return ttl > 0
    if "enqueue_dedup_ttl_seconds" not in capabilities and flag is None:
        return None
    return None


class BridgeCompatibility:
    """Classify Queue capability responses and intent schema majors for the bridge."""

    supported_protocol_major: int = SUPPORTED_PROTOCOL_MAJOR
    supported_intent_schema_major: int = SUPPORTED_INTENT_SCHEMA_MAJOR
    supported_schema_revision: str = SUPPORTED_SCHEMA_REVISION

    def evaluate(
        self,
        *,
        capabilities: Mapping[str, Any] | None,
        intent_schema_major: int,
        intent_schema_minor: int = 0,
        intent_extensions: Mapping[str, Any] | None = None,
        bridge_policy: str = "current",
        unknown_extensible_error: str | None = None,
        discovery_error: str | None = None,
    ) -> CompatibilityResult:
        del intent_schema_minor  # informational; major gates support
        extensions = dict(intent_extensions or {})
        # All supported policies tolerate additive extension keys under major 1.
        ignored = tuple(sorted(extensions.keys()))

        if discovery_error:
            return CompatibilityResult(
                status=CompatibilityStatus.INCOMPATIBLE,
                reason=f"capabilities_discovery_failed:{discovery_error}",
                allows_poll=False,
            )

        if capabilities is None:
            return CompatibilityResult(
                status=CompatibilityStatus.INCOMPATIBLE,
                reason="capabilities_unknown",
                allows_poll=False,
                durable_idempotent_enqueue=None,
            )

        if not isinstance(capabilities, Mapping):
            return CompatibilityResult(
                status=CompatibilityStatus.INCOMPATIBLE,
                reason="capabilities_malformed",
                allows_poll=False,
            )

        if not _REQUIRED_KEYS.issubset(capabilities.keys()):
            missing = sorted(_REQUIRED_KEYS - set(capabilities.keys()))
            return CompatibilityResult(
                status=CompatibilityStatus.INCOMPATIBLE,
                reason=f"capabilities_malformed:missing:{','.join(missing)}",
                allows_poll=False,
            )

        protocol_major = _as_int(capabilities.get("protocol_major"))
        if protocol_major is None:
            return CompatibilityResult(
                status=CompatibilityStatus.INCOMPATIBLE,
                reason="capabilities_malformed:protocol_major",
                allows_poll=False,
            )

        if protocol_major != self.supported_protocol_major:
            return CompatibilityResult(
                status=CompatibilityStatus.INCOMPATIBLE,
                reason=f"protocol_major_mismatch:got={protocol_major}",
                allows_poll=False,
                protocol_major=protocol_major,
            )

        durable = has_durable_idempotent_enqueue(capabilities)
        if durable is None:
            return CompatibilityResult(
                status=CompatibilityStatus.INCOMPATIBLE,
                reason="required_capability_unknown:durable_idempotent_enqueue",
                allows_poll=False,
                protocol_major=protocol_major,
                durable_idempotent_enqueue=None,
            )
        if durable is False:
            return CompatibilityResult(
                status=CompatibilityStatus.INCOMPATIBLE,
                reason="required_capability_missing:durable_idempotent_enqueue",
                allows_poll=False,
                protocol_major=protocol_major,
                durable_idempotent_enqueue=False,
            )

        if int(intent_schema_major) != self.supported_intent_schema_major:
            return CompatibilityResult(
                status=CompatibilityStatus.INCOMPATIBLE,
                reason=f"intent_schema_major_unsupported:got={intent_schema_major}",
                allows_poll=False,
                protocol_major=protocol_major,
                durable_idempotent_enqueue=True,
                ignored_extension_keys=ignored,
            )

        if unknown_extensible_error:
            return self.classify_extensible_error(
                ErrorCode.parse(unknown_extensible_error)
            )

        return CompatibilityResult(
            status=CompatibilityStatus.SUPPORTED,
            reason="compatible",
            allows_poll=True,
            protocol_major=protocol_major,
            durable_idempotent_enqueue=True,
            ignored_extension_keys=ignored,
            details={"bridge_policy": bridge_policy},
        )

    def classify_extensible_error(self, code: ErrorCode) -> CompatibilityResult:
        """Unknown extensible Queue error codes → explicit UNKNOWN (never success)."""
        if code.is_unknown:
            return CompatibilityResult(
                status=CompatibilityStatus.UNKNOWN,
                reason=f"unknown_extensible_error:{code.value}",
                allows_poll=True,
                treat_as_success=False,
            )
        return CompatibilityResult(
            status=CompatibilityStatus.SUPPORTED,
            reason=f"known_error:{code.value}",
            allows_poll=True,
            treat_as_success=False,
        )

    def version_axes(
        self,
        *,
        capabilities: Mapping[str, Any],
        intent_schema_major: int,
        intent_schema_minor: int,
        bridge_package_version: str,
    ) -> dict[str, Any]:
        """Distinct version axes — never collapse protocol/schema/package/intent."""
        return {
            "queue_protocol_major": _as_int(capabilities.get("protocol_major")),
            "queue_schema_revision": str(
                capabilities.get("schema_revision", self.supported_schema_revision)
            ),
            "intent_schema_major": int(intent_schema_major),
            "intent_schema_minor": int(intent_schema_minor),
            "bridge_package_version": str(bridge_package_version),
        }
