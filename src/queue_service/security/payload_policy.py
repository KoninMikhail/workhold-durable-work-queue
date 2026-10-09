"""Opaque payload handling and Phase 3.8 retention handoff.

Phase 3.2 establishes configuration and inspection/indexing denial. Physical
task/payload expiry (delete/detach/purge) is intentionally absent and must be
implemented in Phase 3.8 by consuming
``queue_service.security.payload_policy.PayloadRetentionPolicy``.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Final

# Align with storage hard ceiling (ADR / storage contract).
HARD_PAYLOAD_CEILING_BYTES: Final[int] = 1_048_576
DEFAULT_PAYLOAD_BYTES: Final[int] = 262_144

PAYLOAD_RETENTION_DAYS_MIN: Final[int] = 30
PAYLOAD_RETENTION_DAYS_MAX: Final[int] = 90


class PayloadIndexingRejected(ValueError):
    """Payload must not be projected into searchable/indexable fields."""

    def __init__(self) -> None:
        super().__init__("payload indexing and search derivation are not permitted")

    def __repr__(self) -> str:
        return "PayloadIndexingRejected('payload indexing and search derivation are not permitted')"

    def __str__(self) -> str:
        return "payload indexing and search derivation are not permitted"


class PayloadTooLarge(ValueError):
    """Opaque payload exceeds the deployment hard byte ceiling."""

    def __init__(self) -> None:
        super().__init__("payload exceeds hard byte ceiling")

    def __repr__(self) -> str:
        return "PayloadTooLarge('payload exceeds hard byte ceiling')"

    def __str__(self) -> str:
        return "payload exceeds hard byte ceiling"


@dataclass(frozen=True, slots=True)
class PayloadView:
    """Inspection projection: metadata by default; opaque body only when authorized."""

    metadata: Mapping[str, Any]
    payload: Mapping[str, Any] | None


@dataclass(frozen=True, slots=True)
class PayloadRetentionPolicy:
    """Exact Queue-store-time expiry contract for Phase 3.8 enforcement.

    ``expires_at`` / ``is_expired`` accept only timezone-aware Queue-store
    timestamps. This type performs no row deletion, partition detach, or
    registry purge.
    """

    retention_days: int

    def __post_init__(self) -> None:
        if not (
            PAYLOAD_RETENTION_DAYS_MIN
            <= self.retention_days
            <= PAYLOAD_RETENTION_DAYS_MAX
        ):
            raise ValueError(
                "payload retention_days must be between "
                f"{PAYLOAD_RETENTION_DAYS_MIN} and {PAYLOAD_RETENTION_DAYS_MAX} inclusive"
            )

    def expires_at(self, queue_store_created_at: datetime) -> datetime:
        if queue_store_created_at.tzinfo is None:
            raise ValueError("queue_store_created_at must be timezone-aware")
        return queue_store_created_at + timedelta(days=self.retention_days)

    def is_expired(
        self,
        queue_store_now: datetime,
        expires_at: datetime,
    ) -> bool:
        if queue_store_now.tzinfo is None:
            raise ValueError("queue_store_now must be timezone-aware")
        if expires_at.tzinfo is None:
            raise ValueError("expires_at must be timezone-aware")
        return queue_store_now >= expires_at


@dataclass(frozen=True, slots=True)
class PayloadHandlingPolicy:
    """Opaque JSON payload rules: bound size, no index/search, metadata-only default."""

    max_payload_bytes: int = HARD_PAYLOAD_CEILING_BYTES

    def validate_opaque_json_bytes(self, raw: bytes) -> None:
        if len(raw) > self.max_payload_bytes:
            raise PayloadTooLarge()

    def derive_index_fields(self, _payload: Mapping[str, Any]) -> dict[str, Any]:
        raise PayloadIndexingRejected()

    def derive_search_fields(self, _payload: Mapping[str, Any]) -> dict[str, Any]:
        raise PayloadIndexingRejected()

    def inspect(
        self,
        payload: Mapping[str, Any],
        *,
        payload_bytes: int,
        include_payload: bool = False,
    ) -> PayloadView:
        metadata: dict[str, Any] = {
            "payload_bytes": payload_bytes,
            "content_type": "application/json",
            "opaque": True,
        }
        body = dict(payload) if include_payload else None
        return PayloadView(metadata=metadata, payload=body)
