"""Application outbox bridge helpers for the producer client SDK."""

from __future__ import annotations

from queue_service_producer.bridge.compatibility import (
    SUPPORTED_CAPABILITIES,
    BridgeCompatibility,
    CompatibilityResult,
    CompatibilityStatus,
)
from queue_service_producer.bridge.idempotency import bridge_idempotency_key
from queue_service_producer.bridge.observability import BridgeHealth, BridgeTelemetry
from queue_service_producer.bridge.runner import BridgeRunner
from queue_service_producer.bridge.store import (
    AppStoreHealthSnapshot,
    BoundedPendingDepth,
    OldestPendingSnapshot,
    OutboxIntent,
    OutboxStore,
)

__all__ = [
    "AppStoreHealthSnapshot",
    "BoundedPendingDepth",
    "BridgeCompatibility",
    "BridgeHealth",
    "BridgeRunner",
    "BridgeTelemetry",
    "CompatibilityResult",
    "CompatibilityStatus",
    "OldestPendingSnapshot",
    "OutboxIntent",
    "OutboxStore",
    "SUPPORTED_CAPABILITIES",
    "bridge_idempotency_key",
]
