"""Application outbox bridge helpers for the producer client SDK."""

from __future__ import annotations

from workhold_producer.bridge.compatibility import (
    SUPPORTED_CAPABILITIES,
    BridgeCompatibility,
    CompatibilityResult,
    CompatibilityStatus,
)
from workhold_producer.bridge.idempotency import bridge_idempotency_key
from workhold_producer.bridge.observability import BridgeHealth, BridgeTelemetry
from workhold_producer.bridge.runner import BridgeRunner
from workhold_producer.bridge.store import (
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
