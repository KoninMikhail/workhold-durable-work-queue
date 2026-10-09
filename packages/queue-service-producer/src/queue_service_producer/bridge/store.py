"""Framework-neutral application outbox store port (BRDG-02 / Plan 06-03).

Defines immutable intent/lease/health types and the lifecycle protocol required
by the application-outbox bridge. Queue never owns application tables; adapters
are injected by the application.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, runtime_checkable


@dataclass(frozen=True, slots=True)
class OutboxIntent:
    """Immutable claimed enqueue intent plus current opaque ownership fence."""

    source_namespace: str
    source_row_id: str
    schema_version: int
    target_queue: str
    enqueue_request: Mapping[str, Any]
    created_at: datetime
    ownership_token: str
    generation: int
    lease_expires_at: datetime
    traceparent: str | None = None
    tracestate: str | None = None
    extensions: Mapping[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class BoundedPendingDepth:
    """Bounded count of non-delivered intents under an explicit depth cap."""

    count: int
    depth_cap: int
    capped: bool
    as_of: datetime


@dataclass(frozen=True, slots=True)
class OldestPendingSnapshot:
    """Oldest non-delivered ``created_at`` using application-DB time."""

    created_at: datetime | None
    as_of: datetime


@dataclass(frozen=True, slots=True)
class AppStoreHealthSnapshot:
    """Bounded app-store health without payload or identity fields."""

    as_of: datetime
    connected: bool
    query_ok: bool
    pending_count: int
    pending_capped: bool
    oldest_pending_created_at: datetime | None


@runtime_checkable
class OutboxStore(Protocol):
    """App-owned outbox lifecycle used by the supported bridge runner."""

    def claim(self, *, limit: int, lease_seconds: int) -> Sequence[OutboxIntent]:
        """Claim a bounded batch of pending/reclaimable intents under app-DB time."""

    def mark_delivered(
        self,
        *,
        source_namespace: str,
        source_row_id: str,
        ownership_token: str,
        queue_task_id: str | None = None,
    ) -> bool:
        """Mark delivered only if ``ownership_token`` is the current unexpired lease."""

    def schedule_retry(
        self,
        *,
        source_namespace: str,
        source_row_id: str,
        ownership_token: str,
        available_at_delay_seconds: float,
        failure_code: str | None = None,
    ) -> bool:
        """Clear the current fence and defer availability in the application DB."""

    def mark_terminal_operator_action(
        self,
        *,
        source_namespace: str,
        source_row_id: str,
        ownership_token: str,
        reason: str,
    ) -> bool:
        """Fence-gated transition to terminal operator-action state."""

    def get_pending_depth(self, depth_cap: int) -> BoundedPendingDepth:
        """Return a hard-bounded pending depth (at most ``depth_cap`` + 1 scan)."""

    def get_oldest_pending_created_at(self) -> OldestPendingSnapshot:
        """Return oldest non-delivered ``created_at`` with app-DB ``as_of``."""

    def get_health_snapshot(self, depth_cap: int) -> AppStoreHealthSnapshot:
        """Connectivity + bounded pending depth + oldest pending age inputs."""
