"""Held-session correctness registry expiry purge (STOR-08).

Purges expired rows from ``enqueue_dedup``, ``complete_replay``, and the exact
Phase 3.1 ``admin_replay`` relation in bounded incremental batches. Operates only
through the caller-held SQLAlchemy ``Connection``: no advisory locks, no engine
creation, and no ownership of the outer transaction. Cutoff time is always
Queue-store ``CURRENT_TIMESTAMP`` (never an application clock).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Final, Literal

from sqlalchemy import text
from sqlalchemy.engine import Connection

RegistryName = Literal["enqueue_dedup", "complete_replay", "admin_replay"]

CORRECTNESS_REGISTRIES: Final[tuple[RegistryName, ...]] = (
    "enqueue_dedup",
    "complete_replay",
    "admin_replay",
)

REGISTRY_PURGE_BATCH_SIZE_MIN: Final[int] = 1
REGISTRY_PURGE_BATCH_SIZE_MAX: Final[int] = 10_000

_STORE_NOW_SQL = text("SELECT CURRENT_TIMESTAMP")

# Allowlisted relation names only — never interpolated from caller input beyond
# the frozen CORRECTNESS_REGISTRIES set.
_PURGE_SQL: Final[dict[RegistryName, object]] = {
    name: text(
        f"""
        WITH picked AS (
            SELECT id
            FROM {name}
            WHERE expires_at <= :cutoff
            ORDER BY expires_at ASC, id ASC
            LIMIT :batch_size
            FOR UPDATE OF {name} SKIP LOCKED
        ),
        deleted AS (
            DELETE FROM {name} AS target
            USING picked
            WHERE target.id = picked.id
            RETURNING target.id
        )
        SELECT count(*)::bigint AS deleted_count FROM deleted
        """
    )
    for name in CORRECTNESS_REGISTRIES
}

_OLDEST_REMAINING_SQL: Final[dict[RegistryName, object]] = {
    name: text(f"SELECT min(expires_at) FROM {name}")
    for name in CORRECTNESS_REGISTRIES
}

_MORE_WORK_SQL: Final[dict[RegistryName, object]] = {
    name: text(
        f"""
        SELECT EXISTS (
            SELECT 1 FROM {name}
            WHERE expires_at <= :cutoff
            LIMIT 1
        )
        """
    )
    for name in CORRECTNESS_REGISTRIES
}


@dataclass(frozen=True, slots=True)
class RegistryPurgeOutcome:
    """Typed per-registry purge outcome for maintenance reporting."""

    registry: RegistryName
    examined: int
    deleted: int
    oldest_remaining_expires_at: datetime | None
    more_work: bool


@dataclass(frozen=True, slots=True)
class RegistryPurgeResult:
    """Aggregate one-batch purge across all correctness registries."""

    store_now: datetime
    outcomes: tuple[RegistryPurgeOutcome, ...]


def purge_expired_registries(
    connection: Connection,
    *,
    batch_size: int,
) -> RegistryPurgeResult:
    """Delete at most ``batch_size`` expired rows per correctness registry.

    Requires a caller-held ``connection``. Does not commit, roll back, create
    engines, or take advisory locks. Selection uses indexed ``expires_at,id``
    order with ``FOR UPDATE … SKIP LOCKED`` so concurrent/repeated runs converge
    safely without full-registry scans beyond the batch.
    """
    validated_batch = _validate_batch_size(batch_size)
    store_now = _store_utc_now(connection)
    outcomes = tuple(
        _purge_one(
            connection,
            registry=name,
            batch_size=validated_batch,
            cutoff=store_now,
        )
        for name in CORRECTNESS_REGISTRIES
    )
    return RegistryPurgeResult(store_now=store_now, outcomes=outcomes)


def purge_expired_registry(
    connection: Connection,
    *,
    registry: RegistryName,
    batch_size: int,
) -> RegistryPurgeOutcome:
    """Purge one bounded batch from a single allowlisted correctness registry."""
    if registry not in CORRECTNESS_REGISTRIES:
        raise ValueError(f"unknown correctness registry: {registry!r}")
    validated_batch = _validate_batch_size(batch_size)
    store_now = _store_utc_now(connection)
    return _purge_one(
        connection,
        registry=registry,
        batch_size=validated_batch,
        cutoff=store_now,
    )


def _purge_one(
    connection: Connection,
    *,
    registry: RegistryName,
    batch_size: int,
    cutoff: datetime,
) -> RegistryPurgeOutcome:
    deleted = int(
        connection.execute(
            _PURGE_SQL[registry],
            {"cutoff": cutoff, "batch_size": batch_size},
        ).scalar_one()
    )
    # Examined equals deleted under this SELECT-for-update-then-delete pattern:
    # SKIP LOCKED rows were not examined by this session.
    examined = deleted
    oldest = connection.execute(_OLDEST_REMAINING_SQL[registry]).scalar_one()
    if oldest is not None and isinstance(oldest, datetime) and oldest.tzinfo is None:
        oldest = oldest.replace(tzinfo=timezone.utc)
    more_work = bool(
        connection.execute(_MORE_WORK_SQL[registry], {"cutoff": cutoff}).scalar_one()
    )
    return RegistryPurgeOutcome(
        registry=registry,
        examined=examined,
        deleted=deleted,
        oldest_remaining_expires_at=oldest if isinstance(oldest, datetime) else None,
        more_work=more_work,
    )


def _validate_batch_size(batch_size: object) -> int:
    if isinstance(batch_size, bool) or not isinstance(batch_size, int):
        raise ValueError("registry purge batch_size must be an int")
    if batch_size < REGISTRY_PURGE_BATCH_SIZE_MIN or batch_size > REGISTRY_PURGE_BATCH_SIZE_MAX:
        raise ValueError(
            "registry purge batch_size must be between "
            f"{REGISTRY_PURGE_BATCH_SIZE_MIN} and {REGISTRY_PURGE_BATCH_SIZE_MAX} "
            f"inclusive, got {batch_size}"
        )
    return batch_size


def _store_utc_now(connection: Connection) -> datetime:
    value = connection.execute(_STORE_NOW_SQL).scalar_one()
    if not isinstance(value, datetime):
        raise TypeError("CURRENT_TIMESTAMP did not return datetime")
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value
