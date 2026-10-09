"""Guarded bulk replay and cancellation (REC-02 / OPS-08).

Dry-run previews a bounded indexed candidate set and issues an integrity-bound
expiring confirmation token. Execute re-validates the token, rechecks task state
transactionally, processes hard-bounded batches with per-item idempotency, and
emits redacted aggregate correlation only.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Any, Callable, Final
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from queue_service.domain.queue_control import DomainValidationError
from queue_service.infrastructure.postgres.task_transitions import (
    TaskTransitionRepository,
)
from queue_service.observability.context import emit_correlation, project_correlation
from queue_service.operations.dead_letter import (
    replay_dead_letter,
    validate_replay_reason,
)
from queue_service.settings import (
    ADMIN_REPLAY_TTL_SECONDS_DEFAULT,
    Secret,
)
from queue_service.storage.models import (
    AdminAuditLog,
    BreakGlassElevation,
    Queue,
    TaskActive,
    TaskTerminal,
)

logger = logging.getLogger(__name__)

BULK_CANDIDATE_CAP: Final[int] = 100
BULK_EXECUTE_BATCH_MAX: Final[int] = 25
BULK_SAMPLE_SIZE: Final[int] = 5
BULK_CONFIRM_TTL_SECONDS: Final[int] = 300
BULK_CONFIRM_VERSION: Final[str] = "v1"
BULK_QUEUE_REPLAY_RPS: Final[float] = 10.0
BULK_INSTANCE_REPLAY_RPS: Final[float] = 50.0

_AUDIT_OP_BULK_REPLAY: Final[int] = 7
_AUDIT_OP_BULK_CANCEL: Final[int] = 8
_TERMINAL_DEAD_LETTERED: Final[int] = 11
_TASK_DELAYED: Final[int] = 1
_TASK_READY: Final[int] = 2
_TASK_LEASED: Final[int] = 3

_OP_REPLAY: Final[str] = "bulk_replay"
_OP_CANCEL: Final[str] = "bulk_cancel"

_FORBIDDEN_FILTER_KEYS: Final[frozenset[str]] = frozenset(
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

_BULK_CORRELATION_KEYS: Final[frozenset[str]] = frozenset(
    {
        "request_id",
        "trace_id",
        "actor_id",
        "operation",
        "queue",
        "result",
        "code",
        "candidate_count",
        "batch_size",
        "succeeded_count",
        "skipped_count",
        "failed_count",
    }
)

_CANCEL_STATE_BY_NAME: Final[dict[str, int]] = {
    "delayed": _TASK_DELAYED,
    "ready": _TASK_READY,
    "leased": _TASK_LEASED,
}


@dataclass(frozen=True, slots=True)
class BulkPreviewResult:
    """Dry-run outcome: bounded candidates + confirmation token."""

    operation: str
    queue_name: str
    candidate_count: int
    truncated: bool
    sample_task_ids: tuple[str, ...]
    confirmation_token: str
    confirmation_expires_at: datetime
    max_batch: int


@dataclass(frozen=True, slots=True)
class BulkItemOutcome:
    """Per-task execute outcome without payloads or secrets."""

    task_id: str
    outcome: str
    code: str | None = None


@dataclass(frozen=True, slots=True)
class BulkExecuteResult:
    """Bounded batch execute outcome with explicit partial progress."""

    operation: str
    queue_name: str
    candidate_count: int
    start_index: int
    processed: int
    succeeded: int
    skipped: int
    failed: int
    partial: bool
    next_start_index: int | None
    outcomes: tuple[BulkItemOutcome, ...]
    warning: str | None = None


class BulkConfirmationCodec:
    """HMAC integrity-bound confirmation tokens (Secret + compare_digest)."""

    __slots__ = ("_key",)

    def __init__(self, secret: Secret) -> None:
        material = secret.get_secret_value().encode("utf-8")
        if not material:
            raise ValueError("confirmation signing secret must be non-empty")
        self._key = hashlib.sha256(material).digest()

    def encode(self, payload: Mapping[str, Any]) -> str:
        body = {"v": BULK_CONFIRM_VERSION, "p": dict(payload)}
        raw = json.dumps(body, separators=(",", ":"), sort_keys=True).encode("utf-8")
        if len(raw) > 16_384:
            raise DomainValidationError(
                "validation_failed",
                "confirmation payload exceeds bound",
            )
        digest = hmac.new(self._key, raw, hashlib.sha256).digest()
        return (
            base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")
            + "."
            + base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
        )

    def decode(self, token: str) -> dict[str, Any]:
        if not isinstance(token, str) or not token or len(token) > 24_576:
            raise DomainValidationError(
                "validation_failed",
                "confirmation token is invalid",
            )
        try:
            payload_b64, mac_b64 = token.split(".", 1)
        except ValueError as exc:
            raise DomainValidationError(
                "validation_failed",
                "confirmation token is invalid",
            ) from exc
        try:
            raw = _b64url_decode(payload_b64)
            mac = _b64url_decode(mac_b64)
        except (ValueError, UnicodeDecodeError) as exc:
            raise DomainValidationError(
                "validation_failed",
                "confirmation token is invalid",
            ) from exc
        expected = hmac.new(self._key, raw, hashlib.sha256).digest()
        if not hmac.compare_digest(expected, mac):
            raise DomainValidationError(
                "validation_failed",
                "confirmation token is invalid",
            )
        try:
            body = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DomainValidationError(
                "validation_failed",
                "confirmation token is invalid",
            ) from exc
        if not isinstance(body, dict) or body.get("v") != BULK_CONFIRM_VERSION:
            raise DomainValidationError(
                "validation_failed",
                "confirmation token is invalid",
            )
        payload = body.get("p")
        if not isinstance(payload, dict):
            raise DomainValidationError(
                "validation_failed",
                "confirmation token is invalid",
            )
        return payload


@dataclass
class _TokenBucket:
    rate_per_second: float
    capacity: float
    tokens: float
    updated_monotonic: float

    def refill(self, now: float) -> None:
        elapsed = max(0.0, now - self.updated_monotonic)
        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate_per_second)
        self.updated_monotonic = now

    def try_consume(self, now: float, amount: float = 1.0) -> bool:
        self.refill(now)
        if self.tokens >= amount:
            self.tokens -= amount
            return True
        return False


@dataclass(frozen=True, slots=True)
class DurableReplayElevation:
    """Active durable raiseReplayLimit elevation for one queue."""

    queue_name: str
    factor: float
    expires_at: datetime


@dataclass
class BulkReplayRateGate:
    """Queue/instance token buckets for bulk (and single) replay admissions."""

    queue_rps: float = BULK_QUEUE_REPLAY_RPS
    instance_rps: float = BULK_INSTANCE_REPLAY_RPS
    monotonic_clock: Callable[[], float] = field(default=time.monotonic)
    _lock: Lock = field(default_factory=Lock, init=False, repr=False)
    _queue_buckets: dict[str, _TokenBucket] = field(
        default_factory=dict, init=False, repr=False
    )
    _instance_bucket: _TokenBucket | None = field(default=None, init=False, repr=False)
    _temporary_raises: dict[str, tuple[float, float]] = field(
        default_factory=dict, init=False, repr=False
    )
    # Optional in-process cache of durable elevations (factor, expires_at UTC).
    _durable_cache: dict[str, DurableReplayElevation] = field(
        default_factory=dict, init=False, repr=False
    )

    def __post_init__(self) -> None:
        now = self.monotonic_clock()
        self._instance_bucket = _TokenBucket(
            rate_per_second=self.instance_rps,
            capacity=max(self.instance_rps, 1.0),
            tokens=max(self.instance_rps, 1.0),
            updated_monotonic=now,
        )

    def temporarily_raise(
        self,
        queue_name: str,
        *,
        factor: float,
        ttl_seconds: int,
    ) -> float:
        """Temporarily multiply queue bucket capacity/rate (break-glass only).

        Returns the effective queue RPS after the raise. Does not permanently
        change deployment hard limits; the raise expires after ``ttl_seconds``.
        Process-local only — durable elevations are the multi-replica source of
        truth when ``admit(..., session=)`` is used (D-10).
        """
        if factor < 1.0 or factor > 10.0:
            raise DomainValidationError(
                "validation_failed",
                "replay limit factor must be between 1.0 and 10.0",
            )
        if ttl_seconds < 1 or ttl_seconds > 3600:
            raise DomainValidationError(
                "validation_failed",
                "replay limit ttl_seconds must be between 1 and 3600",
            )
        with self._lock:
            now = self.monotonic_clock()
            effective_rps = self.queue_rps * factor
            self._queue_buckets[queue_name] = _TokenBucket(
                rate_per_second=effective_rps,
                capacity=max(effective_rps, 1.0),
                tokens=max(effective_rps, 1.0),
                updated_monotonic=now,
            )
            self._temporary_raises[queue_name] = (now + float(ttl_seconds), self.queue_rps)
            return effective_rps

    def load_durable_elevation(
        self,
        session: Session,
        queue_name: str,
    ) -> DurableReplayElevation | None:
        """Read active durable elevation; None if absent or expired (D-10/D-11).

        On storage read failure, raises ``DomainValidationError`` (fail closed —
        never silent unlimited). Expired rows are deleted under the caller's
        transaction (flush only).
        """
        try:
            return self._load_durable_elevation(session, queue_name)
        except DomainValidationError:
            raise
        except Exception as exc:
            raise DomainValidationError(
                "dependency_unavailable",
                "durable replay elevation read failed",
            ) from exc

    def _load_durable_elevation(
        self,
        session: Session,
        queue_name: str,
    ) -> DurableReplayElevation | None:
        row = session.execute(
            select(BreakGlassElevation).where(
                BreakGlassElevation.queue_name == queue_name
            )
        ).scalar_one_or_none()
        if row is None:
            self._durable_cache.pop(queue_name, None)
            return None
        store_now = session.scalar(select(func.transaction_timestamp()))
        if store_now is None:
            raise DomainValidationError(
                "dependency_unavailable",
                "durable replay elevation read failed",
            )
        if store_now.tzinfo is None:
            store_now = store_now.replace(tzinfo=timezone.utc)
        expires_at = row.expires_at
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        if expires_at <= store_now:
            session.delete(row)
            session.flush()
            self._durable_cache.pop(queue_name, None)
            return None
        elevation = DurableReplayElevation(
            queue_name=queue_name,
            factor=float(row.factor),
            expires_at=expires_at,
        )
        self._durable_cache[queue_name] = elevation
        return elevation

    def admit(
        self,
        queue_name: str,
        *,
        units: float = 1.0,
        session: Session | None = None,
    ) -> None:
        if units <= 0:
            return
        with self._lock:
            now = self.monotonic_clock()
            pending = self._temporary_raises.get(queue_name)
            if pending is not None and now >= pending[0]:
                self._temporary_raises.pop(queue_name, None)
                self._queue_buckets.pop(queue_name, None)

            durable_factor: float | None = None
            if session is not None:
                try:
                    elevation = self._load_durable_elevation(session, queue_name)
                except DomainValidationError:
                    raise
                except Exception as exc:
                    raise DomainValidationError(
                        "dependency_unavailable",
                        "durable replay elevation read failed",
                    ) from exc
                if elevation is not None:
                    durable_factor = elevation.factor

            assert self._instance_bucket is not None
            queue_bucket = self._queue_buckets.get(queue_name)
            target_rps = (
                self.queue_rps * durable_factor
                if durable_factor is not None
                else self.queue_rps
            )
            # When durable elevation is active, ensure bucket matches elevated RPS
            # even on a fresh gate instance that never called temporarily_raise.
            if durable_factor is not None:
                if (
                    queue_bucket is None
                    or abs(queue_bucket.rate_per_second - target_rps) > 1e-9
                ):
                    queue_bucket = _TokenBucket(
                        rate_per_second=target_rps,
                        capacity=max(target_rps, 1.0),
                        tokens=max(target_rps, 1.0),
                        updated_monotonic=now,
                    )
                    self._queue_buckets[queue_name] = queue_bucket
            elif queue_bucket is None:
                queue_bucket = _TokenBucket(
                    rate_per_second=self.queue_rps,
                    capacity=max(self.queue_rps, 1.0),
                    tokens=max(self.queue_rps, 1.0),
                    updated_monotonic=now,
                )
                self._queue_buckets[queue_name] = queue_bucket
            elif (
                queue_name not in self._temporary_raises
                and abs(queue_bucket.rate_per_second - self.queue_rps) > 1e-9
            ):
                # Durable elevation expired / absent: revert elevated ghost bucket.
                queue_bucket = _TokenBucket(
                    rate_per_second=self.queue_rps,
                    capacity=max(self.queue_rps, 1.0),
                    tokens=max(self.queue_rps, 1.0),
                    updated_monotonic=now,
                )
                self._queue_buckets[queue_name] = queue_bucket

            if not queue_bucket.try_consume(now, units):
                raise DomainValidationError(
                    "resource_exhausted",
                    "bulk replay queue rate limit exceeded",
                )
            if not self._instance_bucket.try_consume(now, units):
                # Refund queue tokens on instance reject.
                queue_bucket.tokens = min(
                    queue_bucket.capacity, queue_bucket.tokens + units
                )
                raise DomainValidationError(
                    "resource_exhausted",
                    "bulk replay instance rate limit exceeded",
                )


def project_bulk_admin_correlation(
    *,
    operation: str,
    request_id: str | None,
    trace_id: str | None,
    actor_id: str | None,
    queue: str | None,
    result: str,
    code: str | None,
    candidate_count: int | None = None,
    batch_size: int | None = None,
    succeeded_count: int | None = None,
    skipped_count: int | None = None,
    failed_count: int | None = None,
    extras: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Project bulk admin diagnostics through the shared allowlist."""
    fields: dict[str, Any] = {
        "request_id": request_id,
        "trace_id": trace_id,
        "actor_id": actor_id,
        "operation": operation,
        "queue": queue,
        "result": result,
        "code": code,
        "candidate_count": candidate_count,
        "batch_size": batch_size,
        "succeeded_count": succeeded_count,
        "skipped_count": skipped_count,
        "failed_count": failed_count,
    }
    polluted: dict[str, Any] = dict(extras or {})
    polluted.update(fields)
    projected = project_correlation(polluted)
    return {k: v for k, v in projected.items() if k in _BULK_CORRELATION_KEYS}


def emit_bulk_admin_correlation(
    logger_: logging.Logger,
    *,
    operation: str,
    request_id: str | None,
    trace_id: str | None,
    actor_id: str | None,
    queue: str | None,
    result: str,
    code: str | None,
    candidate_count: int | None = None,
    batch_size: int | None = None,
    succeeded_count: int | None = None,
    skipped_count: int | None = None,
    failed_count: int | None = None,
    extras: Mapping[str, Any] | None = None,
    span: Any | None = None,
) -> dict[str, Any]:
    """Project then emit bulk admin correlation to logs and optional spans."""
    projected = project_bulk_admin_correlation(
        operation=operation,
        request_id=request_id,
        trace_id=trace_id,
        actor_id=actor_id,
        queue=queue,
        result=result,
        code=code,
        candidate_count=candidate_count,
        batch_size=batch_size,
        succeeded_count=succeeded_count,
        skipped_count=skipped_count,
        failed_count=failed_count,
        extras=extras,
    )
    return emit_correlation(logger_, "bulk_admin", projected, span=span)


def normalize_bulk_filters(filters: Mapping[str, Any] | None) -> dict[str, str]:
    """Return sorted indexed filters; reject free-text/payload/SQL search."""
    if filters is None:
        return {}
    if not isinstance(filters, Mapping):
        raise DomainValidationError("validation_failed", "filters must be an object")
    out: dict[str, str] = {}
    for raw_key, raw_val in filters.items():
        key = str(raw_key).strip().lower()
        if key in _FORBIDDEN_FILTER_KEYS:
            raise DomainValidationError(
                "validation_failed",
                f"filter {key!r} is not allowed",
            )
        if key not in {
            "from",
            "to",
            "failure_code",
            "state",
            "states",
        }:
            raise DomainValidationError(
                "validation_failed",
                f"unknown filter {key!r}",
            )
        if raw_val is None:
            continue
        if not isinstance(raw_val, str):
            raise DomainValidationError(
                "validation_failed",
                f"filter {key!r} must be a string",
            )
        stripped = raw_val.strip()
        if not stripped:
            raise DomainValidationError(
                "validation_failed",
                f"filter {key!r} must be non-empty",
            )
        if len(stripped) > 128:
            raise DomainValidationError(
                "validation_failed",
                f"filter {key!r} exceeds 128 characters",
            )
        out[key] = stripped
    return dict(sorted(out.items()))


def _filters_hash(filters: Mapping[str, str]) -> str:
    canonical = json.dumps(dict(filters), separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _parse_dt(value: str, *, field_name: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise DomainValidationError(
            "validation_failed",
            f"{field_name} must be an ISO-8601 datetime",
        ) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _require_queue(session: Session, queue_name: str) -> Queue:
    queue = session.execute(
        select(Queue).where(Queue.name == queue_name)
    ).scalar_one_or_none()
    if queue is None:
        raise DomainValidationError("queue_not_found", "queue not found")
    return queue


def _select_replay_candidates(
    session: Session,
    *,
    queue: Queue,
    filters: Mapping[str, str],
    cap: int,
) -> tuple[list[UUID], bool]:
    if "from" not in filters or "to" not in filters:
        raise DomainValidationError(
            "validation_failed",
            "bulk replay filters require from and to time bounds",
        )
    from_at = _parse_dt(filters["from"], field_name="from")
    to_at = _parse_dt(filters["to"], field_name="to")
    if to_at < from_at:
        raise DomainValidationError(
            "validation_failed",
            "filter to must be >= from",
        )
    query = (
        select(TaskTerminal.task_id)
        .where(
            TaskTerminal.queue_id == int(queue.id),
            TaskTerminal.state_code == _TERMINAL_DEAD_LETTERED,
            TaskTerminal.terminal_at >= from_at,
            TaskTerminal.terminal_at <= to_at,
        )
        .order_by(TaskTerminal.terminal_at.asc(), TaskTerminal.task_id.asc())
        .limit(cap + 1)
    )
    if "failure_code" in filters:
        query = query.where(TaskTerminal.failure_code == filters["failure_code"])
    rows = list(session.execute(query).scalars())
    truncated = len(rows) > cap
    return list(rows[:cap]), truncated


def _parse_cancel_states(filters: Mapping[str, str]) -> frozenset[int]:
    raw = filters.get("states") or filters.get("state") or "delayed,ready,leased"
    names = [part.strip().lower() for part in raw.split(",") if part.strip()]
    if not names:
        raise DomainValidationError(
            "validation_failed",
            "cancel state filter must list delayed, ready, and/or leased",
        )
    codes: set[int] = set()
    for name in names:
        code = _CANCEL_STATE_BY_NAME.get(name)
        if code is None:
            raise DomainValidationError(
                "validation_failed",
                f"unknown cancel state {name!r}",
            )
        codes.add(code)
    return frozenset(codes)


def _select_cancel_candidates(
    session: Session,
    *,
    queue: Queue,
    filters: Mapping[str, str],
    cap: int,
) -> tuple[list[UUID], bool]:
    state_codes = _parse_cancel_states(filters)
    query = (
        select(TaskActive.task_id)
        .where(
            TaskActive.queue_id == int(queue.id),
            TaskActive.state_code.in_(tuple(sorted(state_codes))),
        )
        .order_by(TaskActive.created_at.asc(), TaskActive.task_id.asc())
        .limit(cap + 1)
    )
    if "from" in filters:
        query = query.where(
            TaskActive.created_at
            >= _parse_dt(filters["from"], field_name="from")
        )
    if "to" in filters:
        query = query.where(
            TaskActive.created_at <= _parse_dt(filters["to"], field_name="to")
        )
    if "from" in filters and "to" in filters:
        if _parse_dt(filters["to"], field_name="to") < _parse_dt(
            filters["from"], field_name="from"
        ):
            raise DomainValidationError(
                "validation_failed",
                "filter to must be >= from",
            )
    if "failure_code" in filters:
        raise DomainValidationError(
            "validation_failed",
            "failure_code filter is not valid for bulk cancel",
        )
    rows = list(session.execute(query).scalars())
    truncated = len(rows) > cap
    return list(rows[:cap]), truncated


def preview_bulk_operation(
    session: Session,
    *,
    operation: str,
    queue_name: str,
    filters: Mapping[str, Any] | None,
    principal_id: str,
    codec: BulkConfirmationCodec,
    candidate_cap: int = BULK_CANDIDATE_CAP,
    confirm_ttl_seconds: int = BULK_CONFIRM_TTL_SECONDS,
    now: datetime | None = None,
) -> BulkPreviewResult:
    """Select a bounded indexed candidate set and issue a confirmation token."""
    if operation not in {_OP_REPLAY, _OP_CANCEL}:
        raise DomainValidationError("validation_failed", "unknown bulk operation")
    if candidate_cap < 1 or candidate_cap > BULK_CANDIDATE_CAP:
        raise DomainValidationError(
            "validation_failed",
            f"candidate_cap must be between 1 and {BULK_CANDIDATE_CAP}",
        )
    if confirm_ttl_seconds < 30 or confirm_ttl_seconds > 3600:
        raise DomainValidationError(
            "validation_failed",
            "confirmation ttl out of bounds",
        )
    normalized = normalize_bulk_filters(filters)
    queue = _require_queue(session, queue_name)
    if operation == _OP_REPLAY:
        candidates, truncated = _select_replay_candidates(
            session, queue=queue, filters=normalized, cap=candidate_cap
        )
    else:
        candidates, truncated = _select_cancel_candidates(
            session, queue=queue, filters=normalized, cap=candidate_cap
        )

    store_now = now or datetime.now(timezone.utc)
    expires_at = store_now + timedelta(seconds=confirm_ttl_seconds)
    candidate_ids = [str(task_id) for task_id in candidates]
    payload = {
        "op": operation,
        "principal_id": principal_id,
        "queue": queue_name,
        "filters_hash": _filters_hash(normalized),
        "filters": normalized,
        "candidate_ids": candidate_ids,
        "candidate_count": len(candidate_ids),
        "max_batch": BULK_EXECUTE_BATCH_MAX,
        "exp": int(expires_at.timestamp()),
    }
    token = codec.encode(payload)
    sample = tuple(candidate_ids[:BULK_SAMPLE_SIZE])
    return BulkPreviewResult(
        operation=operation,
        queue_name=queue_name,
        candidate_count=len(candidate_ids),
        truncated=truncated,
        sample_task_ids=sample,
        confirmation_token=token,
        confirmation_expires_at=expires_at,
        max_batch=BULK_EXECUTE_BATCH_MAX,
    )


def _decode_and_validate_confirmation(
    *,
    codec: BulkConfirmationCodec,
    token: str,
    operation: str,
    principal_id: str,
    queue_name: str,
    filters: Mapping[str, Any] | None,
    now: datetime,
) -> dict[str, Any]:
    payload = codec.decode(token)
    if payload.get("op") != operation:
        raise DomainValidationError(
            "validation_failed",
            "confirmation token operation mismatch",
        )
    if payload.get("principal_id") != principal_id:
        raise DomainValidationError(
            "permission_denied",
            "confirmation token principal mismatch",
        )
    if payload.get("queue") != queue_name:
        raise DomainValidationError(
            "validation_failed",
            "confirmation token queue mismatch",
        )
    try:
        exp = int(payload["exp"])
    except (KeyError, TypeError, ValueError) as exc:
        raise DomainValidationError(
            "validation_failed",
            "confirmation token is invalid",
        ) from exc
    if int(now.timestamp()) > exp:
        raise DomainValidationError(
            "confirmation_expired",
            "confirmation token has expired",
        )
    normalized = normalize_bulk_filters(filters)
    token_filters = payload.get("filters")
    if not isinstance(token_filters, dict):
        raise DomainValidationError(
            "validation_failed",
            "confirmation token is invalid",
        )
    token_normalized = normalize_bulk_filters(token_filters)
    if _filters_hash(normalized) != payload.get("filters_hash"):
        raise DomainValidationError(
            "validation_failed",
            "confirmation filters do not match preview",
        )
    if token_normalized != normalized:
        raise DomainValidationError(
            "validation_failed",
            "confirmation filters do not match preview",
        )
    candidates = payload.get("candidate_ids")
    if not isinstance(candidates, list):
        raise DomainValidationError(
            "validation_failed",
            "confirmation token is invalid",
        )
    if len(candidates) > BULK_CANDIDATE_CAP:
        raise DomainValidationError(
            "validation_failed",
            "confirmation candidate set exceeds bound",
        )
    if int(payload.get("candidate_count", -1)) != len(candidates):
        raise DomainValidationError(
            "validation_failed",
            "confirmation candidate boundary mismatch",
        )
    for item in candidates:
        try:
            UUID(str(item))
        except (TypeError, ValueError) as exc:
            raise DomainValidationError(
                "validation_failed",
                "confirmation token is invalid",
            ) from exc
    return payload


def execute_bulk_replay(
    session: Session,
    *,
    queue_name: str,
    filters: Mapping[str, Any] | None,
    confirmation_token: str,
    reason: str,
    principal_id: str,
    actor_id: str,
    request_id: str,
    idempotency_key: str,
    codec: BulkConfirmationCodec,
    rate_gate: BulkReplayRateGate | None = None,
    start_index: int = 0,
    batch_limit: int | None = None,
    admin_replay_ttl_seconds: int = ADMIN_REPLAY_TTL_SECONDS_DEFAULT,
    now: datetime | None = None,
) -> BulkExecuteResult:
    """Execute a confirmation-bound bulk dead-letter replay batch."""
    store_now = now or datetime.now(timezone.utc)
    payload = _decode_and_validate_confirmation(
        codec=codec,
        token=confirmation_token,
        operation=_OP_REPLAY,
        principal_id=principal_id,
        queue_name=queue_name,
        filters=filters,
        now=store_now,
    )
    candidates = [UUID(str(x)) for x in payload["candidate_ids"]]
    if start_index < 0 or start_index > len(candidates):
        raise DomainValidationError(
            "validation_failed",
            "start_index out of confirmed candidate range",
        )
    limit = BULK_EXECUTE_BATCH_MAX if batch_limit is None else int(batch_limit)
    if limit < 1 or limit > BULK_EXECUTE_BATCH_MAX:
        raise DomainValidationError(
            "validation_failed",
            f"batch_limit must be between 1 and {BULK_EXECUTE_BATCH_MAX}",
        )
    batch = candidates[start_index : start_index + limit]
    if rate_gate is not None and batch:
        rate_gate.admit(queue_name, units=float(len(batch)), session=session)

    bounded_reason = validate_replay_reason(reason)
    outcomes: list[BulkItemOutcome] = []
    succeeded = skipped = failed = 0
    for source_task_id in batch:
        item_key = f"{idempotency_key}:{source_task_id}"
        try:
            result = replay_dead_letter(
                session,
                queue_name=queue_name,
                source_task_id=source_task_id,
                reason=bounded_reason,
                principal_id=principal_id,
                actor_id=actor_id,
                request_id=request_id,
                idempotency_key=item_key,
                admin_replay_ttl_seconds=admin_replay_ttl_seconds,
            )
            if result.replayed:
                skipped += 1
                outcomes.append(
                    BulkItemOutcome(
                        task_id=str(source_task_id),
                        outcome="replayed",
                        code=None,
                    )
                )
            else:
                succeeded += 1
                outcomes.append(
                    BulkItemOutcome(
                        task_id=str(source_task_id),
                        outcome="succeeded",
                        code=None,
                    )
                )
        except DomainValidationError as exc:
            if exc.code in {
                "task_not_found",
                "queue_draining",
                "payload_too_large",
                "resource_exhausted",
            }:
                skipped += 1
                outcomes.append(
                    BulkItemOutcome(
                        task_id=str(source_task_id),
                        outcome="skipped",
                        code=exc.code,
                    )
                )
            else:
                failed += 1
                outcomes.append(
                    BulkItemOutcome(
                        task_id=str(source_task_id),
                        outcome="failed",
                        code=exc.code,
                    )
                )

    processed = len(batch)
    next_index = start_index + processed
    partial = next_index < len(candidates)
    queue = _require_queue(session, queue_name)
    session.add(
        AdminAuditLog(
            audit_at=store_now,
            queue_id=int(queue.id),
            actor_id=actor_id,
            operation_code=_AUDIT_OP_BULK_REPLAY,
            previous_config_version=None,
            new_config_version=None,
            request_id=UUID(request_id),
            details={
                "operation": _OP_REPLAY,
                "candidate_count": len(candidates),
                "start_index": start_index,
                "processed": processed,
                "succeeded": succeeded,
                "skipped": skipped,
                "failed": failed,
                "partial": partial,
                "outcomes": [
                    {"task_id": o.task_id, "outcome": o.outcome, "code": o.code}
                    for o in outcomes
                ],
            },
        )
    )
    session.flush()
    return BulkExecuteResult(
        operation=_OP_REPLAY,
        queue_name=queue_name,
        candidate_count=len(candidates),
        start_index=start_index,
        processed=processed,
        succeeded=succeeded,
        skipped=skipped,
        failed=failed,
        partial=partial,
        next_start_index=next_index if partial else None,
        outcomes=tuple(outcomes),
        warning="Replay is at-least-once and may repeat external effects.",
    )


def execute_bulk_cancel(
    session: Session,
    *,
    queue_name: str,
    filters: Mapping[str, Any] | None,
    confirmation_token: str,
    reason: str,
    principal_id: str,
    actor_id: str,
    request_id: str,
    codec: BulkConfirmationCodec,
    start_index: int = 0,
    batch_limit: int | None = None,
    transitions: TaskTransitionRepository | None = None,
    now: datetime | None = None,
) -> BulkExecuteResult:
    """Execute a confirmation-bound bulk cancel batch (no spawn/events)."""
    store_now = now or datetime.now(timezone.utc)
    payload = _decode_and_validate_confirmation(
        codec=codec,
        token=confirmation_token,
        operation=_OP_CANCEL,
        principal_id=principal_id,
        queue_name=queue_name,
        filters=filters,
        now=store_now,
    )
    _ = validate_replay_reason(reason)  # same bounded reason contract
    candidates = [UUID(str(x)) for x in payload["candidate_ids"]]
    if start_index < 0 or start_index > len(candidates):
        raise DomainValidationError(
            "validation_failed",
            "start_index out of confirmed candidate range",
        )
    limit = BULK_EXECUTE_BATCH_MAX if batch_limit is None else int(batch_limit)
    if limit < 1 or limit > BULK_EXECUTE_BATCH_MAX:
        raise DomainValidationError(
            "validation_failed",
            f"batch_limit must be between 1 and {BULK_EXECUTE_BATCH_MAX}",
        )
    batch = candidates[start_index : start_index + limit]
    repo = transitions if transitions is not None else TaskTransitionRepository()
    outcomes: list[BulkItemOutcome] = []
    succeeded = skipped = failed = 0

    for task_id in batch:
        try:
            active = session.execute(
                select(TaskActive).where(TaskActive.task_id == task_id)
            ).scalar_one_or_none()
            if active is None:
                skipped += 1
                outcomes.append(
                    BulkItemOutcome(
                        task_id=str(task_id),
                        outcome="skipped",
                        code="task_not_found",
                    )
                )
                continue
            state_code = int(active.state_code)
            if state_code not in {_TASK_DELAYED, _TASK_READY, _TASK_LEASED}:
                skipped += 1
                outcomes.append(
                    BulkItemOutcome(
                        task_id=str(task_id),
                        outcome="skipped",
                        code="not_cancellable",
                    )
                )
                continue
            result = repo.cancel_task(
                session,
                task_id=task_id,
                producer_id=str(active.producer_id),
                authorize_queue=lambda name, expected=queue_name: name == expected,
            )
            # Ensure cancel path created no spawn/event side channels.
            projection = result.task
            spawned = projection.get("spawned_task_ids") or []
            events = (
                projection.get("delivery_event_ids")
                or projection.get("event_ids")
                or []
            )
            if spawned or events:
                raise DomainValidationError(
                    "internal_error",
                    "bulk cancel must not create spawn or delivery events",
                )
            if result.replayed:
                skipped += 1
                outcomes.append(
                    BulkItemOutcome(
                        task_id=str(task_id),
                        outcome="replayed",
                        code=None,
                    )
                )
            else:
                succeeded += 1
                outcome_name = (
                    "cancel_requested"
                    if state_code == _TASK_LEASED
                    else "cancelled"
                )
                outcomes.append(
                    BulkItemOutcome(
                        task_id=str(task_id),
                        outcome=outcome_name,
                        code=None,
                    )
                )
        except DomainValidationError as exc:
            failed += 1
            outcomes.append(
                BulkItemOutcome(
                    task_id=str(task_id),
                    outcome="failed",
                    code=exc.code,
                )
            )

    processed = len(batch)
    next_index = start_index + processed
    partial = next_index < len(candidates)
    queue = _require_queue(session, queue_name)
    session.add(
        AdminAuditLog(
            audit_at=store_now,
            queue_id=int(queue.id),
            actor_id=actor_id,
            operation_code=_AUDIT_OP_BULK_CANCEL,
            previous_config_version=None,
            new_config_version=None,
            request_id=UUID(request_id),
            details={
                "operation": _OP_CANCEL,
                "candidate_count": len(candidates),
                "start_index": start_index,
                "processed": processed,
                "succeeded": succeeded,
                "skipped": skipped,
                "failed": failed,
                "partial": partial,
                "outcomes": [
                    {"task_id": o.task_id, "outcome": o.outcome, "code": o.code}
                    for o in outcomes
                ],
                # reason retained in audit only (never correlation)
                "reason": reason if isinstance(reason, str) else None,
            },
        )
    )
    session.flush()
    return BulkExecuteResult(
        operation=_OP_CANCEL,
        queue_name=queue_name,
        candidate_count=len(candidates),
        start_index=start_index,
        processed=processed,
        succeeded=succeeded,
        skipped=skipped,
        failed=failed,
        partial=partial,
        next_start_index=next_index if partial else None,
        outcomes=tuple(outcomes),
        warning=None,
    )


def _b64url_decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)


# Re-export operation constants for handlers/tests.
BULK_OP_REPLAY = _OP_REPLAY
BULK_OP_CANCEL = _OP_CANCEL
