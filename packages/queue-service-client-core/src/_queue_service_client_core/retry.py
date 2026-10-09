"""Opt-in, operation-aware retry helpers (SDK-11).

Retries are never enabled by role clients by default. Callers must construct an
explicit :class:`RetryPolicy` and pass an immutable :class:`RetryRequest`.
``retryable`` / ``retry_after_ms`` hints alone never authorize unsafe retries.
"""

from __future__ import annotations

import asyncio
import json
import random
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path
from typing import TypeVar

from _queue_service_client_core.errors import (
    AuthenticationError,
    LeaseLostError,
    ProtocolError,
    QueueClientError,
    RequestCancelledError,
    TerminalConflictError,
    TimeoutError,
    TransportError,
)

__all__ = [
    "RETRY_CLASS_NEVER",
    "RETRY_CLASS_SAFE_READ",
    "RETRY_CLASS_SAME_IDEMPOTENCY_KEY",
    "RETRY_CLASS_SAME_RESOURCE_IDENTITY",
    "RETRY_CLASS_SAME_TERMINAL_BODY",
    "RETRY_CLASSES",
    "RetryBudget",
    "RetryNotAllowedError",
    "RetryPolicy",
    "RetryRequest",
    "RetryRequestChangedError",
    "async_execute_with_retry",
    "execute_with_retry",
    "load_operation_retry_classes",
    "retry_class_for",
]

RETRY_CLASS_SAFE_READ = "safe-read"
RETRY_CLASS_SAME_IDEMPOTENCY_KEY = "same-idempotency-key"
RETRY_CLASS_SAME_RESOURCE_IDENTITY = "same-resource-identity"
RETRY_CLASS_SAME_TERMINAL_BODY = "same-terminal-body"
RETRY_CLASS_NEVER = "never"

RETRY_CLASSES: frozenset[str] = frozenset(
    {
        RETRY_CLASS_SAFE_READ,
        RETRY_CLASS_SAME_IDEMPOTENCY_KEY,
        RETRY_CLASS_SAME_RESOURCE_IDENTITY,
        RETRY_CLASS_SAME_TERMINAL_BODY,
        RETRY_CLASS_NEVER,
    }
)

_PERMISSION_CODES: frozenset[str] = frozenset(
    {
        "permission_denied",
        "forbidden",
        "unauthorized",
        "unauthenticated",
    }
)

T = TypeVar("T")


class RetryNotAllowedError(QueueClientError):
    """Raised when a retry helper is asked to wrap a never-retry operation."""

    def __init__(self, *, operation_id: str, retry_class: str) -> None:
        self.operation_id = operation_id
        self.retry_class = retry_class
        super().__init__(operation_id)

    def __str__(self) -> str:
        return (
            f"RetryNotAllowedError(operation_id={self.operation_id!r}, "
            f"retry_class={self.retry_class!r})"
        )

    def __repr__(self) -> str:
        return self.__str__()


class RetryRequestChangedError(QueueClientError):
    """Raised when a retry attempt would change key, body, or operation class."""

    def __init__(self, *, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)

    def __str__(self) -> str:
        return f"RetryRequestChangedError(reason={self.reason!r})"

    def __repr__(self) -> str:
        return self.__str__()


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Deterministic opt-in retry budget. No client applies this by default."""

    max_attempts: int
    max_elapsed_s: float
    initial_backoff_s: float = 0.05
    max_backoff_s: float = 2.0
    multiplier: float = 2.0
    jitter_ratio: float = 0.1
    max_server_hint_s: float = 30.0

    def __post_init__(self) -> None:
        if (
            not isinstance(self.max_attempts, int)
            or isinstance(self.max_attempts, bool)
            or self.max_attempts < 1
        ):
            raise ValueError("max_attempts must be a positive integer")
        if self.max_elapsed_s <= 0:
            raise ValueError("max_elapsed_s must be positive")
        if self.initial_backoff_s < 0:
            raise ValueError("initial_backoff_s must be >= 0")
        if self.max_backoff_s < 0:
            raise ValueError("max_backoff_s must be >= 0")
        if self.multiplier < 1.0:
            raise ValueError("multiplier must be >= 1.0")
        if not 0.0 <= self.jitter_ratio <= 1.0:
            raise ValueError("jitter_ratio must be in [0, 1]")
        if self.max_server_hint_s < 0:
            raise ValueError("max_server_hint_s must be >= 0")


@dataclass(frozen=True, slots=True)
class RetryRequest:
    """Immutable request identity required by retry helpers.

    ``body`` must be the exact bytes (or canonical encoding) that will be sent
    on every attempt. Helpers reject any drift in key, resource identity,
    terminal fingerprint, body, operation id, or retry class.

    ``same-resource-identity`` retries are safe because the operation targets
    the same immutable resource identity/path (for example ``task_id`` for
    ``cancelTask``), not because a wire idempotency key exists.
    """

    operation_id: str
    retry_class: str
    body: bytes = b""
    idempotency_key: str | None = None
    terminal_fingerprint: str | None = None
    resource_identity: str | None = None

    def __post_init__(self) -> None:
        if not self.operation_id:
            raise ValueError("operation_id is required")
        if self.retry_class not in RETRY_CLASSES:
            raise ValueError(f"unknown retry_class: {self.retry_class!r}")
        expected = retry_class_for(self.operation_id)
        if expected != self.retry_class:
            raise ValueError(
                f"retry_class {self.retry_class!r} does not match audited "
                f"class {expected!r} for {self.operation_id!r}"
            )
        if self.retry_class == RETRY_CLASS_SAME_IDEMPOTENCY_KEY:
            if not self.idempotency_key:
                raise ValueError(
                    "idempotency_key is required for same-idempotency-key"
                )
        if self.retry_class == RETRY_CLASS_SAME_RESOURCE_IDENTITY:
            if (
                not isinstance(self.resource_identity, str)
                or not self.resource_identity.strip()
            ):
                raise ValueError(
                    "resource_identity is required for same-resource-identity"
                )
        if self.retry_class == RETRY_CLASS_SAME_TERMINAL_BODY:
            if not self.terminal_fingerprint:
                raise ValueError(
                    "terminal_fingerprint is required for same-terminal-body"
                )
        if not isinstance(self.body, (bytes, bytearray)):
            raise TypeError("body must be bytes")
        object.__setattr__(self, "body", bytes(self.body))


@dataclass(slots=True)
class RetryBudget:
    """Mutable attempt/elapsed tracker bound to one :class:`RetryPolicy`."""

    policy: RetryPolicy
    attempts: int = 0
    started_at: float | None = None

    def start(self, *, now: float) -> None:
        if self.started_at is None:
            self.started_at = now

    def record_attempt(self) -> None:
        self.attempts += 1

    def elapsed(self, *, now: float) -> float:
        if self.started_at is None:
            return 0.0
        return max(0.0, now - self.started_at)

    def can_attempt(self, *, now: float) -> bool:
        if self.attempts >= self.policy.max_attempts:
            return False
        if self.started_at is None:
            return True
        return self.elapsed(now=now) < self.policy.max_elapsed_s

    def can_sleep(self, delay_s: float, *, now: float) -> bool:
        if delay_s < 0:
            return False
        if self.started_at is None:
            return delay_s <= self.policy.max_elapsed_s
        return self.elapsed(now=now) + delay_s <= self.policy.max_elapsed_s


def _bundled_manifest_text() -> str:
    ref = files("_queue_service_client_core") / "data" / "client-operation-ownership.json"
    return ref.read_text(encoding="utf-8")


def load_operation_retry_classes(
    path: Path | None = None,
) -> dict[str, str]:
    """Load audited ``operationId -> retry_class`` from the ownership manifest."""

    if path is not None:
        raw = json.loads(path.read_text(encoding="utf-8"))
    else:
        raw = json.loads(_bundled_manifest_text())
    operations = raw.get("operations")
    if not isinstance(operations, list):
        raise ValueError("ownership manifest missing operations array")
    out: dict[str, str] = {}
    for entry in operations:
        if not isinstance(entry, Mapping):
            raise ValueError(f"invalid ownership entry: {entry!r}")
        op_id = entry.get("operationId")
        retry_class = entry.get("retry_class")
        if not isinstance(op_id, str) or not op_id:
            raise ValueError(f"ownership entry missing operationId: {entry!r}")
        if not isinstance(retry_class, str) or retry_class not in RETRY_CLASSES:
            raise ValueError(
                f"{op_id}: retry_class must be one of {sorted(RETRY_CLASSES)}"
            )
        if op_id in out:
            raise ValueError(f"duplicate ownership entry for {op_id}")
        out[op_id] = retry_class
    return out


_RETRY_CLASS_CACHE: dict[str, str] | None = None


def retry_class_for(operation_id: str) -> str:
    """Return the audited retry class for ``operation_id``."""

    global _RETRY_CLASS_CACHE
    if _RETRY_CLASS_CACHE is None:
        _RETRY_CLASS_CACHE = load_operation_retry_classes()
    try:
        return _RETRY_CLASS_CACHE[operation_id]
    except KeyError as exc:
        raise KeyError(f"unknown operation_id: {operation_id!r}") from exc


def _assert_request_unchanged(original: RetryRequest, current: RetryRequest) -> None:
    if current.operation_id != original.operation_id:
        raise RetryRequestChangedError(reason="operation_id changed")
    if current.retry_class != original.retry_class:
        raise RetryRequestChangedError(reason="retry_class changed")
    if current.body != original.body:
        raise RetryRequestChangedError(reason="request body changed")
    if current.idempotency_key != original.idempotency_key:
        raise RetryRequestChangedError(reason="idempotency_key changed")
    if current.terminal_fingerprint != original.terminal_fingerprint:
        raise RetryRequestChangedError(reason="terminal_fingerprint changed")
    if current.resource_identity != original.resource_identity:
        raise RetryRequestChangedError(reason="resource_identity changed")


def _is_permission_error(exc: ProtocolError) -> bool:
    if exc.status_code in {401, 403}:
        return True
    return exc.code.value in _PERMISSION_CODES


def _should_retry_exception(exc: BaseException, *, retry_class: str) -> bool:
    if retry_class == RETRY_CLASS_NEVER:
        return False
    if isinstance(
        exc,
        (
            LeaseLostError,
            AuthenticationError,
            RequestCancelledError,
            TerminalConflictError,
            RetryNotAllowedError,
            RetryRequestChangedError,
            asyncio.CancelledError,
        ),
    ):
        return False
    if isinstance(exc, ProtocolError):
        if _is_permission_error(exc):
            return False
        # Never treat retryable=True as sufficient for never-class (already
        # filtered). For other classes, honor the structured hint only when true.
        return bool(exc.retryable)
    if isinstance(exc, (TransportError, TimeoutError)):
        return True
    return False


def _compute_delay_s(
    *,
    policy: RetryPolicy,
    attempt_index: int,
    exc: BaseException,
    rng: random.Random,
) -> float:
    """``attempt_index`` is zero-based count of failures so far (0 after first fail)."""

    exponential = policy.initial_backoff_s * (policy.multiplier**attempt_index)
    base = min(exponential, policy.max_backoff_s)
    server_hint = 0.0
    if isinstance(exc, ProtocolError) and exc.retry_after_ms is not None:
        server_hint = min(
            max(exc.retry_after_ms, 0) / 1000.0,
            policy.max_server_hint_s,
        )
    delay = max(base, server_hint)
    if policy.jitter_ratio <= 0 or delay <= 0:
        return delay
    # Deterministic when rng is seeded: symmetric jitter in ±ratio.
    span = delay * policy.jitter_ratio
    return max(0.0, delay + rng.uniform(-span, span))


def _ensure_allowed(request: RetryRequest) -> None:
    if request.retry_class == RETRY_CLASS_NEVER:
        raise RetryNotAllowedError(
            operation_id=request.operation_id,
            retry_class=request.retry_class,
        )


def _validate_budget_policy(tracker: RetryBudget, policy: RetryPolicy) -> None:
    if tracker.policy is not policy and (
        tracker.policy.max_attempts != policy.max_attempts
        or tracker.policy.max_elapsed_s != policy.max_elapsed_s
    ):
        raise ValueError("budget policy must match policy argument")


def execute_with_retry(
    request: RetryRequest,
    operation: Callable[[RetryRequest], T],
    *,
    policy: RetryPolicy,
    sleep: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
    rng: random.Random | None = None,
    is_cancelled: Callable[[], bool] | None = None,
    budget: RetryBudget | None = None,
) -> T:
    """Run ``operation(request)`` with an explicit sync retry budget.

    The same frozen ``request`` is passed on every attempt. Cancellation and
    lease loss abort immediately with no further calls.
    """

    _ensure_allowed(request)
    tracker = budget if budget is not None else RetryBudget(policy=policy)
    _validate_budget_policy(tracker, policy)
    entropy = rng if rng is not None else random.Random()
    original = request
    last_exc: BaseException | None = None

    while True:
        now = monotonic()
        tracker.start(now=now)
        if is_cancelled is not None and is_cancelled():
            raise RequestCancelledError(reason="cancelled")
        if not tracker.can_attempt(now=now):
            if last_exc is not None:
                raise last_exc
            raise TimeoutError(timeout_s=policy.max_elapsed_s)

        _assert_request_unchanged(original, request)
        tracker.record_attempt()
        try:
            return operation(request)
        except BaseException as exc:
            last_exc = exc
            if not _should_retry_exception(exc, retry_class=request.retry_class):
                raise
            now = monotonic()
            if not tracker.can_attempt(now=now):
                raise
            delay = _compute_delay_s(
                policy=policy,
                attempt_index=tracker.attempts - 1,
                exc=exc,
                rng=entropy,
            )
            if not tracker.can_sleep(delay, now=now):
                raise
            if is_cancelled is not None and is_cancelled():
                raise RequestCancelledError(reason="cancelled") from exc
            if delay > 0:
                sleep(delay)
                if is_cancelled is not None and is_cancelled():
                    raise RequestCancelledError(reason="cancelled") from exc


async def async_execute_with_retry(
    request: RetryRequest,
    operation: Callable[[RetryRequest], Awaitable[T]],
    *,
    policy: RetryPolicy,
    sleep: Callable[[float], Awaitable[None]] | None = None,
    monotonic: Callable[[], float] = time.monotonic,
    rng: random.Random | None = None,
    is_cancelled: Callable[[], bool] | None = None,
    budget: RetryBudget | None = None,
) -> T:
    """Async counterpart of :func:`execute_with_retry`."""

    _ensure_allowed(request)
    tracker = budget if budget is not None else RetryBudget(policy=policy)
    _validate_budget_policy(tracker, policy)
    entropy = rng if rng is not None else random.Random()
    async_sleep = sleep if sleep is not None else asyncio.sleep
    original = request
    last_exc: BaseException | None = None

    while True:
        now = monotonic()
        tracker.start(now=now)
        if is_cancelled is not None and is_cancelled():
            raise RequestCancelledError(reason="cancelled")
        # Cooperative cancel checkpoint before each attempt.
        await asyncio.sleep(0)
        if not tracker.can_attempt(now=now):
            if last_exc is not None:
                raise last_exc
            raise TimeoutError(timeout_s=policy.max_elapsed_s)

        _assert_request_unchanged(original, request)
        tracker.record_attempt()
        try:
            return await operation(request)
        except BaseException as exc:
            last_exc = exc
            if isinstance(exc, asyncio.CancelledError):
                raise
            if not _should_retry_exception(exc, retry_class=request.retry_class):
                raise
            now = monotonic()
            if not tracker.can_attempt(now=now):
                raise
            delay = _compute_delay_s(
                policy=policy,
                attempt_index=tracker.attempts - 1,
                exc=exc,
                rng=entropy,
            )
            if not tracker.can_sleep(delay, now=now):
                raise
            if is_cancelled is not None and is_cancelled():
                raise RequestCancelledError(reason="cancelled") from exc
            if delay > 0:
                await async_sleep(delay)
                if is_cancelled is not None and is_cancelled():
                    raise RequestCancelledError(reason="cancelled") from exc
