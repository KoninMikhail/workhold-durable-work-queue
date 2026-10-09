"""Framework-neutral client instrumentation (SDK-12).

Events are allowlisted and low-cardinality. Hooks never receive headers, query
values, bodies, payloads, queue/task/claim identifiers, tokens, idempotency keys,
or incident text. Hook failures cannot alter Queue HTTP behavior.
"""

from __future__ import annotations

import asyncio
import re
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Final, Protocol, TypeVar, runtime_checkable

from _queue_service_client_core.errors import (
    AuthenticationError,
    LeaseLostError,
    MalformedResponseError,
    ProtocolError,
    QueueClientError,
    RequestCancelledError,
    TimeoutError as ClientTimeoutError,
    TransportError,
)
from _queue_service_client_core.redaction import redact_text

__all__ = [
    "ALLOWED_RESULT_LABELS",
    "OPERATION_EVENT_KINDS",
    "OPERATION_STATUSES",
    "AsyncInstrumentation",
    "AsyncInstrumentationHooks",
    "HookErrorSink",
    "InstrumentationHooks",
    "NullObservationSink",
    "ObservationSink",
    "OperationEvent",
    "OperationSpan",
    "RequestObservation",
    "SyncInstrumentation",
    "observe_request",
]

T = TypeVar("T")

ALLOWED_RESULT_LABELS: Final[frozenset[str]] = frozenset(
    {
        "success",
        "empty",
        "timeout",
        "cancelled",
        "protocol_error",
        "transport_error",
        "malformed",
        "error",
    }
)

OPERATION_EVENT_KINDS: Final[frozenset[str]] = frozenset(
    {
        "start",
        "attempt",
        "success",
        "failure",
        "cancelled",
    }
)

OPERATION_STATUSES: Final[frozenset[str]] = frozenset(
    {
        "started",
        "attempting",
        "ok",
        "error",
        "cancelled",
    }
)

_ROUTE_TEMPLATE_RE = re.compile(
    r"^/(?:[A-Za-z0-9._-]+|\{[A-Za-z_][A-Za-z0-9_]*\})"
    r"(?:/(?:[A-Za-z0-9._-]+|\{[A-Za-z_][A-Za-z0-9_]*\}))*$"
)

HookErrorSink = Callable[[BaseException], None]


@dataclass(frozen=True, slots=True)
class RequestObservation:
    """Bounded observation of one client request outcome (legacy helper)."""

    operation: str
    result: str
    duration_s: float
    status_code: int | None = None
    code: str | None = None

    def __post_init__(self) -> None:
        if self.result not in ALLOWED_RESULT_LABELS:
            raise ValueError(f"unsupported observation result: {self.result!r}")
        object.__setattr__(self, "operation", redact_text(self.operation)[:64])
        if self.code is not None:
            object.__setattr__(self, "code", redact_text(self.code)[:64])

    def as_labels(self) -> Mapping[str, str]:
        labels: dict[str, str] = {
            "operation": self.operation,
            "result": self.result,
        }
        if self.status_code is not None:
            labels["status_code"] = str(self.status_code)
        if self.code is not None:
            labels["code"] = self.code
        return labels


ObservationSink = Callable[[RequestObservation], None]


class NullObservationSink:
    """Default no-op sink."""

    def __call__(self, observation: RequestObservation) -> None:
        return None


def observe_request(
    sink: ObservationSink | None,
    *,
    operation: str,
    result: str,
    duration_s: float,
    status_code: int | None = None,
    code: str | None = None,
) -> None:
    if sink is None:
        return
    sink(
        RequestObservation(
            operation=operation,
            result=result,
            duration_s=duration_s,
            status_code=status_code,
            code=code,
        )
    )


@dataclass(frozen=True, slots=True)
class OperationEvent:
    """Allowlisted operation telemetry. Contains no secrets or resource IDs."""

    kind: str
    operation_id: str
    method: str
    route_template: str
    attempt: int
    duration_s: float
    status: str
    status_code: int | None = None
    error_code: str | None = None
    retryable: bool | None = None
    request_id: str | None = None

    def __post_init__(self) -> None:
        if self.kind not in OPERATION_EVENT_KINDS:
            raise ValueError(f"unsupported operation event kind: {self.kind!r}")
        if self.status not in OPERATION_STATUSES:
            raise ValueError(f"unsupported operation status: {self.status!r}")
        if self.attempt < 1:
            raise ValueError("attempt must be >= 1")
        if self.duration_s < 0:
            raise ValueError("duration_s must be >= 0")
        object.__setattr__(self, "operation_id", _clean_token(self.operation_id, 128))
        object.__setattr__(self, "method", _clean_method(self.method))
        object.__setattr__(
            self, "route_template", _clean_route_template(self.route_template)
        )
        if self.error_code is not None:
            object.__setattr__(self, "error_code", _clean_token(self.error_code, 64))
        if self.request_id is not None:
            object.__setattr__(self, "request_id", _clean_token(self.request_id, 128))

    def as_labels(self) -> Mapping[str, str]:
        labels: dict[str, str] = {
            "kind": self.kind,
            "operation_id": self.operation_id,
            "method": self.method,
            "route_template": self.route_template,
            "attempt": str(self.attempt),
            "status": self.status,
        }
        if self.status_code is not None:
            labels["status_code"] = str(self.status_code)
        if self.error_code is not None:
            labels["error_code"] = self.error_code
        if self.retryable is not None:
            labels["retryable"] = "true" if self.retryable else "false"
        # request_id stays on the event for traces/logs; omit from metric labels
        # (high cardinality).
        return labels


@dataclass(slots=True)
class OperationSpan:
    """In-flight operation timing and identity for event emission."""

    operation_id: str
    method: str
    route_template: str
    started_at: float
    attempt: int = 1


@runtime_checkable
class InstrumentationHooks(Protocol):
    """Sync hooks for allowlisted operation events."""

    def on_start(self, event: OperationEvent) -> None: ...

    def on_attempt(self, event: OperationEvent) -> None: ...

    def on_success(self, event: OperationEvent) -> None: ...

    def on_failure(self, event: OperationEvent) -> None: ...

    def on_cancelled(self, event: OperationEvent) -> None: ...


@runtime_checkable
class AsyncInstrumentationHooks(Protocol):
    """Async hooks for allowlisted operation events."""

    async def on_start(self, event: OperationEvent) -> None: ...

    async def on_attempt(self, event: OperationEvent) -> None: ...

    async def on_success(self, event: OperationEvent) -> None: ...

    async def on_failure(self, event: OperationEvent) -> None: ...

    async def on_cancelled(self, event: OperationEvent) -> None: ...


class SyncInstrumentation:
    """Emit sync operation events to hooks without affecting call outcomes."""

    def __init__(
        self,
        hooks: InstrumentationHooks | None = None,
        *,
        hook_error_sink: HookErrorSink | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._hooks = hooks
        self._hook_error_sink = hook_error_sink
        self._monotonic = monotonic

    def start(
        self,
        *,
        operation_id: str,
        method: str,
        route_template: str,
        attempt: int = 1,
    ) -> OperationSpan:
        span = OperationSpan(
            operation_id=_clean_token(operation_id, 128),
            method=_clean_method(method),
            route_template=_clean_route_template(route_template),
            started_at=self._monotonic(),
            attempt=max(1, attempt),
        )
        self._emit(
            "on_start",
            _build_event(
                kind="start",
                span=span,
                status="started",
                duration_s=0.0,
            ),
        )
        return span

    def attempt(self, span: OperationSpan, *, attempt: int | None = None) -> None:
        if attempt is not None:
            span.attempt = max(1, attempt)
        self._emit(
            "on_attempt",
            _build_event(
                kind="attempt",
                span=span,
                status="attempting",
                duration_s=self._elapsed(span),
            ),
        )

    def success(
        self,
        span: OperationSpan,
        *,
        status_code: int | None = None,
        request_id: str | None = None,
    ) -> None:
        self._emit(
            "on_success",
            _build_event(
                kind="success",
                span=span,
                status="ok",
                duration_s=self._elapsed(span),
                status_code=status_code,
                request_id=request_id,
            ),
        )

    def failure(self, span: OperationSpan, exc: BaseException) -> None:
        fields = _failure_fields(exc)
        self._emit(
            "on_failure",
            _build_event(
                kind="failure",
                span=span,
                status="error",
                duration_s=self._elapsed(span),
                status_code=fields.status_code,
                error_code=fields.error_code,
                retryable=fields.retryable,
                request_id=fields.request_id,
            ),
        )

    def cancelled(self, span: OperationSpan) -> None:
        self._emit(
            "on_cancelled",
            _build_event(
                kind="cancelled",
                span=span,
                status="cancelled",
                duration_s=self._elapsed(span),
                error_code="cancelled",
                retryable=False,
            ),
        )

    def run(
        self,
        call: Callable[[], T],
        *,
        operation_id: str,
        method: str,
        route_template: str,
        attempt: int = 1,
    ) -> T:
        """Run ``call`` once with start/attempt/success|failure|cancelled events."""

        span = self.start(
            operation_id=operation_id,
            method=method,
            route_template=route_template,
            attempt=attempt,
        )
        self.attempt(span, attempt=attempt)
        try:
            result = call()
        except RequestCancelledError:
            self.cancelled(span)
            raise
        except asyncio.CancelledError:
            self.cancelled(span)
            raise
        except BaseException as exc:
            self.failure(span, exc)
            raise
        status_code = getattr(result, "status_code", None)
        if not isinstance(status_code, int):
            status_code = None
        self.success(span, status_code=status_code)
        return result

    def _elapsed(self, span: OperationSpan) -> float:
        return max(0.0, self._monotonic() - span.started_at)

    def _emit(self, hook_name: str, event: OperationEvent) -> None:
        if self._hooks is None:
            return
        try:
            hook = getattr(self._hooks, hook_name)
            hook(event)
        except Exception as exc:
            # Isolate only Exception. CancelledError / KeyboardInterrupt /
            # SystemExit must propagate so HTTP is not started after cancel.
            _report_hook_error(self._hook_error_sink, exc)


class AsyncInstrumentation:
    """Emit async operation events to hooks without affecting call outcomes."""

    def __init__(
        self,
        hooks: AsyncInstrumentationHooks | None = None,
        *,
        hook_error_sink: HookErrorSink | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._hooks = hooks
        self._hook_error_sink = hook_error_sink
        self._monotonic = monotonic

    async def start(
        self,
        *,
        operation_id: str,
        method: str,
        route_template: str,
        attempt: int = 1,
    ) -> OperationSpan:
        span = OperationSpan(
            operation_id=_clean_token(operation_id, 128),
            method=_clean_method(method),
            route_template=_clean_route_template(route_template),
            started_at=self._monotonic(),
            attempt=max(1, attempt),
        )
        await self._emit(
            "on_start",
            _build_event(
                kind="start",
                span=span,
                status="started",
                duration_s=0.0,
            ),
        )
        return span

    async def attempt(
        self, span: OperationSpan, *, attempt: int | None = None
    ) -> None:
        if attempt is not None:
            span.attempt = max(1, attempt)
        await self._emit(
            "on_attempt",
            _build_event(
                kind="attempt",
                span=span,
                status="attempting",
                duration_s=self._elapsed(span),
            ),
        )

    async def success(
        self,
        span: OperationSpan,
        *,
        status_code: int | None = None,
        request_id: str | None = None,
    ) -> None:
        await self._emit(
            "on_success",
            _build_event(
                kind="success",
                span=span,
                status="ok",
                duration_s=self._elapsed(span),
                status_code=status_code,
                request_id=request_id,
            ),
        )

    async def failure(self, span: OperationSpan, exc: BaseException) -> None:
        fields = _failure_fields(exc)
        await self._emit(
            "on_failure",
            _build_event(
                kind="failure",
                span=span,
                status="error",
                duration_s=self._elapsed(span),
                status_code=fields.status_code,
                error_code=fields.error_code,
                retryable=fields.retryable,
                request_id=fields.request_id,
            ),
        )

    async def cancelled(self, span: OperationSpan) -> None:
        await self._emit(
            "on_cancelled",
            _build_event(
                kind="cancelled",
                span=span,
                status="cancelled",
                duration_s=self._elapsed(span),
                error_code="cancelled",
                retryable=False,
            ),
        )

    async def run(
        self,
        call: Callable[[], Awaitable[T]],
        *,
        operation_id: str,
        method: str,
        route_template: str,
        attempt: int = 1,
    ) -> T:
        span = await self.start(
            operation_id=operation_id,
            method=method,
            route_template=route_template,
            attempt=attempt,
        )
        await self.attempt(span, attempt=attempt)
        try:
            result = await call()
        except RequestCancelledError:
            await self.cancelled(span)
            raise
        except asyncio.CancelledError:
            await self.cancelled(span)
            raise
        except BaseException as exc:
            await self.failure(span, exc)
            raise
        status_code = getattr(result, "status_code", None)
        if not isinstance(status_code, int):
            status_code = None
        await self.success(span, status_code=status_code)
        return result

    def _elapsed(self, span: OperationSpan) -> float:
        return max(0.0, self._monotonic() - span.started_at)

    async def _emit(self, hook_name: str, event: OperationEvent) -> None:
        if self._hooks is None:
            return
        try:
            hook = getattr(self._hooks, hook_name)
            await hook(event)
        except Exception as exc:
            # Isolate only Exception. CancelledError / KeyboardInterrupt /
            # SystemExit must propagate so HTTP is not started after cancel.
            _report_hook_error(self._hook_error_sink, exc)


@dataclass(frozen=True, slots=True)
class _FailureFields:
    error_code: str
    retryable: bool | None
    status_code: int | None = None
    request_id: str | None = None


def _failure_fields(exc: BaseException) -> _FailureFields:
    """Map exceptions to allowlisted failure labels (no messages or IDs)."""

    if isinstance(exc, ProtocolError):
        return _FailureFields(
            error_code=exc.code.value,
            retryable=bool(exc.retryable),
            status_code=exc.status_code,
            request_id=exc.request_id,
        )
    if isinstance(exc, ClientTimeoutError):
        return _FailureFields(error_code="timeout", retryable=True)
    if isinstance(exc, TransportError):
        return _FailureFields(error_code="transport_error", retryable=True)
    if isinstance(exc, MalformedResponseError):
        return _FailureFields(
            error_code="malformed",
            retryable=False,
            status_code=exc.status_code,
        )
    if isinstance(exc, AuthenticationError):
        return _FailureFields(
            error_code=exc.code.value,
            retryable=False,
            status_code=exc.status_code,
            request_id=exc.request_id,
        )
    if isinstance(exc, LeaseLostError):
        # claim_id is intentionally omitted from events.
        return _FailureFields(error_code="lease_lost", retryable=False)
    if isinstance(exc, RequestCancelledError):
        return _FailureFields(error_code="cancelled", retryable=False)
    if isinstance(exc, QueueClientError):
        return _FailureFields(error_code=type(exc).__name__, retryable=None)
    return _FailureFields(error_code=type(exc).__name__, retryable=None)


def _build_event(
    *,
    kind: str,
    span: OperationSpan,
    status: str,
    duration_s: float,
    status_code: int | None = None,
    error_code: str | None = None,
    retryable: bool | None = None,
    request_id: str | None = None,
) -> OperationEvent:
    return OperationEvent(
        kind=kind,
        operation_id=span.operation_id,
        method=span.method,
        route_template=span.route_template,
        attempt=span.attempt,
        duration_s=duration_s,
        status=status,
        status_code=status_code,
        error_code=error_code,
        retryable=retryable,
        request_id=request_id,
    )


def _report_hook_error(sink: HookErrorSink | None, exc: BaseException) -> None:
    if sink is None:
        return
    try:
        sink(exc)
    except Exception:
        return


def _clean_method(method: str) -> str:
    cleaned = _clean_token(method, 16).upper()
    if cleaned not in {"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"}:
        raise ValueError(f"unsupported HTTP method for instrumentation: {method!r}")
    return cleaned


def _clean_route_template(route_template: str) -> str:
    cleaned = redact_text(route_template.strip())
    if (
        not cleaned.startswith("/")
        or "://" in cleaned
        or "?" in cleaned
        or "#" in cleaned
    ):
        raise ValueError("route_template must be a path template, not a raw URL")
    if "@" in cleaned or " " in cleaned:
        raise ValueError("route_template contains forbidden characters")
    if not _ROUTE_TEMPLATE_RE.fullmatch(cleaned):
        raise ValueError(
            "route_template must use static segments and {param} placeholders only"
        )
    return cleaned[:256]


def _clean_token(value: str, max_len: int) -> str:
    cleaned = redact_text(value.strip())
    if not cleaned:
        raise ValueError("value must be non-empty")
    if any(ch in cleaned for ch in (" ", "\n", "\r", "\t")):
        raise ValueError("value must not contain whitespace")
    return cleaned[:max_len]
