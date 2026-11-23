"""Project Workhold client failures to six bounded JSON fields."""

from __future__ import annotations

from typing import Any

from _workhold_client_core.errors import (
    MalformedResponseError,
    ProtocolError,
    RequestCancelledError,
    TimeoutError,
    TransportError,
)

_FIELDS = (
    "kind",
    "code",
    "retryable",
    "status_code",
    "request_id",
    "retry_after_ms",
)


def project_error(exc: BaseException) -> dict[str, Any]:
    """Map one exception to kind, code, and the four nullable facts.

    Exception text, reasons, messages, and details stay out of the result.
    ``retryable`` is copied only when the exception already carries it.
    """

    projected: dict[str, Any] = {key: None for key in _FIELDS}
    if isinstance(exc, ProtocolError):
        projected["kind"] = "protocol"
        code = exc.code
        projected["code"] = code.value if hasattr(code, "value") else None
        projected["retryable"] = exc.retryable
        projected["status_code"] = exc.status_code
        projected["request_id"] = exc.request_id
        projected["retry_after_ms"] = exc.retry_after_ms
    elif isinstance(exc, MalformedResponseError):
        projected["kind"] = "malformed_response"
        projected["status_code"] = exc.status_code
    elif isinstance(exc, TransportError):
        projected["kind"] = "transport"
    elif isinstance(exc, TimeoutError):
        projected["kind"] = "timeout"
    elif isinstance(exc, RequestCancelledError):
        projected["kind"] = "cancelled"
    elif isinstance(exc, ValueError):
        projected["kind"] = "invalid_argument"
    else:
        projected["kind"] = "internal"
    return projected
