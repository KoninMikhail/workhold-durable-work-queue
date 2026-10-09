"""Structured SDK errors without credential or payload leakage."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from _queue_service_client_core.models import ErrorCode, ProtocolErrorBody


class QueueClientError(Exception):
    """Base error for the Queue SDK."""

    def __str__(self) -> str:
        return self.__class__.__name__

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}()"


class TransportError(QueueClientError):
    """Low-level transport failure (connection reset, DNS, etc.)."""

    def __init__(self, *, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)

    def __str__(self) -> str:
        return f"TransportError(reason={self.reason!r})"

    def __repr__(self) -> str:
        return self.__str__()


class TimeoutError(QueueClientError):
    """Request exceeded the configured timeout."""

    def __init__(self, *, timeout_s: float) -> None:
        self.timeout_s = timeout_s
        super().__init__("timeout")

    def __str__(self) -> str:
        return f"TimeoutError(timeout_s={self.timeout_s!r})"

    def __repr__(self) -> str:
        return self.__str__()


class RequestCancelledError(QueueClientError):
    """Outstanding sync request was cancelled by the caller (distinct from timeout)."""

    def __init__(self, *, reason: str = "cancelled") -> None:
        self.reason = reason
        super().__init__(reason)

    def __str__(self) -> str:
        return f"RequestCancelledError(reason={self.reason!r})"

    def __repr__(self) -> str:
        return self.__str__()


class MalformedResponseError(QueueClientError):
    """Response could not be parsed into the expected protocol shape."""

    def __init__(self, *, status_code: int, reason: str) -> None:
        self.status_code = status_code
        self.reason = reason
        super().__init__(reason)

    def __str__(self) -> str:
        return (
            f"MalformedResponseError(status_code={self.status_code!r}, "
            f"reason={self.reason!r})"
        )

    def __repr__(self) -> str:
        return self.__str__()


class ProtocolError(QueueClientError):
    """Structured protocol ``Error`` envelope from the Queue service."""

    def __init__(
        self,
        *,
        status_code: int,
        body: ProtocolErrorBody,
    ) -> None:
        self.status_code = status_code
        self.code = body.code
        self.message = body.message
        self.retryable = body.retryable
        self.request_id = body.request_id
        self.details: Mapping[str, Any] = body.details
        self.retry_after_ms = body.retry_after_ms
        super().__init__(body.code.value)

    def __str__(self) -> str:
        return (
            f"{self.__class__.__name__}(status_code={self.status_code!r}, "
            f"code={self.code.value!r}, retryable={self.retryable!r}, "
            f"request_id={self.request_id!r})"
        )

    def __repr__(self) -> str:
        return self.__str__()


class AuthenticationError(ProtocolError):
    """HTTP 401 / unauthenticated protocol failure."""


class LeaseLostError(QueueClientError):
    """Local claim handle is no longer usable for Queue mutations."""

    def __init__(self, *, claim_id: str) -> None:
        self.claim_id = claim_id
        super().__init__("lease_lost")

    def __str__(self) -> str:
        return f"LeaseLostError(claim_id={self.claim_id!r})"

    def __repr__(self) -> str:
        return self.__str__()


class TerminalConflictError(QueueClientError):
    """Terminal body differs from the body already accepted for this claim."""

    def __init__(self, *, claim_id: str, reason: str) -> None:
        self.claim_id = claim_id
        self.reason = reason
        super().__init__(reason)

    def __str__(self) -> str:
        return (
            f"TerminalConflictError(claim_id={self.claim_id!r}, "
            f"reason={self.reason!r})"
        )

    def __repr__(self) -> str:
        return self.__str__()


def raise_for_protocol_status(status_code: int, payload: object) -> None:
    """Raise a structured protocol error from an error-status JSON body."""

    try:
        body = ProtocolErrorBody.parse(payload)
    except ValueError as exc:
        raise MalformedResponseError(status_code=status_code, reason=str(exc)) from exc

    if status_code == 401 or body.code.value == "unauthenticated":
        raise AuthenticationError(status_code=status_code, body=body)
    raise ProtocolError(status_code=status_code, body=body)


# Re-export ErrorCode for callers that catch ProtocolError and inspect codes.
__all__ = [
    "AuthenticationError",
    "ErrorCode",
    "LeaseLostError",
    "MalformedResponseError",
    "ProtocolError",
    "QueueClientError",
    "RequestCancelledError",
    "TerminalConflictError",
    "TimeoutError",
    "TransportError",
    "raise_for_protocol_status",
]
