"""Success/error TransportResponse builders for scripted client tests."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from _workhold_client_core.errors import (
    RequestCancelledError,
    TimeoutError as ClientTimeoutError,
    TransportError,
)
from _workhold_client_core.transport import TransportResponse

_DEFAULT_PROTOCOL_ERROR_REQUEST_ID = "00000000-0000-4000-8000-000000000001"


def _raw_json(body: object | None) -> bytes:
    if body is None:
        return b""
    return json.dumps(body, separators=(",", ":"), sort_keys=True).encode("utf-8")


def json_response(
    status_code: int,
    body: object | None = None,
    *,
    headers: Mapping[str, str] | None = None,
    raw_body: bytes | None = None,
) -> TransportResponse:
    hdrs = {"content-type": "application/json"}
    if headers:
        hdrs.update({str(k): str(v) for k, v in headers.items()})
    raw = raw_body if raw_body is not None else _raw_json(body)
    return TransportResponse(
        status_code=status_code,
        headers=hdrs,
        body=body,
        raw_body=raw,
    )


def ok(body: object, *, headers: Mapping[str, str] | None = None) -> TransportResponse:
    return json_response(200, body, headers=headers)


def no_content(*, headers: Mapping[str, str] | None = None) -> TransportResponse:
    return TransportResponse(
        status_code=204,
        headers=dict(headers or {}),
        body=None,
        raw_body=b"",
    )


def protocol_error(
    status_code: int,
    *,
    code: str,
    message: str = "error",
    retryable: bool = False,
    request_id: str | None = None,
    details: Mapping[str, Any] | None = None,
    retry_after_ms: int | None = None,
    headers: Mapping[str, str] | None = None,
) -> TransportResponse:
    body: dict[str, Any] = {
        "code": code,
        "message": message,
        "retryable": retryable,
        "request_id": request_id or _DEFAULT_PROTOCOL_ERROR_REQUEST_ID,
        "details": dict(details or {}),
    }
    if retry_after_ms is not None:
        body["retry_after_ms"] = retry_after_ms
    return json_response(status_code, body, headers=headers)


def lease_lost_error(
    *,
    message: str = "lease lost",
    request_id: str | None = None,
) -> TransportResponse:
    return protocol_error(
        409,
        code="lease_lost",
        message=message,
        retryable=False,
        request_id=request_id,
    )


def uncertain_transport_error(*, reason: str = "connection_reset") -> TransportError:
    return TransportError(reason=reason)


def timeout_error(*, timeout_s: float = 1.0) -> ClientTimeoutError:
    return ClientTimeoutError(timeout_s=timeout_s)


def cancelled_error(*, reason: str = "cancelled") -> RequestCancelledError:
    return RequestCancelledError(reason=reason)
