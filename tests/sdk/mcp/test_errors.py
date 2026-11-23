"""Bounded Workhold error projection for MCP tools (MCP-04, MCP-06)."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

import pytest

from _workhold_client_core.errors import (
    AuthenticationError,
    MalformedResponseError,
    ProtocolError,
    RequestCancelledError,
    TimeoutError as ClientTimeoutError,
    TransportError,
)
from _workhold_client_core.transport import HttpJsonTransport
from workhold_admin import ObserverClient
from workhold_client_testing import protocol_error
from workhold_mcp.errors import project_error

_CANARY = "WHMCP-TOKEN-CANARY"
_TASK_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
_ERROR_KEYS = (
    "kind",
    "code",
    "retryable",
    "status_code",
    "request_id",
    "retry_after_ms",
)


class _ProtocolHandler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return

    def do_GET(self) -> None:  # noqa: N802
        raw = self.server.raw_body  # type: ignore[attr-defined]
        self.send_response(self.server.status_code)  # type: ignore[attr-defined]
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)


def _raise_through_observer(
    *,
    status_code: int,
    code: str,
    retryable: bool,
    request_id: str,
    retry_after_ms: int | None = None,
) -> AuthenticationError | ProtocolError:
    response = protocol_error(
        status_code,
        code=code,
        message=f"denied {_CANARY}",
        retryable=retryable,
        request_id=request_id,
        details={"echo": _CANARY},
        retry_after_ms=retry_after_ms,
    )
    raw = json.dumps(response.body).encode("utf-8")
    server = HTTPServer(("127.0.0.1", 0), _ProtocolHandler)
    server.status_code = response.status_code  # type: ignore[attr-defined]
    server.raw_body = raw  # type: ignore[attr-defined]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address[:2]
    base_url = f"http://{host}:{port}"
    try:
        client = ObserverClient(
            HttpJsonTransport(base_url, timeout_s=2.0),
            bearer_token="observer-secret-token",
            admin_transport=HttpJsonTransport(base_url, timeout_s=2.0),
        )
        with pytest.raises((AuthenticationError, ProtocolError)) as exc_info:
            client.get_task(_TASK_ID)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
    return exc_info.value


def _assert_canary_absent(projected: dict[str, Any]) -> None:
    blob = json.dumps(projected)
    assert _CANARY not in blob
    assert _CANARY not in str(projected)
    assert "reason" not in projected
    assert "message" not in projected
    assert "details" not in projected


def test_protocol_errors_keep_code_and_retryable() -> None:
    unauthenticated = _raise_through_observer(
        status_code=401,
        code="unauthenticated",
        retryable=False,
        request_id="22222222-2222-4222-8222-222222222222",
    )
    assert type(unauthenticated) is AuthenticationError
    assert project_error(unauthenticated) == {
        "kind": "protocol",
        "code": "unauthenticated",
        "retryable": False,
        "status_code": 401,
        "request_id": "22222222-2222-4222-8222-222222222222",
        "retry_after_ms": None,
    }
    _assert_canary_absent(project_error(unauthenticated))

    denied = _raise_through_observer(
        status_code=403,
        code="permission_denied",
        retryable=False,
        request_id="33333333-3333-4333-8333-333333333333",
    )
    assert type(denied) is ProtocolError
    assert project_error(denied) == {
        "kind": "protocol",
        "code": "permission_denied",
        "retryable": False,
        "status_code": 403,
        "request_id": "33333333-3333-4333-8333-333333333333",
        "retry_after_ms": None,
    }

    missing = _raise_through_observer(
        status_code=404,
        code="task_not_found",
        retryable=False,
        request_id="44444444-4444-4444-8444-444444444444",
    )
    assert type(missing) is ProtocolError
    assert project_error(missing)["code"] == "task_not_found"
    assert project_error(missing)["retryable"] is False
    assert project_error(missing)["kind"] == "protocol"
    assert project_error(missing)["status_code"] == 404

    unavailable = _raise_through_observer(
        status_code=503,
        code="dependency_unavailable",
        retryable=True,
        request_id="55555555-5555-4555-8555-555555555555",
        retry_after_ms=1500,
    )
    assert type(unavailable) is ProtocolError
    projected = project_error(unavailable)
    assert projected == {
        "kind": "protocol",
        "code": "dependency_unavailable",
        "retryable": True,
        "status_code": 503,
        "request_id": "55555555-5555-4555-8555-555555555555",
        "retry_after_ms": 1500,
    }
    assert tuple(projected) == _ERROR_KEYS
    _assert_canary_absent(projected)


def test_transport_reason_is_omitted() -> None:
    transport = project_error(TransportError(reason=f"{_CANARY} bearer"))
    malformed = project_error(
        MalformedResponseError(status_code=422, reason=f"unreadable {_CANARY}")
    )
    assert transport == {
        "kind": "transport",
        "code": None,
        "retryable": None,
        "status_code": None,
        "request_id": None,
        "retry_after_ms": None,
    }
    assert malformed["kind"] == "malformed_response"
    assert malformed["status_code"] == 422
    assert malformed["code"] is None
    assert tuple(transport) == _ERROR_KEYS
    assert tuple(malformed) == _ERROR_KEYS
    _assert_canary_absent(transport)
    _assert_canary_absent(malformed)


def test_timeout_cancel_value_and_internal() -> None:
    cases = (
        (ClientTimeoutError(timeout_s=3.5), "timeout"),
        (RequestCancelledError(reason=_CANARY), "cancelled"),
        (ValueError(_CANARY), "invalid_argument"),
        (RuntimeError(_CANARY), "internal"),
    )
    for exc, kind in cases:
        projected = project_error(exc)
        assert tuple(projected) == _ERROR_KEYS
        assert projected["kind"] == kind
        assert projected["code"] is None
        assert projected["retryable"] is None
        assert projected["status_code"] is None
        assert projected["request_id"] is None
        assert projected["retry_after_ms"] is None
        _assert_canary_absent(projected)
