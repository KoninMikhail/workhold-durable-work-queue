"""Core transport + structured error mapping tests."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

import pytest

from _queue_service_client_core.errors import (
    AuthenticationError,
    MalformedResponseError,
    ProtocolError,
    QueueClientError,
    TimeoutError as ClientTimeoutError,
    TransportError,
)
from _queue_service_client_core.transport import HttpJsonTransport


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return

    def _respond(self, status: int, payload: object | None, *, raw: bytes | None = None) -> None:
        body = raw if raw is not None else (
            b"" if payload is None else json.dumps(payload).encode("utf-8")
        )
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body:
            self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        path = self.path.split("?", 1)[0]
        responder = self.server.routes.get(("GET", path))  # type: ignore[attr-defined]
        if responder is None:
            self._respond(404, {"code": "task_not_found", "message": "x", "retryable": False,
                                "request_id": "r", "details": {}})
            return
        status, payload, raw = responder()
        self._respond(status, payload, raw=raw)

    def do_POST(self) -> None:  # noqa: N802
        path = self.path.split("?", 1)[0]
        responder = self.server.routes.get(("POST", path))  # type: ignore[attr-defined]
        if responder is None:
            self._respond(404, {"code": "task_not_found", "message": "x", "retryable": False,
                                "request_id": "r", "details": {}})
            return
        status, payload, raw = responder()
        self._respond(status, payload, raw=raw)


@pytest.fixture
def server() -> Any:
    httpd = HTTPServer(("127.0.0.1", 0), _Handler)
    httpd.routes = {}  # type: ignore[attr-defined]
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    host, port = httpd.server_address[:2]
    try:
        yield httpd, f"http://{host}:{port}"
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=2)


def _error(
    *,
    code: str = "validation_failed",
    message: str = "bad",
    retryable: bool = False,
    retry_after_ms: int | None = None,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "code": code,
        "message": message,
        "retryable": retryable,
        "request_id": "22222222-2222-4222-8222-222222222222",
        "details": {},
    }
    if retry_after_ms is not None:
        body["retry_after_ms"] = retry_after_ms
    return body


def test_protocol_error_preserves_retry_hints(server: Any) -> None:
    httpd, base = server
    httpd.routes[("POST", "/v1/queues/orders/tasks")] = (
        lambda: (429, _error(code="resource_exhausted", retryable=True, retry_after_ms=1500), None)
    )
    transport = HttpJsonTransport(base, timeout_s=2.0)
    with pytest.raises(ProtocolError) as exc_info:
        transport.request(
            "POST",
            "/v1/queues/orders/tasks",
            headers={"Authorization": "Bearer secret-token"},
            json_body={"payload": {}},
        )
    err = exc_info.value
    assert err.retryable is True
    assert err.retry_after_ms == 1500
    assert err.code.value == "resource_exhausted"
    assert "secret-token" not in f"{err!s}{err!r}"


def test_authentication_error_mapping(server: Any) -> None:
    httpd, base = server
    httpd.routes[("GET", "/v1/capabilities")] = (
        lambda: (401, _error(code="unauthenticated", message="nope"), None)
    )
    with pytest.raises(AuthenticationError) as exc_info:
        HttpJsonTransport(base).request(
            "GET",
            "/v1/capabilities",
            headers={"Authorization": "Bearer leak-me"},
        )
    assert isinstance(exc_info.value, ProtocolError)
    assert isinstance(exc_info.value, QueueClientError)
    assert "leak-me" not in f"{exc_info.value!s}{exc_info.value!r}"


def test_malformed_non_json_body(server: Any) -> None:
    httpd, base = server
    httpd.routes[("GET", "/v1/capabilities")] = (lambda: (200, None, b"not-json{"))
    with pytest.raises(MalformedResponseError) as exc_info:
        HttpJsonTransport(base).request("GET", "/v1/capabilities")
    assert isinstance(exc_info.value, QueueClientError)
    assert not isinstance(exc_info.value, ProtocolError)


def test_malformed_error_envelope(server: Any) -> None:
    httpd, base = server
    httpd.routes[("GET", "/v1/capabilities")] = (lambda: (400, {"oops": True}, None))
    with pytest.raises(MalformedResponseError):
        HttpJsonTransport(base).request("GET", "/v1/capabilities")


def test_timeout_is_distinguishable() -> None:
    with pytest.raises(ClientTimeoutError) as exc_info:
        HttpJsonTransport("http://127.0.0.1:1", timeout_s=0.05).request(
            "GET",
            "/v1/capabilities",
            headers={"Authorization": "Bearer timeout-secret"},
        )
    assert isinstance(exc_info.value, QueueClientError)
    assert not isinstance(exc_info.value, ProtocolError)
    assert "timeout-secret" not in f"{exc_info.value!s}{exc_info.value!r}"


def test_transport_error_on_dns_failure() -> None:
    with pytest.raises((TransportError, ClientTimeoutError)) as exc_info:
        HttpJsonTransport(
            "http://no-such-host.invalid.example",
            timeout_s=0.5,
        ).request("GET", "/v1/capabilities")
    assert isinstance(exc_info.value, QueueClientError)
