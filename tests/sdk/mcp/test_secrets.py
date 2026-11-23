"""Credential canaries for observer tool results."""

from __future__ import annotations

import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

import pytest

from _workhold_client_core.errors import TransportError
from _workhold_client_core.transport import HttpJsonTransport
from workhold_admin.observer import ObserverClient
from workhold_client_testing import ScriptedSyncTransport, ok, protocol_error
from workhold_mcp.server import call_tool

_TOKEN = "WHMCP-TOKEN-CANARY"
_PAYLOAD = "WHMCP-PAYLOAD-CANARY"
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


def _observer(transport: Any) -> ObserverClient:
    return ObserverClient(
        transport,
        bearer_token=_TOKEN,
        admin_transport=transport,
    )


def _task_body() -> dict[str, Any]:
    return {
        "task_id": _TASK_ID,
        "queue_name": "orders",
        "producer_id": "producer-1",
        "state": "ready",
        "priority": 0,
        "available_at": "2026-09-19T00:00:00Z",
        "retry_policy_version": 1,
        "created_at": "2026-09-19T00:00:00Z",
        "spawned_task_ids": [],
        "delivery_event_ids": [],
        "payload": {"marker": _PAYLOAD},
    }


def _assert_token_absent(*chunks: str) -> None:
    for chunk in chunks:
        assert _TOKEN not in chunk
        assert "TransportError(reason=" not in chunk


def test_handler_hides_transport_reason(
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    observer = _observer(ScriptedSyncTransport([TransportError(reason=_TOKEN)]))
    result = call_tool(observer, "observer_get_task", {"task_id": _TASK_ID})
    captured = capsys.readouterr()
    text = result.content[0].text
    projected = result.structured_content

    assert result.is_error is True
    assert projected["kind"] == "transport"
    assert tuple(projected) == _ERROR_KEYS
    assert json.loads(text) == projected
    _assert_token_absent(text, json.dumps(projected), captured.out, captured.err, caplog.text)


@pytest.mark.parametrize(
    ("status_code", "code", "retryable", "request_id", "retry_after_ms"),
    [
        (
            401,
            "unauthenticated",
            False,
            "22222222-2222-4222-8222-222222222222",
            None,
        ),
        (
            403,
            "permission_denied",
            False,
            "33333333-3333-4333-8333-333333333333",
            None,
        ),
        (
            404,
            "task_not_found",
            False,
            "44444444-4444-4444-8444-444444444444",
            None,
        ),
        (
            503,
            "dependency_unavailable",
            True,
            "55555555-5555-4555-8555-555555555555",
            1500,
        ),
    ],
)
def test_handler_keeps_protocol_code(
    status_code: int,
    code: str,
    retryable: bool,
    request_id: str,
    retry_after_ms: int | None,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    response = protocol_error(
        status_code,
        code=code,
        message=f"denied {_TOKEN}",
        retryable=retryable,
        request_id=request_id,
        details={"echo": _TOKEN},
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
        observer = _observer(HttpJsonTransport(base_url, timeout_s=2.0))
        result = call_tool(observer, "observer_get_task", {"task_id": _TASK_ID})
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    captured = capsys.readouterr()
    text = result.content[0].text
    projected = result.structured_content
    assert result.is_error is True
    assert projected["kind"] == "protocol"
    assert projected["code"] == code
    assert projected["retryable"] is retryable
    assert projected["retry_after_ms"] == retry_after_ms
    assert tuple(projected) == _ERROR_KEYS
    assert json.loads(text) == projected
    _assert_token_absent(text, json.dumps(projected), captured.out, captured.err, caplog.text)


def test_payload_canary_survives_success(
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    observer = _observer(ScriptedSyncTransport([ok(_task_body())]))
    result = call_tool(observer, "observer_get_task", {"task_id": _TASK_ID})
    captured = capsys.readouterr()
    text = result.content[0].text
    projected = result.structured_content

    assert result.is_error is False
    assert projected["payload"] == {"marker": _PAYLOAD}
    parsed = json.loads(text)
    assert parsed == projected
    assert parsed["payload"]["marker"] == _PAYLOAD
    _assert_token_absent(text, json.dumps(projected), captured.out, captured.err, caplog.text)
    assert _PAYLOAD in text
    assert _PAYLOAD in json.dumps(projected)
