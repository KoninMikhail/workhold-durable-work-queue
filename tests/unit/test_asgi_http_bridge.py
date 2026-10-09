"""Focused ASGI/stdlib bridge tests for disconnect vs missing-response fail-closed."""

from __future__ import annotations

from typing import Any

import pytest

from workhold.roles.api import _run_asgi


def _scope(*, cancelled: bool | None = None) -> dict[str, Any]:
    scope: dict[str, Any] = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": "/v1/claims",
        "raw_path": b"/v1/claims",
        "query_string": b"",
        "headers": [],
        "client": ("127.0.0.1", 12345),
        "server": ("127.0.0.1", 8080),
    }
    if cancelled is not None:
        scope["queue_request_cancelled"] = lambda: cancelled
    return scope


async def _silent_app(
    scope: dict[str, Any],
    receive: Any,
    send: Any,
) -> None:
    """Mimic claim disconnect cancel: exit without starting an HTTP response."""
    await receive()
    return


async def _ok_app(
    scope: dict[str, Any],
    receive: Any,
    send: Any,
) -> None:
    await receive()
    await send(
        {
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"content-type", b"application/json")],
        }
    )
    await send({"type": "http.response.body", "body": b'{"ok":true}'})


async def _boom_app(
    scope: dict[str, Any],
    receive: Any,
    send: Any,
) -> None:
    await receive()
    raise RuntimeError("genuine application failure")


def test_disconnected_no_response_does_not_fabricate_500() -> None:
    status, headers, body = _run_asgi(_silent_app, _scope(cancelled=True), b"{}")
    assert status is None
    assert headers == []
    assert body == b""


def test_connected_missing_response_fails_closed_500() -> None:
    status, headers, body = _run_asgi(_silent_app, _scope(cancelled=False), b"{}")
    assert status == 500
    assert headers == []
    assert body == b""


def test_missing_cancel_probe_missing_response_fails_closed_500() -> None:
    status, _headers, body = _run_asgi(_silent_app, _scope(cancelled=None), b"{}")
    assert status == 500
    assert body == b""


def test_ordinary_exception_propagates_unchanged() -> None:
    with pytest.raises(RuntimeError, match="genuine application failure"):
        _run_asgi(_boom_app, _scope(cancelled=False), b"{}")


def test_ordinary_exception_still_propagates_when_disconnect_confirmed() -> None:
    with pytest.raises(RuntimeError, match="genuine application failure"):
        _run_asgi(_boom_app, _scope(cancelled=True), b"{}")


def test_normal_response_unchanged() -> None:
    status, headers, body = _run_asgi(_ok_app, _scope(cancelled=False), b"{}")
    assert status == 200
    assert (b"content-type", b"application/json") in headers
    assert body == b'{"ok":true}'
