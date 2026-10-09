"""Focused sync HttpJsonTransport cancellation ownership tests."""

from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest

from _queue_service_client_core.errors import RequestCancelledError
from _queue_service_client_core.transport import HttpJsonTransport


class _HoldHandler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return

    def do_GET(self) -> None:  # noqa: N802
        path = self.path.split("?", 1)[0]
        entered: dict[str, threading.Event] = self.server.entered  # type: ignore[attr-defined]
        release: dict[str, threading.Event] = self.server.release  # type: ignore[attr-defined]
        marker = entered.get(path)
        hold = release.get(path)
        if marker is not None:
            marker.set()
        if hold is not None:
            hold.wait(timeout=30.0)
        body = b'{"ok":true}'
        try:
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError):
            return


@pytest.fixture
def hold_server() -> Any:
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), _HoldHandler)
    httpd.entered = {}  # type: ignore[attr-defined]
    httpd.release = {}  # type: ignore[attr-defined]
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    host, port = httpd.server_address[:2]
    try:
        yield httpd, f"http://{host}:{port}"
    finally:
        for event in httpd.release.values():  # type: ignore[attr-defined]
            event.set()
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=2)


def _active_snapshot(transport: HttpJsonTransport) -> set[object]:
    with transport._active_lock:
        return set(transport._active)


def test_second_cancellable_request_does_not_overwrite_first(
    hold_server: Any,
) -> None:
    httpd, base = hold_server
    entered_a = threading.Event()
    entered_b = threading.Event()
    release_a = threading.Event()
    release_b = threading.Event()
    httpd.entered["/v1/a"] = entered_a
    httpd.entered["/v1/b"] = entered_b
    httpd.release["/v1/a"] = release_a
    httpd.release["/v1/b"] = release_b

    transport = HttpJsonTransport(base, timeout_s=30.0)
    cancel_a = threading.Event()
    cancel_b = threading.Event()
    errors: list[BaseException | None] = [None, None]

    def _run(index: int, path: str, cancel: threading.Event) -> None:
        try:
            transport.request("GET", path, cancellation=cancel)
        except BaseException as exc:  # noqa: BLE001
            errors[index] = exc

    t1 = threading.Thread(target=_run, args=(0, "/v1/a", cancel_a), daemon=True)
    t2 = threading.Thread(target=_run, args=(1, "/v1/b", cancel_b), daemon=True)
    t1.start()
    t2.start()
    assert entered_a.wait(timeout=2.0)
    assert entered_b.wait(timeout=2.0)

    active = _active_snapshot(transport)
    assert len(active) == 2

    release_a.set()
    release_b.set()
    t1.join(timeout=3.0)
    t2.join(timeout=3.0)
    assert not t1.is_alive() and not t2.is_alive()
    assert _active_snapshot(transport) == set()
    assert errors == [None, None]


def test_cancel_active_cancels_all_concurrent_cancellable_requests(
    hold_server: Any,
) -> None:
    httpd, base = hold_server
    entered_a = threading.Event()
    entered_b = threading.Event()
    release_a = threading.Event()
    release_b = threading.Event()
    httpd.entered["/v1/a"] = entered_a
    httpd.entered["/v1/b"] = entered_b
    httpd.release["/v1/a"] = release_a
    httpd.release["/v1/b"] = release_b

    transport = HttpJsonTransport(base, timeout_s=30.0)
    cancel_a = threading.Event()
    cancel_b = threading.Event()
    errors: list[BaseException | None] = [None, None]

    def _run(index: int, path: str, cancel: threading.Event) -> None:
        try:
            transport.request("GET", path, cancellation=cancel)
        except BaseException as exc:  # noqa: BLE001
            errors[index] = exc

    t1 = threading.Thread(target=_run, args=(0, "/v1/a", cancel_a), daemon=True)
    t2 = threading.Thread(target=_run, args=(1, "/v1/b", cancel_b), daemon=True)
    t1.start()
    t2.start()
    assert entered_a.wait(timeout=2.0)
    assert entered_b.wait(timeout=2.0)
    assert len(_active_snapshot(transport)) == 2

    transport.cancel_active()
    t1.join(timeout=3.0)
    t2.join(timeout=3.0)
    assert not t1.is_alive() and not t2.is_alive()
    assert isinstance(errors[0], RequestCancelledError)
    assert isinstance(errors[1], RequestCancelledError)
    assert _active_snapshot(transport) == set()

    release_a.set()
    release_b.set()


def test_request_unregister_removes_only_own_connection(hold_server: Any) -> None:
    httpd, base = hold_server
    entered_a = threading.Event()
    entered_b = threading.Event()
    release_a = threading.Event()
    release_b = threading.Event()
    httpd.entered["/v1/a"] = entered_a
    httpd.entered["/v1/b"] = entered_b
    httpd.release["/v1/a"] = release_a
    httpd.release["/v1/b"] = release_b

    transport = HttpJsonTransport(base, timeout_s=30.0)
    cancel_a = threading.Event()
    cancel_b = threading.Event()
    errors: list[BaseException | None] = [None, None]

    def _run(index: int, path: str, cancel: threading.Event) -> None:
        try:
            transport.request("GET", path, cancellation=cancel)
        except BaseException as exc:  # noqa: BLE001
            errors[index] = exc

    t1 = threading.Thread(target=_run, args=(0, "/v1/a", cancel_a), daemon=True)
    t2 = threading.Thread(target=_run, args=(1, "/v1/b", cancel_b), daemon=True)
    t1.start()
    t2.start()
    assert entered_a.wait(timeout=2.0)
    assert entered_b.wait(timeout=2.0)
    both = _active_snapshot(transport)
    assert len(both) == 2

    release_a.set()
    t1.join(timeout=3.0)
    assert not t1.is_alive()
    remaining = _active_snapshot(transport)
    assert len(remaining) == 1
    assert remaining.issubset(both)
    assert errors[0] is None

    release_b.set()
    t2.join(timeout=3.0)
    assert not t2.is_alive()
    assert _active_snapshot(transport) == set()
    assert errors[1] is None


def test_event_cancellation_watcher_cancels_only_matching_request(
    hold_server: Any,
) -> None:
    httpd, base = hold_server
    entered_a = threading.Event()
    entered_b = threading.Event()
    release_a = threading.Event()
    release_b = threading.Event()
    httpd.entered["/v1/a"] = entered_a
    httpd.entered["/v1/b"] = entered_b
    httpd.release["/v1/a"] = release_a
    httpd.release["/v1/b"] = release_b

    transport = HttpJsonTransport(base, timeout_s=30.0)
    cancel_a = threading.Event()
    cancel_b = threading.Event()
    errors: list[BaseException | None] = [None, None]

    def _run(index: int, path: str, cancel: threading.Event) -> None:
        try:
            transport.request("GET", path, cancellation=cancel)
        except BaseException as exc:  # noqa: BLE001
            errors[index] = exc

    t1 = threading.Thread(target=_run, args=(0, "/v1/a", cancel_a), daemon=True)
    t2 = threading.Thread(target=_run, args=(1, "/v1/b", cancel_b), daemon=True)
    t1.start()
    t2.start()
    assert entered_a.wait(timeout=2.0)
    assert entered_b.wait(timeout=2.0)

    cancel_a.set()
    t1.join(timeout=3.0)
    assert not t1.is_alive()
    assert isinstance(errors[0], RequestCancelledError)
    assert t2.is_alive()
    remaining = _active_snapshot(transport)
    assert len(remaining) == 1

    release_b.set()
    t2.join(timeout=3.0)
    assert not t2.is_alive()
    assert errors[1] is None
    assert _active_snapshot(transport) == set()


def test_non_cancellable_request_does_not_register_active(
    hold_server: Any,
) -> None:
    httpd, base = hold_server
    entered = threading.Event()
    release = threading.Event()
    httpd.entered["/v1/plain"] = entered
    httpd.release["/v1/plain"] = release
    release.set()

    transport = HttpJsonTransport(base, timeout_s=5.0)
    response = transport.request("GET", "/v1/plain")
    assert response.body == {"ok": True}
    assert _active_snapshot(transport) == set()
