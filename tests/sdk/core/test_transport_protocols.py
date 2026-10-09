"""Shared request builders and sync/async transport protocol contracts."""

from __future__ import annotations

import asyncio
import json
import threading
from collections.abc import Mapping
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

import pytest

from _workhold_client_core.config import ClientConfig
from _workhold_client_core.errors import (
    MalformedResponseError,
    ProtocolError,
    TimeoutError as ClientTimeoutError,
)
from _workhold_client_core.requests import prepare_json_request
from _workhold_client_core.transport import (
    AsyncTransport,
    HttpJsonTransport,
    SyncTransport,
    TransportResponse,
)


@dataclass
class _RecordedCall:
    method: str
    path: str
    headers: Mapping[str, str]
    json_body: object | None
    query: Mapping[str, str] | None


class _ScriptedSyncTransport:
    def __init__(self, responses: list[TransportResponse | BaseException]) -> None:
        self._responses = list(responses)
        self.calls: list[_RecordedCall] = []

    def request(
        self,
        method: str,
        path: str,
        *,
        headers: Mapping[str, str] | None = None,
        json_body: object | None = None,
        query: Mapping[str, str] | None = None,
        expect_body: bool = True,
        read_timeout_s: float | None = None,
        total_timeout_s: float | None = None,
        cancellation: object | None = None,
    ) -> TransportResponse:
        self.calls.append(
            _RecordedCall(method, path, dict(headers or {}), json_body, query)
        )
        outcome = self._responses.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


class _ScriptedAsyncTransport:
    def __init__(self, responses: list[TransportResponse | BaseException]) -> None:
        self._responses = list(responses)
        self.calls: list[_RecordedCall] = []

    async def request(
        self,
        method: str,
        path: str,
        *,
        headers: Mapping[str, str] | None = None,
        json_body: object | None = None,
        query: Mapping[str, str] | None = None,
        expect_body: bool = True,
        read_timeout_s: float | None = None,
        total_timeout_s: float | None = None,
        cancellation: object | None = None,
    ) -> TransportResponse:
        if isinstance(cancellation, asyncio.Event) and cancellation.is_set():
            raise asyncio.CancelledError()
        self.calls.append(
            _RecordedCall(method, path, dict(headers or {}), json_body, query)
        )
        outcome = self._responses.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


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
        self._dispatch("GET")

    def do_POST(self) -> None:  # noqa: N802
        self._dispatch("POST")

    def _dispatch(self, method: str) -> None:
        path = self.path.split("?", 1)[0]
        responder = self.server.routes.get((method, path))  # type: ignore[attr-defined]
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


def test_prepare_json_request_is_identical_for_sync_and_async_base_urls() -> None:
    sync_req = prepare_json_request(
        "https://queue.example.com/",
        "POST",
        "/v1/queues/orders/tasks",
        headers={"Authorization": "Bearer secret-token", "Idempotency-Key": "k1"},
        json_body={"payload": {"x": 1}, "priority": 0},
        query={"cursor": "abc"},
    )
    async_req = prepare_json_request(
        "https://queue.example.com",
        "post",
        "/v1/queues/orders/tasks",
        headers={"Authorization": "Bearer secret-token", "Idempotency-Key": "k1"},
        json_body={"payload": {"x": 1}, "priority": 0},
        query={"cursor": "abc"},
    )
    assert sync_req == async_req
    assert sync_req.url.endswith("/v1/queues/orders/tasks?cursor=abc")
    assert sync_req.method == "POST"
    assert sync_req.headers["Content-Type"] == "application/json; charset=utf-8"


def test_sync_transport_protocol_is_runtime_checkable() -> None:
    transport = _ScriptedSyncTransport(
        [TransportResponse(status_code=200, headers={}, body={"ok": True}, raw_body=b"{}")]
    )
    assert isinstance(transport, SyncTransport)


def test_async_transport_protocol_is_runtime_checkable() -> None:
    transport = _ScriptedAsyncTransport(
        [TransportResponse(status_code=200, headers={}, body={"ok": True}, raw_body=b"{}")]
    )
    assert isinstance(transport, AsyncTransport)


def test_http_json_transport_end_to_end_with_query(server: Any) -> None:
    httpd, base = server
    httpd.routes[("POST", "/v1/queues/orders/tasks")] = (
        lambda: (200, {"ok": True}, None)
    )
    response = HttpJsonTransport(base).request(
        "POST",
        "/v1/queues/orders/tasks",
        headers={"Authorization": "Bearer wire-secret"},
        json_body={"payload": {}, "priority": 0},
        query={"limit": "10"},
    )
    assert response.body == {"ok": True}
    assert "wire-secret" not in f"{response!r}"


def test_malformed_response_is_structured(server: Any) -> None:
    httpd, base = server
    httpd.routes[("GET", "/v1/capabilities")] = (lambda: (200, None, b"not-json{"))
    with pytest.raises(MalformedResponseError):
        HttpJsonTransport(base).request("GET", "/v1/capabilities")


def test_timeout_error_has_no_secret_leakage() -> None:
    with pytest.raises(ClientTimeoutError) as exc_info:
        HttpJsonTransport("http://127.0.0.1:1", timeout_s=0.05).request(
            "GET",
            "/v1/capabilities",
            headers={"Authorization": "Bearer timeout-secret"},
        )
    assert "timeout-secret" not in f"{exc_info.value!s}{exc_info.value!r}"


def test_protocol_error_preserves_retry_hints_without_secret_leakage(server: Any) -> None:
    httpd, base = server
    httpd.routes[("POST", "/v1/queues/orders/tasks")] = (
        lambda: (
            429,
            {
                "code": "resource_exhausted",
                "message": "slow down",
                "retryable": True,
                "retry_after_ms": 1500,
                "request_id": "22222222-2222-4222-8222-222222222222",
                "details": {},
            },
            None,
        )
    )
    with pytest.raises(ProtocolError) as exc_info:
        HttpJsonTransport(base, timeout_s=2.0).request(
            "POST",
            "/v1/queues/orders/tasks",
            headers={"Authorization": "Bearer secret-token"},
            json_body={"payload": {}},
        )
    err = exc_info.value
    assert err.retryable is True
    assert err.retry_after_ms == 1500
    assert "secret-token" not in f"{err!s}{err!r}"


def test_async_cancellation_propagates_without_retry() -> None:
    transport = _ScriptedAsyncTransport([])

    async def _run() -> None:
        cancel = asyncio.Event()
        cancel.set()
        await transport.request(
            "GET",
            "/v1/capabilities",
            cancellation=cancel,
        )

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(_run())
    assert transport.calls == []


def test_httpx_async_transport_forwards_tls_and_timeouts() -> None:
    pytest.importorskip("httpx")

    from _workhold_client_core.transport_async import (
        HttpxAsyncTransport,
        _client_cert_setting,
        _httpx_timeout,
        _verify_setting,
    )

    config = ClientConfig(
        public_base_url="https://queue.example.com",
        connect_timeout_s=1.5,
        read_timeout_s=9.0,
        total_timeout_s=11.0,
        verify_tls=False,
        ca_cert_path="/tmp/ca.pem",
        client_cert_path="/tmp/client.pem",
        client_key_path="/tmp/client.key",
    )
    assert _verify_setting(config) == "/tmp/ca.pem"
    assert _client_cert_setting(config) == ("/tmp/client.pem", "/tmp/client.key")
    httpx_timeout = _httpx_timeout(config)
    assert httpx_timeout.connect == 1.5
    assert httpx_timeout.read == 9.0
    assert httpx_timeout.pool == 11.0

    class _DummyClient:
        timeout = httpx_timeout

    transport = HttpxAsyncTransport(config, client=_DummyClient())
    assert transport.config is config
    assert transport._client.timeout.connect == 1.5


def test_ssl_context_default_verifies_tls() -> None:
    import ssl

    from _workhold_client_core.transport import ssl_context_from_config

    ctx = ssl_context_from_config(
        ClientConfig(public_base_url="https://queue.example.com")
    )
    assert ctx.verify_mode == ssl.CERT_REQUIRED
    assert ctx.check_hostname is True


def test_ssl_context_verify_tls_false_disables_verification() -> None:
    import ssl

    from _workhold_client_core.transport import ssl_context_from_config

    ctx = ssl_context_from_config(
        ClientConfig(public_base_url="https://queue.example.com", verify_tls=False)
    )
    assert ctx.verify_mode == ssl.CERT_NONE
    assert ctx.check_hostname is False


def test_ssl_context_ca_path_takes_precedence_over_verify_false(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import ssl

    from _workhold_client_core.transport import ssl_context_from_config

    captured: dict[str, object] = {}

    def _fake_default_context(
        *args: object,
        cafile: str | None = None,
        capath: str | None = None,
        cadata: object = None,
        **kwargs: object,
    ) -> ssl.SSLContext:
        captured["cafile"] = cafile
        return ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

    monkeypatch.setattr(ssl, "create_default_context", _fake_default_context)

    ssl_context_from_config(
        ClientConfig(
            public_base_url="https://queue.example.com",
            verify_tls=False,
            ca_cert_path="/tmp/custom-ca.pem",
        )
    )
    assert captured["cafile"] == "/tmp/custom-ca.pem"


def test_ssl_context_loads_client_cert_chain(monkeypatch: pytest.MonkeyPatch) -> None:
    import ssl

    from _workhold_client_core.transport import ssl_context_from_config

    loaded: dict[str, object] = {}

    def _fake_load(
        self: ssl.SSLContext,
        certfile: str,
        keyfile: str | None = None,
        password: object = None,
    ) -> None:
        loaded["certfile"] = certfile
        loaded["keyfile"] = keyfile

    monkeypatch.setattr(ssl.SSLContext, "load_cert_chain", _fake_load)

    ssl_context_from_config(
        ClientConfig(
            public_base_url="https://queue.example.com",
            client_cert_path="/tmp/client.pem",
            client_key_path="/tmp/client.key",
        )
    )
    assert loaded == {"certfile": "/tmp/client.pem", "keyfile": "/tmp/client.key"}


def test_http_json_transport_passes_ssl_context_to_urlopen(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import ssl
    from urllib.request import Request

    from _workhold_client_core.transport import HttpJsonTransport

    config = ClientConfig(
        public_base_url="https://queue.example.com",
        verify_tls=False,
    )
    expected_ctx = ssl._create_unverified_context()
    monkeypatch.setattr(
        "_workhold_client_core.transport.ssl_context_from_config",
        lambda _cfg: expected_ctx,
    )

    class _FakeResponse:
        status = 200
        headers = {"Content-Type": "application/json"}

        def read(self) -> bytes:
            return b'{"ok":true}'

        def getcode(self) -> int:
            return 200

        def __enter__(self) -> _FakeResponse:
            return self

        def __exit__(self, *args: object) -> None:
            return None

    captured: dict[str, object] = {}

    def _fake_urlopen(
        req: Request,
        data: object = None,
        timeout: object = None,
        *,
        context: ssl.SSLContext | None = None,
        **kwargs: object,
    ) -> _FakeResponse:
        captured["context"] = context
        captured["timeout"] = timeout
        return _FakeResponse()

    monkeypatch.setattr(
        "_workhold_client_core.transport.urlopen",
        _fake_urlopen,
    )

    response = HttpJsonTransport.from_config(config).request("GET", "/v1/capabilities")
    assert response.body == {"ok": True}
    assert captured["context"] is expected_ctx
    assert captured["timeout"] == config.sync_timeout_s()

def test_sync_transport_applies_per_call_total_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    import ssl
    from urllib.request import Request

    config = ClientConfig.for_public(
        "https://queue.example.com",
        read_timeout_s=30.0,
        total_timeout_s=30.0,
    )
    captured: dict[str, object] = {}

    class _FakeResponse:
        status = 200
        headers = {"Content-Type": "application/json"}

        def read(self) -> bytes:
            return b'{"ok":true}'

        def getcode(self) -> int:
            return 200

        def __enter__(self) -> _FakeResponse:
            return self

        def __exit__(self, *args: object) -> None:
            return None

    def _fake_urlopen(
        req: Request,
        data: object = None,
        timeout: object = None,
        *,
        context: ssl.SSLContext | None = None,
        **kwargs: object,
    ) -> _FakeResponse:
        captured["timeout"] = timeout
        return _FakeResponse()

    monkeypatch.setattr(
        "_workhold_client_core.transport.urlopen",
        _fake_urlopen,
    )
    HttpJsonTransport.from_config(config).request(
        "GET",
        "/v1/capabilities",
        total_timeout_s=25.0,
    )
    assert captured["timeout"] == 25.0


def test_sync_cancellation_before_start_raises() -> None:
    from _workhold_client_core.errors import RequestCancelledError

    cancel = threading.Event()
    cancel.set()
    transport = HttpJsonTransport("http://127.0.0.1:9", timeout_s=0.1)
    with pytest.raises(RequestCancelledError):
        transport.request("GET", "/v1/capabilities", cancellation=cancel)


def test_prepare_json_request_rejects_nan() -> None:
    with pytest.raises(ValueError):
        prepare_json_request(
            "https://queue.example.com",
            "POST",
            "/v1/queues/orders/tasks",
            json_body={"value": float("nan")},
        )


@pytest.mark.asyncio
async def test_httpx_async_transport_cancels_inflight_event() -> None:
    pytest.importorskip("httpx")

    from _workhold_client_core.config import ClientConfig
    from _workhold_client_core.transport_async import HttpxAsyncTransport

    class _FakeResponse:
        status_code = 200
        headers: dict[str, str] = {}
        content = b'{"ok":true}'

    class _SlowClient:
        async def request(self, method: str, url: str, **kwargs: object) -> _FakeResponse:
            await asyncio.sleep(1.0)
            return _FakeResponse()

    config = ClientConfig.for_public(
        "http://127.0.0.1",
        read_timeout_s=30.0,
        total_timeout_s=30.0,
    )
    cancel = asyncio.Event()
    transport = HttpxAsyncTransport(config, client=_SlowClient())

    task = asyncio.create_task(
        transport.request("GET", "/v1/capabilities", cancellation=cancel)
    )
    await asyncio.sleep(0.05)
    cancel.set()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_httpx_async_transport_cancels_inflight_on_outer_task_cancel() -> None:
    pytest.importorskip("httpx")

    from _workhold_client_core.config import ClientConfig
    from _workhold_client_core.transport_async import HttpxAsyncTransport

    request_cancelled = asyncio.Event()

    class _FakeResponse:
        status_code = 200
        headers: dict[str, str] = {}
        content = b'{"ok":true}'

    class _SlowClient:
        async def request(self, method: str, url: str, **kwargs: object) -> _FakeResponse:
            try:
                await asyncio.sleep(1.0)
            except asyncio.CancelledError:
                request_cancelled.set()
                raise
            raise AssertionError("request should have been cancelled")

    config = ClientConfig.for_public("http://127.0.0.1")
    cancel = asyncio.Event()
    transport = HttpxAsyncTransport(config, client=_SlowClient())

    task = asyncio.create_task(
        transport.request("GET", "/v1/capabilities", cancellation=cancel)
    )
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    await asyncio.wait_for(request_cancelled.wait(), timeout=1.0)


@pytest.mark.asyncio
async def test_httpx_async_transport_honours_is_cancelled_duck_type() -> None:
    pytest.importorskip("httpx")

    from _workhold_client_core.config import ClientConfig
    from _workhold_client_core.transport_async import HttpxAsyncTransport

    class _CancelHandle:
        def __init__(self) -> None:
            self._cancelled = False

        def is_cancelled(self) -> bool:
            return self._cancelled

    class _FakeResponse:
        status_code = 200
        headers: dict[str, str] = {}
        content = b'{"ok":true}'

    class _SlowClient:
        async def request(self, method: str, url: str, **kwargs: object) -> _FakeResponse:
            await asyncio.sleep(1.0)
            return _FakeResponse()

    handle = _CancelHandle()
    config = ClientConfig.for_public("http://127.0.0.1")
    transport = HttpxAsyncTransport(config, client=_SlowClient())

    task = asyncio.create_task(
        transport.request("GET", "/v1/capabilities", cancellation=handle)
    )
    await asyncio.sleep(0.05)
    handle._cancelled = True
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_async_transport_shim_forwards_query() -> None:
    pytest.importorskip("httpx")

    from _workhold_client_core.async_transport import HttpxAsyncTransport as Shim
    from _workhold_client_core.transport import TransportResponse

    captured: dict[str, object] = {}

    class _SpyInner:
        async def request(
            self,
            method: str,
            path: str,
            *,
            headers: Mapping[str, str] | None = None,
            json_body: object | None = None,
            query: Mapping[str, str] | None = None,
            expect_body: bool = True,
            read_timeout_s: float | None = None,
            total_timeout_s: float | None = None,
            cancellation: object | None = None,
        ) -> TransportResponse:
            captured.update(
                {
                    "method": method,
                    "path": path,
                    "query": dict(query or {}),
                }
            )
            return TransportResponse(
                status_code=200,
                headers={},
                body={"ok": True},
                raw_body=b"{}",
            )

    shim = Shim("http://example.com")
    shim._inner = _SpyInner()
    await shim.request("GET", "/v1/capabilities", query={"limit": "10"})
    assert captured == {
        "method": "GET",
        "path": "/v1/capabilities",
        "query": {"limit": "10"},
    }
