"""Sync/async transport protocols and stdlib HTTP/JSON implementation."""

from __future__ import annotations

import http.client
import json
import socket
import ssl
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol, runtime_checkable
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from _queue_service_client_core.config import ClientConfig
from _queue_service_client_core.errors import (
    MalformedResponseError,
    RequestCancelledError,
    TimeoutError as ClientTimeoutError,
    TransportError,
    raise_for_protocol_status,
)
from _queue_service_client_core.instrumentation import (
    AsyncInstrumentation,
    SyncInstrumentation,
)
from _queue_service_client_core.redaction import redact_headers, redact_text
from _queue_service_client_core.requests import (
    encode_path_segment,
    prepare_json_request,
)

# Re-export for callers that historically imported from transport.
__all__ = [
    "AsyncTransport",
    "HttpJsonTransport",
    "SyncTransport",
    "TransportResponse",
    "async_request_with_instrumentation",
    "encode_path_segment",
    "parse_response_body",
    "redact_headers",
    "request_with_instrumentation",
    "ssl_context_from_config",
]


@dataclass(frozen=True, slots=True)
class TransportResponse:
    status_code: int
    headers: Mapping[str, str]
    body: object | None
    raw_body: bytes


@runtime_checkable
class SyncTransport(Protocol):
    """Injectable synchronous HTTP transport contract."""

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
        """Send one request. ``path`` is absolute under the service root (``/v1/...``)."""
        ...


@runtime_checkable
class AsyncTransport(Protocol):
    """Injectable asynchronous HTTP transport contract."""

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
        """Send one request. Cancellation must propagate; never retry."""
        ...


class _OwnedConnection:
    """Tracks one outstanding owned sync connection for explicit cancellation."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._conn: http.client.HTTPConnection | None = None
        self._sock: socket.socket | None = None
        self._cancelled = False
        self._finished = False

    def attach(self, conn: http.client.HTTPConnection) -> None:
        with self._lock:
            if self._cancelled:
                _force_close_connection(conn)
                raise RequestCancelledError()
            self._conn = conn
            self._sock = getattr(conn, "sock", None)

    def finish(self) -> None:
        with self._lock:
            self._finished = True
            self._conn = None
            self._sock = None

    def cancel(self) -> None:
        with self._lock:
            self._cancelled = True
            conn = self._conn
            sock = self._sock
            self._conn = None
            self._sock = None
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                sock.close()
            except OSError:
                pass
        if conn is not None:
            _force_close_connection(conn)

    @property
    def cancelled(self) -> bool:
        with self._lock:
            return self._cancelled


def _force_close_connection(conn: http.client.HTTPConnection) -> None:
    try:
        sock = getattr(conn, "sock", None)
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
    except Exception:
        pass
    try:
        conn.close()
    except OSError:
        pass


def _cancellation_is_set(cancellation: object | None) -> bool:
    if cancellation is None:
        return False
    if isinstance(cancellation, threading.Event):
        return cancellation.is_set()
    is_set = getattr(cancellation, "is_set", None)
    if callable(is_set) and is_set():
        return True
    is_cancelled = getattr(cancellation, "is_cancelled", None)
    if callable(is_cancelled) and is_cancelled():
        return True
    cancelled = getattr(cancellation, "cancelled", None)
    if callable(cancelled) and cancelled():
        return True
    return False


class HttpJsonTransport:
    """Thin stdlib HTTP/JSON transport. Does not retry; callers own idempotency."""

    def __init__(
        self,
        base_url: str,
        *,
        timeout_s: float = 30.0,
        config: ClientConfig | None = None,
    ) -> None:
        if config is not None:
            self._config = config
        else:
            if not base_url:
                raise ValueError("base_url is required")
            self._config = ClientConfig.for_public(
                base_url,
                read_timeout_s=timeout_s,
                total_timeout_s=timeout_s,
            )
        self._base_url = self._config.public_base_url
        self._active_lock = threading.Lock()
        # All in-flight cancellable requests; concurrent use is supported.
        self._active: set[_OwnedConnection] = set()

    @classmethod
    def from_config(cls, config: ClientConfig) -> HttpJsonTransport:
        return cls("", config=config)

    @property
    def config(self) -> ClientConfig:
        return self._config

    @property
    def timeout_s(self) -> float:
        return self._config.sync_timeout_s()

    def cancel_active(self) -> None:
        """Close every outstanding owned request socket, if any.

        Snapshots under the registry lock, then cancels outside it so socket
        shutdown cannot block other register/unregister operations.
        """

        with self._active_lock:
            active = list(self._active)
        for owned in active:
            owned.cancel()

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
        if _cancellation_is_set(cancellation):
            raise RequestCancelledError()

        prepared = prepare_json_request(
            self._base_url,
            method,
            path,
            headers=headers,
            json_body=json_body,
            query=query,
            expect_body=expect_body,
        )
        timeout_s = self._resolve_timeout(
            read_timeout_s=read_timeout_s,
            total_timeout_s=total_timeout_s,
        )
        context = ssl_context_from_config(self._config)

        # Cancellable path owns an http.client connection so shutdown can close
        # the socket and the server observes disconnect. Non-cancellable path
        # keeps the historical urlopen implementation.
        if cancellation is not None:
            return self._request_cancellable(
                prepared,
                timeout_s=timeout_s,
                context=context,
                cancellation=cancellation,
            )

        request = Request(
            prepared.url,
            data=prepared.body,
            headers=prepared.headers,
            method=prepared.method,
        )
        try:
            with urlopen(request, timeout=timeout_s, context=context) as response:
                raw = response.read() if prepared.expect_body else b""
                status = int(getattr(response, "status", response.getcode()))
                resp_headers = {k.lower(): v for k, v in response.headers.items()}
                return build_transport_response(status, resp_headers, raw)
        except HTTPError as exc:
            raw = exc.read() if exc.fp is not None else b""
            parsed = parse_response_body(raw, status_code=exc.code)
            raise_for_protocol_status(exc.code, parsed)
            raise  # pragma: no cover — raise_for_protocol_status always raises
        except ClientTimeoutError:
            raise
        except RequestCancelledError:
            raise
        except TimeoutError as exc:
            raise ClientTimeoutError(timeout_s=timeout_s) from exc
        except URLError as exc:
            reason = exc.reason
            if isinstance(reason, ClientTimeoutError):
                raise reason
            if _is_timeout_reason(reason):
                raise ClientTimeoutError(timeout_s=timeout_s) from exc
            raise TransportError(reason=_safe_reason(reason)) from exc
        except OSError as exc:
            if _is_timeout_reason(exc):
                raise ClientTimeoutError(timeout_s=timeout_s) from exc
            raise TransportError(reason=_safe_reason(exc)) from exc

    def _resolve_timeout(
        self,
        *,
        read_timeout_s: float | None,
        total_timeout_s: float | None,
    ) -> float:
        if total_timeout_s is not None:
            if total_timeout_s <= 0:
                raise ValueError("total_timeout_s must be positive")
            return total_timeout_s
        if read_timeout_s is not None:
            if read_timeout_s <= 0:
                raise ValueError("read_timeout_s must be positive")
            return read_timeout_s
        return self._config.sync_timeout_s()

    def _request_cancellable(
        self,
        prepared: object,
        *,
        timeout_s: float,
        context: ssl.SSLContext,
        cancellation: object,
    ) -> TransportResponse:
        from _queue_service_client_core.requests import PreparedRequest

        assert isinstance(prepared, PreparedRequest)
        if _cancellation_is_set(cancellation):
            raise RequestCancelledError()

        owned = _OwnedConnection()
        with self._active_lock:
            self._active.add(owned)

        watcher_stop = threading.Event()

        def _watch() -> None:
            # Cancels only this request's connection — never another slot.
            while not watcher_stop.is_set():
                if _cancellation_is_set(cancellation):
                    owned.cancel()
                    return
                if watcher_stop.wait(0.05):
                    return

        watcher = threading.Thread(
            target=_watch,
            name="queue-sync-cancel-watch",
            daemon=True,
        )
        watcher.start()

        parsed = urlparse(prepared.url)
        if parsed.hostname is None:
            raise TransportError(reason="invalid_url")
        port = parsed.port
        if port is None:
            port = 443 if parsed.scheme == "https" else 80
        path = parsed.path or "/"
        if parsed.query:
            path = f"{path}?{parsed.query}"

        try:
            if parsed.scheme == "https":
                conn: http.client.HTTPConnection = http.client.HTTPSConnection(
                    parsed.hostname,
                    port,
                    timeout=timeout_s,
                    context=context,
                )
            else:
                conn = http.client.HTTPConnection(
                    parsed.hostname,
                    port,
                    timeout=timeout_s,
                )
            owned.attach(conn)
            try:
                conn.request(
                    prepared.method,
                    path,
                    body=prepared.body,
                    headers=prepared.headers,
                )
                with owned._lock:
                    owned._sock = getattr(conn, "sock", None) or owned._sock
                if owned.cancelled or _cancellation_is_set(cancellation):
                    raise RequestCancelledError()

                # Poll getresponse with a short socket timeout so cancellation can
                # be observed on Windows, where closing the peer may not unblock
                # a blocking getresponse promptly.
                deadline = time.monotonic() + timeout_s
                sock = getattr(conn, "sock", None)
                if sock is not None:
                    sock.settimeout(min(0.25, timeout_s))
                response = None
                while response is None:
                    if owned.cancelled or _cancellation_is_set(cancellation):
                        raise RequestCancelledError()
                    if time.monotonic() >= deadline:
                        raise ClientTimeoutError(timeout_s=timeout_s)
                    try:
                        response = conn.getresponse()
                    except TimeoutError:
                        continue
                    except socket.timeout:
                        continue
                    except OSError as exc:
                        if owned.cancelled or _cancellation_is_set(cancellation):
                            raise RequestCancelledError() from exc
                        if _is_timeout_reason(exc):
                            continue
                        raise

                raw = response.read() if prepared.expect_body else b""
                status = int(response.status)
                resp_headers = {k.lower(): v for k, v in response.headers.items()}
            finally:
                _force_close_connection(conn)

            if owned.cancelled or _cancellation_is_set(cancellation):
                raise RequestCancelledError()
            if status >= 400:
                parsed_body = parse_response_body(raw, status_code=status)
                raise_for_protocol_status(status, parsed_body)
            return build_transport_response(status, resp_headers, raw)
        except RequestCancelledError:
            raise
        except ClientTimeoutError:
            raise
        except TimeoutError as exc:
            if owned.cancelled or _cancellation_is_set(cancellation):
                raise RequestCancelledError() from exc
            raise ClientTimeoutError(timeout_s=timeout_s) from exc
        except (http.client.HTTPException, OSError) as exc:
            if owned.cancelled or _cancellation_is_set(cancellation):
                raise RequestCancelledError() from exc
            if _is_timeout_reason(exc):
                raise ClientTimeoutError(timeout_s=timeout_s) from exc
            raise TransportError(reason=_safe_reason(exc)) from exc
        finally:
            watcher_stop.set()
            owned.finish()
            with self._active_lock:
                self._active.discard(owned)


def ssl_context_from_config(config: ClientConfig) -> ssl.SSLContext:
    """Build an ``ssl.SSLContext`` matching ``ClientConfig`` TLS fields.

    Semantics mirror the async httpx defaults: a custom CA path takes precedence
    over ``verify_tls=False``; client certificate chain is loaded when set.
    """

    if config.ca_cert_path is not None:
        context = ssl.create_default_context(cafile=config.ca_cert_path)
    elif config.verify_tls:
        context = ssl.create_default_context()
    else:
        context = ssl._create_unverified_context()

    if config.client_cert_path is not None:
        context.load_cert_chain(
            certfile=config.client_cert_path,
            keyfile=config.client_key_path,
        )
    return context


def build_transport_response(
    status_code: int,
    headers: Mapping[str, str],
    raw: bytes,
) -> TransportResponse:
    if status_code >= 400:
        parsed = parse_response_body(raw, status_code=status_code)
        raise_for_protocol_status(status_code, parsed)
    body: object | None
    if not raw:
        body = None
    else:
        body = parse_response_body(raw, status_code=status_code)
    return TransportResponse(
        status_code=status_code,
        headers=dict(headers),
        body=body,
        raw_body=raw,
    )


def parse_response_body(raw: bytes, *, status_code: int) -> object:
    if not raw:
        raise MalformedResponseError(
            status_code=status_code,
            reason="empty response body",
        )
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise MalformedResponseError(
            status_code=status_code,
            reason="response body is not utf-8",
        ) from exc
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise MalformedResponseError(
            status_code=status_code,
            reason="response body is not JSON",
        ) from exc


def _is_timeout_reason(reason: object) -> bool:
    if isinstance(reason, ClientTimeoutError):
        return True
    if isinstance(reason, socket.timeout):
        return True
    if isinstance(reason, TimeoutError):
        return True
    text = str(reason).lower()
    return "timed out" in text or "timeout" in text


def _safe_reason(reason: object) -> str:
    """Stringify a transport reason without copying request bodies or tokens."""

    if isinstance(reason, BaseException):
        return reason.__class__.__name__
    text = redact_text(str(reason))
    return text[:200]


def request_with_instrumentation(
    transport: SyncTransport,
    instrumentation: SyncInstrumentation,
    *,
    operation_id: str,
    method: str,
    route_template: str,
    path: str,
    attempt: int = 1,
    headers: Mapping[str, str] | None = None,
    json_body: object | None = None,
    query: Mapping[str, str] | None = None,
    expect_body: bool = True,
    read_timeout_s: float | None = None,
    total_timeout_s: float | None = None,
    cancellation: object | None = None,
) -> TransportResponse:
    """Send one sync request while emitting allowlisted instrumentation events.

    Sensitive inputs (``headers``, ``query``, ``json_body``, expanded ``path``)
    are never passed to hooks — only ``operation_id``, ``method``, and
    ``route_template`` appear on events.
    """

    return instrumentation.run(
        lambda: transport.request(
            method,
            path,
            headers=headers,
            json_body=json_body,
            query=query,
            expect_body=expect_body,
            read_timeout_s=read_timeout_s,
            total_timeout_s=total_timeout_s,
            cancellation=cancellation,
        ),
        operation_id=operation_id,
        method=method,
        route_template=route_template,
        attempt=attempt,
    )


async def async_request_with_instrumentation(
    transport: AsyncTransport,
    instrumentation: AsyncInstrumentation,
    *,
    operation_id: str,
    method: str,
    route_template: str,
    path: str,
    attempt: int = 1,
    headers: Mapping[str, str] | None = None,
    json_body: object | None = None,
    query: Mapping[str, str] | None = None,
    expect_body: bool = True,
    read_timeout_s: float | None = None,
    total_timeout_s: float | None = None,
    cancellation: object | None = None,
) -> TransportResponse:
    """Async counterpart of :func:`request_with_instrumentation`."""

    return await instrumentation.run(
        lambda: transport.request(
            method,
            path,
            headers=headers,
            json_body=json_body,
            query=query,
            expect_body=expect_body,
            read_timeout_s=read_timeout_s,
            total_timeout_s=total_timeout_s,
            cancellation=cancellation,
        ),
        operation_id=operation_id,
        method=method,
        route_template=route_template,
        attempt=attempt,
    )
