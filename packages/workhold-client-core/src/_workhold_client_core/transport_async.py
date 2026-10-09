"""Default async transport backed by httpx (optional ``[async]`` extra)."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import Any

from _workhold_client_core.config import ClientConfig
from _workhold_client_core.errors import (
    TimeoutError as ClientTimeoutError,
    TransportError,
    raise_for_protocol_status,
)
from _workhold_client_core.redaction import redact_text
from _workhold_client_core.requests import prepare_json_request
from _workhold_client_core.transport import (
    TransportResponse,
    _cancellation_is_set,
    _is_timeout_reason,
    build_transport_response,
    parse_response_body,
)

__all__ = ["HttpxAsyncTransport"]


class HttpxAsyncTransport:
    """Async HTTP/JSON transport using httpx. Does not retry."""

    def __init__(
        self,
        config: ClientConfig,
        *,
        client: Any | None = None,
    ) -> None:
        try:
            import httpx
        except ImportError as exc:  # pragma: no cover - guarded by optional extra
            raise ImportError(
                "httpx is required for HttpxAsyncTransport; "
                "install workhold-client-core[async]"
            ) from exc

        self._config = config
        self._base_url = config.public_base_url
        self._httpx = httpx
        if client is not None:
            self._client = client
            self._owns_client = False
        else:
            self._client = httpx.AsyncClient(
                verify=_verify_setting(config),
                cert=_client_cert_setting(config),
                timeout=_httpx_timeout(config),
            )
            self._owns_client = True

    @classmethod
    def from_config(cls, config: ClientConfig) -> HttpxAsyncTransport:
        return cls(config)

    @property
    def config(self) -> ClientConfig:
        return self._config

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def __aenter__(self) -> HttpxAsyncTransport:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.aclose()

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
        _check_cancellation(cancellation)

        prepared = prepare_json_request(
            self._base_url,
            method,
            path,
            headers=headers,
            json_body=json_body,
            query=query,
            expect_body=expect_body,
        )
        call_timeout = _per_call_httpx_timeout(
            self._config,
            self._httpx,
            read_timeout_s=read_timeout_s,
            total_timeout_s=total_timeout_s,
        )
        effective_total = (
            total_timeout_s
            if total_timeout_s is not None
            else self._config.sync_timeout_s()
        )
        request_kwargs: dict[str, Any] = {
            "headers": prepared.headers,
            "content": prepared.body,
        }
        if call_timeout is not None:
            request_kwargs["timeout"] = call_timeout
        try:
            response = await _await_client_request(
                self._client,
                prepared.method,
                prepared.url,
                cancellation=cancellation,
                **request_kwargs,
            )
        except asyncio.CancelledError:
            raise
        except self._httpx.TimeoutException as exc:
            raise ClientTimeoutError(timeout_s=effective_total) from exc
        except self._httpx.TransportError as exc:
            if _is_timeout_reason(exc):
                raise ClientTimeoutError(timeout_s=effective_total) from exc
            raise TransportError(reason=_safe_reason(exc)) from exc

        _check_cancellation(cancellation)

        raw = response.content if prepared.expect_body else b""
        resp_headers = {k.lower(): v for k, v in response.headers.items()}
        if response.status_code >= 400:
            parsed = parse_response_body(raw, status_code=response.status_code)
            raise_for_protocol_status(response.status_code, parsed)
        return build_transport_response(response.status_code, resp_headers, raw)


def _check_cancellation(cancellation: object | None) -> None:
    if _cancellation_is_set(cancellation):
        raise asyncio.CancelledError()


async def _await_client_request(
    client: Any,
    method: str,
    url: str,
    *,
    cancellation: object | None,
    **request_kwargs: Any,
) -> Any:
    """Race an httpx request against cooperative cancellation."""

    if cancellation is None:
        return await client.request(method, url, **request_kwargs)

    request_task = asyncio.create_task(client.request(method, url, **request_kwargs))
    cancel_task = asyncio.create_task(_wait_for_cancellation(cancellation))
    try:
        done, _pending = await asyncio.wait(
            {request_task, cancel_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if cancel_task in done:
            request_task.cancel()
            try:
                await request_task
            except asyncio.CancelledError:
                pass
            raise asyncio.CancelledError()
        cancel_task.cancel()
        try:
            await cancel_task
        except asyncio.CancelledError:
            pass
        return request_task.result()
    except asyncio.CancelledError:
        request_task.cancel()
        cancel_task.cancel()
        await asyncio.gather(request_task, cancel_task, return_exceptions=True)
        raise


async def _wait_for_cancellation(cancellation: object) -> None:
    if isinstance(cancellation, asyncio.Event):
        await cancellation.wait()
        return
    if isinstance(cancellation, asyncio.Task):
        await cancellation
        return
    wait = getattr(cancellation, "wait", None)
    if callable(wait):
        result = wait()
        if asyncio.iscoroutine(result):
            await result
            return
    while not _cancellation_is_set(cancellation):
        await asyncio.sleep(0.05)


def _verify_setting(config: ClientConfig) -> bool | str:
    if config.ca_cert_path is not None:
        return config.ca_cert_path
    return config.verify_tls


def _client_cert_setting(config: ClientConfig) -> str | tuple[str, str] | None:
    if config.client_cert_path is None:
        return None
    if config.client_key_path is not None:
        return (config.client_cert_path, config.client_key_path)
    return config.client_cert_path


def _httpx_timeout(config: ClientConfig) -> Any:
    import httpx

    if config.total_timeout_s is not None:
        return httpx.Timeout(
            config.total_timeout_s,
            connect=config.connect_timeout_s,
            read=config.read_timeout_s,
        )
    return httpx.Timeout(
        None,
        connect=config.connect_timeout_s,
        read=config.read_timeout_s,
    )


def _per_call_httpx_timeout(
    config: ClientConfig,
    httpx: Any,
    *,
    read_timeout_s: float | None,
    total_timeout_s: float | None,
) -> Any | None:
    """Build a per-request httpx timeout, or ``None`` to keep the client default."""

    if read_timeout_s is None and total_timeout_s is None:
        return None
    read = read_timeout_s if read_timeout_s is not None else config.read_timeout_s
    if read <= 0:
        raise ValueError("read_timeout_s must be positive")
    if total_timeout_s is not None:
        if total_timeout_s <= 0:
            raise ValueError("total_timeout_s must be positive")
        return httpx.Timeout(
            total_timeout_s,
            connect=config.connect_timeout_s,
            read=read,
        )
    return httpx.Timeout(
        None,
        connect=config.connect_timeout_s,
        read=read,
    )


def _safe_reason(reason: object) -> str:
    if isinstance(reason, BaseException):
        return reason.__class__.__name__
    text = redact_text(str(reason))
    return text[:200]
