"""Compatibility shim: URL-based async transport for 20-02 role clients."""

from __future__ import annotations

from collections.abc import Mapping

from _queue_service_client_core.config import ClientConfig
from _queue_service_client_core.transport import AsyncTransport, TransportResponse
from _queue_service_client_core.transport_async import HttpxAsyncTransport as _CoreHttpx

__all__ = ["AsyncTransport", "HttpxAsyncTransport"]


class HttpxAsyncTransport:
    """Adapter over core ``HttpxAsyncTransport`` using a public base URL."""

    def __init__(
        self,
        base_url: str,
        *,
        timeout_s: float = 30.0,
        client: object | None = None,
        config: ClientConfig | None = None,
    ) -> None:
        if config is None:
            config = ClientConfig(
                public_base_url=base_url,
                read_timeout_s=timeout_s,
                total_timeout_s=timeout_s,
            )
        self._inner = _CoreHttpx(config, client=client)
        self._timeout_s = timeout_s

    @property
    def timeout_s(self) -> float:
        return self._timeout_s

    @property
    def config(self) -> ClientConfig:
        return self._inner.config

    async def aclose(self) -> None:
        await self._inner.aclose()

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
        return await self._inner.request(
            method,
            path,
            headers=headers,
            json_body=json_body,
            query=query,
            expect_body=expect_body,
            read_timeout_s=read_timeout_s,
            total_timeout_s=total_timeout_s,
            cancellation=cancellation,
        )
