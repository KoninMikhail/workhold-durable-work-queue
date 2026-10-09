"""Async producer SDK adapter over the OpenAPI producer-authorized surface."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from _workhold_client_core.async_transport import AsyncTransport
from _workhold_client_core.capabilities import Capabilities
from _workhold_client_core.codecs import PayloadEncoder, encode_payload
from _workhold_client_core.errors import MalformedResponseError
from _workhold_client_core.models import (
    CancelResponse,
    EnqueueResponse,
    ResolveSubmissionResponse,
    Task,
)
from _workhold_client_core.priority import PRIORITY_DEFAULT, validate_priority
from _workhold_client_core.transport import encode_path_segment

from workhold_producer.client import _AVAILABLE_AT_OMITTED


class AsyncProducerClient:
    """Async producer operations mirroring :class:`ProducerClient`."""

    def __init__(
        self,
        transport: AsyncTransport,
        *,
        bearer_token: str,
        owns_transport: bool = False,
    ) -> None:
        if not bearer_token or not bearer_token.strip():
            raise ValueError("bearer_token is required")
        self._transport = transport
        self._bearer_token = bearer_token
        self._owns_transport = owns_transport

    @classmethod
    def from_url(
        cls,
        base_url: str,
        *,
        bearer_token: str,
        timeout_s: float = 30.0,
    ) -> AsyncProducerClient:
        from _workhold_client_core.async_transport import HttpxAsyncTransport

        transport = HttpxAsyncTransport(base_url, timeout_s=timeout_s)
        return cls(transport, bearer_token=bearer_token, owns_transport=True)

    async def __aenter__(self) -> AsyncProducerClient:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        if self._owns_transport:
            await self._transport.aclose()

    def __repr__(self) -> str:
        return "AsyncProducerClient(transport=..., bearer_token=<redacted>)"

    def _auth_headers(self, extra: dict[str, str] | None = None) -> dict[str, str]:
        headers = {"Authorization": f"Bearer {self._bearer_token}"}
        if extra:
            headers.update(extra)
        return headers

    async def get_capabilities(self) -> Capabilities:
        response = await self._transport.request(
            "GET",
            "/v1/capabilities",
            headers=self._auth_headers(),
            json_body=None,
        )
        return self._parse(Capabilities.parse, response.status_code, response.body)

    async def enqueue(
        self,
        queue_name: str,
        *,
        idempotency_key: str,
        payload: Any,
        priority: int = PRIORITY_DEFAULT,
        available_at: datetime | None = None,
        payload_encoder: PayloadEncoder[Any] | None = None,
        max_payload_bytes: int | None = None,
        capabilities: Capabilities | None = None,
    ) -> EnqueueResponse:
        wire_available_at: str | None | object = _AVAILABLE_AT_OMITTED
        if available_at is not None:
            self._require_aware_datetime(available_at)
            wire_available_at = available_at.isoformat()
        wire_priority = validate_priority(priority)
        limit = max_payload_bytes
        if limit is None and capabilities is not None:
            limit = capabilities.payload_runtime_max_bytes
        wire_payload = encode_payload(
            payload, payload_encoder, max_bytes=limit
        )
        return await self._enqueue_with_available_at_raw(
            queue_name,
            idempotency_key=idempotency_key,
            payload=wire_payload,
            priority=wire_priority,
            available_at=wire_available_at,
        )

    async def _enqueue_with_available_at_raw(
        self,
        queue_name: str,
        *,
        idempotency_key: str,
        payload: Any,
        priority: int = PRIORITY_DEFAULT,
        available_at: str | None | object = _AVAILABLE_AT_OMITTED,
    ) -> EnqueueResponse:
        wire_priority = validate_priority(priority)
        body: dict[str, Any] = {"payload": payload, "priority": wire_priority}
        if available_at is not _AVAILABLE_AT_OMITTED:
            body["available_at"] = available_at
        path = f"/v1/queues/{encode_path_segment(queue_name)}/tasks"
        response = await self._transport.request(
            "POST",
            path,
            headers=self._auth_headers({"Idempotency-Key": idempotency_key}),
            json_body=body,
        )
        return self._parse(EnqueueResponse.parse, response.status_code, response.body)

    @staticmethod
    def _require_aware_datetime(value: datetime) -> None:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("available_at must be timezone-aware")

    async def resolve_submission(
        self,
        queue_name: str,
        *,
        idempotency_key: str,
    ) -> ResolveSubmissionResponse:
        path = f"/v1/queues/{encode_path_segment(queue_name)}/submissions:resolve"
        response = await self._transport.request(
            "POST",
            path,
            headers=self._auth_headers(),
            json_body={"idempotency_key": idempotency_key},
        )
        return self._parse(
            ResolveSubmissionResponse.parse, response.status_code, response.body
        )

    async def inspect_task(self, task_id: str) -> Task:
        path = f"/v1/tasks/{encode_path_segment(task_id)}"
        response = await self._transport.request(
            "GET",
            path,
            headers=self._auth_headers(),
            json_body=None,
        )
        return self._parse(Task.parse, response.status_code, response.body)

    async def cancel_task(self, task_id: str, *, reason: str | None = None) -> CancelResponse:
        body: dict[str, Any] = {}
        if reason is not None:
            body["reason"] = reason
        path = f"/v1/tasks/{encode_path_segment(task_id)}:cancel"
        response = await self._transport.request(
            "POST",
            path,
            headers=self._auth_headers(),
            json_body=body,
        )
        return self._parse(CancelResponse.parse, response.status_code, response.body)

    @staticmethod
    def _parse(parser: Any, status_code: int, body: object | None) -> Any:
        if body is None:
            raise MalformedResponseError(
                status_code=status_code,
                reason="empty success response body",
            )
        try:
            return parser(body)
        except ValueError as exc:
            raise MalformedResponseError(
                status_code=status_code,
                reason=str(exc),
            ) from exc


__all__ = ["AsyncProducerClient"]
