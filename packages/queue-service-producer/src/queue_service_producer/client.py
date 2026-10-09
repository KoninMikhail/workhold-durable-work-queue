"""Thin producer SDK adapter over the OpenAPI producer-authorized surface."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from _queue_service_client_core.capabilities import Capabilities
from _queue_service_client_core.codecs import PayloadEncoder, encode_payload
from _queue_service_client_core.errors import MalformedResponseError
from _queue_service_client_core.models import (
    CancelResponse,
    EnqueueResponse,
    ResolveSubmissionResponse,
    Task,
)
from _queue_service_client_core.priority import PRIORITY_DEFAULT, validate_priority
from _queue_service_client_core.transport import HttpJsonTransport, encode_path_segment

# Internal wire sentinel: public ``None`` omits the JSON key; bridge may pass
# explicit JSON null. Not exported from ``queue_service_producer``.
_AVAILABLE_AT_OMITTED = object()


class ProducerClient:
    """Producer operations: capabilities, enqueue, resolve, inspect, cancel.

    The SDK does not silently retry requests. Callers decide when an idempotent
    replay is safe; ``ProtocolError.retryable`` and ``retry_after_ms`` are hints.
    """

    def __init__(self, transport: HttpJsonTransport, *, bearer_token: str) -> None:
        if not bearer_token or not bearer_token.strip():
            raise ValueError("bearer_token is required")
        self._transport = transport
        # Stored for Authorization only; never included in __repr__/errors.
        self._bearer_token = bearer_token

    def __repr__(self) -> str:
        return "ProducerClient(transport=..., bearer_token=<redacted>)"

    def _auth_headers(self, extra: dict[str, str] | None = None) -> dict[str, str]:
        headers = {"Authorization": f"Bearer {self._bearer_token}"}
        if extra:
            headers.update(extra)
        return headers

    def get_capabilities(self) -> Capabilities:
        """GET ``/v1/capabilities`` (OpenAPI ``getCapabilities``)."""

        response = self._transport.request(
            "GET",
            "/v1/capabilities",
            headers=self._auth_headers(),
            json_body=None,
        )
        return self._parse(Capabilities.parse, response.status_code, response.body)

    def enqueue(
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
        """POST ``/v1/queues/{queue_name}/tasks`` (OpenAPI ``enqueueTask``).

        Optional ``payload_encoder`` runs before payload/request size enforcement.
        Default keeps ``payload`` as the opaque JSON wire value.
        """

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
        return self._enqueue_with_available_at_raw(
            queue_name,
            idempotency_key=idempotency_key,
            payload=wire_payload,
            priority=wire_priority,
            available_at=wire_available_at,
        )

    def _enqueue_with_available_at_raw(
        self,
        queue_name: str,
        *,
        idempotency_key: str,
        payload: Any,
        priority: int = PRIORITY_DEFAULT,
        available_at: str | None | object = _AVAILABLE_AT_OMITTED,
    ) -> EnqueueResponse:
        """Internal immutable-json adapter for trusted in-package bridge callers."""

        wire_priority = validate_priority(priority)
        body: dict[str, Any] = {"payload": payload, "priority": wire_priority}
        if available_at is not _AVAILABLE_AT_OMITTED:
            body["available_at"] = available_at
        path = f"/v1/queues/{encode_path_segment(queue_name)}/tasks"
        response = self._transport.request(
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

    def resolve_submission(
        self,
        queue_name: str,
        *,
        idempotency_key: str,
    ) -> ResolveSubmissionResponse:
        """POST ``/v1/queues/{queue_name}/submissions:resolve``."""

        path = f"/v1/queues/{encode_path_segment(queue_name)}/submissions:resolve"
        response = self._transport.request(
            "POST",
            path,
            headers=self._auth_headers(),
            json_body={"idempotency_key": idempotency_key},
        )
        return self._parse(
            ResolveSubmissionResponse.parse, response.status_code, response.body
        )

    def inspect_task(self, task_id: str) -> Task:
        """GET ``/v1/tasks/{task_id}`` (OpenAPI ``getTask``, producer-authorized)."""

        path = f"/v1/tasks/{encode_path_segment(task_id)}"
        response = self._transport.request(
            "GET",
            path,
            headers=self._auth_headers(),
            json_body=None,
        )
        return self._parse(Task.parse, response.status_code, response.body)

    def cancel_task(self, task_id: str, *, reason: str | None = None) -> CancelResponse:
        """POST ``/v1/tasks/{task_id}:cancel`` (OpenAPI ``cancelTask``)."""

        body: dict[str, Any] = {}
        if reason is not None:
            body["reason"] = reason
        path = f"/v1/tasks/{encode_path_segment(task_id)}:cancel"
        response = self._transport.request(
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
