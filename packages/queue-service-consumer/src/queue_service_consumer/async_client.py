"""Async consumer SDK adapter over the OpenAPI worker-authorized lease surface."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from typing import Any

from _queue_service_client_core.async_transport import AsyncTransport
from _queue_service_client_core.capabilities import Capabilities
from _queue_service_client_core.capability_guard import (
    require_delivery_events,
    require_long_polling,
)
from _queue_service_client_core.codecs import (
    PayloadDecoder,
    PayloadEncoder,
)
from _queue_service_client_core.errors import (
    LeaseLostError,
    MalformedResponseError,
    ProtocolError,
    TerminalConflictError,
    TimeoutError as ClientTimeoutError,
)
from _queue_service_client_core.instrumentation import observe_request
from _queue_service_client_core.models import (
    AckCancelResult,
    CompleteResult,
    FailResult,
    HeartbeatResult,
    Task,
)
from _queue_service_client_core.transport import encode_path_segment

from queue_service_consumer.client import (
    _CLAIM_TOKEN_HEADER,
    _encode_spawn_items,
    _fingerprint,
    _require_bool,
    _require_int,
    _require_mapping,
    _require_str,
)


class AsyncConsumerClient:
    """Async consumer operations mirroring :class:`ConsumerClient`.

    Positive ``wait_seconds`` uses the same capability gate and per-call
    ``wait+5`` / ``wait+10`` budgets as sync. ``asyncio.CancelledError``
    propagates unchanged and is never wrapped as timeout/protocol error.
    """

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
    ) -> AsyncConsumerClient:
        from _queue_service_client_core.async_transport import HttpxAsyncTransport

        transport = HttpxAsyncTransport(base_url, timeout_s=timeout_s)
        return cls(transport, bearer_token=bearer_token, owns_transport=True)

    async def __aenter__(self) -> AsyncConsumerClient:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        if self._owns_transport:
            await self._transport.aclose()

    def __repr__(self) -> str:
        return "AsyncConsumerClient(transport=..., bearer_token=<redacted>)"

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

    async def claim(
        self,
        *,
        queues: Sequence[str],
        worker_id: str,
        lease_seconds: int,
        max_tasks: int = 1,
        wait_seconds: int = 0,
        capabilities: Capabilities | None = None,
        cancellation: object | None = None,
        payload_decoder: PayloadDecoder[Any] | None = None,
    ) -> list[AsyncClaim]:
        """POST ``/v1/claims`` (OpenAPI ``claimTasks``).

        MVP locks ``max_tasks`` to exact ``1`` locally before any capability
        fetch or transport call. Positive ``wait_seconds`` still requires
        authenticated live long-polling capabilities (fail-closed). Zero wait
        bypasses the long-poll feature guard. Empty ``tasks: []`` is success
        (including long-poll expiry). ``asyncio.CancelledError`` propagates
        unchanged.
        """

        if isinstance(max_tasks, bool) or type(max_tasks) is not int:
            raise ValueError("max_tasks must be an integer")
        if max_tasks != 1:
            raise ValueError("MVP claim requires max_tasks=1")
        if isinstance(wait_seconds, bool) or type(wait_seconds) is not int:
            raise ValueError("wait_seconds must be an integer")
        if wait_seconds < 0:
            raise ValueError("wait_seconds must be >= 0")
        if not queues:
            raise ValueError("queues must be non-empty")
        if not worker_id:
            raise ValueError("worker_id is required")

        if wait_seconds > 0:
            caps = (
                capabilities
                if capabilities is not None
                else await self.get_capabilities()
            )
            require_long_polling(caps, wait_seconds)

        read_timeout_s: float | None = None
        total_timeout_s: float | None = None
        if wait_seconds > 0:
            config = getattr(self._transport, "config", None)
            if config is None:
                raise ValueError(
                    "positive wait_seconds requires transport.config for long-poll budgets"
                )
            read_timeout_s, total_timeout_s = config.ensure_long_poll_budgets(wait_seconds)

        body = {
            "queues": list(queues),
            "max_tasks": 1,
            "lease_seconds": lease_seconds,
            "wait_seconds": wait_seconds,
            "worker_id": worker_id,
        }
        try:
            response = await self._transport.request(
                "POST",
                "/v1/claims",
                headers=self._auth_headers(),
                json_body=body,
                read_timeout_s=read_timeout_s,
                total_timeout_s=total_timeout_s,
                cancellation=cancellation,
            )
        except asyncio.CancelledError:
            observe_request(None, operation="claimTasks", result="cancelled", duration_s=0.0)
            raise
        except ClientTimeoutError:
            observe_request(None, operation="claimTasks", result="timeout", duration_s=0.0)
            raise

        payload = _require_mapping(response.status_code, response.body)
        try:
            tasks_raw = payload["tasks"]
            server_time = _require_str(payload["server_time"], "server_time")
            recommended = _require_int(
                payload["recommended_heartbeat_seconds"],
                "recommended_heartbeat_seconds",
            )
            if "queue_states" not in payload:
                raise ValueError("claim response missing queue_states")
            if not isinstance(payload["queue_states"], Mapping):
                raise ValueError("claim response queue_states must be an object")
        except KeyError as exc:
            raise MalformedResponseError(
                status_code=response.status_code,
                reason=f"claim response missing field: {exc.args[0]}",
            ) from exc
        except ValueError as exc:
            raise MalformedResponseError(
                status_code=response.status_code,
                reason=str(exc),
            ) from exc

        if not isinstance(tasks_raw, list):
            raise MalformedResponseError(
                status_code=response.status_code,
                reason="claim response tasks must be an array",
            )
        if not tasks_raw:
            observe_request(None, operation="claimTasks", result="empty", duration_s=0.0)
            return []
        if len(tasks_raw) > max_tasks:
            raise MalformedResponseError(
                status_code=response.status_code,
                reason=(
                    f"claim response returned {len(tasks_raw)} tasks, "
                    f"max allowed is {max_tasks}"
                ),
            )

        claims: list[AsyncClaim] = []
        for item in tasks_raw:
            try:
                claims.append(
                    AsyncClaim.from_claimed_task(
                        self,
                        item,
                        server_time=server_time,
                        recommended_heartbeat_seconds=recommended,
                        payload_decoder=payload_decoder,
                    )
                )
            except ValueError as exc:
                raise MalformedResponseError(
                    status_code=response.status_code,
                    reason=str(exc),
                ) from exc
        observe_request(None, operation="claimTasks", result="success", duration_s=0.0)
        return claims

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


class AsyncClaim:
    """Local fenced-lease handle for one claimed task (async)."""

    def __init__(
        self,
        client: AsyncConsumerClient,
        *,
        task: Task,
        claim_id: str,
        generation: int,
        claimed_at: str,
        lease_expires_at: str,
        worker_id: str,
        cancel_requested: bool,
        claim_token: str,
        server_time: str,
        recommended_heartbeat_seconds: int,
        payload_decoder: PayloadDecoder[Any] | None = None,
    ) -> None:
        self._client = client
        self.task = task
        self.claim_id = claim_id
        self.generation = generation
        self.claimed_at = claimed_at
        self.lease_expires_at = lease_expires_at
        self.worker_id = worker_id
        self.cancel_requested = cancel_requested
        self.server_time = server_time
        self.recommended_heartbeat_seconds = recommended_heartbeat_seconds
        self.payload_decoder = payload_decoder
        self._claim_token = claim_token
        self._lease_lost = False
        self._terminal: tuple[str, str] | None = None

    @classmethod
    def from_claimed_task(
        cls,
        client: AsyncConsumerClient,
        raw: object,
        *,
        server_time: str,
        recommended_heartbeat_seconds: int,
        payload_decoder: PayloadDecoder[Any] | None = None,
    ) -> AsyncClaim:
        if not isinstance(raw, Mapping):
            raise ValueError("claimed task must be an object")
        if "task" not in raw or "claim" not in raw:
            raise ValueError("claimed task requires task and claim")
        claim_raw = raw["claim"]
        if not isinstance(claim_raw, Mapping):
            raise ValueError("claim grant must be an object")
        required = (
            "claim_id",
            "generation",
            "claimed_at",
            "lease_expires_at",
            "worker_id",
            "cancel_requested",
            "claim_token",
        )
        missing = [key for key in required if key not in claim_raw]
        if missing:
            raise ValueError(f"claim grant missing fields: {missing}")
        return cls(
            client,
            task=Task.parse(raw["task"]),
            claim_id=_require_str(claim_raw["claim_id"], "claim.claim_id"),
            generation=_require_int(claim_raw["generation"], "claim.generation"),
            claimed_at=_require_str(claim_raw["claimed_at"], "claim.claimed_at"),
            lease_expires_at=_require_str(
                claim_raw["lease_expires_at"], "claim.lease_expires_at"
            ),
            worker_id=_require_str(claim_raw["worker_id"], "claim.worker_id"),
            cancel_requested=_require_bool(
                claim_raw["cancel_requested"], "claim.cancel_requested"
            ),
            claim_token=_require_str(claim_raw["claim_token"], "claim.claim_token"),
            server_time=server_time,
            recommended_heartbeat_seconds=recommended_heartbeat_seconds,
            payload_decoder=payload_decoder,
        )

    @property
    def lease_lost(self) -> bool:
        return self._lease_lost

    @property
    def is_terminal(self) -> bool:
        return self._terminal is not None

    def __repr__(self) -> str:
        return (
            f"AsyncClaim(claim_id={self.claim_id!r}, generation={self.generation!r}, "
            f"lease_lost={self._lease_lost!r})"
        )

    def __str__(self) -> str:
        return self.__repr__()

    async def heartbeat(self, *, lease_seconds: int) -> HeartbeatResult:
        self._ensure_usable()
        body = {"generation": self.generation, "lease_seconds": lease_seconds}
        result = await self._request_lease("heartbeat", body, HeartbeatResult.parse)
        self.lease_expires_at = result.claim.lease_expires_at
        self.cancel_requested = result.claim.cancel_requested
        self.server_time = result.server_time
        self.recommended_heartbeat_seconds = result.recommended_heartbeat_seconds
        return result

    async def complete(
        self,
        *,
        spawn: Sequence[Mapping[str, Any]] | None = None,
        events: Sequence[Mapping[str, Any]] | None = None,
        capabilities: Capabilities | None = None,
        spawn_encoder: PayloadEncoder[Any] | None = None,
        max_payload_bytes: int | None = None,
    ) -> CompleteResult:
        self._ensure_usable()
        limit = max_payload_bytes
        if limit is None and capabilities is not None:
            limit = capabilities.payload_runtime_max_bytes
        wire_spawn = _encode_spawn_items(
            spawn, encoder=spawn_encoder, max_payload_bytes=limit
        )
        if events:
            caps = (
                capabilities
                if capabilities is not None
                else await self._client.get_capabilities()
            )
            require_delivery_events(caps, events)
        body: dict[str, Any] = {
            "generation": self.generation,
            "spawn": wire_spawn,
        }
        if events:
            body["events"] = [dict(item) for item in events]
        fingerprint = self._check_terminal("complete", body)
        result = await self._request_lease("complete", body, CompleteResult.parse)
        self._terminal = ("complete", fingerprint)
        return result

    async def fail(
        self,
        *,
        retryable: bool,
        failure_code: str,
        failure_detail: str | None = None,
    ) -> FailResult:
        self._ensure_usable()
        body: dict[str, Any] = {
            "generation": self.generation,
            "retryable": retryable,
            "failure_code": failure_code,
        }
        if failure_detail is not None:
            body["failure_detail"] = failure_detail
        fingerprint = self._check_terminal("fail", body)
        result = await self._request_lease("fail", body, FailResult.parse)
        self._terminal = ("fail", fingerprint)
        return result

    async def ack_cancel(self) -> AckCancelResult:
        self._ensure_usable()
        body = {"generation": self.generation}
        fingerprint = self._check_terminal("ack_cancel", body)
        result = await self._request_lease("ack-cancel", body, AckCancelResult.parse)
        self._terminal = ("ack_cancel", fingerprint)
        return result

    def _ensure_usable(self) -> None:
        if self._lease_lost:
            raise LeaseLostError(claim_id=self.claim_id)

    def _check_terminal(self, kind: str, body: Mapping[str, Any]) -> str:
        fingerprint = _fingerprint(body)
        if self._terminal is None:
            return fingerprint
        prev_kind, prev_fp = self._terminal
        if prev_kind == kind and prev_fp == fingerprint:
            return fingerprint
        raise TerminalConflictError(
            claim_id=self.claim_id,
            reason="changed terminal body or operation",
        )

    def _lease_headers(self) -> dict[str, str]:
        return self._client._auth_headers({_CLAIM_TOKEN_HEADER: self._claim_token})

    async def _request_lease(
        self,
        operation: str,
        body: Mapping[str, Any],
        parser: Any,
    ) -> Any:
        path = f"/v1/claims/{encode_path_segment(self.claim_id)}:{operation}"
        try:
            response = await self._client._transport.request(
                "POST",
                path,
                headers=self._lease_headers(),
                json_body=dict(body),
            )
        except ProtocolError as exc:
            if exc.code.value == "lease_lost" and self._terminal is None:
                self._lease_lost = True
            raise
        payload = _require_mapping(response.status_code, response.body)
        try:
            return parser(payload)
        except ValueError as exc:
            raise MalformedResponseError(
                status_code=response.status_code,
                reason=str(exc),
            ) from exc


__all__ = ["AsyncClaim", "AsyncConsumerClient"]
