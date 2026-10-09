"""SDK-12 async instrumentation: parity with sync allowlisted events."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from _queue_service_client_core.errors import (
    ProtocolError,
    RequestCancelledError,
    TransportError,
)
from _queue_service_client_core.instrumentation import (
    AsyncInstrumentation,
    OperationEvent,
)
from _queue_service_client_core.models import ErrorCode, ProtocolErrorBody
from _queue_service_client_core.transport import (
    TransportResponse,
    async_request_with_instrumentation,
)

SECRET = "CANARY_SECRET_do_not_leak_9f3a"
CLAIM = "claim_CANARY_secret_id"
TOKEN = "tok_CANARY_bearer_leak"
IDEMPOTENCY = "idem_CANARY_key_leak"
PAYLOAD = {"credit_card": "CANARY_PAN_4111", "note": SECRET}


class _RecordingAsyncHooks:
    def __init__(self) -> None:
        self.events: list[OperationEvent] = []
        self.fail_on: set[str] = set()
        self.raise_exc: BaseException = RuntimeError(f"async-hook-boom:{SECRET}")

    async def _record(self, name: str, event: OperationEvent) -> None:
        self.events.append(event)
        if name in self.fail_on:
            raise self.raise_exc

    async def on_start(self, event: OperationEvent) -> None:
        await self._record("start", event)

    async def on_attempt(self, event: OperationEvent) -> None:
        await self._record("attempt", event)

    async def on_success(self, event: OperationEvent) -> None:
        await self._record("success", event)

    async def on_failure(self, event: OperationEvent) -> None:
        await self._record("failure", event)

    async def on_cancelled(self, event: OperationEvent) -> None:
        await self._record("cancelled", event)


class _ScriptedAsyncTransport:
    def __init__(self, outcomes: list[TransportResponse | BaseException]) -> None:
        self._outcomes = list(outcomes)
        self.calls: list[dict[str, Any]] = []

    async def request(
        self,
        method: str,
        path: str,
        *,
        headers: dict[str, str] | None = None,
        json_body: object | None = None,
        query: dict[str, str] | None = None,
        expect_body: bool = True,
        read_timeout_s: float | None = None,
        total_timeout_s: float | None = None,
        cancellation: object | None = None,
    ) -> TransportResponse:
        self.calls.append(
            {
                "method": method,
                "path": path,
                "headers": dict(headers or {}),
                "json_body": json_body,
                "query": dict(query or {}),
                "read_timeout_s": read_timeout_s,
                "total_timeout_s": total_timeout_s,
            }
        )
        outcome = self._outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


def _protocol_error() -> ProtocolError:
    body = ProtocolErrorBody(
        code=ErrorCode.parse("internal_error"),
        message=f"incident:{SECRET}",
        retryable=True,
        request_id="req_async_1",
        details={"payload": PAYLOAD, "claim_id": CLAIM},
    )
    return ProtocolError(status_code=503, body=body)


def _assert_no_secrets(blob: str) -> None:
    assert SECRET not in blob
    assert CLAIM not in blob
    assert TOKEN not in blob
    assert IDEMPOTENCY not in blob
    assert "CANARY_PAN" not in blob
    assert "incident:" not in blob


def _event_blob(events: list[OperationEvent]) -> str:
    return repr(events) + "".join(repr(dict(e.as_labels())) for e in events)


@pytest.mark.asyncio
async def test_async_success_matches_sync_event_sequence() -> None:
    hooks = _RecordingAsyncHooks()
    instr = AsyncInstrumentation(hooks)
    transport = _ScriptedAsyncTransport(
        [
            TransportResponse(
                status_code=200, headers={}, body={"ok": True}, raw_body=b"{}"
            )
        ]
    )

    response = await async_request_with_instrumentation(
        transport,
        instr,
        operation_id="getTask",
        method="GET",
        route_template="/v1/tasks/{task_id}",
        path=f"/v1/tasks/{CLAIM}",
        headers={"Authorization": f"Bearer {TOKEN}"},
        query={"idempotency_key": IDEMPOTENCY},
        json_body=PAYLOAD,
    )

    assert response.status_code == 200
    assert [e.kind for e in hooks.events] == ["start", "attempt", "success"]
    assert hooks.events[0].route_template == "/v1/tasks/{task_id}"
    assert hooks.events[-1].status_code == 200
    _assert_no_secrets(_event_blob(hooks.events))


@pytest.mark.asyncio
async def test_async_failure_and_retry_parity() -> None:
    hooks = _RecordingAsyncHooks()
    instr = AsyncInstrumentation(hooks)
    transport = _ScriptedAsyncTransport(
        [
            _protocol_error(),
            TransportResponse(status_code=200, headers={}, body={}, raw_body=b"{}"),
        ]
    )

    span = await instr.start(
        operation_id="getTask",
        method="GET",
        route_template="/v1/tasks/{task_id}",
    )
    await instr.attempt(span, attempt=1)
    with pytest.raises(ProtocolError):
        await transport.request(
            "GET",
            f"/v1/tasks/{CLAIM}",
            headers={"Authorization": f"Bearer {TOKEN}"},
            json_body=PAYLOAD,
        )
    await instr.failure(span, _protocol_error())
    await instr.attempt(span, attempt=2)
    response = await transport.request("GET", f"/v1/tasks/{CLAIM}")
    await instr.success(span, status_code=response.status_code)

    assert [e.kind for e in hooks.events] == [
        "start",
        "attempt",
        "failure",
        "attempt",
        "success",
    ]
    assert hooks.events[1].attempt == 1
    assert hooks.events[2].error_code == "internal_error"
    assert hooks.events[2].retryable is True
    assert hooks.events[2].request_id == "req_async_1"
    assert "request_id" not in hooks.events[2].as_labels()
    assert hooks.events[3].attempt == 2
    _assert_no_secrets(_event_blob(hooks.events))


@pytest.mark.asyncio
async def test_async_cancellation_not_synthetic_protocol_failure() -> None:
    hooks = _RecordingAsyncHooks()
    instr = AsyncInstrumentation(hooks)
    transport = _ScriptedAsyncTransport([RequestCancelledError()])

    with pytest.raises(RequestCancelledError):
        await async_request_with_instrumentation(
            transport,
            instr,
            operation_id="claimTasks",
            method="POST",
            route_template="/v1/claims",
            path="/v1/claims",
            headers={"Authorization": f"Bearer {TOKEN}"},
            json_body={"claim_token": TOKEN, "payload": PAYLOAD},
        )

    assert [e.kind for e in hooks.events] == ["start", "attempt", "cancelled"]
    assert hooks.events[-1].status == "cancelled"
    assert hooks.events[-1].kind != "failure"
    _assert_no_secrets(_event_blob(hooks.events))


@pytest.mark.asyncio
async def test_async_malicious_hooks_isolated_from_transport() -> None:
    hooks = _RecordingAsyncHooks()
    hooks.fail_on = {"start", "attempt", "success"}

    def sink(exc: BaseException) -> None:
        raise RuntimeError(f"sink-boom:{SECRET}")

    instr = AsyncInstrumentation(hooks, hook_error_sink=sink)
    transport = _ScriptedAsyncTransport(
        [TransportResponse(status_code=200, headers={}, body={}, raw_body=b"{}")]
    )

    response = await async_request_with_instrumentation(
        transport,
        instr,
        operation_id="getTask",
        method="GET",
        route_template="/v1/tasks/{task_id}",
        path=f"/v1/tasks/{CLAIM}",
        headers={"Authorization": f"Bearer {TOKEN}"},
        json_body=PAYLOAD,
    )

    assert response.status_code == 200
    assert len(transport.calls) == 1
    assert [e.kind for e in hooks.events] == ["start", "attempt", "success"]
    _assert_no_secrets(_event_blob(hooks.events))


@pytest.mark.asyncio
async def test_async_transport_error_stable_code() -> None:
    hooks = _RecordingAsyncHooks()
    instr = AsyncInstrumentation(hooks)
    transport = _ScriptedAsyncTransport([TransportError(reason=f"dns:{SECRET}")])

    with pytest.raises(TransportError):
        await async_request_with_instrumentation(
            transport,
            instr,
            operation_id="getTask",
            method="GET",
            route_template="/v1/tasks/{task_id}",
            path=f"/v1/tasks/{CLAIM}",
        )

    failure = hooks.events[-1]
    assert failure.kind == "failure"
    assert failure.error_code == "transport_error"
    _assert_no_secrets(_event_blob(hooks.events))


@pytest.mark.asyncio
async def test_async_cancelled_error_from_hook_propagates_without_http() -> None:
    hooks = _RecordingAsyncHooks()
    hooks.fail_on = {"start"}
    hooks.raise_exc = asyncio.CancelledError()
    instr = AsyncInstrumentation(hooks)
    transport = _ScriptedAsyncTransport(
        [TransportResponse(status_code=200, headers={}, body={}, raw_body=b"{}")]
    )

    with pytest.raises(asyncio.CancelledError):
        await async_request_with_instrumentation(
            transport,
            instr,
            operation_id="getTask",
            method="GET",
            route_template="/v1/tasks/{task_id}",
            path=f"/v1/tasks/{CLAIM}",
        )

    assert transport.calls == []
    assert [e.kind for e in hooks.events] == ["start"]


@pytest.mark.asyncio
async def test_async_instrumentation_forwards_timeouts() -> None:
    hooks = _RecordingAsyncHooks()
    instr = AsyncInstrumentation(hooks)
    transport = _ScriptedAsyncTransport(
        [
            TransportResponse(
                status_code=200, headers={}, body={"ok": True}, raw_body=b"{}"
            )
        ]
    )

    await async_request_with_instrumentation(
        transport,
        instr,
        operation_id="claimTasks",
        method="POST",
        route_template="/v1/claims",
        path="/v1/claims",
        read_timeout_s=45.0,
        total_timeout_s=50.0,
    )

    assert transport.calls[0]["read_timeout_s"] == 45.0
    assert transport.calls[0]["total_timeout_s"] == 50.0
