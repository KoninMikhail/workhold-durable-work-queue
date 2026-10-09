"""SDK-12 sync instrumentation: allowlisted events, isolation, no secret leakage."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from _queue_service_client_core.errors import (
    LeaseLostError,
    ProtocolError,
    RequestCancelledError,
    TimeoutError as ClientTimeoutError,
    TransportError,
)
from _queue_service_client_core.instrumentation import (
    OperationEvent,
    SyncInstrumentation,
)
from _queue_service_client_core.models import ErrorCode, ProtocolErrorBody
from _queue_service_client_core.transport import (
    TransportResponse,
    request_with_instrumentation,
)

SECRET = "CANARY_SECRET_do_not_leak_9f3a"
CLAIM = "claim_CANARY_secret_id"
TOKEN = "tok_CANARY_bearer_leak"
IDEMPOTENCY = "idem_CANARY_key_leak"
PAYLOAD = {"credit_card": "CANARY_PAN_4111", "note": SECRET}


class _RecordingHooks:
    def __init__(self) -> None:
        self.events: list[OperationEvent] = []
        self.fail_on: set[str] = set()
        self.raise_exc: BaseException = RuntimeError(f"hook-boom:{SECRET}")

    def _record(self, name: str, event: OperationEvent) -> None:
        self.events.append(event)
        if name in self.fail_on:
            raise self.raise_exc

    def on_start(self, event: OperationEvent) -> None:
        self._record("start", event)

    def on_attempt(self, event: OperationEvent) -> None:
        self._record("attempt", event)

    def on_success(self, event: OperationEvent) -> None:
        self._record("success", event)

    def on_failure(self, event: OperationEvent) -> None:
        self._record("failure", event)

    def on_cancelled(self, event: OperationEvent) -> None:
        self._record("cancelled", event)


class _ScriptedTransport:
    def __init__(self, outcomes: list[TransportResponse | BaseException]) -> None:
        self._outcomes = list(outcomes)
        self.calls: list[dict[str, Any]] = []

    def request(
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
            }
        )
        outcome = self._outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


def _protocol_error(*, retryable: bool = True) -> ProtocolError:
    body = ProtocolErrorBody(
        code=ErrorCode.parse("internal_error"),
        message=f"incident:{SECRET}",
        retryable=retryable,
        request_id="req_safe_1",
        details={"payload": PAYLOAD, "claim_id": CLAIM},
    )
    return ProtocolError(status_code=500, body=body)


def _assert_no_secrets(blob: str) -> None:
    assert SECRET not in blob
    assert CLAIM not in blob
    assert TOKEN not in blob
    assert IDEMPOTENCY not in blob
    assert "CANARY_PAN" not in blob
    assert "incident:" not in blob


def _event_blob(events: list[OperationEvent]) -> str:
    return repr(events) + "".join(repr(dict(e.as_labels())) for e in events)


def test_success_emits_start_attempt_success_allowlisted_fields() -> None:
    hooks = _RecordingHooks()
    sink_errors: list[BaseException] = []
    instr = SyncInstrumentation(hooks, hook_error_sink=sink_errors.append)
    transport = _ScriptedTransport(
        [
            TransportResponse(
                status_code=200, headers={}, body={"ok": True}, raw_body=b"{}"
            )
        ]
    )

    response = request_with_instrumentation(
        transport,
        instr,
        operation_id="getTask",
        method="GET",
        route_template="/v1/tasks/{task_id}",
        path=f"/v1/tasks/{CLAIM}",
        headers={"Authorization": f"Bearer {TOKEN}", "X-Queue-Claim-Token": TOKEN},
        query={"idempotency_key": IDEMPOTENCY, "q": SECRET},
        json_body=PAYLOAD,
    )

    assert response.status_code == 200
    assert [e.kind for e in hooks.events] == ["start", "attempt", "success"]
    start, attempt, success = hooks.events
    assert start.operation_id == "getTask"
    assert start.method == "GET"
    assert start.route_template == "/v1/tasks/{task_id}"
    assert start.attempt == 1
    assert start.status == "started"
    assert attempt.status == "attempting"
    assert success.status == "ok"
    assert success.status_code == 200
    assert success.error_code is None
    assert CLAIM not in success.route_template
    _assert_no_secrets(_event_blob(hooks.events))
    assert sink_errors == []


def test_protocol_failure_emits_stable_code_without_incident_text() -> None:
    hooks = _RecordingHooks()
    instr = SyncInstrumentation(hooks)
    transport = _ScriptedTransport([_protocol_error(retryable=True)])

    with pytest.raises(ProtocolError):
        request_with_instrumentation(
            transport,
            instr,
            operation_id="enqueueTask",
            method="POST",
            route_template="/v1/queues/{queue_name}/tasks",
            path=f"/v1/queues/{SECRET}/tasks",
            headers={"Authorization": f"Bearer {TOKEN}"},
            json_body={"payload": PAYLOAD, "idempotency_key": IDEMPOTENCY},
        )

    assert [e.kind for e in hooks.events] == ["start", "attempt", "failure"]
    failure = hooks.events[-1]
    assert failure.error_code == "internal_error"
    assert failure.retryable is True
    assert failure.request_id == "req_safe_1"
    assert "request_id" not in failure.as_labels()
    assert failure.status_code == 500
    _assert_no_secrets(_event_blob(hooks.events))


def test_retry_attempts_emit_incrementing_attempt_numbers() -> None:
    hooks = _RecordingHooks()
    instr = SyncInstrumentation(hooks)
    transport = _ScriptedTransport(
        [
            TransportError(reason=f"reset:{SECRET}"),
            TransportResponse(status_code=200, headers={}, body={}, raw_body=b"{}"),
        ]
    )

    span = instr.start(
        operation_id="getTask",
        method="GET",
        route_template="/v1/tasks/{task_id}",
    )
    instr.attempt(span, attempt=1)
    with pytest.raises(TransportError):
        transport.request(
            "GET",
            f"/v1/tasks/{CLAIM}",
            headers={"Authorization": f"Bearer {TOKEN}"},
            json_body=PAYLOAD,
            query={"cursor": SECRET},
        )
    instr.failure(span, TransportError(reason="TransportError"))
    instr.attempt(span, attempt=2)
    response = transport.request("GET", f"/v1/tasks/{CLAIM}")
    instr.success(span, status_code=response.status_code)

    kinds = [e.kind for e in hooks.events]
    assert kinds == ["start", "attempt", "failure", "attempt", "success"]
    assert hooks.events[1].attempt == 1
    assert hooks.events[3].attempt == 2
    assert hooks.events[2].error_code == "transport_error"
    _assert_no_secrets(_event_blob(hooks.events))


def test_cancellation_is_distinct_from_protocol_failure() -> None:
    hooks = _RecordingHooks()
    instr = SyncInstrumentation(hooks)
    transport = _ScriptedTransport([RequestCancelledError()])

    with pytest.raises(RequestCancelledError):
        request_with_instrumentation(
            transport,
            instr,
            operation_id="claimTasks",
            method="POST",
            route_template="/v1/claims",
            path="/v1/claims",
            headers={"Authorization": f"Bearer {TOKEN}"},
            json_body={"queues": [SECRET], "claim_token": TOKEN},
        )

    assert [e.kind for e in hooks.events] == ["start", "attempt", "cancelled"]
    cancelled = hooks.events[-1]
    assert cancelled.status == "cancelled"
    assert cancelled.error_code == "cancelled"
    assert cancelled.retryable is False
    assert cancelled.kind != "failure"
    _assert_no_secrets(_event_blob(hooks.events))


def test_malicious_hooks_cannot_change_http_outcome() -> None:
    hooks = _RecordingHooks()
    hooks.fail_on = {"start", "attempt", "success", "failure", "cancelled"}
    exploding_sink_calls = {"n": 0}

    def sink(exc: BaseException) -> None:
        exploding_sink_calls["n"] += 1
        raise RuntimeError(f"sink-boom:{SECRET}")

    instr = SyncInstrumentation(hooks, hook_error_sink=sink)
    transport = _ScriptedTransport(
        [TransportResponse(status_code=204, headers={}, body=None, raw_body=b"")]
    )

    response = request_with_instrumentation(
        transport,
        instr,
        operation_id="completeClaim",
        method="POST",
        route_template="/v1/claims/{claim_id}/complete",
        path=f"/v1/claims/{CLAIM}/complete",
        headers={"X-Queue-Claim-Token": TOKEN},
        json_body={"result": PAYLOAD},
    )

    assert response.status_code == 204
    assert len(transport.calls) == 1
    assert len(hooks.events) == 3
    _assert_no_secrets(_event_blob(hooks.events))
    assert exploding_sink_calls["n"] >= 1


def test_lease_lost_omits_claim_identifier() -> None:
    hooks = _RecordingHooks()
    instr = SyncInstrumentation(hooks)
    transport = _ScriptedTransport([LeaseLostError(claim_id=CLAIM)])

    with pytest.raises(LeaseLostError):
        request_with_instrumentation(
            transport,
            instr,
            operation_id="heartbeatClaim",
            method="POST",
            route_template="/v1/claims/{claim_id}/heartbeat",
            path=f"/v1/claims/{CLAIM}/heartbeat",
            headers={"X-Queue-Claim-Token": TOKEN},
        )

    failure = hooks.events[-1]
    assert failure.kind == "failure"
    assert failure.error_code == "lease_lost"
    assert CLAIM not in _event_blob(hooks.events)
    _assert_no_secrets(_event_blob(hooks.events))


def test_timeout_maps_to_stable_error_code() -> None:
    hooks = _RecordingHooks()
    instr = SyncInstrumentation(hooks)
    transport = _ScriptedTransport([ClientTimeoutError(timeout_s=1.0)])

    with pytest.raises(ClientTimeoutError):
        request_with_instrumentation(
            transport,
            instr,
            operation_id="getTask",
            method="GET",
            route_template="/v1/tasks/{task_id}",
            path=f"/v1/tasks/{CLAIM}",
        )

    failure = hooks.events[-1]
    assert failure.error_code == "timeout"
    assert failure.retryable is True


def test_raw_url_route_template_rejected() -> None:
    instr = SyncInstrumentation(_RecordingHooks())
    with pytest.raises(ValueError):
        instr.start(
            operation_id="getTask",
            method="GET",
            route_template=f"https://evil.example/v1/tasks/{CLAIM}?token={TOKEN}",
        )


def test_allowlisted_label_keys_are_stable() -> None:
    event = OperationEvent(
        kind="success",
        operation_id="getTask",
        method="GET",
        route_template="/v1/tasks/{task_id}",
        attempt=1,
        duration_s=0.01,
        status="ok",
        status_code=200,
        error_code=None,
        retryable=None,
        request_id="req_1",
    )
    assert event.request_id == "req_1"
    assert set(event.as_labels()) == {
        "kind",
        "operation_id",
        "method",
        "route_template",
        "attempt",
        "status",
        "status_code",
    }
    assert "request_id" not in event.as_labels()


def test_cancelled_error_from_hook_propagates_without_http() -> None:
    hooks = _RecordingHooks()
    hooks.fail_on = {"start"}
    hooks.raise_exc = asyncio.CancelledError()
    instr = SyncInstrumentation(hooks)
    transport = _ScriptedTransport(
        [TransportResponse(status_code=200, headers={}, body={}, raw_body=b"{}")]
    )

    with pytest.raises(asyncio.CancelledError):
        request_with_instrumentation(
            transport,
            instr,
            operation_id="getTask",
            method="GET",
            route_template="/v1/tasks/{task_id}",
            path=f"/v1/tasks/{CLAIM}",
        )

    assert transport.calls == []
    assert [e.kind for e in hooks.events] == ["start"]
