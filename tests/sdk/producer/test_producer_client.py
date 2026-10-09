"""ProducerClient recording-server tests (SDK-05 / SDK-08).

Contracts follow OpenAPI ``enqueueTask``, ``resolveSubmission``, ``getTask``,
``cancelTask``, and ``getCapabilities``. No silent retries; diagnostics must not
leak bearer tokens, payloads, or idempotency keys.
"""

from __future__ import annotations

import json
import threading
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any
from urllib.parse import unquote

import pytest

from _queue_service_client_core.errors import (
    AuthenticationError,
    MalformedResponseError,
    ProtocolError,
    QueueClientError,
    TimeoutError as ClientTimeoutError,
)
from _queue_service_client_core.models import ErrorCode, TaskState
from _queue_service_client_core.priority import PRIORITY_MAX, PRIORITY_MIN
from _queue_service_client_core.transport import HttpJsonTransport
from queue_service_producer import ProducerClient
import queue_service_producer as producer_pkg


class _RecordingHandler(BaseHTTPRequestHandler):
    """Shared handler; routes and recorded requests live on the server instance."""

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return

    def _record(self, body: bytes) -> None:
        headers = {k.lower(): v for k, v in self.headers.items()}
        path = self.path.split("?", 1)[0]
        self.server.recorded.append(  # type: ignore[attr-defined]
            {
                "method": self.command,
                "path": path,
                "headers": headers,
                "body": body,
            }
        )

    def _respond(self, status: int, payload: dict[str, Any] | list[Any] | None) -> None:
        raw = b"" if payload is None else json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("X-Request-ID", "11111111-1111-4111-8111-111111111111")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        if raw:
            self.wfile.write(raw)

    def _dispatch(self) -> None:
        length = int(self.headers.get("Content-Length", "0") or "0")
        body = self.rfile.read(length) if length else b""
        self._record(body)
        path = unquote(self.path.split("?", 1)[0])
        key = (self.command, path)
        responder = self.server.routes.get(key)  # type: ignore[attr-defined]
        if responder is None:
            self._respond(
                404,
                {
                    "code": "task_not_found",
                    "message": "missing route",
                    "retryable": False,
                    "request_id": "00000000-0000-4000-8000-000000000099",
                    "details": {},
                },
            )
            return
        status, payload = responder(body, {k.lower(): v for k, v in self.headers.items()})
        self._respond(status, payload)

    def do_GET(self) -> None:  # noqa: N802
        self._dispatch()

    def do_POST(self) -> None:  # noqa: N802
        self._dispatch()


@pytest.fixture
def recording_server() -> Any:
    server = HTTPServer(("127.0.0.1", 0), _RecordingHandler)
    server.recorded = []  # type: ignore[attr-defined]
    server.routes = {}  # type: ignore[attr-defined]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address[:2]
    base_url = f"http://{host}:{port}"
    try:
        yield server, base_url
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def _task_body(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "task_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "queue_name": "orders",
        "producer_id": "producer-1",
        "state": "ready",
        "priority": 0,
        "available_at": "2026-09-19T00:00:00Z",
        "retry_policy_version": 1,
        "created_at": "2026-09-19T00:00:00Z",
        "spawned_task_ids": [],
        "delivery_event_ids": [],
    }
    body.update(overrides)
    return body


def _capabilities_body(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "protocol_major": 1,
        "protocol_version": "1.0",
        "schema_revision": "0001",
        "scheduling": True,
        "priority": True,
        "delivery_events": False,
        "batch_claim": False,
        "long_polling": False,
        "max_claim_tasks": 1,
        "max_wait_seconds": 0,
        "payload_runtime_max_bytes": 262144,
        "payload_hard_max_bytes": 1048576,
        "enqueue_dedup_ttl_seconds": 7776000,
        "enqueue_dedup_ttl_min_seconds": 2592000,
        "enqueue_dedup_ttl_max_seconds": 31536000,
        "terminal_replay_ttl_seconds": 604800,
        "terminal_replay_ttl_min_seconds": 86400,
        "terminal_replay_ttl_max_seconds": 2592000,
        "admin_replay_ttl_seconds": 2592000,
        "admin_replay_ttl_min_seconds": 604800,
        "admin_replay_ttl_max_seconds": 7776000,
    }
    body.update(overrides)
    return body


def _error(
    *,
    code: str = "validation_failed",
    message: str = "bad request",
    retryable: bool = False,
    details: dict[str, Any] | None = None,
    retry_after_ms: int | None = None,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "code": code,
        "message": message,
        "retryable": retryable,
        "request_id": "22222222-2222-4222-8222-222222222222",
        "details": {} if details is None else details,
    }
    if retry_after_ms is not None:
        body["retry_after_ms"] = retry_after_ms
    return body


def _client(
    base_url: str, *, token: str = "secret-token-value", timeout: float = 2.0
) -> ProducerClient:
    transport = HttpJsonTransport(base_url, timeout_s=timeout)
    return ProducerClient(transport, bearer_token=token)


def test_package_exports_producer_surface_only() -> None:
    from queue_service_producer import AsyncProducerClient

    assert "ProducerClient" in producer_pkg.__all__
    assert "AsyncProducerClient" in producer_pkg.__all__
    assert producer_pkg.ProducerClient is ProducerClient
    assert producer_pkg.AsyncProducerClient is AsyncProducerClient
    for forbidden in (
        "ConsumerClient",
        "ConsumerSupervisor",
        "ObserverClient",
        "AdminClient",
        "BreakGlassClient",
        "WorkerClient",
        "WorkerSupervisor",
    ):
        assert forbidden not in producer_pkg.__all__
        assert not hasattr(producer_pkg, forbidden)
    client = ProducerClient(
        HttpJsonTransport("http://127.0.0.1:9", timeout_s=0.1),
        bearer_token="token",
    )
    for forbidden_method in (
        "claim",
        "claim_tasks",
        "heartbeat",
        "complete",
        "fail",
        "ack_cancel",
        "list_queues",
        "create_queue",
    ):
        assert not hasattr(client, forbidden_method)
    # Clean role API names (no prototype inspect/cancel aliases).
    assert hasattr(client, "inspect_task")
    assert hasattr(client, "cancel_task")
    assert not hasattr(client, "inspect")
    assert not hasattr(client, "cancel")


@pytest.mark.parametrize("token", ["", "   ", "\t"])
def test_whitespace_bearer_token_rejected(token: str) -> None:
    transport = HttpJsonTransport("http://127.0.0.1:9", timeout_s=0.1)
    with pytest.raises(ValueError, match="bearer_token"):
        ProducerClient(transport, bearer_token=token)


def test_get_capabilities_uses_openapi_contract(recording_server: Any) -> None:
    server, base_url = recording_server
    server.routes[("GET", "/v1/capabilities")] = (
        lambda body, headers: (200, _capabilities_body(future_gate=True))
    )
    caps = _client(base_url).get_capabilities()
    assert caps.protocol_major == 1
    assert caps.priority is True
    assert caps.extra["future_gate"] is True
    assert len(server.recorded) == 1
    rec = server.recorded[0]
    assert rec["method"] == "GET"
    assert rec["path"] == "/v1/capabilities"
    assert rec["headers"]["authorization"] == "Bearer secret-token-value"
    assert rec["body"] == b""


def test_enqueue_uses_openapi_path_method_headers_and_body(recording_server: Any) -> None:
    server, base_url = recording_server

    def respond(body: bytes, headers: dict[str, str]) -> tuple[int, dict[str, Any]]:
        return 201, {"task": _task_body(), "replayed": False}

    server.routes[("POST", "/v1/queues/orders/tasks")] = respond
    client = _client(base_url)
    result = client.enqueue(
        "orders",
        idempotency_key="idem-1",
        payload={"order_id": 42},
        priority=0,
    )
    assert result.replayed is False
    assert result.task.task_id == "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    assert result.task.state.value == "ready"
    assert result.task.state.is_unknown is False

    assert len(server.recorded) == 1
    rec = server.recorded[0]
    assert rec["method"] == "POST"
    assert rec["path"] == "/v1/queues/orders/tasks"
    assert rec["headers"]["authorization"] == "Bearer secret-token-value"
    assert rec["headers"]["idempotency-key"] == "idem-1"
    assert rec["headers"]["content-type"].startswith("application/json")
    assert json.loads(rec["body"].decode("utf-8")) == {
        "payload": {"order_id": 42},
        "priority": 0,
    }


def test_enqueue_replay_success_is_normal_response(recording_server: Any) -> None:
    server, base_url = recording_server
    server.routes[("POST", "/v1/queues/orders/tasks")] = (
        lambda body, headers: (200, {"task": _task_body(), "replayed": True})
    )
    result = _client(base_url).enqueue(
        "orders",
        idempotency_key="idem-replay",
        payload={"x": 1},
        priority=0,
    )
    assert result.replayed is True


def test_enqueue_fingerprint_conflict_is_structured_protocol_error(
    recording_server: Any,
) -> None:
    server, base_url = recording_server
    server.routes[("POST", "/v1/queues/orders/tasks")] = (
        lambda body, headers: (
            409,
            _error(
                code="idempotency_conflict",
                message="fingerprint mismatch",
                details={"field": "payload"},
            ),
        )
    )
    client = _client(base_url)
    with pytest.raises(ProtocolError) as exc_info:
        client.enqueue(
            "orders", idempotency_key="idem-conflict", payload={"x": 2}, priority=0
        )
    err = exc_info.value
    assert err.status_code == 409
    assert err.code.value == "idempotency_conflict"
    assert err.code.is_unknown is False
    assert err.retryable is False
    assert err.request_id == "22222222-2222-4222-8222-222222222222"
    assert err.details == {"field": "payload"}
    text = f"{err!s}{err!r}"
    assert "secret-token-value" not in text
    assert '{"x": 2}' not in text
    assert "order_id" not in text
    assert "idem-conflict" not in text
    assert len(server.recorded) == 1  # no silent retry


def test_retryable_hint_preserved_without_automatic_retry(recording_server: Any) -> None:
    server, base_url = recording_server
    server.routes[("POST", "/v1/queues/orders/tasks")] = (
        lambda body, headers: (
            503,
            _error(
                code="dependency_unavailable",
                message="db down",
                retryable=True,
                retry_after_ms=250,
            ),
        )
    )
    with pytest.raises(ProtocolError) as exc_info:
        _client(base_url).enqueue(
            "orders", idempotency_key="idem-retry", payload={}, priority=0
        )
    err = exc_info.value
    assert err.retryable is True
    assert err.retry_after_ms == 250
    assert len(server.recorded) == 1


def test_resolve_inspect_cancel_use_exact_openapi_shapes(recording_server: Any) -> None:
    server, base_url = recording_server

    server.routes[("POST", "/v1/queues/orders/submissions:resolve")] = (
        lambda body, headers: (
            200,
            {
                "task": _task_body(),
                "dedup_expires_at": "2026-12-18T00:00:00Z",
            },
        )
    )
    server.routes[("GET", "/v1/tasks/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")] = (
        lambda body, headers: (200, _task_body(state="leased", payload=None))
    )
    server.routes[("POST", "/v1/tasks/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa:cancel")] = (
        lambda body, headers: (200, {"task": _task_body(state="cancelled")})
    )

    client = _client(base_url)
    resolved = client.resolve_submission("orders", idempotency_key="idem-resolve")
    assert resolved.dedup_expires_at == "2026-12-18T00:00:00Z"
    assert resolved.task.queue_name == "orders"

    inspected = client.inspect_task("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
    assert inspected.state.value == "leased"

    cancelled = client.cancel_task(
        "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", reason="user_aborted"
    )
    assert cancelled.task.state.value == "cancelled"

    assert [r["method"] + " " + r["path"] for r in server.recorded] == [
        "POST /v1/queues/orders/submissions:resolve",
        "GET /v1/tasks/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "POST /v1/tasks/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa:cancel",
    ]
    resolve_body = json.loads(server.recorded[0]["body"].decode("utf-8"))
    assert resolve_body == {"idempotency_key": "idem-resolve"}
    assert server.recorded[0]["headers"]["authorization"] == "Bearer secret-token-value"
    assert server.recorded[1]["body"] == b""
    cancel_body = json.loads(server.recorded[2]["body"].decode("utf-8"))
    assert cancel_body == {"reason": "user_aborted"}


def test_unknown_additive_fields_and_enum_values_are_tolerated(
    recording_server: Any,
) -> None:
    server, base_url = recording_server
    server.routes[("GET", "/v1/tasks/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")] = (
        lambda body, headers: (
            200,
            _task_body(
                state="awaiting_approval",
                future_flag=True,
                nested={"ok": 1},
            ),
        )
    )
    task = _client(base_url).inspect_task("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
    assert task.state.value == "awaiting_approval"
    assert task.state.is_unknown is True
    assert isinstance(task.state, TaskState)
    assert task.extra["future_flag"] is True
    assert task.extra["nested"] == {"ok": 1}


def test_unknown_error_code_preserved_as_fallback(recording_server: Any) -> None:
    server, base_url = recording_server
    server.routes[("POST", "/v1/queues/orders/tasks")] = (
        lambda body, headers: (
            400,
            _error(code="future_admission_denied", message="new code", retryable=True),
        )
    )
    with pytest.raises(ProtocolError) as exc_info:
        _client(base_url).enqueue(
            "orders", idempotency_key="k", payload={}, priority=0
        )
    err = exc_info.value
    assert err.code.value == "future_admission_denied"
    assert err.code.is_unknown is True
    assert isinstance(err.code, ErrorCode)
    assert err.retryable is True


def test_authentication_timeout_malformed_and_protocol_errors_are_distinguishable(
    recording_server: Any,
) -> None:
    server, base_url = recording_server

    server.routes[("GET", "/v1/tasks/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")] = (
        lambda body, headers: (401, _error(code="unauthenticated", message="nope"))
    )
    with pytest.raises(AuthenticationError) as auth_info:
        _client(base_url).inspect_task("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
    assert isinstance(auth_info.value, ProtocolError)
    assert isinstance(auth_info.value, QueueClientError)
    assert "secret-token-value" not in f"{auth_info.value!s}{auth_info.value!r}"

    server.routes[("GET", "/v1/tasks/cccccccc-cccc-4ccc-8ccc-cccccccccccc")] = (
        lambda body, headers: (200, {"task_id": "only-partial"})
    )
    with pytest.raises(MalformedResponseError) as malformed_info:
        _client(base_url).inspect_task("cccccccc-cccc-4ccc-8ccc-cccccccccccc")
    assert isinstance(malformed_info.value, QueueClientError)
    assert not isinstance(malformed_info.value, ProtocolError)
    text = f"{malformed_info.value!s}{malformed_info.value!r}"
    assert "secret-token-value" not in text

    with pytest.raises(ClientTimeoutError) as timeout_info:
        _client("http://127.0.0.1:1", timeout=0.05).inspect_task(
            "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
        )
    assert isinstance(timeout_info.value, QueueClientError)
    assert not isinstance(timeout_info.value, ProtocolError)
    assert "secret-token-value" not in f"{timeout_info.value!s}{timeout_info.value!r}"


def test_enqueue_omits_available_at_when_public_none(recording_server: Any) -> None:
    server, base_url = recording_server
    server.routes[("POST", "/v1/queues/orders/tasks")] = (
        lambda body, headers: (201, {"task": _task_body(), "replayed": False})
    )
    _client(base_url).enqueue(
        "orders",
        idempotency_key="idem-no-delay",
        payload={"x": 1},
        priority=0,
        available_at=None,
    )
    wire = json.loads(server.recorded[0]["body"].decode("utf-8"))
    assert "available_at" not in wire
    assert wire == {"payload": {"x": 1}, "priority": 0}


def test_enqueue_serializes_aware_datetime(recording_server: Any) -> None:
    server, base_url = recording_server
    server.routes[("POST", "/v1/queues/orders/tasks")] = (
        lambda body, headers: (201, {"task": _task_body(), "replayed": False})
    )
    when = datetime(2026, 9, 20, 12, 30, 0, tzinfo=UTC)
    _client(base_url).enqueue(
        "orders",
        idempotency_key="idem-delay",
        payload={"x": 1},
        priority=0,
        available_at=when,
    )
    wire = json.loads(server.recorded[0]["body"].decode("utf-8"))
    assert wire["available_at"] == "2026-09-20T12:30:00+00:00"


def test_enqueue_rejects_naive_datetime_before_transport(recording_server: Any) -> None:
    server, base_url = recording_server
    client = _client(base_url)
    naive = datetime(2026, 9, 20, 12, 30, 0)
    with pytest.raises(ValueError, match="timezone-aware"):
        client.enqueue(
            "orders",
            idempotency_key="idem-naive",
            payload={"x": 1},
            priority=0,
            available_at=naive,
        )
    assert server.recorded == []


def test_private_raw_enqueue_available_for_bridge_not_public_alias() -> None:
    assert not hasattr(producer_pkg, "_AVAILABLE_AT_OMITTED")
    client = ProducerClient(
        HttpJsonTransport("http://127.0.0.1:9", timeout_s=0.1),
        bearer_token="token",
    )
    assert hasattr(client, "_enqueue_with_available_at_raw")


def test_error_and_response_diagnostics_never_leak_secrets(
    recording_server: Any,
) -> None:
    server, base_url = recording_server
    secret = "super-secret-bearer-xyz"
    idem = "idem-leak-key-should-not-appear"
    payload = {"credit_card": "4111111111111111", "nested": {"token": "leak-me"}}

    server.routes[("POST", "/v1/queues/orders/tasks")] = (
        lambda body, headers: (
            500,
            _error(code="internal_error", message="boom", retryable=True),
        )
    )
    client = _client(base_url, token=secret)
    with pytest.raises(ProtocolError) as exc_info:
        client.enqueue(
            "orders",
            idempotency_key=idem,
            payload=payload,
            priority=0,
        )
    blob = (
        f"{exc_info.value!s}{exc_info.value!r}{exc_info.value.message}"
        f"{client!r}{client!s}"
    )
    assert secret not in blob
    assert idem not in blob
    assert "4111111111111111" not in blob
    assert "leak-me" not in blob
    assert "credit_card" not in blob


@pytest.mark.parametrize("priority", [PRIORITY_MIN, PRIORITY_MAX, 42, -100])
def test_enqueue_sends_exact_bounded_priority_on_wire(
    recording_server: Any,
    priority: int,
) -> None:
    server, base_url = recording_server
    server.routes[("POST", "/v1/queues/orders/tasks")] = (
        lambda body, headers: (
            201,
            {"task": _task_body(priority=priority), "replayed": False},
        )
    )
    _client(base_url).enqueue(
        "orders",
        idempotency_key=f"idem-priority-{priority}",
        payload={"x": 1},
        priority=priority,
    )
    wire = json.loads(server.recorded[0]["body"].decode("utf-8"))
    assert wire["priority"] == priority
    assert type(wire["priority"]) is int


@pytest.mark.parametrize(
    "priority",
    [
        PRIORITY_MIN - 1,
        PRIORITY_MAX + 1,
        True,
        False,
        "100",
        1.5,
        float(PRIORITY_MAX),
        None,
    ],
)
def test_enqueue_rejects_invalid_priority_before_transport(
    recording_server: Any,
    priority: object,
) -> None:
    server, base_url = recording_server
    client = _client(base_url)
    with pytest.raises((ValueError, TypeError)):
        client.enqueue(
            "orders",
            idempotency_key="idem-invalid-priority",
            payload={"x": 1},
            priority=priority,  # type: ignore[arg-type]
        )
    assert server.recorded == []


def test_enqueue_default_zero_remains_source_compatible(recording_server: Any) -> None:
    server, base_url = recording_server
    server.routes[("POST", "/v1/queues/orders/tasks")] = (
        lambda body, headers: (201, {"task": _task_body(), "replayed": False})
    )
    _client(base_url).enqueue(
        "orders",
        idempotency_key="idem-default-priority",
        payload={"x": 1},
    )
    wire = json.loads(server.recorded[0]["body"].decode("utf-8"))
    assert wire["priority"] == 0


def test_path_encoding_for_queue_and_task_ids(recording_server: Any) -> None:
    server, base_url = recording_server
    # Handler looks up routes after unquote; wire path on the wire stays encoded.
    server.routes[("POST", "/v1/queues/a/b/tasks")] = (
        lambda body, headers: (
            201,
            {"task": _task_body(queue_name="a/b"), "replayed": False},
        )
    )
    server.routes[("GET", "/v1/tasks/id with space")] = (
        lambda body, headers: (200, _task_body(task_id="id with space"))
    )
    _client(base_url).enqueue(
        "a/b",
        idempotency_key="idem-enc",
        payload={},
    )
    _client(base_url).inspect_task("id with space")
    assert server.recorded[0]["path"] == "/v1/queues/a%2Fb/tasks"
    assert server.recorded[1]["path"] == "/v1/tasks/id%20with%20space"
