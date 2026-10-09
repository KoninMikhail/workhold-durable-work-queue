"""ConsumerClient recording-server tests (SDK-06 / SDK-08).

Contracts follow OpenAPI ``claimTasks``, ``heartbeatClaim``, ``completeClaim``,
``failClaim``, ``acknowledgeClaimCancellation``, and ``getCapabilities``.
Claim tokens must never appear in URL/query/repr/errors; lease loss latches
locally; empty claim is ``[]``; terminal body replay is fingerprint-gated.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any
from urllib.parse import unquote

import pytest

from _workhold_client_core.errors import (
    LeaseLostError,
    ProtocolError,
    TerminalConflictError,
)
from _workhold_client_core.capabilities import Capabilities
from _workhold_client_core.transport import HttpJsonTransport
import workhold_consumer as consumer_pkg
from workhold_consumer import Claim, ConsumerClient, ConsumerSupervisor
from tests.fixtures.claim_long_poll import (
    CLAIM_TOKEN_SENTINEL,
    assert_no_forbidden_diagnostics,
    delayed_empty_claim_responder,
    long_poll_recording_server,  # noqa: F401  — pytest fixture via module import
)


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
                "raw_path": self.path,
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
                    "code": "claim_not_found",
                    "message": "missing route",
                    "retryable": False,
                    "request_id": "00000000-0000-4000-8000-000000000099",
                    "details": {},
                },
            )
            return
        status, payload = responder(
            body, {k.lower(): v for k, v in self.headers.items()}
        )
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


CLAIM_ID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
CLAIM_TOKEN = "claim-token-secret-value-do-not-leak"
TASK_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
WORKER_BEARER = "worker-bearer-secret"


def _task_body(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "task_id": TASK_ID,
        "queue_name": "orders",
        "producer_id": "producer-1",
        "state": "leased",
        "priority": 0,
        "available_at": "2026-09-19T00:00:00Z",
        "retry_policy_version": 1,
        "created_at": "2026-09-19T00:00:00Z",
        "spawned_task_ids": [],
        "delivery_event_ids": [],
        "payload": {"order_id": 7},
    }
    body.update(overrides)
    return body


def _claim_grant(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "claim_id": CLAIM_ID,
        "generation": 3,
        "claimed_at": "2026-09-19T00:01:00Z",
        "lease_expires_at": "2026-09-19T00:02:00Z",
        "worker_id": "worker-1",
        "cancel_requested": False,
        "claim_token": CLAIM_TOKEN,
    }
    body.update(overrides)
    return body


def _claim_summary(**overrides: Any) -> dict[str, Any]:
    grant = _claim_grant(**overrides)
    grant.pop("claim_token", None)
    return grant


def _claim_response(*, empty: bool = False) -> dict[str, Any]:
    tasks: list[dict[str, Any]] = []
    if not empty:
        tasks.append({"task": _task_body(), "claim": _claim_grant()})
    return {
        "tasks": tasks,
        "server_time": "2026-09-19T00:01:00Z",
        "recommended_heartbeat_seconds": 10,
        "queue_states": {"orders": "active"},
    }


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
) -> dict[str, Any]:
    return {
        "code": code,
        "message": message,
        "retryable": retryable,
        "request_id": "22222222-2222-4222-8222-222222222222",
        "details": {} if details is None else details,
    }


def _client(base_url: str, *, token: str = WORKER_BEARER) -> ConsumerClient:
    transport = HttpJsonTransport(base_url, timeout_s=2.0)
    return ConsumerClient(transport, bearer_token=token)


@pytest.mark.parametrize("token", [None, "", "   ", "\t", "\n"])
def test_whitespace_bearer_token_rejected(token: str | None) -> None:
    transport = HttpJsonTransport("http://127.0.0.1:9", timeout_s=0.1)
    with pytest.raises(ValueError, match="bearer_token"):
        ConsumerClient(transport, bearer_token=token)  # type: ignore[arg-type]


def test_bearer_token_preserved_exactly() -> None:
    transport = HttpJsonTransport("http://127.0.0.1:9", timeout_s=0.1)
    exact = " tok en "
    client = ConsumerClient(transport, bearer_token=exact)
    assert client._auth_headers()["Authorization"] == f"Bearer {exact}"


def test_package_exports_consumer_surface_only() -> None:
    assert "ConsumerClient" in consumer_pkg.__all__
    assert "Claim" in consumer_pkg.__all__
    assert "ConsumerSupervisor" in consumer_pkg.__all__
    assert consumer_pkg.ConsumerClient is ConsumerClient
    assert consumer_pkg.Claim is Claim
    assert consumer_pkg.ConsumerSupervisor is ConsumerSupervisor
    for forbidden in (
        "ProducerClient",
        "ObserverClient",
        "AdminClient",
        "BreakGlassClient",
        "WorkerClient",
        "WorkerSupervisor",
    ):
        assert forbidden not in consumer_pkg.__all__
        assert not hasattr(consumer_pkg, forbidden)
    client = ConsumerClient(
        HttpJsonTransport("http://127.0.0.1:9", timeout_s=0.1),
        bearer_token="token",
    )
    for forbidden_method in (
        "enqueue",
        "resolve_submission",
        "inspect_task",
        "cancel_task",
        "list_queues",
        "create_queue",
    ):
        assert not hasattr(client, forbidden_method)


def test_get_capabilities_uses_openapi_contract(recording_server: Any) -> None:
    server, base_url = recording_server
    server.routes[("GET", "/v1/capabilities")] = (
        lambda body, headers: (200, _capabilities_body())
    )
    caps = _client(base_url).get_capabilities()
    assert caps.protocol_major == 1
    assert caps.max_claim_tasks == 1
    assert len(server.recorded) == 1
    rec = server.recorded[0]
    assert rec["method"] == "GET"
    assert rec["path"] == "/v1/capabilities"
    assert rec["headers"]["authorization"] == f"Bearer {WORKER_BEARER}"
    assert WORKER_BEARER not in repr(_client(base_url))


def test_empty_claim_returns_empty_list_not_error(recording_server: Any) -> None:
    server, base_url = recording_server
    server.routes[("POST", "/v1/claims")] = (
        lambda body, headers: (200, _claim_response(empty=True))
    )
    claims = _client(base_url).claim(
        queues=["orders"],
        worker_id="worker-1",
        lease_seconds=60,
    )
    assert claims == []
    assert len(server.recorded) == 1
    rec = server.recorded[0]
    assert rec["method"] == "POST"
    assert rec["path"] == "/v1/claims"
    assert rec["headers"]["authorization"] == f"Bearer {WORKER_BEARER}"
    assert json.loads(rec["body"].decode("utf-8")) == {
        "queues": ["orders"],
        "max_tasks": 1,
        "lease_seconds": 60,
        "wait_seconds": 0,
        "worker_id": "worker-1",
    }


def test_successful_claim_retains_openapi_metadata(recording_server: Any) -> None:
    server, base_url = recording_server
    server.routes[("POST", "/v1/claims")] = (
        lambda body, headers: (200, _claim_response())
    )
    claims = _client(base_url).claim(
        queues=["orders"],
        worker_id="worker-1",
        lease_seconds=60,
    )
    assert len(claims) == 1
    claim = claims[0]
    assert isinstance(claim, Claim)
    assert claim.claim_id == CLAIM_ID
    assert claim.generation == 3
    assert claim.claimed_at == "2026-09-19T00:01:00Z"
    assert claim.lease_expires_at == "2026-09-19T00:02:00Z"
    assert claim.worker_id == "worker-1"
    assert claim.cancel_requested is False
    assert claim.server_time == "2026-09-19T00:01:00Z"
    assert claim.recommended_heartbeat_seconds == 10
    assert claim.task.task_id == TASK_ID
    assert claim.task.payload == {"order_id": 7}
    assert claim.lease_lost is False
    assert CLAIM_TOKEN not in repr(claim)
    assert CLAIM_TOKEN not in str(claim)


def test_lease_requests_put_claim_id_in_path_and_token_only_in_header(
    recording_server: Any,
) -> None:
    server, base_url = recording_server
    server.routes[("POST", "/v1/claims")] = (
        lambda body, headers: (200, _claim_response())
    )
    server.routes[("POST", f"/v1/claims/{CLAIM_ID}:heartbeat")] = (
        lambda body, headers: (
            200,
            {
                "claim": _claim_summary(
                    lease_expires_at="2026-09-19T00:03:00Z",
                    cancel_requested=True,
                ),
                "server_time": "2026-09-19T00:02:00Z",
                "recommended_heartbeat_seconds": 8,
            },
        )
    )
    server.routes[("POST", f"/v1/claims/{CLAIM_ID}:complete")] = (
        lambda body, headers: (
            200,
            {
                "task_id": TASK_ID,
                "state": "succeeded",
                "spawned_task_ids": ["cccccccc-cccc-4ccc-8ccc-cccccccccccc"],
                "replayed": False,
            },
        )
    )
    server.routes[("POST", f"/v1/claims/{CLAIM_ID}:fail")] = (
        lambda body, headers: (
            200,
            {
                "task_id": TASK_ID,
                "state": "retry_scheduled",
                "available_at": "2026-09-19T00:05:00Z",
                "replayed": False,
            },
        )
    )
    server.routes[("POST", f"/v1/claims/{CLAIM_ID}:ack-cancel")] = (
        lambda body, headers: (
            200,
            {
                "task_id": TASK_ID,
                "state": "cancelled",
                "terminal_at": "2026-09-19T00:04:00Z",
                "replayed": False,
            },
        )
    )

    client = _client(base_url)
    claim = client.claim(queues=["orders"], worker_id="worker-1", lease_seconds=60)[0]

    hb = claim.heartbeat(lease_seconds=60)
    assert claim.cancel_requested is True
    assert claim.lease_expires_at == "2026-09-19T00:03:00Z"
    assert claim.recommended_heartbeat_seconds == 8
    assert claim.server_time == "2026-09-19T00:02:00Z"
    assert hb.recommended_heartbeat_seconds == 8
    assert json.loads(server.recorded[-1]["body"].decode("utf-8")) == {
        "generation": 3,
        "lease_seconds": 60,
    }

    claim2 = client.claim(queues=["orders"], worker_id="worker-1", lease_seconds=60)[0]
    complete = claim2.complete(
        spawn=[
            {
                "queue_name": "orders",
                "idempotency_key": "spawn-1",
                "payload": {"n": 1},
                "priority": 0,
            }
        ]
    )
    assert complete.spawned_task_ids == ("cccccccc-cccc-4ccc-8ccc-cccccccccccc",)
    assert complete.replayed is False
    assert complete.state.value == "succeeded"
    assert json.loads(server.recorded[-1]["body"].decode("utf-8")) == {
        "generation": 3,
        "spawn": [
            {
                "queue_name": "orders",
                "idempotency_key": "spawn-1",
                "payload": {"n": 1},
                "priority": 0,
            }
        ],
    }

    claim3 = client.claim(queues=["orders"], worker_id="worker-1", lease_seconds=60)[0]
    fail = claim3.fail(
        retryable=True,
        failure_code="TransientError",
        failure_detail="boom",
    )
    assert fail.state.value == "retry_scheduled"
    assert fail.available_at == "2026-09-19T00:05:00Z"
    assert fail.replayed is False
    assert json.loads(server.recorded[-1]["body"].decode("utf-8")) == {
        "generation": 3,
        "retryable": True,
        "failure_code": "TransientError",
        "failure_detail": "boom",
    }

    claim4 = client.claim(queues=["orders"], worker_id="worker-1", lease_seconds=60)[0]
    ack = claim4.ack_cancel()
    assert ack.state.value == "cancelled"
    assert ack.terminal_at == "2026-09-19T00:04:00Z"
    assert ack.replayed is False
    assert json.loads(server.recorded[-1]["body"].decode("utf-8")) == {"generation": 3}

    lease_ops = [r for r in server.recorded if r["path"] != "/v1/claims"]
    assert len(lease_ops) == 4
    for rec in lease_ops:
        assert CLAIM_ID in rec["path"]
        assert CLAIM_TOKEN not in rec["path"]
        assert CLAIM_TOKEN not in rec["raw_path"]
        assert rec["headers"].get("x-queue-claim-token") == CLAIM_TOKEN
        assert rec["headers"]["authorization"] == f"Bearer {WORKER_BEARER}"
        body_text = rec["body"].decode("utf-8") if rec["body"] else ""
        assert CLAIM_TOKEN not in body_text
        assert "claim_token" not in body_text


def test_lease_lost_latches_and_blocks_later_mutations_without_http(
    recording_server: Any,
) -> None:
    server, base_url = recording_server
    server.routes[("POST", "/v1/claims")] = (
        lambda body, headers: (200, _claim_response())
    )
    server.routes[("POST", f"/v1/claims/{CLAIM_ID}:heartbeat")] = (
        lambda body, headers: (409, _error(code="lease_lost", message="fence lost"))
    )
    claim = _client(base_url).claim(
        queues=["orders"], worker_id="worker-1", lease_seconds=60
    )[0]
    with pytest.raises(ProtocolError) as exc_info:
        claim.heartbeat(lease_seconds=60)
    assert exc_info.value.code.value == "lease_lost"
    assert claim.lease_lost is True
    assert CLAIM_TOKEN not in f"{exc_info.value!s}{exc_info.value!r}"

    before = len(server.recorded)
    with pytest.raises(LeaseLostError):
        claim.heartbeat(lease_seconds=60)
    with pytest.raises(LeaseLostError):
        claim.complete(spawn=[])
    with pytest.raises(LeaseLostError):
        claim.fail(retryable=False, failure_code="HardFail")
    with pytest.raises(LeaseLostError):
        claim.ack_cancel()
    assert len(server.recorded) == before


def test_same_body_terminal_replay_succeeds_changed_body_conflicts(
    recording_server: Any,
) -> None:
    server, base_url = recording_server
    complete_calls = {"n": 0}

    def complete_responder(
        body: bytes, headers: dict[str, str]
    ) -> tuple[int, dict[str, Any]]:
        complete_calls["n"] += 1
        return (
            200,
            {
                "task_id": TASK_ID,
                "state": "succeeded",
                "spawned_task_ids": [],
                "replayed": complete_calls["n"] > 1,
            },
        )

    server.routes[("POST", "/v1/claims")] = (
        lambda body, headers: (200, _claim_response())
    )
    server.routes[("POST", f"/v1/claims/{CLAIM_ID}:complete")] = complete_responder

    claim = _client(base_url).claim(
        queues=["orders"], worker_id="worker-1", lease_seconds=60
    )[0]
    first = claim.complete(spawn=[])
    assert first.replayed is False
    second = claim.complete(spawn=[])
    assert second.replayed is True
    assert complete_calls["n"] == 2

    before = len(server.recorded)
    with pytest.raises(TerminalConflictError):
        claim.complete(
            spawn=[
                {
                    "queue_name": "orders",
                    "idempotency_key": "other",
                    "payload": {},
                    "priority": 0,
                }
            ]
        )
    with pytest.raises(TerminalConflictError):
        claim.fail(retryable=False, failure_code="Other")
    assert len(server.recorded) == before


def test_claim_token_absent_from_url_repr_and_exceptions(recording_server: Any) -> None:
    server, base_url = recording_server
    server.routes[("POST", "/v1/claims")] = (
        lambda body, headers: (200, _claim_response())
    )
    server.routes[("POST", f"/v1/claims/{CLAIM_ID}:fail")] = (
        lambda body, headers: (409, _error(code="lease_lost", message="fence lost"))
    )
    claim = _client(base_url).claim(
        queues=["orders"], worker_id="worker-1", lease_seconds=60
    )[0]
    assert CLAIM_TOKEN not in f"{claim!r}{claim!s}"
    with pytest.raises(ProtocolError) as exc_info:
        claim.fail(retryable=False, failure_code="X")
    assert CLAIM_TOKEN not in f"{exc_info.value!s}{exc_info.value!r}"
    assert CLAIM_TOKEN not in server.recorded[-1]["path"]
    assert CLAIM_TOKEN not in server.recorded[-1]["raw_path"]

@pytest.mark.parametrize(
    ("max_tasks", "match"),
    [
        (True, "integer"),
        (1.0, "integer"),
        ("1", "integer"),
        (0, "max_tasks=1"),
        (-1, "max_tasks=1"),
        (2, "max_tasks=1"),
    ],
)
def test_claim_rejects_invalid_max_tasks_before_network(
    recording_server: Any,
    max_tasks: object,
    match: str,
) -> None:
    server, base_url = recording_server
    server.routes[("GET", "/v1/capabilities")] = (
        lambda body, headers: (
            200,
            _capabilities_body(batch_claim=True, max_claim_tasks=8),
        )
    )
    server.routes[("POST", "/v1/claims")] = (
        lambda body, headers: (200, _claim_response())
    )
    with pytest.raises(ValueError, match=match):
        _client(base_url).claim(
            queues=["orders"],
            worker_id="worker-1",
            lease_seconds=60,
            max_tasks=max_tasks,  # type: ignore[arg-type]
            capabilities=Capabilities.parse(
                _capabilities_body(batch_claim=True, max_claim_tasks=8)
            ),
        )
    assert server.recorded == []


def test_claim_sends_wait_seconds_when_capability_allows(recording_server: Any) -> None:
    from _workhold_client_core.config import ClientConfig
    from _workhold_client_core.errors import RequestCancelledError  # noqa: F401

    server, base_url = recording_server
    caps = _capabilities_body(long_polling=True, max_wait_seconds=20)
    server.routes[("GET", "/v1/capabilities")] = lambda body, headers: (200, caps)
    server.routes[("POST", "/v1/claims")] = (
        lambda body, headers: (200, _claim_response(empty=True))
    )
    transport = HttpJsonTransport.from_config(
        ClientConfig.for_public(base_url, read_timeout_s=30.0, total_timeout_s=30.0)
    )
    client = ConsumerClient(transport, bearer_token=WORKER_BEARER)
    claims = client.claim(
        queues=["orders"],
        worker_id="worker-1",
        lease_seconds=60,
        wait_seconds=15,
        capabilities=Capabilities.parse(caps),
    )
    assert claims == []
    body = json.loads(server.recorded[-1]["body"].decode("utf-8"))
    assert body["wait_seconds"] == 15
    assert body["max_tasks"] == 1


def test_positive_wait_fails_closed_without_capability(recording_server: Any) -> None:
    server, base_url = recording_server
    caps = _capabilities_body(long_polling=False, max_wait_seconds=0)
    client = _client(base_url)
    with pytest.raises(ValueError, match="long_polling"):
        client.claim(
            queues=["orders"],
            worker_id="worker-1",
            lease_seconds=60,
            wait_seconds=15,
            capabilities=Capabilities.parse(caps),
        )
    assert server.recorded == []


def test_undersized_transport_budget_fails_before_http(recording_server: Any) -> None:
    from _workhold_client_core.config import ClientConfig

    server, base_url = recording_server
    caps = _capabilities_body(long_polling=True, max_wait_seconds=20)
    transport = HttpJsonTransport.from_config(
        ClientConfig.for_public(base_url, read_timeout_s=10.0, total_timeout_s=10.0)
    )
    client = ConsumerClient(transport, bearer_token=WORKER_BEARER)
    with pytest.raises(ValueError, match="read_timeout_s"):
        client.claim(
            queues=["orders"],
            worker_id="worker-1",
            lease_seconds=60,
            wait_seconds=15,
            capabilities=Capabilities.parse(caps),
        )
    assert server.recorded == []


def test_empty_long_poll_expiry_is_success_not_timeout(
    long_poll_recording_server: Any,
) -> None:
    from _workhold_client_core.config import ClientConfig

    release = threading.Event()
    long_poll_recording_server.set_route(
        "POST",
        "/v1/claims",
        delayed_empty_claim_responder(delay_seconds=2.0, release=release),
    )
    caps = _capabilities_body(long_polling=True, max_wait_seconds=20)
    transport = HttpJsonTransport.from_config(
        ClientConfig.for_public(
            long_poll_recording_server.base_url,
            read_timeout_s=30.0,
            total_timeout_s=30.0,
        )
    )
    client = ConsumerClient(transport, bearer_token=WORKER_BEARER)
    release.set()
    claims = client.claim(
        queues=["orders"],
        worker_id="worker-1",
        lease_seconds=60,
        wait_seconds=5,
        capabilities=Capabilities.parse(caps),
    )
    assert claims == []


def test_transport_timeout_distinct_from_empty(
    long_poll_recording_server: Any,
) -> None:
    from _workhold_client_core.errors import TimeoutError as ClientTimeoutError

    hold = threading.Event()
    long_poll_recording_server.set_route(
        "POST",
        "/v1/claims",
        delayed_empty_claim_responder(delay_seconds=30.0, release=hold),
    )
    transport_fast = HttpJsonTransport(
        long_poll_recording_server.base_url, timeout_s=0.2
    )
    client = ConsumerClient(transport_fast, bearer_token=WORKER_BEARER)
    with pytest.raises(ClientTimeoutError) as exc_info:
        client.claim(queues=["orders"], worker_id="worker-1", lease_seconds=60)
    assert "TimeoutError" in repr(exc_info.value)
    assert WORKER_BEARER not in repr(exc_info.value)
    hold.set()


def test_sync_cancellation_closes_outstanding_request(
    long_poll_recording_server: Any,
) -> None:
    from _workhold_client_core.config import ClientConfig
    from _workhold_client_core.errors import RequestCancelledError

    hold = threading.Event()
    entered = threading.Event()

    def responder(body: bytes, headers: dict[str, str]):
        entered.set()
        return delayed_empty_claim_responder(delay_seconds=30.0, release=hold)(
            body, headers
        )

    long_poll_recording_server.set_route("POST", "/v1/claims", responder)
    caps = _capabilities_body(long_polling=True, max_wait_seconds=20)
    transport = HttpJsonTransport.from_config(
        ClientConfig.for_public(
            long_poll_recording_server.base_url,
            read_timeout_s=30.0,
            total_timeout_s=30.0,
        )
    )
    client = ConsumerClient(transport, bearer_token=WORKER_BEARER)
    cancel = threading.Event()
    errors: list[BaseException] = []

    def _run() -> None:
        try:
            client.claim(
                queues=["orders"],
                worker_id="worker-1",
                lease_seconds=60,
                wait_seconds=15,
                capabilities=Capabilities.parse(caps),
                cancellation=cancel,
            )
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    assert entered.wait(timeout=2.0)
    cancel.set()
    transport.cancel_active()
    thread.join(timeout=3.0)
    assert not thread.is_alive()
    assert len(errors) == 1
    assert isinstance(errors[0], RequestCancelledError)
    assert_no_forbidden_diagnostics(repr(errors[0]))
    assert CLAIM_TOKEN_SENTINEL not in repr(errors[0])
    hold.set()


def test_surplus_tasks_raises_malformed_response(recording_server: Any) -> None:
    from _workhold_client_core.errors import MalformedResponseError

    server, base_url = recording_server
    payload = _claim_response()
    payload["tasks"].append({"task": _task_body(), "claim": _claim_grant()})
    server.routes[("POST", "/v1/claims")] = lambda body, headers: (200, payload)

    with pytest.raises(MalformedResponseError) as exc_info:
        _client(base_url).claim(
            queues=["orders"],
            worker_id="worker-1",
            lease_seconds=60,
        )
    assert "2 tasks" in str(exc_info.value)
    assert "max allowed is 1" in str(exc_info.value)


def test_invalid_queue_states_raises_malformed_response(recording_server: Any) -> None:
    from _workhold_client_core.errors import MalformedResponseError

    server, base_url = recording_server
    payload = _claim_response()
    payload["queue_states"] = []
    server.routes[("POST", "/v1/claims")] = lambda body, headers: (200, payload)

    with pytest.raises(MalformedResponseError) as exc_info:
        _client(base_url).claim(
            queues=["orders"],
            worker_id="worker-1",
            lease_seconds=60,
        )
    assert "queue_states must be an object" in str(exc_info.value)


def _fake_claim_then_complete_transport(
    *,
    complete_bodies: list[dict[str, Any]],
) -> Any:
    """Minimal sync transport: one claim grant, then record complete bodies."""

    class _Transport:
        def request(self, method: str, path: str, **kwargs: Any) -> Any:
            if path == "/v1/claims":

                class _ClaimResp:
                    status_code = 200
                    body = _claim_response()

                return _ClaimResp()
            if path.endswith(":complete"):
                complete_bodies.append(kwargs["json_body"])

                class _CompleteResp:
                    status_code = 200
                    body = {
                        "task_id": TASK_ID,
                        "state": "succeeded",
                        "spawned_task_ids": [],
                        "replayed": False,
                    }

                return _CompleteResp()
            raise AssertionError(f"unexpected path {path}")

    return _Transport()


def test_complete_explicit_max_payload_bytes_rejects_oversize_without_encoder() -> None:
    from _workhold_client_core.codecs import measure_json_bytes

    complete_bodies: list[dict[str, Any]] = []
    client = ConsumerClient(
        _fake_claim_then_complete_transport(complete_bodies=complete_bodies),
        bearer_token=WORKER_BEARER,
    )
    claim = client.claim(queues=["orders"], worker_id="worker-1", lease_seconds=60)[0]
    payload = {"blob": "x" * 64}
    tiny = measure_json_bytes(payload) - 1
    with pytest.raises(ValueError, match="payload exceeds"):
        claim.complete(
            spawn=[
                {
                    "queue_name": "orders",
                    "idempotency_key": "spawn-oversized",
                    "payload": payload,
                    "priority": 0,
                }
            ],
            max_payload_bytes=tiny,
        )
    assert complete_bodies == []


def test_complete_explicit_max_payload_bytes_keeps_wire_when_within_limit() -> None:
    complete_bodies: list[dict[str, Any]] = []
    client = ConsumerClient(
        _fake_claim_then_complete_transport(complete_bodies=complete_bodies),
        bearer_token=WORKER_BEARER,
    )
    claim = client.claim(queues=["orders"], worker_id="worker-1", lease_seconds=60)[0]
    payload = {"n": 1, "tag": "ok"}
    claim.complete(
        spawn=[
            {
                "queue_name": "orders",
                "idempotency_key": "spawn-ok",
                "payload": payload,
                "priority": 0,
            }
        ],
        max_payload_bytes=1024,
    )
    assert complete_bodies[0]["spawn"][0]["payload"] is payload
    assert complete_bodies[0]["spawn"][0]["payload"] == {"n": 1, "tag": "ok"}
