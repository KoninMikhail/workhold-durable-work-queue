"""AdminClient recording-server tests for queue, policy and state control (SDK-07/08)."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

import pytest

from _workhold_client_core.errors import (
    AuthenticationError,
    MalformedResponseError,
    ProtocolError,
    QueueClientError,
)
from _workhold_client_core.transport import HttpJsonTransport
import workhold_admin as admin_pkg
from workhold_admin import AdminClient, ObserverClient
from workhold_admin.models import (
    BackoffStrategy,
    ConfigVersion,
    PolicyVersion,
    QueueState,
    RetryPolicyDraft,
)


class _RecordingHandler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return

    def _record(self, body: bytes) -> None:
        parsed = urlparse(self.path)
        headers = {k.lower(): v for k, v in self.headers.items()}
        self.server.recorded.append(  # type: ignore[attr-defined]
            {
                "method": self.command,
                "path": parsed.path,
                "query": parsed.query,
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
        path = unquote(urlparse(self.path).path)
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


def _policy_draft(**overrides: Any) -> RetryPolicyDraft:
    defaults = {
        "enabled": True,
        "max_attempts": 3,
        "backoff_strategy": BackoffStrategy("fixed"),
        "retry_delay_seconds": 5,
    }
    defaults.update(overrides)
    return RetryPolicyDraft(**defaults)


def _queue_body(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "queue_id": "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
        "name": "orders",
        "state": "active",
        "config_version": 1,
        "active_policy": {
            "version": 1,
            "enabled": True,
            "max_attempts": 3,
            "backoff_strategy": "fixed",
            "retry_delay_seconds": 5,
            "created_at": "2026-09-19T00:00:00Z",
        },
        "created_at": "2026-09-19T00:00:00Z",
        "updated_at": "2026-09-19T00:00:00Z",
    }
    body.update(overrides)
    return body


def _mutation_result(**queue_overrides: Any) -> dict[str, Any]:
    return {
        "queue": _queue_body(**queue_overrides),
        "replayed": False,
        "admin_replay_expires_at": "2026-10-19T00:00:00Z",
    }


def _client(base_url: str, *, token: str = "admin-secret-token") -> AdminClient:
    transport = HttpJsonTransport(base_url, timeout_s=2.0)
    return AdminClient(transport, bearer_token=token)


def _query_dict(query: str) -> dict[str, list[str]]:
    return parse_qs(query, keep_blank_values=True)


def test_package_exports_admin_queue_control_surface() -> None:
    assert "AdminClient" in admin_pkg.__all__
    assert admin_pkg.AdminClient is AdminClient
    client = _client("http://127.0.0.1:9")
    for method in (
        "list_queues",
        "create_queue",
        "get_queue",
        "create_queue_policy",
        "activate_queue_policy",
        "set_queue_state",
    ):
        assert hasattr(client, method)


def test_observer_has_no_admin_mutations() -> None:
    public = HttpJsonTransport("http://127.0.0.1:9", timeout_s=0.1)
    admin = HttpJsonTransport("http://127.0.0.1:9", timeout_s=0.1)
    observer = ObserverClient(public, bearer_token="token", admin_transport=admin)
    for forbidden in (
        "create_queue",
        "create_queue_policy",
        "activate_queue_policy",
        "set_queue_state",
        "list_queues",
    ):
        assert not hasattr(observer, forbidden)


def test_list_queues_query_contract(recording_server: Any) -> None:
    server, base_url = recording_server
    server.routes[("GET", "/admin/v1/queues")] = (
        lambda body, headers: (
            200,
            {"items": [_queue_body()], "next_cursor": "cursor-1"},
        )
    )
    page = _client(base_url).list_queues(cursor="cursor-0", limit=25)
    assert len(page.items) == 1
    assert page.items[0].name == "orders"
    assert page.next_cursor == "cursor-1"
    rec = server.recorded[0]
    assert rec["method"] == "GET"
    assert rec["path"] == "/admin/v1/queues"
    q = _query_dict(rec["query"])
    assert q["limit"] == ["25"]
    assert q["cursor"] == ["cursor-0"]
    assert rec["headers"]["authorization"] == "Bearer admin-secret-token"


def test_create_queue_wire_contract(recording_server: Any) -> None:
    server, base_url = recording_server
    server.routes[("POST", "/admin/v1/queues")] = (
        lambda body, headers: (200, _mutation_result())
    )
    policy = _policy_draft(enabled=False, max_attempts=1, retry_delay_seconds=0)
    result = _client(base_url).create_queue(
        "orders.v2",
        initial_policy=policy,
        idempotency_key="idem-create-1",
    )
    assert result.replayed is False
    assert result.queue.name == "orders"
    rec = server.recorded[0]
    assert rec["path"] == "/admin/v1/queues"
    assert rec["headers"]["idempotency-key"] == "idem-create-1"
    wire = json.loads(rec["body"].decode("utf-8"))
    assert wire == {
        "name": "orders.v2",
        "initial_policy": {
            "enabled": False,
            "max_attempts": 1,
            "backoff_strategy": "fixed",
            "retry_delay_seconds": 0,
        },
    }


def test_get_queue_uses_admin_transport(recording_server: Any) -> None:
    server, base_url = recording_server
    server.routes[("GET", "/admin/v1/queues/orders")] = (
        lambda body, headers: (200, _queue_body(config_version=2))
    )
    queue = _client(base_url).get_queue("orders")
    assert queue.config_version == 2
    assert server.recorded[0]["path"] == "/admin/v1/queues/orders"


def test_create_queue_policy_wire_contract(recording_server: Any) -> None:
    server, base_url = recording_server
    server.routes[("POST", "/admin/v1/queues/orders/policies")] = (
        lambda body, headers: (200, _mutation_result())
    )
    policy = _policy_draft(enabled=False, max_attempts=2, retry_delay_seconds=3)
    _client(base_url).create_queue_policy(
        "orders",
        policy,
        idempotency_key="idem-policy-1",
    )
    rec = server.recorded[0]
    assert rec["path"] == "/admin/v1/queues/orders/policies"
    assert rec["headers"]["idempotency-key"] == "idem-policy-1"
    assert json.loads(rec["body"].decode("utf-8")) == policy.to_wire()


def test_activate_queue_policy_wire_contract(recording_server: Any) -> None:
    server, base_url = recording_server
    server.routes[("POST", "/admin/v1/queues/orders/policies/2:activate")] = (
        lambda body, headers: (
            200,
            _mutation_result(config_version=2, active_policy=_queue_body()["active_policy"]),
        )
    )
    result = _client(base_url).activate_queue_policy(
        "orders",
        PolicyVersion(2),
        expected_config_version=ConfigVersion(1),
        idempotency_key="idem-activate-1",
    )
    assert result.queue.config_version == 2
    rec = server.recorded[0]
    assert rec["path"] == "/admin/v1/queues/orders/policies/2:activate"
    assert rec["headers"]["idempotency-key"] == "idem-activate-1"
    assert json.loads(rec["body"].decode("utf-8")) == {"expected_config_version": 1}


def test_set_queue_state_wire_contract(recording_server: Any) -> None:
    server, base_url = recording_server
    server.routes[("POST", "/admin/v1/queues/orders:set-state")] = (
        lambda body, headers: (
            200,
            _mutation_result(state="paused", config_version=2),
        )
    )
    result = _client(base_url).set_queue_state(
        "orders",
        QueueState("paused"),
        expected_config_version=1,
        idempotency_key="idem-pause-1",
    )
    assert result.queue.state.value == "paused"
    rec = server.recorded[0]
    assert rec["path"] == "/admin/v1/queues/orders:set-state"
    assert json.loads(rec["body"].decode("utf-8")) == {
        "expected_config_version": 1,
        "state": "paused",
    }


def test_stale_config_version_conflict_is_visible_without_retry(
    recording_server: Any,
) -> None:
    server, base_url = recording_server
    server.routes[("POST", "/admin/v1/queues/orders:set-state")] = (
        lambda body, headers: (
            412,
            {
                "code": "config_version_conflict",
                "message": "stale config version",
                "retryable": True,
                "request_id": "55555555-5555-4555-8555-555555555555",
                "details": {"expected": 1, "actual": 3},
            },
        )
    )
    client = _client(base_url)
    with pytest.raises(ProtocolError) as exc_info:
        client.set_queue_state(
            "orders",
            "paused",
            expected_config_version=1,
            idempotency_key="idem-stale",
        )
    assert exc_info.value.code.value == "config_version_conflict"
    assert exc_info.value.status_code == 412
    assert exc_info.value.retryable is True
    assert len(server.recorded) == 1


def test_activate_stale_conflict_is_visible(recording_server: Any) -> None:
    server, base_url = recording_server
    server.routes[("POST", "/admin/v1/queues/orders/policies/2:activate")] = (
        lambda body, headers: (
            412,
            {
                "code": "config_version_conflict",
                "message": "stale",
                "retryable": True,
                "request_id": "66666666-6666-4666-8666-666666666666",
                "details": {},
            },
        )
    )
    with pytest.raises(ProtocolError) as exc_info:
        _client(base_url).activate_queue_policy(
            "orders",
            2,
            expected_config_version=99,
            idempotency_key="idem-stale-act",
        )
    assert exc_info.value.code.value == "config_version_conflict"


@pytest.mark.parametrize("limit", [0, 101, True])
def test_page_limit_rejected_before_transport(
    recording_server: Any,
    limit: object,
) -> None:
    server, base_url = recording_server
    with pytest.raises(ValueError):
        _client(base_url).list_queues(limit=limit)  # type: ignore[arg-type]
    assert server.recorded == []


def test_idempotency_key_rejected_before_transport(recording_server: Any) -> None:
    server, base_url = recording_server
    policy = _policy_draft()
    with pytest.raises(ValueError, match="non-empty"):
        _client(base_url).create_queue("orders", initial_policy=policy, idempotency_key="")
    with pytest.raises(ValueError, match="256"):
        _client(base_url).create_queue(
            "orders",
            initial_policy=policy,
            idempotency_key="x" * 257,
        )
    assert server.recorded == []


def test_unknown_queue_state_rejected_before_transport(recording_server: Any) -> None:
    server, base_url = recording_server
    with pytest.raises(ValueError, match="active, paused, or draining"):
        _client(base_url).set_queue_state(
            "orders",
            QueueState("warming_up"),
            expected_config_version=1,
            idempotency_key="idem-1",
        )
    assert server.recorded == []


def test_non_fixed_backoff_rejected_before_transport(recording_server: Any) -> None:
    server, base_url = recording_server
    policy = RetryPolicyDraft(
        enabled=True,
        max_attempts=1,
        backoff_strategy=BackoffStrategy("exponential"),
        retry_delay_seconds=1,
    )
    with pytest.raises(ValueError, match="fixed"):
        policy.to_wire()
    with pytest.raises(ValueError):
        _client(base_url).create_queue(
            "orders",
            initial_policy=policy,
            idempotency_key="idem-1",
        )
    assert server.recorded == []


def test_invalid_queue_name_rejected_before_transport(recording_server: Any) -> None:
    server, base_url = recording_server
    policy = _policy_draft()
    with pytest.raises(ValueError, match="pattern"):
        _client(base_url).create_queue(
            "BadQueue",
            initial_policy=policy,
            idempotency_key="idem-1",
        )
    assert server.recorded == []


def test_config_and_policy_version_bounds(recording_server: Any) -> None:
    server, base_url = recording_server
    client = _client(base_url)
    policy = _policy_draft()
    with pytest.raises(ValueError, match="at least 1"):
        client.activate_queue_policy(
            "orders",
            0,
            expected_config_version=1,
            idempotency_key="idem-1",
        )
    with pytest.raises(ValueError, match="at least 1"):
        client.set_queue_state(
            "orders",
            "paused",
            expected_config_version=0,
            idempotency_key="idem-1",
        )
    with pytest.raises(ValueError, match="86400"):
        _client(base_url).create_queue(
            "orders",
            initial_policy=_policy_draft(retry_delay_seconds=86401),
            idempotency_key="idem-1",
        )
    assert server.recorded == []


def test_malformed_mutation_response_fails_distinctly(recording_server: Any) -> None:
    server, base_url = recording_server
    server.routes[("POST", "/admin/v1/queues")] = (
        lambda body, headers: (200, {"replayed": False})
    )
    with pytest.raises(MalformedResponseError) as exc_info:
        _client(base_url).create_queue(
            "orders",
            initial_policy=_policy_draft(),
            idempotency_key="idem-1",
        )
    assert isinstance(exc_info.value, QueueClientError)
    assert "queue" in str(exc_info.value)


def test_authentication_error_is_distinguishable(recording_server: Any) -> None:
    server, base_url = recording_server
    server.routes[("GET", "/admin/v1/queues/orders")] = (
        lambda body, headers: (
            401,
            {
                "code": "unauthenticated",
                "message": "nope",
                "retryable": False,
                "request_id": "22222222-2222-4222-8222-222222222222",
                "details": {},
            },
        )
    )
    with pytest.raises(AuthenticationError):
        _client(base_url).get_queue("orders")


def test_error_diagnostics_never_leak_bearer_token(recording_server: Any) -> None:
    server, base_url = recording_server
    secret = "super-secret-admin-token"
    server.routes[("GET", "/admin/v1/queues")] = (
        lambda body, headers: (
            500,
            {
                "code": "internal_error",
                "message": "boom",
                "retryable": True,
                "request_id": "44444444-4444-4444-8444-444444444444",
                "details": {},
            },
        )
    )
    client = _client(base_url, token=secret)
    with pytest.raises(ProtocolError) as exc_info:
        client.list_queues()
    blob = f"{exc_info.value!s}{exc_info.value!r}{client!r}{client!s}"
    assert secret not in blob


@pytest.mark.parametrize("token", [None, "", "   ", "\t", "\n"])
def test_constructor_requires_token(token: str | None) -> None:
    transport = HttpJsonTransport("http://127.0.0.1:9", timeout_s=0.1)
    with pytest.raises(ValueError, match="bearer_token"):
        AdminClient(transport, bearer_token=token)  # type: ignore[arg-type]
    client = AdminClient(transport, bearer_token="token")
    assert "redacted" in repr(client)
    exact = " tok en "
    preserved = AdminClient(transport, bearer_token=exact)
    assert preserved._auth_headers()["Authorization"] == f"Bearer {exact}"
