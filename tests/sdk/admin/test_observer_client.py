"""ObserverClient recording-server tests (SDK-07 / SDK-08)."""

from __future__ import annotations

import json
import threading
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

import pytest

from _workhold_client_core.errors import (
    AuthenticationError,
    MalformedResponseError,
    ProtocolError,
    QueueClientError,
    TimeoutError as ClientTimeoutError,
)
from _workhold_client_core.models import TaskState
from _workhold_client_core.transport import HttpJsonTransport
import workhold_admin as admin_pkg
from workhold_admin import ObserverClient
from workhold_admin.models import (
    AttemptOutcome,
    MaintenanceOutcome,
    QueueState,
    StatsFreshness,
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


def _attempt_body(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "attempt_id": 1,
        "task_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "claim_id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        "generation": 1,
        "claimed_at": "2026-09-19T00:00:00Z",
        "worker_id": "worker-1",
        "lease_expires_at": "2026-09-19T00:05:00Z",
        "outcome": "succeeded",
    }
    body.update(overrides)
    return body


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
            "retry_delay_seconds": 30,
            "created_at": "2026-09-19T00:00:00Z",
        },
        "created_at": "2026-09-19T00:00:00Z",
        "updated_at": "2026-09-19T00:00:00Z",
    }
    body.update(overrides)
    return body


def _stats_body(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "as_of": "2026-09-19T00:00:00Z",
        "generated_at": "2026-09-19T00:00:01Z",
        "age_seconds": 1.0,
        "freshness": "fresh",
        "queues": [
            {
                "name": "orders",
                "ready_depth": 2,
                "delayed_depth": 0,
                "leased_depth": 1,
                "as_of": "2026-09-19T00:00:00Z",
                "freshness": "fresh",
            }
        ],
        "retry": {
            "availability": "available",
            "source": "process_telemetry",
            "total": 0,
        },
        "dead_letter": {
            "availability": "available",
            "source": "process_telemetry",
            "total": 0,
        },
        "maintenance": {"availability": "available"},
    }
    body.update(overrides)
    return body


def _maintenance_body(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "updated_at": "2026-09-19T00:00:00Z",
        "outcome": "succeeded",
        "maintenance_run_id": "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
    }
    body.update(overrides)
    return body


def _client(
    public_url: str,
    admin_url: str,
    *,
    token: str = "observer-secret-token",
    timeout: float = 2.0,
) -> ObserverClient:
    public = HttpJsonTransport(public_url, timeout_s=timeout)
    admin = HttpJsonTransport(admin_url, timeout_s=timeout)
    return ObserverClient(public, bearer_token=token, admin_transport=admin)


def _query_dict(query: str) -> dict[str, list[str]]:
    return parse_qs(query, keep_blank_values=True)


def test_package_exports_observer_surface(recording_server: Any) -> None:
    assert "ObserverClient" in admin_pkg.__all__
    assert admin_pkg.ObserverClient is ObserverClient
    for forbidden in ("ProducerClient", "ConsumerClient", "ConsumerSupervisor"):
        assert forbidden not in admin_pkg.__all__
        assert not hasattr(admin_pkg, forbidden)

    client = _client("http://127.0.0.1:9", "http://127.0.0.1:9", token="token")
    for forbidden_method in (
        "enqueue",
        "cancel_task",
        "claim",
        "create_queue",
        "set_queue_state",
        "activate_queue_policy",
        "run_maintenance",
        "replay_dead_letter",
        "execute_bulk_replay",
        "execute_bulk_cancel",
        "force_lease_expiry",
        "repair_registry_entry",
    ):
        assert not hasattr(client, forbidden_method)


def test_get_capabilities_uses_public_plane(recording_server: Any) -> None:
    server, base_url = recording_server
    server.routes[("GET", "/v1/capabilities")] = (
        lambda body, headers: (200, _capabilities_body())
    )
    caps = _client(base_url, base_url).get_capabilities()
    assert caps.protocol_major == 1
    rec = server.recorded[0]
    assert rec["method"] == "GET"
    assert rec["path"] == "/v1/capabilities"
    assert rec["headers"]["authorization"] == "Bearer observer-secret-token"


def test_get_task_uses_public_plane(recording_server: Any) -> None:
    server, base_url = recording_server
    task_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    server.routes[("GET", f"/v1/tasks/{task_id}")] = (
        lambda body, headers: (200, _task_body())
    )
    task = _client(base_url, base_url).get_task(task_id)
    assert task.task_id == task_id
    assert task.state.value == "ready"
    rec = server.recorded[0]
    assert rec["path"] == f"/v1/tasks/{task_id}"
    assert rec["body"] == b""


def test_list_task_attempts_query_contract(recording_server: Any) -> None:
    server, base_url = recording_server
    task_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"

    def respond(body: bytes, headers: dict[str, str]) -> tuple[int, dict[str, Any]]:
        return 200, {
            "items": [_attempt_body()],
            "next_cursor": "cursor-1",
        }

    server.routes[("GET", f"/v1/tasks/{task_id}/attempts")] = respond
    page = _client(base_url, base_url).list_task_attempts(
        task_id, cursor="cursor-0", limit=25
    )
    assert len(page.items) == 1
    assert page.items[0].outcome.value == "succeeded"
    assert page.next_cursor == "cursor-1"
    rec = server.recorded[0]
    assert rec["path"] == f"/v1/tasks/{task_id}/attempts"
    q = _query_dict(rec["query"])
    assert q["limit"] == ["25"]
    assert q["cursor"] == ["cursor-0"]


def test_admin_reads_use_admin_transport(recording_server: Any) -> None:
    server, base_url = recording_server
    server.routes[("GET", "/admin/v1/queues/orders")] = (
        lambda body, headers: (200, _queue_body())
    )
    server.routes[("GET", "/admin/v1/maintenance")] = (
        lambda body, headers: (200, _maintenance_body())
    )
    client = _client(base_url, base_url)
    queue = client.get_queue("orders")
    assert queue.name == "orders"
    assert queue.state.value == "active"
    status = client.get_maintenance_status()
    assert status.outcome is not None
    assert status.outcome.value == "succeeded"
    assert [r["path"] for r in server.recorded] == [
        "/admin/v1/queues/orders",
        "/admin/v1/maintenance",
    ]


def test_list_inspection_tasks_query_contract(recording_server: Any) -> None:
    server, base_url = recording_server
    server.routes[("GET", "/admin/v1/tasks")] = (
        lambda body, headers: (200, {"items": [_task_body()], "next_cursor": None})
    )
    page = _client(base_url, base_url).list_inspection_tasks("orders", limit=50)
    assert len(page.items) == 1
    q = _query_dict(server.recorded[0]["query"])
    assert q["queue_name"] == ["orders"]
    assert q["limit"] == ["50"]
    assert "cursor" not in q


def test_list_inspection_attempts_requires_time_bounds(recording_server: Any) -> None:
    server, base_url = recording_server
    server.routes[("GET", "/admin/v1/attempts")] = (
        lambda body, headers: (200, {"items": [], "next_cursor": None})
    )
    start = datetime(2026, 9, 19, 0, 0, 0, tzinfo=UTC)
    end = datetime(2026, 9, 19, 1, 0, 0, tzinfo=UTC)
    _client(base_url, base_url).list_inspection_attempts(
        "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        time_from=start,
        time_to=end,
        limit=10,
    )
    q = _query_dict(server.recorded[0]["query"])
    assert q["task_id"] == ["aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"]
    assert q["from"] == ["2026-09-19T00:00:00Z"]
    assert q["to"] == ["2026-09-19T01:00:00Z"]
    assert q["limit"] == ["10"]


def test_list_dead_letters_query_contract(recording_server: Any) -> None:
    server, base_url = recording_server
    server.routes[("GET", "/admin/v1/dead-letters")] = (
        lambda body, headers: (
            200,
            {"items": [_task_body(state="dead_lettered")], "next_cursor": "next"},
        )
    )
    start = datetime(2026, 9, 18, 0, 0, 0, tzinfo=UTC)
    end = datetime(2026, 9, 19, 0, 0, 0, tzinfo=UTC)
    page = _client(base_url, base_url).list_dead_letters(
        "orders", time_from=start, time_to=end, cursor="cur", limit=20
    )
    assert page.items[0].state.value == "dead_lettered"
    q = _query_dict(server.recorded[0]["query"])
    assert q["queue_name"] == ["orders"]
    assert q["cursor"] == ["cur"]
    assert q["limit"] == ["20"]


def test_get_stats_query_contract(recording_server: Any) -> None:
    server, base_url = recording_server
    server.routes[("GET", "/admin/v1/stats")] = (
        lambda body, headers: (200, _stats_body())
    )
    snapshot = _client(base_url, base_url).get_stats()
    assert snapshot.freshness.value == "fresh"
    assert snapshot.queues[0].name == "orders"
    rec = server.recorded[0]
    assert rec["path"] == "/admin/v1/stats"
    assert rec["query"] == ""


@pytest.mark.parametrize("limit", [0, 101, True, "50"])
def test_page_limit_rejected_before_transport(
    recording_server: Any,
    limit: object,
) -> None:
    server, base_url = recording_server
    client = _client(base_url, base_url)
    with pytest.raises(ValueError):
        client.list_task_attempts(
            "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            limit=limit,  # type: ignore[arg-type]
        )
    assert server.recorded == []


def test_cursor_length_rejected_before_transport(recording_server: Any) -> None:
    server, base_url = recording_server
    client = _client(base_url, base_url)
    with pytest.raises(ValueError, match="512"):
        client.list_inspection_tasks("orders", cursor="x" * 513)
    assert server.recorded == []


def test_invalid_queue_name_rejected_before_transport(recording_server: Any) -> None:
    server, base_url = recording_server
    client = _client(base_url, base_url)
    with pytest.raises(ValueError, match="pattern"):
        client.get_queue("BadQueue")
    assert server.recorded == []


def test_invalid_task_id_rejected_before_transport(recording_server: Any) -> None:
    server, base_url = recording_server
    client = _client(base_url, base_url)
    with pytest.raises(ValueError, match="UUID"):
        client.get_task("not-a-uuid")
    assert server.recorded == []


def test_time_range_validation_before_transport(recording_server: Any) -> None:
    server, base_url = recording_server
    client = _client(base_url, base_url)
    start = datetime(2026, 9, 20, 0, 0, 0, tzinfo=UTC)
    end = datetime(2026, 9, 19, 0, 0, 0, tzinfo=UTC)
    with pytest.raises(ValueError, match="no later than"):
        client.list_dead_letters("orders", time_from=start, time_to=end)
    naive = datetime(2026, 9, 19, 0, 0, 0)
    with pytest.raises(ValueError, match="timezone-aware"):
        client.list_inspection_attempts(
            "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            time_from=naive,
            time_to=start,
        )
    assert server.recorded == []


def test_unknown_additive_fields_and_enums_tolerated(recording_server: Any) -> None:
    server, base_url = recording_server
    server.routes[("GET", "/admin/v1/queues/orders")] = (
        lambda body, headers: (
            200,
            _queue_body(
                state="warming_up",
                future_flag=True,
                active_policy={
                    "version": 1,
                    "enabled": True,
                    "max_attempts": 3,
                    "backoff_strategy": "exponential",
                    "retry_delay_seconds": 30,
                    "created_at": "2026-09-19T00:00:00Z",
                    "beta": True,
                },
            ),
        )
    )
    server.routes[("GET", "/admin/v1/stats")] = (
        lambda body, headers: (
            200,
            _stats_body(freshness="degraded", extra_metric=99),
        )
    )
    client = _client(base_url, base_url)
    queue = client.get_queue("orders")
    assert queue.state.value == "warming_up"
    assert queue.state.is_unknown is True
    assert isinstance(queue.state, QueueState)
    assert queue.extra["future_flag"] is True
    assert queue.active_policy.backoff_strategy.is_unknown is True

    stats = client.get_stats()
    assert stats.freshness.is_unknown is True
    assert isinstance(stats.freshness, StatsFreshness)
    assert stats.extra["extra_metric"] == 99


def test_malformed_required_fields_fail_distinctly(recording_server: Any) -> None:
    server, base_url = recording_server
    server.routes[("GET", "/admin/v1/maintenance")] = (
        lambda body, headers: (200, {"last_started_at": "2026-09-19T00:00:00Z"})
    )
    with pytest.raises(MalformedResponseError) as exc_info:
        _client(base_url, base_url).get_maintenance_status()
    assert isinstance(exc_info.value, QueueClientError)
    assert not isinstance(exc_info.value, ProtocolError)
    assert "updated_at" in str(exc_info.value)


def test_authentication_and_protocol_errors_are_distinguishable(
    recording_server: Any,
) -> None:
    server, base_url = recording_server
    server.routes[("GET", "/v1/tasks/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")] = (
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
        _client(base_url, base_url).get_task("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")

    server.routes[("GET", "/admin/v1/queues/orders")] = (
        lambda body, headers: (
            403,
            {
                "code": "permission_denied",
                "message": "denied",
                "retryable": False,
                "request_id": "33333333-3333-4333-8333-333333333333",
                "details": {},
            },
        )
    )
    with pytest.raises(ProtocolError) as exc_info:
        _client(base_url, base_url, token="other-token").get_queue("orders")
    assert exc_info.value.code.value == "permission_denied"
    text = f"{exc_info.value!s}{exc_info.value!r}"
    assert "other-token" not in text
    assert "observer-secret-token" not in text


def test_error_diagnostics_never_leak_bearer_token(recording_server: Any) -> None:
    server, base_url = recording_server
    secret = "super-secret-observer-token"
    server.routes[("GET", "/admin/v1/maintenance")] = (
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
    client = _client(base_url, base_url, token=secret)
    with pytest.raises(ProtocolError) as exc_info:
        client.get_maintenance_status()
    blob = f"{exc_info.value!s}{exc_info.value!r}{client!r}{client!s}"
    assert secret not in blob


@pytest.mark.parametrize("token", [None, "", "   ", "\t", "\n"])
def test_constructor_requires_token_and_admin_transport(token: str | None) -> None:
    public = HttpJsonTransport("http://127.0.0.1:9", timeout_s=0.1)
    admin = HttpJsonTransport("http://127.0.0.1:9", timeout_s=0.1)
    with pytest.raises(ValueError, match="bearer_token"):
        ObserverClient(
            public, bearer_token=token, admin_transport=admin  # type: ignore[arg-type]
        )
    client = ObserverClient(public, bearer_token="token", admin_transport=admin)
    assert "redacted" in repr(client)
    exact = " tok en "
    preserved = ObserverClient(public, bearer_token=exact, admin_transport=admin)
    assert preserved._auth_headers()["Authorization"] == f"Bearer {exact}"


def test_valid_queue_name_with_dots_on_wire(recording_server: Any) -> None:
    server, base_url = recording_server
    server.routes[("GET", "/admin/v1/queues/orders.v2")] = (
        lambda body, headers: (200, _queue_body(name="orders.v2"))
    )
    _client(base_url, base_url).get_queue("orders.v2")
    assert server.recorded[0]["path"] == "/admin/v1/queues/orders.v2"


def test_invalid_queue_name_with_slash_rejected_before_transport(
    recording_server: Any,
) -> None:
    server, base_url = recording_server
    with pytest.raises(ValueError, match="pattern"):
        _client(base_url, base_url).get_queue("a/b")
    assert server.recorded == []


def test_attempt_and_maintenance_unknown_enums_preserved(recording_server: Any) -> None:
    server, base_url = recording_server
    task_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    server.routes[("GET", f"/v1/tasks/{task_id}/attempts")] = (
        lambda body, headers: (
            200,
            {"items": [_attempt_body(outcome="orphaned")], "next_cursor": None},
        )
    )
    server.routes[("GET", "/admin/v1/maintenance")] = (
        lambda body, headers: (
            200,
            _maintenance_body(outcome="deferred", future=True),
        )
    )
    client = _client(base_url, base_url)
    attempt_page = client.list_task_attempts(task_id)
    assert attempt_page.items[0].outcome.value == "orphaned"
    assert attempt_page.items[0].outcome.is_unknown is True
    assert isinstance(attempt_page.items[0].outcome, AttemptOutcome)

    status = client.get_maintenance_status()
    assert status.outcome is not None
    assert status.outcome.value == "deferred"
    assert status.outcome.is_unknown is True
    assert isinstance(status.outcome, MaintenanceOutcome)
    assert status.extra["future"] is True


def test_default_page_limit_omitted_from_query_when_not_default(
    recording_server: Any,
) -> None:
    server, base_url = recording_server
    task_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    server.routes[("GET", f"/v1/tasks/{task_id}/attempts")] = (
        lambda body, headers: (200, {"items": [], "next_cursor": None})
    )
    _client(base_url, base_url).list_task_attempts(task_id)
    q = _query_dict(server.recorded[0]["query"])
    assert q["limit"] == ["50"]


def test_timeout_error_is_distinct(recording_server: Any) -> None:
    with pytest.raises(ClientTimeoutError):
        _client("http://127.0.0.1:1", "http://127.0.0.1:1", timeout=0.05).get_capabilities()
