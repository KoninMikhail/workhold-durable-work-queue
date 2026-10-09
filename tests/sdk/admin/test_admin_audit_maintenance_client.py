"""AdminClient audit and routine maintenance tests (SDK-07 / SDK-08 / CTRL-04 / CTRL-06)."""

from __future__ import annotations

import json
import threading
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

import pytest

from _queue_service_client_core.errors import (
    AuthenticationError,
    MalformedResponseError,
    ProtocolError,
    QueueClientError,
)
from _queue_service_client_core.transport import HttpJsonTransport
import queue_service_admin as admin_pkg
from queue_service_admin import AdminClient, ObserverClient
from queue_service_admin.models import AuditOperation, MaintenanceOutcome


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


def _audit_record(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "audit_id": 42,
        "audit_at": "2026-09-19T00:00:00Z",
        "actor_id": "admin-operator",
        "operation": "run_maintenance",
        "request_id": "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
        "queue_id": None,
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


def _maintenance_run_result(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "status": _maintenance_body(),
        "replayed": False,
        "admin_replay_expires_at": "2026-10-19T00:00:00Z",
    }
    body.update(overrides)
    return body


def _admin_client(
    public_url: str,
    admin_url: str,
    *,
    token: str = "admin-secret-token",
    timeout: float = 2.0,
) -> AdminClient:
    public = HttpJsonTransport(public_url, timeout_s=timeout)
    admin = HttpJsonTransport(admin_url, timeout_s=timeout)
    return AdminClient(public, bearer_token=token, admin_transport=admin)


def _observer_client(
    public_url: str,
    admin_url: str,
    *,
    token: str = "observer-secret-token",
) -> ObserverClient:
    public = HttpJsonTransport(public_url, timeout_s=2.0)
    admin = HttpJsonTransport(admin_url, timeout_s=2.0)
    return ObserverClient(public, bearer_token=token, admin_transport=admin)


def _query_dict(query: str) -> dict[str, list[str]]:
    return parse_qs(query, keep_blank_values=True)


def test_package_exports_admin_audit_maintenance_surface() -> None:
    assert "AdminClient" in admin_pkg.__all__
    assert admin_pkg.AdminClient is AdminClient
    assert "AuditPage" in admin_pkg.__all__
    assert "MaintenanceRunResult" in admin_pkg.__all__


def test_observer_lacks_admin_audit_and_maintenance_mutation() -> None:
    client = _observer_client("http://127.0.0.1:9", "http://127.0.0.1:9")
    assert hasattr(client, "get_maintenance_status")
    assert not hasattr(client, "list_admin_audit")
    assert not hasattr(client, "run_maintenance")


def test_admin_lacks_break_glass_methods(recording_server: Any) -> None:
    server, base_url = recording_server
    client = _admin_client(base_url, base_url)
    for forbidden in (
        "drop_expired_partition",
        "force_lease_expiry",
        "force_delivery_reclaim",
        "force_delivery_dead_letter",
        "reconcile_counters",
        "raise_replay_limit",
        "repair_registry_entry",
    ):
        assert not hasattr(client, forbidden)


def test_list_admin_audit_query_contract(recording_server: Any) -> None:
    server, base_url = recording_server
    server.routes[("GET", "/admin/v1/audit")] = (
        lambda body, headers: (
            200,
            {"items": [_audit_record()], "next_cursor": "audit-cur-1"},
        )
    )
    start = datetime(2026, 9, 18, 0, 0, 0, tzinfo=UTC)
    end = datetime(2026, 9, 19, 0, 0, 0, tzinfo=UTC)
    page = _admin_client(base_url, base_url).list_admin_audit(
        time_from=start,
        time_to=end,
        queue_name="orders",
        cursor="audit-cur-0",
        limit=25,
    )
    assert len(page.items) == 1
    assert page.items[0].operation.value == "run_maintenance"
    assert page.next_cursor == "audit-cur-1"
    rec = server.recorded[0]
    assert rec["method"] == "GET"
    assert rec["path"] == "/admin/v1/audit"
    assert rec["headers"]["authorization"] == "Bearer admin-secret-token"
    q = _query_dict(rec["query"])
    assert q["from"] == ["2026-09-18T00:00:00Z"]
    assert q["to"] == ["2026-09-19T00:00:00Z"]
    assert q["queue_name"] == ["orders"]
    assert q["cursor"] == ["audit-cur-0"]
    assert q["limit"] == ["25"]


def test_get_maintenance_status_composed_from_observer(recording_server: Any) -> None:
    server, base_url = recording_server
    server.routes[("GET", "/admin/v1/maintenance")] = (
        lambda body, headers: (200, _maintenance_body())
    )
    status = _admin_client(base_url, base_url).get_maintenance_status()
    assert status.outcome is not None
    assert status.outcome.value == "succeeded"
    rec = server.recorded[0]
    assert rec["path"] == "/admin/v1/maintenance"
    assert rec["method"] == "GET"


def test_run_maintenance_idempotency_header_contract(recording_server: Any) -> None:
    server, base_url = recording_server
    server.routes[("POST", "/admin/v1/maintenance:run")] = (
        lambda body, headers: (200, _maintenance_run_result(replayed=True))
    )
    result = _admin_client(base_url, base_url).run_maintenance(
        idempotency_key="maint-run-key-1"
    )
    assert result.replayed is True
    assert result.status.updated_at == "2026-09-19T00:00:00Z"
    rec = server.recorded[0]
    assert rec["method"] == "POST"
    assert rec["path"] == "/admin/v1/maintenance:run"
    assert rec["headers"]["idempotency-key"] == "maint-run-key-1"
    assert rec["body"] == b""


@pytest.mark.parametrize("key", ["", "x" * 257, True])
def test_idempotency_key_rejected_before_transport(
    recording_server: Any,
    key: object,
) -> None:
    server, base_url = recording_server
    client = _admin_client(base_url, base_url)
    with pytest.raises(ValueError):
        client.run_maintenance(idempotency_key=key)  # type: ignore[arg-type]
    assert server.recorded == []


def test_audit_time_range_validation_before_transport(recording_server: Any) -> None:
    server, base_url = recording_server
    client = _admin_client(base_url, base_url)
    start = datetime(2026, 9, 20, 0, 0, 0, tzinfo=UTC)
    end = datetime(2026, 9, 19, 0, 0, 0, tzinfo=UTC)
    with pytest.raises(ValueError, match="no later than"):
        client.list_admin_audit(time_from=start, time_to=end)
    naive = datetime(2026, 9, 19, 0, 0, 0)
    with pytest.raises(ValueError, match="timezone-aware"):
        client.list_admin_audit(time_from=naive, time_to=start)
    assert server.recorded == []


def test_unknown_audit_operation_tolerated(recording_server: Any) -> None:
    server, base_url = recording_server
    server.routes[("GET", "/admin/v1/audit")] = (
        lambda body, headers: (
            200,
            {
                "items": [_audit_record(operation="future_operation", beta=True)],
                "next_cursor": None,
            },
        )
    )
    start = datetime(2026, 9, 18, 0, 0, 0, tzinfo=UTC)
    end = datetime(2026, 9, 19, 0, 0, 0, tzinfo=UTC)
    page = _admin_client(base_url, base_url).list_admin_audit(
        time_from=start,
        time_to=end,
    )
    record = page.items[0]
    assert record.operation.value == "future_operation"
    assert record.operation.is_unknown is True
    assert isinstance(record.operation, AuditOperation)
    assert record.extra["beta"] is True


def test_malformed_audit_page_fails_distinctly(recording_server: Any) -> None:
    server, base_url = recording_server
    server.routes[("GET", "/admin/v1/audit")] = (
        lambda body, headers: (200, {"next_cursor": None})
    )
    start = datetime(2026, 9, 18, 0, 0, 0, tzinfo=UTC)
    end = datetime(2026, 9, 19, 0, 0, 0, tzinfo=UTC)
    with pytest.raises(MalformedResponseError) as exc_info:
        _admin_client(base_url, base_url).list_admin_audit(time_from=start, time_to=end)
    assert isinstance(exc_info.value, QueueClientError)
    assert "items" in str(exc_info.value)


def test_malformed_maintenance_run_result_fails_distinctly(recording_server: Any) -> None:
    server, base_url = recording_server
    server.routes[("POST", "/admin/v1/maintenance:run")] = (
        lambda body, headers: (200, {"replayed": False})
    )
    with pytest.raises(MalformedResponseError) as exc_info:
        _admin_client(base_url, base_url).run_maintenance(idempotency_key="key-1")
    assert "status" in str(exc_info.value)


def test_authentication_and_protocol_errors_are_distinguishable(
    recording_server: Any,
) -> None:
    server, base_url = recording_server
    server.routes[("GET", "/admin/v1/audit")] = (
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
    start = datetime(2026, 9, 18, 0, 0, 0, tzinfo=UTC)
    end = datetime(2026, 9, 19, 0, 0, 0, tzinfo=UTC)
    with pytest.raises(AuthenticationError):
        _admin_client(base_url, base_url).list_admin_audit(time_from=start, time_to=end)

    server.routes[("POST", "/admin/v1/maintenance:run")] = (
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
        _admin_client(base_url, base_url, token="other-token").run_maintenance(
            idempotency_key="key-2"
        )
    assert exc_info.value.code.value == "permission_denied"


def test_error_diagnostics_never_leak_token_or_idempotency_key(
    recording_server: Any,
) -> None:
    server, base_url = recording_server
    secret = "super-secret-admin-token"
    idem = "super-secret-idempotency-key-value"
    server.routes[("POST", "/admin/v1/maintenance:run")] = (
        lambda body, headers: (
            500,
            {
                "code": "internal_error",
                "message": "boom",
                "retryable": True,
                "request_id": "44444444-4444-4444-8444-444444444444",
                "details": {"reason": "partition lag", "payload": {"secret": "data"}},
            },
        )
    )
    client = _admin_client(base_url, base_url, token=secret)
    with pytest.raises(ProtocolError) as exc_info:
        client.run_maintenance(idempotency_key=idem)
    blob = f"{exc_info.value!s}{exc_info.value!r}{client!r}{client!s}"
    assert secret not in blob
    assert idem not in blob
    assert "data" not in blob


def test_observer_reads_maintenance_admin_reads_audit(recording_server: Any) -> None:
    server, base_url = recording_server
    server.routes[("GET", "/admin/v1/maintenance")] = (
        lambda body, headers: (200, _maintenance_body(outcome="skipped_lock"))
    )
    server.routes[("GET", "/admin/v1/audit")] = (
        lambda body, headers: (200, {"items": [_audit_record()], "next_cursor": None})
    )
    observer = _observer_client(base_url, base_url)
    status = observer.get_maintenance_status()
    assert status.outcome is not None
    assert status.outcome.value == "skipped_lock"
    assert isinstance(status.outcome, MaintenanceOutcome)
    assert not hasattr(observer, "list_admin_audit")

    start = datetime(2026, 9, 18, 0, 0, 0, tzinfo=UTC)
    end = datetime(2026, 9, 19, 0, 0, 0, tzinfo=UTC)
    page = _admin_client(base_url, base_url).list_admin_audit(time_from=start, time_to=end)
    assert page.items[0].operation.value == "run_maintenance"
    paths = [rec["path"] for rec in server.recorded]
    assert "/admin/v1/maintenance" in paths
    assert "/admin/v1/audit" in paths


@pytest.mark.parametrize("token", [None, "", "   ", "\t", "\n"])
def test_constructor_requires_token_and_redacts_repr(token: str | None) -> None:
    public = HttpJsonTransport("http://127.0.0.1:9", timeout_s=0.1)
    admin = HttpJsonTransport("http://127.0.0.1:9", timeout_s=0.1)
    with pytest.raises(ValueError, match="bearer_token"):
        AdminClient(
            public, bearer_token=token, admin_transport=admin  # type: ignore[arg-type]
        )
    client = AdminClient(public, bearer_token="token", admin_transport=admin)
    assert "redacted" in repr(client)
