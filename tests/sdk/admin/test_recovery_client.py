"""AdminClient recovery recording-server tests (SDK-07 / SDK-08 / REC-01 / REC-02)."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any
from urllib.parse import unquote, urlparse

import pytest

from _queue_service_client_core.errors import MalformedResponseError, ProtocolError
from _queue_service_client_core.transport import HttpJsonTransport
import queue_service_admin as admin_pkg
from queue_service_admin import AdminClient, BulkPreviewResult, ObserverClient
from queue_service_admin.models import (
    CONFIRMATION_TOKEN_MAX_LENGTH,
    BulkOperation,
    validate_bulk_filters,
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


def _client(base_url: str, *, token: str = "admin-secret-token") -> AdminClient:
    transport = HttpJsonTransport(base_url, timeout_s=2.0)
    return AdminClient(transport, bearer_token=token)


def _replay_result(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "task_id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        "source_task_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "queue": "orders",
        "policy_version": 1,
        "replayed": False,
        "warning": "Replay is at-least-once and may repeat external side effects.",
        "admin_replay_expires_at": "2026-10-19T00:00:00Z",
    }
    body.update(overrides)
    return body


def _preview_result(operation: str, **overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "operation": operation,
        "queue": "orders",
        "candidate_count": 2,
        "truncated": False,
        "sample_task_ids": ["aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"],
        "confirmation_token": "preview-token-abc",
        "confirmation_expires_at": "2026-09-19T01:00:00Z",
        "max_batch": 25,
    }
    body.update(overrides)
    return body


def _execute_result(operation: str, **overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "operation": operation,
        "queue": "orders",
        "candidate_count": 2,
        "start_index": 0,
        "processed": 1,
        "succeeded": 1,
        "skipped": 0,
        "failed": 0,
        "partial": False,
        "outcomes": [
            {
                "task_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                "outcome": "replayed" if operation == "bulk_replay" else "cancelled",
            }
        ],
    }
    body.update(overrides)
    return body


def test_package_exports_admin_recovery_surface() -> None:
    assert "AdminClient" in admin_pkg.__all__
    assert admin_pkg.AdminClient is AdminClient
    client = _client("http://127.0.0.1:9")
    for method in (
        "replay_dead_letter",
        "preview_bulk_replay",
        "execute_bulk_replay",
        "preview_bulk_cancel",
        "execute_bulk_cancel",
    ):
        assert hasattr(client, method)
    observer = ObserverClient(
        HttpJsonTransport("http://127.0.0.1:9", timeout_s=0.1),
        bearer_token="token",
        admin_transport=HttpJsonTransport("http://127.0.0.1:9", timeout_s=0.1),
    )
    for forbidden in (
        "replay_dead_letter",
        "execute_bulk_replay",
        "execute_bulk_cancel",
    ):
        assert not hasattr(observer, forbidden)


def test_replay_dead_letter_wire_contract(recording_server: Any) -> None:
    server, base_url = recording_server
    task_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    path = f"/admin/v1/queues/orders/dead-letters/{task_id}:replay"
    server.routes[("POST", path)] = lambda body, headers: (200, _replay_result())

    result = _client(base_url).replay_dead_letter(
        "orders",
        task_id,
        idempotency_key="replay-key-1",
        reason="operator recovery",
    )
    assert result.source_task_id == task_id
    assert result.replayed is False
    assert "at-least-once" in result.warning.lower()
    rec = server.recorded[0]
    assert rec["method"] == "POST"
    assert rec["path"] == path
    assert rec["headers"]["authorization"] == "Bearer admin-secret-token"
    assert rec["headers"]["idempotency-key"] == "replay-key-1"
    payload = json.loads(rec["body"].decode("utf-8"))
    assert payload == {"reason": "operator recovery"}


def test_preview_bulk_replay_serializes_filters(recording_server: Any) -> None:
    server, base_url = recording_server
    path = "/admin/v1/queues/orders/bulk:preview-replay"
    server.routes[("POST", path)] = lambda body, headers: (
        200,
        _preview_result("bulk_replay"),
    )
    preview = _client(base_url).preview_bulk_replay(
        "orders",
        filters={"failure_code": "timeout", "from": "2026-09-18T00:00:00Z"},
    )
    assert preview.operation.value == "bulk_replay"
    assert preview.confirmation_token == "preview-token-abc"
    payload = json.loads(server.recorded[0]["body"].decode("utf-8"))
    assert payload["filters"]["failure_code"] == "timeout"


def test_execute_bulk_replay_requires_preview_and_idempotency(recording_server: Any) -> None:
    server, base_url = recording_server
    path = "/admin/v1/queues/orders/bulk:execute-replay"
    server.routes[("POST", path)] = lambda body, headers: (
        200,
        _execute_result("bulk_replay"),
    )
    preview = BulkPreviewResult.parse(_preview_result("bulk_replay"))
    result = _client(base_url).execute_bulk_replay(
        "orders",
        preview=preview,
        idempotency_key="bulk-replay-key",
        reason="bulk recovery",
        filters={"failure_code": "timeout"},
        start_index=0,
        batch_limit=10,
    )
    assert result.processed == 1
    assert result.outcomes[0].outcome.value == "replayed"
    rec = server.recorded[0]
    assert rec["headers"]["idempotency-key"] == "bulk-replay-key"
    payload = json.loads(rec["body"].decode("utf-8"))
    assert payload["confirmation_token"] == "preview-token-abc"
    assert payload["filters"] == {"failure_code": "timeout"}
    assert payload["reason"] == "bulk recovery"
    assert payload["start_index"] == 0
    assert payload["batch_limit"] == 10


def test_execute_bulk_cancel_omits_idempotency_header(recording_server: Any) -> None:
    server, base_url = recording_server
    path = "/admin/v1/queues/orders/bulk:execute-cancel"
    server.routes[("POST", path)] = lambda body, headers: (
        200,
        _execute_result("bulk_cancel"),
    )
    preview = BulkPreviewResult.parse(_preview_result("bulk_cancel"))
    _client(base_url).execute_bulk_cancel(
        "orders",
        preview=preview,
        reason="bulk cancel",
        filters={"state": "ready"},
    )
    rec = server.recorded[0]
    assert "idempotency-key" not in rec["headers"]
    payload = json.loads(rec["body"].decode("utf-8"))
    assert payload["confirmation_token"] == "preview-token-abc"
    assert payload["filters"] == {"state": "ready"}


def test_preview_bulk_cancel_wire_contract(recording_server: Any) -> None:
    server, base_url = recording_server
    path = "/admin/v1/queues/orders/bulk:preview-cancel"
    server.routes[("POST", path)] = (
        lambda body, headers: (200, _preview_result("bulk_cancel"))
    )
    preview = _client(base_url).preview_bulk_cancel("orders")
    assert preview.operation.value == "bulk_cancel"
    assert json.loads(server.recorded[0]["body"].decode("utf-8")) == {}


def test_execute_rejects_mismatched_preview_operation(recording_server: Any) -> None:
    preview = BulkPreviewResult.parse(_preview_result("bulk_cancel"))
    client = _client("http://127.0.0.1:9")
    with pytest.raises(ValueError, match="bulk_replay"):
        client.execute_bulk_replay(
            "orders",
            preview=preview,
            idempotency_key="key",
            reason="reason",
            filters={},
        )


def test_execute_rejects_mismatched_preview_queue(recording_server: Any) -> None:
    preview = BulkPreviewResult.parse(_preview_result("bulk_replay", queue="billing"))
    client = _client("http://127.0.0.1:9")
    with pytest.raises(ValueError, match="queue"):
        client.execute_bulk_replay(
            "orders",
            preview=preview,
            idempotency_key="key",
            reason="reason",
            filters={},
        )


@pytest.mark.parametrize(
    ("method_name", "kwargs"),
    [
        (
            "replay_dead_letter",
            {
                "queue_name": "orders",
                "task_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                "idempotency_key": "",
                "reason": "ok",
            },
        ),
        (
            "execute_bulk_replay",
            {
                "queue_name": "orders",
                "preview": BulkPreviewResult.parse(_preview_result("bulk_replay")),
                "idempotency_key": "x" * 257,
                "reason": "ok",
                "filters": {},
            },
        ),
    ],
)
def test_validation_rejects_before_transport(
    recording_server: Any,
    method_name: str,
    kwargs: dict[str, Any],
) -> None:
    server, base_url = recording_server
    client = _client(base_url)
    with pytest.raises(ValueError):
        getattr(client, method_name)(**kwargs)
    assert server.recorded == []


def test_reason_length_rejected_before_transport(recording_server: Any) -> None:
    server, base_url = recording_server
    client = _client(base_url)
    with pytest.raises(ValueError, match="512"):
        client.replay_dead_letter(
            "orders",
            "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            idempotency_key="key",
            reason="x" * 513,
        )
    assert server.recorded == []


def test_batch_limit_bounds_rejected_before_transport(recording_server: Any) -> None:
    preview = BulkPreviewResult.parse(_preview_result("bulk_replay"))
    client = _client("http://127.0.0.1:9")
    with pytest.raises(ValueError, match="25"):
        client.execute_bulk_replay(
            "orders",
            preview=preview,
            idempotency_key="key",
            reason="ok",
            filters={},
            batch_limit=26,
        )


def test_bulk_preview_repr_redacts_confirmation_token() -> None:
    preview = BulkPreviewResult(
        operation=BulkOperation("bulk_replay"),
        queue="orders",
        candidate_count=1,
        truncated=False,
        sample_task_ids=("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",),
        confirmation_token="distinct-preview-secret-token",
        confirmation_expires_at="2026-09-19T01:00:00Z",
        max_batch=25,
    )
    rendered = repr(preview)
    assert "distinct-preview-secret-token" not in rendered
    assert "confirmation_token=<redacted>" in rendered
    assert str(preview) == rendered
    assert preview.confirmation_token == "distinct-preview-secret-token"


def test_validate_bulk_filters_rejects_forbidden_and_unknown_keys() -> None:
    with pytest.raises(ValueError, match="payload.*not allowed"):
        validate_bulk_filters({"payload": "x"})
    with pytest.raises(ValueError, match="unknown filter 'foo'"):
        validate_bulk_filters({"foo": "bar"})
    assert validate_bulk_filters({"Failure_Code": " timeout "}) == {
        "failure_code": "timeout",
    }


def test_parse_rejects_empty_confirmation_token() -> None:
    with pytest.raises(ValueError, match="confirmation_token"):
        BulkPreviewResult.parse(_preview_result("bulk_replay", confirmation_token=""))


def test_parse_rejects_oversized_confirmation_token() -> None:
    with pytest.raises(ValueError, match="24576"):
        BulkPreviewResult.parse(
            _preview_result(
                "bulk_replay",
                confirmation_token="x" * (CONFIRMATION_TOKEN_MAX_LENGTH + 1),
            )
        )


def test_execute_rejects_empty_confirmation_token_before_transport(
    recording_server: Any,
) -> None:
    server, base_url = recording_server
    preview = BulkPreviewResult.parse(_preview_result("bulk_replay"))
    empty_token_preview = BulkPreviewResult(
        operation=preview.operation,
        queue=preview.queue,
        candidate_count=preview.candidate_count,
        truncated=preview.truncated,
        sample_task_ids=preview.sample_task_ids,
        confirmation_token="",
        confirmation_expires_at=preview.confirmation_expires_at,
        max_batch=preview.max_batch,
    )
    client = _client(base_url)
    with pytest.raises(ValueError, match="confirmation_token"):
        client.execute_bulk_replay(
            "orders",
            preview=empty_token_preview,
            idempotency_key="key",
            reason="ok",
            filters={},
        )
    assert server.recorded == []


def test_execute_rejects_oversized_confirmation_token_before_transport(
    recording_server: Any,
) -> None:
    server, base_url = recording_server
    preview = BulkPreviewResult.parse(_preview_result("bulk_replay"))
    oversized_preview = BulkPreviewResult(
        operation=preview.operation,
        queue=preview.queue,
        candidate_count=preview.candidate_count,
        truncated=preview.truncated,
        sample_task_ids=preview.sample_task_ids,
        confirmation_token="x" * (CONFIRMATION_TOKEN_MAX_LENGTH + 1),
        confirmation_expires_at=preview.confirmation_expires_at,
        max_batch=preview.max_batch,
    )
    client = _client(base_url)
    with pytest.raises(ValueError, match="24576"):
        client.execute_bulk_replay(
            "orders",
            preview=oversized_preview,
            idempotency_key="key",
            reason="ok",
            filters={},
        )
    assert server.recorded == []


def test_filter_value_length_rejected_before_transport(recording_server: Any) -> None:
    server, base_url = recording_server
    client = _client(base_url)
    with pytest.raises(ValueError, match="128"):
        client.preview_bulk_replay("orders", filters={"failure_code": "x" * 129})
    assert server.recorded == []


def test_replay_result_preserves_lineage_fields(recording_server: Any) -> None:
    server, base_url = recording_server
    task_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    path = f"/admin/v1/queues/orders/dead-letters/{task_id}:replay"
    server.routes[("POST", path)] = lambda body, headers: (
        200,
        _replay_result(replayed=True, source_task_id=task_id),
    )
    result = _client(base_url).replay_dead_letter(
        "orders",
        task_id,
        idempotency_key="dup-key",
        reason="retry",
    )
    assert result.source_task_id == task_id
    assert result.replayed is True


def test_bulk_execute_partial_counts_preserved(recording_server: Any) -> None:
    server, base_url = recording_server
    path = "/admin/v1/queues/orders/bulk:execute-replay"
    server.routes[("POST", path)] = lambda body, headers: (
        200,
        _execute_result(
            "bulk_replay",
            partial=True,
            processed=2,
            succeeded=1,
            skipped=1,
            failed=0,
            next_start_index=2,
        ),
    )
    preview = BulkPreviewResult.parse(_preview_result("bulk_replay"))
    result = _client(base_url).execute_bulk_replay(
        "orders",
        preview=preview,
        idempotency_key="key",
        reason="partial",
        filters={},
    )
    assert result.partial is True
    assert result.next_start_index == 2
    assert result.succeeded == 1
    assert result.skipped == 1


def test_unknown_bulk_operation_enum_tolerated(recording_server: Any) -> None:
    server, base_url = recording_server
    path = "/admin/v1/queues/orders/bulk:preview-replay"
    server.routes[("POST", path)] = lambda body, headers: (
        200,
        _preview_result("future_bulk_op"),
    )
    preview = _client(base_url).preview_bulk_replay("orders")
    assert preview.operation.value == "future_bulk_op"
    assert preview.operation.is_unknown is True
    assert isinstance(preview.operation, BulkOperation)


def test_malformed_replay_response_raises_distinct_error(recording_server: Any) -> None:
    server, base_url = recording_server
    task_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    path = f"/admin/v1/queues/orders/dead-letters/{task_id}:replay"
    server.routes[("POST", path)] = lambda body, headers: (200, {"task_id": task_id})
    with pytest.raises(MalformedResponseError, match="source_task_id"):
        _client(base_url).replay_dead_letter(
            "orders",
            task_id,
            idempotency_key="key",
            reason="ok",
        )


def test_error_diagnostics_never_leak_bearer_token(recording_server: Any) -> None:
    server, base_url = recording_server
    secret = "super-secret-admin-token"
    path = "/admin/v1/queues/orders/bulk:preview-replay"
    server.routes[("POST", path)] = (
        lambda body, headers: (
            403,
            {
                "code": "permission_denied",
                "message": "denied",
                "retryable": False,
                "request_id": "44444444-4444-4444-8444-444444444444",
                "details": {},
            },
        )
    )
    client = _client(base_url, token=secret)
    with pytest.raises(ProtocolError) as exc_info:
        client.preview_bulk_replay("orders")
    blob = f"{exc_info.value!s}{exc_info.value!r}{client!r}{client!s}"
    assert secret not in blob


@pytest.mark.parametrize("token", [None, "", "   ", "\t", "\n"])
def test_constructor_requires_token(token: str | None) -> None:
    transport = HttpJsonTransport("http://127.0.0.1:9", timeout_s=0.1)
    with pytest.raises(ValueError, match="bearer_token"):
        AdminClient(transport, bearer_token=token)  # type: ignore[arg-type]
    client = AdminClient(transport, bearer_token="token")
    assert "redacted" in repr(client)
