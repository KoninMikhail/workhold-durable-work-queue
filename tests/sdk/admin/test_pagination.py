"""Bounded sync pagination for Observer/Admin cursor-list ops (SDK-10)."""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

import pytest

from _queue_service_client_core.errors import ProtocolError
from _queue_service_client_core.transport import HttpJsonTransport
from queue_service_admin import AdminClient, ObserverClient
from queue_service_admin.pagination import (
    bounded_item_iterator,
    bounded_page_iterator,
    iter_admin_audit_pages,
    iter_dead_letter_pages,
    iter_inspection_attempt_pages,
    iter_inspection_task_pages,
    iter_queue_pages,
    iter_task_attempt_pages,
    iter_task_attempts,
)

TASK_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
TOKEN = "observer-secret-token"


@dataclass(frozen=True, slots=True)
class _FakePage:
    items: tuple[str, ...]
    next_cursor: str | None


class _RecordingHandler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return

    def _record(self, body: bytes) -> None:
        parsed = urlparse(self.path)
        self.server.recorded.append(  # type: ignore[attr-defined]
            {
                "method": self.command,
                "path": unquote(parsed.path),
                "query": parsed.query,
                "headers": {k.lower(): v for k, v in self.headers.items()},
                "body": body,
            }
        )

    def _respond(self, status: int, payload: dict[str, Any] | None) -> None:
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
        responder = self.server.routes.get((self.command, path))  # type: ignore[attr-defined]
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
        status, payload = responder(
            body, {k.lower(): v for k, v in self.headers.items()}
        )
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


def _observer(base_url: str) -> ObserverClient:
    return ObserverClient(
        HttpJsonTransport(base_url, timeout_s=2.0),
        bearer_token=TOKEN,
        admin_transport=HttpJsonTransport(base_url, timeout_s=2.0),
    )


def _admin(base_url: str) -> AdminClient:
    return AdminClient(
        HttpJsonTransport(base_url, timeout_s=2.0),
        bearer_token=TOKEN,
    )


def _query(rec: dict[str, Any]) -> dict[str, list[str]]:
    return parse_qs(rec["query"], keep_blank_values=True)


def _attempt_body(attempt_id: int = 1) -> dict[str, Any]:
    return {
        "attempt_id": attempt_id,
        "task_id": TASK_ID,
        "claim_id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        "generation": 1,
        "claimed_at": "2026-09-19T00:00:00Z",
        "worker_id": "worker-1",
        "lease_expires_at": "2026-09-19T00:05:00Z",
        "outcome": "succeeded",
    }


def _task_body(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "task_id": TASK_ID,
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


def _queue_body() -> dict[str, Any]:
    return {
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


def _audit_body() -> dict[str, Any]:
    return {
        "audit_id": 1,
        "audit_at": "2026-09-19T00:00:00Z",
        "actor_id": "admin-1",
        "operation": "run_maintenance",
        "request_id": "11111111-1111-4111-8111-111111111111",
    }


def test_bounds_required_and_must_be_positive() -> None:
    with pytest.raises(ValueError, match="at least one"):
        bounded_page_iterator(lambda _c: _FakePage((), None))
    with pytest.raises(ValueError, match="max_pages"):
        bounded_page_iterator(lambda _c: _FakePage((), None), max_pages=0)
    with pytest.raises(ValueError, match="max_items"):
        bounded_item_iterator(lambda _c: _FakePage((), None), max_items=-1)


def test_page_iterator_respects_max_pages_and_exposes_cursor() -> None:
    calls: list[str | None] = []

    def fetch(cursor: str | None) -> _FakePage:
        calls.append(cursor)
        if cursor is None:
            return _FakePage(("a", "b"), "c1")
        if cursor == "c1":
            return _FakePage(("c",), "c2")
        return _FakePage(("d",), None)

    it = bounded_page_iterator(fetch, max_pages=2)
    pages = list(it)
    assert [p.items for p in pages] == [("a", "b"), ("c",)]
    assert calls == [None, "c1"]
    assert it.page_count == 2
    assert it.item_count == 3
    assert it.last_cursor == "c2"


def test_item_iterator_stops_exactly_at_max_items_without_extra_fetch() -> None:
    calls: list[str | None] = []

    def fetch(cursor: str | None) -> _FakePage:
        calls.append(cursor)
        if cursor is None:
            return _FakePage(("a", "b", "c"), "c1")
        return _FakePage(("d", "e"), "c2")

    it = bounded_item_iterator(fetch, max_items=4)
    assert list(it) == ["a", "b", "c", "d"]
    assert calls == [None, "c1"]
    assert it.item_count == 4
    assert it.page_count == 2
    assert it.last_cursor == "c2"


def test_item_iterator_max_items_only_stops_on_empty_cursor_pages() -> None:
    calls: list[str | None] = []

    def fetch(cursor: str | None) -> _FakePage:
        calls.append(cursor)
        return _FakePage((), "still-going")

    it = bounded_item_iterator(fetch, max_items=10)
    assert list(it) == []
    assert len(calls) == 10
    assert it.page_count == 10
    assert it.last_cursor == "still-going"


def test_item_iterator_max_items_within_first_page_skips_next_request() -> None:
    calls: list[str | None] = []

    def fetch(cursor: str | None) -> _FakePage:
        calls.append(cursor)
        return _FakePage(("a", "b", "c"), "c1")

    it = bounded_item_iterator(fetch, max_items=2)
    assert list(it) == ["a", "b"]
    assert calls == [None]
    assert it.page_count == 1


def test_iter_task_attempt_pages_preserves_limit_and_cursor(
    recording_server: Any,
) -> None:
    server, base_url = recording_server

    def respond(body: bytes, headers: dict[str, str]) -> tuple[int, dict[str, Any]]:
        q = parse_qs(server.recorded[-1]["query"], keep_blank_values=True)
        cursor = q.get("cursor", [None])[0]
        if cursor is None:
            return 200, {
                "items": [_attempt_body(1), _attempt_body(2)],
                "next_cursor": "cur-1",
            }
        return 200, {"items": [_attempt_body(3)], "next_cursor": None}

    server.routes[("GET", f"/v1/tasks/{TASK_ID}/attempts")] = respond
    it = iter_task_attempt_pages(_observer(base_url), TASK_ID, max_pages=5, limit=2)
    pages = list(it)
    assert [len(p.items) for p in pages] == [2, 1]
    assert it.last_cursor is None
    assert it.page_count == 2
    queries = [_query(rec) for rec in server.recorded]
    assert queries[0]["limit"] == ["2"]
    assert "cursor" not in queries[0]
    assert queries[1]["limit"] == ["2"]
    assert queries[1]["cursor"] == ["cur-1"]


def test_iter_task_attempts_honors_max_items(recording_server: Any) -> None:
    server, base_url = recording_server
    n = {"count": 0}

    def respond(body: bytes, headers: dict[str, str]) -> tuple[int, dict[str, Any]]:
        n["count"] += 1
        if n["count"] == 1:
            return 200, {
                "items": [_attempt_body(1), _attempt_body(2)],
                "next_cursor": "more",
            }
        return 200, {
            "items": [_attempt_body(3), _attempt_body(4)],
            "next_cursor": None,
        }

    server.routes[("GET", f"/v1/tasks/{TASK_ID}/attempts")] = respond
    items = list(
        iter_task_attempts(_observer(base_url), TASK_ID, max_items=3, limit=2)
    )
    assert [item.attempt_id for item in items] == [1, 2, 3]
    assert n["count"] == 2


def test_tampered_cursor_protocol_error_propagates(recording_server: Any) -> None:
    server, base_url = recording_server

    def respond(body: bytes, headers: dict[str, str]) -> tuple[int, dict[str, Any]]:
        q = parse_qs(server.recorded[-1]["query"], keep_blank_values=True)
        if "cursor" in q:
            return 400, {
                "code": "validation_failed",
                "message": "tampered cursor",
                "retryable": False,
                "request_id": "55555555-5555-4555-8555-555555555555",
                "details": {},
            }
        return 200, {"items": [_attempt_body(1)], "next_cursor": "bad"}

    server.routes[("GET", f"/v1/tasks/{TASK_ID}/attempts")] = respond
    it = iter_task_attempt_pages(_observer(base_url), TASK_ID, max_pages=5)
    assert len(next(it).items) == 1
    with pytest.raises(ProtocolError) as exc_info:
        next(it)
    assert exc_info.value.code.value == "validation_failed"
    assert len(server.recorded) == 2


def test_observer_and_admin_wrappers_preserve_filters(recording_server: Any) -> None:
    server, base_url = recording_server
    t_from = datetime(2026, 9, 19, 0, 0, tzinfo=UTC)
    t_to = t_from + timedelta(hours=1)

    server.routes[("GET", "/admin/v1/tasks")] = (
        lambda body, headers: (200, {"items": [_task_body()], "next_cursor": None})
    )
    server.routes[("GET", "/admin/v1/attempts")] = (
        lambda body, headers: (200, {"items": [_attempt_body()], "next_cursor": None})
    )
    server.routes[("GET", "/admin/v1/dead-letters")] = (
        lambda body, headers: (
            200,
            {"items": [_task_body(state="dead_lettered")], "next_cursor": None},
        )
    )
    server.routes[("GET", "/admin/v1/queues")] = (
        lambda body, headers: (200, {"items": [_queue_body()], "next_cursor": None})
    )
    server.routes[("GET", "/admin/v1/audit")] = (
        lambda body, headers: (200, {"items": [_audit_body()], "next_cursor": None})
    )

    obs = _observer(base_url)
    adm = _admin(base_url)
    list(iter_inspection_task_pages(obs, "orders", max_pages=1, limit=10))
    list(
        iter_inspection_attempt_pages(
            obs, TASK_ID, time_from=t_from, time_to=t_to, max_pages=1, limit=7
        )
    )
    list(
        iter_dead_letter_pages(
            obs, "orders", time_from=t_from, time_to=t_to, max_pages=1, limit=8
        )
    )
    list(iter_queue_pages(adm, max_pages=1, limit=9))
    list(
        iter_admin_audit_pages(
            adm,
            time_from=t_from,
            time_to=t_to,
            queue_name="orders",
            max_pages=1,
            limit=11,
        )
    )

    by_path = {rec["path"]: _query(rec) for rec in server.recorded}
    assert by_path["/admin/v1/tasks"]["queue_name"] == ["orders"]
    assert by_path["/admin/v1/tasks"]["limit"] == ["10"]
    assert by_path["/admin/v1/attempts"]["task_id"] == [TASK_ID]
    assert by_path["/admin/v1/attempts"]["from"] == ["2026-09-19T00:00:00Z"]
    assert by_path["/admin/v1/attempts"]["to"] == ["2026-09-19T01:00:00Z"]
    assert by_path["/admin/v1/attempts"]["limit"] == ["7"]
    assert by_path["/admin/v1/dead-letters"]["queue_name"] == ["orders"]
    assert by_path["/admin/v1/dead-letters"]["limit"] == ["8"]
    assert by_path["/admin/v1/queues"]["limit"] == ["9"]
    assert by_path["/admin/v1/audit"]["queue_name"] == ["orders"]
    assert by_path["/admin/v1/audit"]["limit"] == ["11"]
