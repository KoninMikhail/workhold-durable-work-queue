"""Shared recording-server fixtures for sync/async parity tests."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any
from urllib.parse import unquote, urlparse

import pytest


class RecordingHandler(BaseHTTPRequestHandler):
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
                "raw_path": self.path,
                "headers": headers,
                "body": body,
            }
        )

    def _respond(self, status: int, payload: dict[str, Any] | list[Any] | None) -> None:
        raw = b"" if payload is None else json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
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
    server = HTTPServer(("127.0.0.1", 0), RecordingHandler)
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


def normalize_recorded(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Drop volatile fields; compare wire shape only."""

    out: list[dict[str, Any]] = []
    for rec in records:
        out.append(
            {
                "method": rec["method"],
                "path": rec["path"],
                "query": rec.get("query", ""),
                "headers": {
                    k: v
                    for k, v in rec["headers"].items()
                    if k
                    not in {
                        "host",
                        "accept-encoding",
                        "user-agent",
                        "connection",
                        "content-length",
                    }
                },
                "body": rec["body"],
            }
        )
    return out
