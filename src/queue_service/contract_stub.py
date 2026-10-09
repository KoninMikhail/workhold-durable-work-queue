"""Phase 3.1 HTTP contract stub — every OpenAPI operation is skeleton-only 501.

This process is a temporary conformance target. It does not implement Queue
protocol behavior, authentication, delivery, or migrations. Production
ErrorCode never includes skeleton_operation_unsupported.
"""

from __future__ import annotations

import json
import os
import re
import signal
import sys
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

HARD_MAX_BYTES = 1_048_576
DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8080
SKELETON_MESSAGE = "operation is not implemented"
NOT_FOUND_MESSAGE = "resource not found"
TOO_LARGE_MESSAGE = "request body exceeds hard maximum"


def _openapi_path() -> Path:
    candidates = [
        Path.cwd() / "openapi" / "queue.openapi.json",
        Path("/app/openapi/queue.openapi.json"),
        Path(__file__).resolve().parents[2] / "openapi" / "queue.openapi.json",
    ]
    # When running from src/ layout: parents[2] is repo root.
    # When installed under site-packages, also try walking upward.
    here = Path(__file__).resolve().parent
    for _ in range(6):
        candidates.append(here / "openapi" / "queue.openapi.json")
        here = here.parent
    for path in candidates:
        if path.is_file():
            return path
    raise FileNotFoundError("openapi/queue.openapi.json not found")


def _load_openapi() -> dict[str, Any]:
    return json.loads(_openapi_path().read_text(encoding="utf-8"))


def _template_to_regex(template: str) -> re.Pattern[str]:
    parts: list[str] = []
    i = 0
    while i < len(template):
        if template[i] == "{":
            end = template.index("}", i)
            parts.append(r"[^/]+")
            i = end + 1
        else:
            parts.append(re.escape(template[i]))
            i += 1
    return re.compile("^" + "".join(parts) + "$")


def _build_routes(spec: dict[str, Any]) -> list[tuple[str, re.Pattern[str], str]]:
    """Return (method, path_regex, operation_id) for every OpenAPI operation."""
    routes: list[tuple[str, re.Pattern[str], str]] = []
    for path_template, item in spec.get("paths", {}).items():
        if not isinstance(item, dict):
            continue
        path_re = _template_to_regex(path_template)
        for method, operation in item.items():
            if method.startswith("x-") or not isinstance(operation, dict):
                continue
            operation_id = operation.get("operationId")
            if not isinstance(operation_id, str):
                continue
            routes.append((method.upper(), path_re, operation_id))
    return routes


class ContractStubState:
    def __init__(self) -> None:
        self.spec = _load_openapi()
        self.skeleton = self.spec["x-queue-conformance-skeleton"]
        self.hard_max_bytes = int(
            self.spec.get("x-queue-request-hard-max-bytes", HARD_MAX_BYTES)
        )
        self.routes = _build_routes(self.spec)
        self.shutdown_event = threading.Event()


STATE = ContractStubState()


def _error_body(
    *,
    code: str,
    message: str,
    retryable: bool,
    retry_after_ms: int | None,
) -> dict[str, Any]:
    return {
        "code": code,
        "message": message,
        "retryable": retryable,
        "retry_after_ms": retry_after_ms,
        "request_id": str(uuid.uuid4()),
        "details": {},
    }


class ContractStubHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    timeout = 30

    def log_message(self, format: str, *args: object) -> None:  # noqa: A003
        # Keep process quiet; never log bodies or auth material.
        sys.stderr.write(
            "%s - %s\n" % (self.address_string(), format % args)
        )

    def do_GET(self) -> None:  # noqa: N802
        self._dispatch()

    def do_POST(self) -> None:  # noqa: N802
        self._dispatch()

    def do_PUT(self) -> None:  # noqa: N802
        self._dispatch()

    def do_PATCH(self) -> None:  # noqa: N802
        self._dispatch()

    def do_DELETE(self) -> None:  # noqa: N802
        self._dispatch()

    def do_HEAD(self) -> None:  # noqa: N802
        self._dispatch(include_body=False)

    def do_OPTIONS(self) -> None:  # noqa: N802
        self._dispatch()

    def _read_body_bounded(self) -> bytes | None:
        length_header = self.headers.get("Content-Length")
        if length_header is None:
            return b""
        try:
            length = int(length_header)
        except ValueError:
            self._write_error(
                400,
                _error_body(
                    code="validation_failed",
                    message="invalid Content-Length",
                    retryable=False,
                    retry_after_ms=None,
                ),
            )
            return None
        if length < 0:
            self._write_error(
                400,
                _error_body(
                    code="validation_failed",
                    message="invalid Content-Length",
                    retryable=False,
                    retry_after_ms=None,
                ),
            )
            return None
        if length > STATE.hard_max_bytes:
            # Reject before reading/parsing the body into application memory.
            # Drain at most the hard ceiling to avoid leaving a half-read socket,
            # but never retain or echo the payload.
            remaining = length
            chunk = 65_536
            while remaining > 0:
                to_read = min(chunk, remaining, STATE.hard_max_bytes)
                data = self.rfile.read(to_read)
                if not data:
                    break
                remaining -= len(data)
                if length > STATE.hard_max_bytes and (length - remaining) >= STATE.hard_max_bytes:
                    break
            self._write_error(
                413,
                _error_body(
                    code="payload_too_large",
                    message=TOO_LARGE_MESSAGE,
                    retryable=False,
                    retry_after_ms=None,
                ),
            )
            return None
        return self.rfile.read(length)

    def _match_operation(self, method: str, path: str) -> str | None:
        for route_method, path_re, operation_id in STATE.routes:
            if route_method == method and path_re.match(path):
                return operation_id
        return None

    def _dispatch(self, *, include_body: bool = True) -> None:
        parsed = urlparse(self.path)
        path = parsed.path or "/"
        method = self.command.upper()

        body = self._read_body_bounded()
        if body is None:
            return
        # Intentionally discard body — never echo payload or tokens.
        del body

        operation_id = self._match_operation(method, path)
        if operation_id is None:
            self._write_error(
                404,
                _error_body(
                    code="task_not_found",
                    message=NOT_FOUND_MESSAGE,
                    retryable=False,
                    retry_after_ms=None,
                ),
                include_body=include_body,
            )
            return

        skeleton = STATE.skeleton
        payload = _error_body(
            code=str(skeleton["body"]["code"]["const"]),
            message=SKELETON_MESSAGE,
            retryable=bool(skeleton["body"]["retryable"]["const"]),
            retry_after_ms=None,
        )
        headers = {str(k): str(v) for k, v in skeleton.get("headers", {}).items()}
        self._write_json(
            int(skeleton["http_status"]),
            payload,
            extra_headers=headers,
            include_body=include_body,
        )

    def _write_error(
        self,
        status: int,
        payload: dict[str, Any],
        *,
        include_body: bool = True,
    ) -> None:
        self._write_json(status, payload, include_body=include_body)

    def _write_json(
        self,
        status: int,
        payload: dict[str, Any],
        *,
        extra_headers: dict[str, str] | None = None,
        include_body: bool = True,
    ) -> None:
        raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw) if include_body else 0))
        self.send_header("Connection", "close")
        if extra_headers:
            for key, value in extra_headers.items():
                self.send_header(key, value)
        self.end_headers()
        if include_body:
            self.wfile.write(raw)


class QuietThreadingHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True
    request_queue_size = 32
    timeout = 1.0


def _install_signal_handlers(server: QuietThreadingHTTPServer) -> None:
    def _stop(_signum: int, _frame: object) -> None:
        STATE.shutdown_event.set()
        # shutdown() must not run on the signal thread on all platforms;
        # schedule from a helper thread.
        threading.Thread(target=server.shutdown, name="stub-shutdown", daemon=True).start()

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    # Windows: CTRL_BREAK_EVENT is delivered as SIGBREAK when the child is
    # started with CREATE_NEW_PROCESS_GROUP.
    sigbreak = getattr(signal, "SIGBREAK", None)
    if sigbreak is not None:
        signal.signal(sigbreak, _stop)


def run(
    host: str | None = None,
    port: int | None = None,
) -> int:
    bind_host = host if host is not None else os.environ.get("QUEUE_CONTRACT_HOST", DEFAULT_HOST)
    bind_port = (
        port
        if port is not None
        else int(os.environ.get("QUEUE_CONTRACT_PORT", str(DEFAULT_PORT)))
    )
    server = QuietThreadingHTTPServer((bind_host, bind_port), ContractStubHandler)
    _install_signal_handlers(server)
    sys.stderr.write(
        f"queue contract stub listening on http://{bind_host}:{bind_port}\n"
    )
    try:
        server.serve_forever(poll_interval=0.25)
    finally:
        server.server_close()
    return 0


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
