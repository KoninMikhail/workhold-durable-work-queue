"""Test-only fault injection for uncertain enqueue commit windows.

Provides:
- ``PostgresPreCommitGate`` — hold a ``queues`` row lock, prove the API backend
  is blocked on that lock via ``pg_stat_activity`` / ``pg_blocking_pids``, then
  terminate the blocked backend before commit.
- ``DropCommittedResponseProxy`` — loopback one-shot TCP/HTTP proxy that buffers
  a successful upstream response and closes the producer socket without
  forwarding any response bytes.

No production runtime hooks, fault headers, or non-loopback binds.
"""

from __future__ import annotations

import json
import select
import socket
import threading
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

import psycopg

# Documented queue-state lock query markers (EnqueueRepository.lock_named_queue).
QUEUE_LOCK_SQL_MARKERS: tuple[str, ...] = ("queues", "for update")


def _to_psycopg_conninfo(url: str) -> str:
    if url.startswith("postgresql+psycopg://"):
        return "postgresql://" + url.removeprefix("postgresql+psycopg://")
    return url


def _query_matches_queue_lock(query: str | None) -> bool:
    if not query:
        return False
    lowered = query.lower()
    return all(marker in lowered for marker in QUEUE_LOCK_SQL_MARKERS)


@dataclass(frozen=True)
class BufferedHttpResponse:
    """Upstream HTTP response retained only for test assertions."""

    status_line: str
    status_code: int
    headers: dict[str, str]
    body: bytes

    def json(self) -> Any:
        return json.loads(self.body.decode("utf-8"))


class PostgresPreCommitGate:
    """Hold ``SELECT … FROM queues … FOR UPDATE`` until enqueue is proven blocked."""

    def __init__(self, *, database_url: str, schema: str) -> None:
        self._database_url = database_url
        self._schema = schema
        self._gate_conn: psycopg.Connection | None = None
        self._observer: psycopg.Connection | None = None
        self.gate_pid: int | None = None
        self.blocked_pid: int | None = None
        self.blocked_query: str | None = None

    def enter(self, queue_id: int) -> None:
        """Open an independent transaction and lock the exact queues row."""
        if self._gate_conn is not None:
            raise RuntimeError("gate already entered")
        conn = psycopg.connect(_to_psycopg_conninfo(self._database_url))
        conn.autocommit = True
        conn.execute(f'SET search_path TO "{self._schema}"')
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute("SELECT pg_backend_pid()")
            self.gate_pid = int(cur.fetchone()[0])
            cur.execute(
                "SELECT id FROM queues WHERE id = %s FOR UPDATE",
                (int(queue_id),),
            )
            row = cur.fetchone()
            if row is None:
                conn.rollback()
                conn.close()
                raise LookupError(f"queue id {queue_id} not found for gate")
        self._gate_conn = conn
        observer = psycopg.connect(_to_psycopg_conninfo(self._database_url))
        observer.autocommit = True
        self._observer = observer

    def wait_until_blocked(self, deadline: float) -> int:
        """Poll until exactly one non-gate backend is blocked by this gate PID.

        Uses monotonic ``deadline`` (``time.monotonic()``). Success requires the
        blocked backend's active SQL to target the documented queue-row lock query.
        """
        if self._observer is None or self.gate_pid is None:
            raise RuntimeError("gate not entered")
        last_seen: list[tuple[int, str | None]] = []
        while time.monotonic() < deadline:
            with self._observer.cursor() as cur:
                cur.execute(
                    """
                    SELECT a.pid, a.query
                    FROM pg_stat_activity AS a
                    WHERE a.pid <> %s
                      AND a.datname = current_database()
                      AND a.pid IS NOT NULL
                      AND %s = ANY (pg_blocking_pids(a.pid))
                    ORDER BY a.pid
                    """,
                    (self.gate_pid, self.gate_pid),
                )
                rows = [(int(r[0]), r[1]) for r in cur.fetchall()]
            last_seen = rows
            matching = [
                (pid, query)
                for pid, query in rows
                if _query_matches_queue_lock(query)
            ]
            if len(matching) == 1 and len(rows) == 1:
                self.blocked_pid = matching[0][0]
                self.blocked_query = matching[0][1]
                return self.blocked_pid
            time.sleep(0.01)
        raise TimeoutError(
            "enqueue backend did not block on gate queue-row lock before deadline; "
            f"last_seen={last_seen!r} gate_pid={self.gate_pid}"
        )

    def terminate_blocked_backend(self) -> None:
        """Terminate the proven blocked API backend via ``pg_terminate_backend``."""
        if self._observer is None or self.blocked_pid is None:
            raise RuntimeError("no blocked backend to terminate")
        with self._observer.cursor() as cur:
            cur.execute("SELECT pg_terminate_backend(%s)", (self.blocked_pid,))
            ok = bool(cur.fetchone()[0])
        if not ok:
            raise RuntimeError(
                f"pg_terminate_backend({self.blocked_pid}) returned false"
            )

    def release(self) -> None:
        """Release the held queue-row lock and close gate connections."""
        if self._gate_conn is not None:
            try:
                self._gate_conn.rollback()
            except Exception:  # noqa: BLE001 — cleanup
                pass
            try:
                self._gate_conn.close()
            except Exception:  # noqa: BLE001
                pass
            self._gate_conn = None
        if self._observer is not None:
            try:
                self._observer.close()
            except Exception:  # noqa: BLE001
                pass
            self._observer = None


class DropCommittedResponseProxy:
    """Loopback-only one-shot proxy: buffer upstream success, drop to producer."""

    def __init__(self) -> None:
        self.buffered_response: BufferedHttpResponse | None = None
        self._thread: threading.Thread | None = None
        self._error: BaseException | None = None
        self._done = threading.Event()
        self._listen_sock: socket.socket | None = None
        self.listen_url: str | None = None

    def serve_once(self, upstream_url: str, deadline: float) -> str:
        """Bind ``127.0.0.1`` only, handle exactly one request, return listen URL."""
        if self._thread is not None:
            raise RuntimeError("serve_once already started")
        parts = urlsplit(upstream_url)
        if parts.scheme != "http":
            raise ValueError("upstream_url must be http")
        upstream_host = parts.hostname or "127.0.0.1"
        upstream_port = int(parts.port or 80)
        upstream_base_path = parts.path or ""

        listen = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listen.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listen.bind(("127.0.0.1", 0))
        listen.listen(1)
        listen.settimeout(max(0.05, deadline - time.monotonic()))
        self._listen_sock = listen
        host, port = listen.getsockname()
        self.listen_url = f"http://{host}:{port}"

        def _run() -> None:
            client: socket.socket | None = None
            upstream: socket.socket | None = None
            try:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("proxy deadline before accept")
                listen.settimeout(remaining)
                client, addr = listen.accept()
                if not addr[0].startswith("127."):
                    raise RuntimeError(f"refusing non-loopback client {addr!r}")
                client.settimeout(max(0.05, deadline - time.monotonic()))
                request = _recv_http_message(client, deadline)
                upstream = socket.create_connection(
                    (upstream_host, upstream_port),
                    timeout=max(0.05, deadline - time.monotonic()),
                )
                upstream.settimeout(max(0.05, deadline - time.monotonic()))
                # Preserve request target; prepend upstream base path if needed.
                rewritten = _rewrite_request_target(request, upstream_base_path)
                upstream.sendall(rewritten)
                raw_response = _recv_http_message(upstream, deadline)
                self.buffered_response = _parse_http_response(raw_response)
                # Prove success to the test; producer gets zero bytes.
                if self.buffered_response.status_code not in {200, 201}:
                    raise AssertionError(
                        "upstream enqueue did not return success: "
                        f"{self.buffered_response.status_line!r}"
                    )
                # Close producer-facing socket without forwarding response bytes.
            except BaseException as exc:  # noqa: BLE001 — surface to waiter
                self._error = exc
            finally:
                for sock in (client, upstream, listen):
                    if sock is None:
                        continue
                    try:
                        sock.close()
                    except Exception:  # noqa: BLE001
                        pass
                self._listen_sock = None
                self._done.set()

        self._thread = threading.Thread(
            target=_run, name="drop-committed-response-proxy", daemon=True
        )
        self._thread.start()
        return self.listen_url

    def wait(self, deadline: float) -> BufferedHttpResponse:
        """Wait until the one-shot exchange finishes; return buffered upstream."""
        remaining = deadline - time.monotonic()
        if remaining <= 0 or not self._done.wait(timeout=max(0.0, remaining)):
            raise TimeoutError("proxy did not finish before deadline")
        if self._error is not None:
            raise RuntimeError(f"proxy failed: {self._error!r}") from self._error
        if self.buffered_response is None:
            raise RuntimeError("proxy finished without buffering a response")
        return self.buffered_response

    def close(self) -> None:
        if self._listen_sock is not None:
            try:
                self._listen_sock.close()
            except Exception:  # noqa: BLE001
                pass
            self._listen_sock = None
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=2.0)


def _recv_http_message(sock: socket.socket, deadline: float) -> bytes:
    """Read one full HTTP/1.x message (headers + body by Content-Length)."""
    buf = bytearray()
    header_end = -1
    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        sock.settimeout(max(0.05, remaining))
        ready, _, _ = select.select([sock], [], [], max(0.0, remaining))
        if not ready:
            continue
        chunk = sock.recv(65536)
        if not chunk:
            break
        buf.extend(chunk)
        if header_end < 0:
            header_end = buf.find(b"\r\n\r\n")
            if header_end < 0:
                continue
            header_blob = bytes(buf[:header_end])
            content_length = _content_length(header_blob)
            total = header_end + 4 + content_length
            if len(buf) >= total:
                return bytes(buf[:total])
        else:
            content_length = _content_length(bytes(buf[:header_end]))
            total = header_end + 4 + content_length
            if len(buf) >= total:
                return bytes(buf[:total])
    raise TimeoutError("incomplete HTTP message before deadline")


def _content_length(header_blob: bytes) -> int:
    for line in header_blob.split(b"\r\n")[1:]:
        if b":" not in line:
            continue
        name, value = line.split(b":", 1)
        if name.strip().lower() == b"content-length":
            return max(0, int(value.strip() or b"0"))
    return 0


def _parse_http_response(raw: bytes) -> BufferedHttpResponse:
    header_end = raw.find(b"\r\n\r\n")
    if header_end < 0:
        raise ValueError("response missing header terminator")
    header_blob = raw[:header_end]
    body = raw[header_end + 4 :]
    lines = header_blob.split(b"\r\n")
    status_line = lines[0].decode("latin-1")
    parts = status_line.split(" ", 2)
    status_code = int(parts[1]) if len(parts) >= 2 else 0
    headers: dict[str, str] = {}
    for line in lines[1:]:
        if b":" not in line:
            continue
        name, value = line.split(b":", 1)
        headers[name.decode("latin-1").strip().lower()] = value.decode("latin-1").strip()
    return BufferedHttpResponse(
        status_line=status_line,
        status_code=status_code,
        headers=headers,
        body=body,
    )


def _rewrite_request_target(raw_request: bytes, upstream_base_path: str) -> bytes:
    """Forward request bytes; adjust absolute-form targets if needed."""
    header_end = raw_request.find(b"\r\n\r\n")
    if header_end < 0:
        return raw_request
    start_line, rest = raw_request.split(b"\r\n", 1)
    pieces = start_line.split(b" ", 2)
    if len(pieces) != 3:
        return raw_request
    method, target, version = pieces
    if target.startswith(b"http://") or target.startswith(b"https://"):
        # origin-form for upstream
        parsed = urlsplit(target.decode("latin-1"))
        path = parsed.path or "/"
        if parsed.query:
            path = f"{path}?{parsed.query}"
        target = path.encode("latin-1")
    if upstream_base_path and upstream_base_path not in {"", "/"}:
        # Upstream listen URL already includes host:port only; path is absolute.
        pass
    new_start = b" ".join((method, target, version))
    return new_start + b"\r\n" + rest
