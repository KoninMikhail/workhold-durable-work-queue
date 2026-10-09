"""Deterministic local HTTP Delivery sink for chaos / integration tests.

Records bounded metadata and digests only — never auth credentials or raw
payload dumps in failure output. Optional inbox mode applies one logical effect
per stable CloudEvents ``id`` (consumer-side deduplication for at-least-once
publication evidence).
"""

from __future__ import annotations

import hashlib
import json
import threading
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any


@dataclass(frozen=True, slots=True)
class DeliveryAttemptRecord:
    """One observed POST without retaining secrets or full payload text."""

    received_at_mono: float
    content_type: str | None
    event_id: str | None
    body_sha256: str
    body_len: int
    path: str


@dataclass
class HttpDeliverySink:
    """Loopback HTTP sink with optional inbox-style unique-ID effects."""

    inbox_mode: bool = True
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    attempts: list[DeliveryAttemptRecord] = field(default_factory=list)
    _inbox_applied: set[str] = field(default_factory=set, repr=False)
    logical_effects: int = 0
    _server: HTTPServer | None = field(default=None, repr=False)
    _thread: threading.Thread | None = field(default=None, repr=False)
    _hold_first_response: threading.Event | None = field(default=None, repr=False)
    _first_request_seen: threading.Event = field(
        default_factory=threading.Event, repr=False
    )

    @property
    def base_url(self) -> str:
        if self._server is None:
            raise RuntimeError("sink is not started")
        host, port = self._server.server_address[:2]
        return f"http://{host}:{port}/delivery"

    @property
    def attempt_count(self) -> int:
        with self._lock:
            return len(self.attempts)

    def start(self) -> None:
        if self._server is not None:
            raise RuntimeError("sink already started")
        sink = self

        class _Handler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
                return

            def do_POST(self) -> None:  # noqa: N802
                length = int(self.headers.get("Content-Length", "0") or "0")
                body = self.rfile.read(length) if length else b""
                content_type = self.headers.get("Content-Type")
                # Intentionally ignore Authorization — never record secrets.
                event_id: str | None = None
                try:
                    parsed = json.loads(body.decode("utf-8"))
                    if isinstance(parsed, dict):
                        raw_id = parsed.get("id")
                        if isinstance(raw_id, str):
                            event_id = raw_id
                except (UnicodeDecodeError, json.JSONDecodeError):
                    event_id = None
                digest = hashlib.sha256(body).hexdigest()
                record = DeliveryAttemptRecord(
                    received_at_mono=time.monotonic(),
                    content_type=content_type,
                    event_id=event_id,
                    body_sha256=digest,
                    body_len=len(body),
                    path=self.path,
                )
                with sink._lock:
                    sink.attempts.append(record)
                    if sink.inbox_mode and event_id is not None:
                        if event_id not in sink._inbox_applied:
                            sink._inbox_applied.add(event_id)
                            sink.logical_effects += 1
                    elif not sink.inbox_mode:
                        sink.logical_effects += 1
                sink._first_request_seen.set()
                hold = sink._hold_first_response
                if hold is not None and len(sink.attempts) == 1:
                    hold.wait(timeout=30.0)
                self.send_response(200)
                self.send_header("Content-Length", "0")
                self.end_headers()

        server = HTTPServer(("127.0.0.1", 0), _Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self._server = server
        self._thread = thread

    def stop(self) -> None:
        server = self._server
        thread = self._thread
        self._server = None
        self._thread = None
        if server is not None:
            server.shutdown()
            server.server_close()
        if thread is not None:
            thread.join(timeout=2.0)

    def arm_hold_first_response(self) -> threading.Event:
        """Block the first 2xx until the returned event is set (optional races)."""
        gate = threading.Event()
        self._hold_first_response = gate
        return gate

    def wait_attempts(self, n: int, *, timeout: float = 30.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.attempt_count >= n:
                return
            time.sleep(0.05)
        raise TimeoutError(
            f"expected {n} sink attempts within {timeout}s; got {self.attempt_count}"
        )

    def bodies_byte_equivalent(self) -> bool:
        with self._lock:
            if len(self.attempts) < 2:
                return False
            digests = {a.body_sha256 for a in self.attempts}
            ids = {a.event_id for a in self.attempts}
        return len(digests) == 1 and len(ids) == 1 and None not in ids

    def __enter__(self) -> HttpDeliverySink:
        self.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.stop()


def http_delivery_sink(*, inbox_mode: bool = True) -> Iterator[HttpDeliverySink]:
    """Context-manager helper for tests that prefer ``with`` over fixtures."""
    sink = HttpDeliverySink(inbox_mode=inbox_mode)
    sink.start()
    try:
        yield sink
    finally:
        sink.stop()
