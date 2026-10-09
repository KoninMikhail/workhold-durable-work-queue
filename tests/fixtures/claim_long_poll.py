"""Shared deterministic seams for Phase 20.1 long-poll verification (Wave 0).

Fixtures expose event/barrier coordination, recording-server delayed response and
disconnect behavior, and resource probes without leaking payloads, bearer/claim
tokens, worker IDs, or raw URLs in diagnostics.
"""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

import pytest

# Sentinel values for secret-scan assertions in scaffold tests.
PAYLOAD_SENTINEL = "LP_PAYLOAD_SENTINEL_do_not_log_9f3a"
CLAIM_TOKEN_SENTINEL = "LP_CLAIM_TOKEN_SENTINEL_do_not_log_7c2b"
BEARER_SENTINEL = "LP_BEARER_SENTINEL_do_not_log_4d5e"
WORKER_ID_SENTINEL = "LP_WORKER_ID_SENTINEL_do_not_log_1a2b"

FORBIDDEN_DIAGNOSTIC_SUBSTRINGS: tuple[str, ...] = (
    PAYLOAD_SENTINEL,
    CLAIM_TOKEN_SENTINEL,
    BEARER_SENTINEL,
    WORKER_ID_SENTINEL,
)


@dataclass
class FakeMonotonicClock:
    """Deterministic monotonic clock for generation/deadline tests."""

    _now: float = 0.0

    def monotonic(self) -> float:
        return self._now

    def advance(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError("advance must be non-negative")
        self._now += seconds


@dataclass
class GenerationBarrier:
    """Queue-generation coordination seam for observe/check/wait tests."""

    generation: int = 0
    _changed: threading.Event = field(default_factory=threading.Event)

    def snapshot(self) -> int:
        return self.generation

    def bump(self) -> None:
        self.generation += 1
        self._changed.set()
        self._changed = threading.Event()

    def wait_for_change(
        self,
        *,
        observed: int,
        timeout: float,
    ) -> bool:
        if self.generation != observed:
            return True
        return self._changed.wait(timeout=timeout)


@dataclass
class PoolCheckoutProbe:
    """Tracks pooled SQLAlchemy checkouts during a long-poll wait interval."""

    checked_out: int = 0
    peak: int = 0

    def checkout(self) -> None:
        self.checked_out += 1
        self.peak = max(self.peak, self.checked_out)

    def checkin(self) -> None:
        self.checked_out = max(0, self.checked_out - 1)

    def assert_idle_during_wait(self) -> None:
        assert self.checked_out == 0, (
            f"expected zero pooled checkouts during wait, saw {self.checked_out}"
        )


@dataclass
class ListenerConnectionProbe:
    """Tracks dedicated autocommit LISTEN connections for the API replica."""

    active: int = 0
    peak: int = 0

    def open_listener(self) -> None:
        self.active += 1
        self.peak = max(self.peak, self.active)

    def close_listener(self) -> None:
        self.active = max(0, self.active - 1)

    def assert_one_per_replica(self, replica_count: int = 1) -> None:
        assert self.peak == replica_count, (
            f"expected {replica_count} dedicated listener(s), peak={self.peak}"
        )


def assert_no_forbidden_diagnostics(text: str) -> None:
    for forbidden in FORBIDDEN_DIAGNOSTIC_SUBSTRINGS:
        assert forbidden not in text, f"diagnostic leaked forbidden value: {forbidden!r}"


class _LongPollRecordingHandler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return

    def _read_body(self) -> bytes:
        length = int(self.headers.get("Content-Length", "0") or "0")
        return self.rfile.read(length) if length else b""

    def _record(self, body: bytes) -> None:
        self.server.recorded.append(  # type: ignore[attr-defined]
            {
                "method": self.command,
                "path": self.path.split("?", 1)[0],
                "headers": {k.lower(): v for k, v in self.headers.items()},
                "body": body,
            }
        )

    def _respond(self, status: int, payload: Mapping[str, Any] | None) -> None:
        raw = b"" if payload is None else json.dumps(dict(payload)).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        if raw:
            self.wfile.write(raw)

    def _dispatch(self) -> None:
        body = self._read_body()
        self._record(body)
        path = self.path.split("?", 1)[0]
        responder = self.server.routes.get((self.command, path))  # type: ignore[attr-defined]
        if responder is None:
            self._respond(
                404,
                {
                    "code": "validation_failed",
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


@dataclass
class LongPollRecordingServer:
    """Loopback server with delayed empty claim, task grant, or abrupt disconnect."""

    server: HTTPServer
    thread: threading.Thread
    base_url: str
    recorded: list[dict[str, Any]]

    def set_route(
        self,
        method: str,
        path: str,
        responder: Callable[[bytes, Mapping[str, str]], tuple[int, Mapping[str, Any] | None]],
    ) -> None:
        self.server.routes[(method.upper(), path)] = responder  # type: ignore[attr-defined]

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)


def empty_claim_response(*, wait_seconds: int = 0) -> dict[str, Any]:
    return {
        "tasks": [],
        "server_time": "2026-09-22T00:00:00Z",
        "recommended_heartbeat_seconds": 30,
        "queue_states": {},
        "wait_seconds": wait_seconds,
    }


def delayed_empty_claim_responder(
    *,
    delay_seconds: float,
    release: threading.Event | None = None,
    clock: FakeMonotonicClock | None = None,
) -> Callable[[bytes, Mapping[str, str]], tuple[int, Mapping[str, Any] | None]]:
    """Return a claim handler that waits on ``release`` or sleeps ``delay_seconds``."""

    def _respond(_body: bytes, _headers: Mapping[str, str]) -> tuple[int, Mapping[str, Any] | None]:
        if release is not None:
            if not release.wait(timeout=max(delay_seconds, 0.01)):
                pass
        elif clock is not None:
            deadline = clock.monotonic() + delay_seconds
            while clock.monotonic() < deadline:
                pass
        else:
            time.sleep(delay_seconds)
        return 200, empty_claim_response()

    return _respond


def disconnect_responder() -> Callable[[bytes, Mapping[str, str]], tuple[int, Mapping[str, Any] | None]]:
    """Simulate abrupt handler abort without emitting a response body."""

    def _respond(_body: bytes, _headers: Mapping[str, str]) -> tuple[int, Mapping[str, Any] | None]:
        raise ConnectionAbortedError("simulated client disconnect")

    return _respond


@pytest.fixture
def fake_monotonic_clock() -> FakeMonotonicClock:
    return FakeMonotonicClock()


@pytest.fixture
def generation_barrier() -> GenerationBarrier:
    return GenerationBarrier()


@pytest.fixture
def pool_checkout_probe() -> PoolCheckoutProbe:
    return PoolCheckoutProbe()


@pytest.fixture
def listener_connection_probe() -> ListenerConnectionProbe:
    return ListenerConnectionProbe()


@pytest.fixture
def long_poll_recording_server() -> Iterator[LongPollRecordingServer]:
    server = HTTPServer(("127.0.0.1", 0), _LongPollRecordingHandler)
    server.recorded = []  # type: ignore[attr-defined]
    server.routes = {}  # type: ignore[attr-defined]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address[:2]
    handle = LongPollRecordingServer(
        server=server,
        thread=thread,
        base_url=f"http://{host}:{port}",
        recorded=server.recorded,  # type: ignore[attr-defined]
    )
    try:
        yield handle
    finally:
        handle.close()


# Rows from 20.1-VALIDATION.md deterministic matrix (Wave 0 scaffold registry).
WAVE0_SCENARIO_IDS: tuple[str, ...] = (
    "wait_seconds_zero_immediate",
    "successful_empty_expiry",
    "transport_timeout_distinct",
    "client_cancellation_distinct",
    "generation_race_before_wait",
    "generation_race_after_empty_attempt",
    "fallback_reconciliation_tick",
    "shutdown_during_wait",
    "disconnect_before_arrival",
    "waiter_cap_admission",
    "pool_checkout_idle_during_wait",
    "dedicated_listener_one_per_replica",
    "raw_sync_async_parity",
    "two_replica_wake",
    "two_waiters_one_task",
)
