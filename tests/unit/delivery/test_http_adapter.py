"""Unit tests for secured HTTP DeliveryTransport (DLVR-03 / DLVR-04 / ADR 018)."""

from __future__ import annotations

import asyncio
import ipaddress
import json
import logging
import socket
import threading
import time
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any
from uuid import uuid4

import pytest

from workhold.delivery.cloudevents import CLOUDEVENTS_JSON_MEDIA_TYPE
from workhold.delivery.relay import DeliveryDisposition
from workhold.delivery.repository import ClaimedDeliveryEvent
from workhold.delivery.transports.http import (
    HttpDeliveryConfig,
    HttpDeliveryTransport,
)
from workhold.settings import EnvironmentMode, Secret


class _SinkHandler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length) if length else b""
        self.server.recorded.append(  # type: ignore[attr-defined]
            {
                "method": self.command,
                "path": self.path,
                "headers": {k.lower(): v for k, v in self.headers.items()},
                "body": body,
            }
        )
        status = getattr(self.server, "status", 200)
        response_body = getattr(self.server, "response_body", b"")
        headers = dict(getattr(self.server, "response_headers", {}))
        sleep_s = float(getattr(self.server, "sleep_seconds", 0.0))
        if sleep_s > 0:
            time.sleep(sleep_s)
        self.send_response(status)
        for key, value in headers.items():
            self.send_header(key, value)
        self.send_header("Content-Length", str(len(response_body)))
        self.end_headers()
        if response_body:
            self.wfile.write(response_body)


@pytest.fixture
def http_sink() -> Any:
    server = HTTPServer(("127.0.0.1", 0), _SinkHandler)
    server.recorded = []  # type: ignore[attr-defined]
    server.status = 200  # type: ignore[attr-defined]
    server.response_body = b""  # type: ignore[attr-defined]
    server.response_headers = {}  # type: ignore[attr-defined]
    server.sleep_seconds = 0.0  # type: ignore[attr-defined]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2.0)


def _claimed(*, envelope: dict[str, Any] | None = None) -> ClaimedDeliveryEvent:
    eid = uuid4()
    now = datetime.now(UTC)
    body = envelope or {
        "id": str(eid),
        "source": "urn:test:queue",
        "specversion": "1.0",
        "type": "com.example.done.v1",
        "time": "2026-09-19T12:00:00Z",
        "data": {"secret_payload": "SHOULD_NOT_LEAK"},
    }
    return ClaimedDeliveryEvent(
        event_id=eid,
        claim_token=uuid4(),
        generation=1,
        envelope=body,
        relay_principal_id="relay-test",
        delivery_attempt=1,
        claimed_at=now,
        lease_expires_at=now + timedelta(seconds=30),
        available_at=now,
        source_task_id=uuid4(),
        ordinal=0,
    )


def _dev_config(url: str, **overrides: Any) -> HttpDeliveryConfig:
    base: dict[str, Any] = dict(
        webhook_url=url,
        environment=EnvironmentMode.DEVELOPMENT,
        allowed_hosts=frozenset({"127.0.0.1", "localhost"}),
        allowed_cidrs=(
            ipaddress.ip_network("127.0.0.0/8"),
            ipaddress.ip_network("::1/128"),
        ),
        connect_timeout_seconds=1.0,
        read_timeout_seconds=1.0,
        total_timeout_seconds=2.0,
        max_response_bytes=4096,
        bearer_token=Secret("super-secret-token"),
        circuit_failure_threshold=3,
        circuit_success_threshold=1,
        circuit_open_seconds=0.5,
        half_open_max_probes=1,
        retry_after_cap_seconds=30.0,
    )
    base.update(overrides)
    return HttpDeliveryConfig(**base)


def test_posts_stored_cloudevents_json_with_stable_body(http_sink: Any) -> None:
    host, port = http_sink.server_address[:2]
    url = f"http://{host}:{port}/hooks/delivery"
    transport = HttpDeliveryTransport(_dev_config(url))
    event = _claimed()
    expected = json.dumps(
        dict(event.envelope),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    ).encode("utf-8")

    first = asyncio.run(transport.publish(event))
    second = asyncio.run(transport.publish(event))

    assert first.disposition is DeliveryDisposition.ACKNOWLEDGED
    assert second.disposition is DeliveryDisposition.ACKNOWLEDGED
    assert len(http_sink.recorded) == 2
    for rec in http_sink.recorded:
        assert rec["method"] == "POST"
        assert rec["path"] == "/hooks/delivery"
        assert rec["headers"]["content-type"] == CLOUDEVENTS_JSON_MEDIA_TYPE
        assert "ce-" not in " ".join(rec["headers"])
        assert rec["body"] == expected
        assert rec["headers"].get("authorization") == "Bearer super-secret-token"


def test_event_cannot_select_destination_or_auth(http_sink: Any) -> None:
    host, port = http_sink.server_address[:2]
    configured = f"http://{host}:{port}/configured"
    transport = HttpDeliveryTransport(_dev_config(configured))
    event = _claimed(
        envelope={
            "id": str(uuid4()),
            "source": "urn:evil",
            "specversion": "1.0",
            "type": "t.v1",
            "webhook_url": "http://evil.example/steal",
            "authorization": "Bearer stolen",
            "url": "http://evil.example/steal",
        }
    )
    result = asyncio.run(transport.publish(event))
    assert result.disposition is DeliveryDisposition.ACKNOWLEDGED
    assert len(http_sink.recorded) == 1
    assert http_sink.recorded[0]["path"] == "/configured"
    assert http_sink.recorded[0]["headers"].get("authorization") == (
        "Bearer super-secret-token"
    )


def test_production_rejects_non_https_userinfo_fragment_and_disallowed_hosts() -> None:
    with pytest.raises(ValueError, match="https|scheme"):
        _dev_config(
            "http://example.com/hook",
            environment=EnvironmentMode.PRODUCTION,
            allowed_hosts=frozenset({"example.com"}),
            allowed_cidrs=(ipaddress.ip_network("0.0.0.0/0"),),
        )
    with pytest.raises(ValueError, match="userinfo|credentials"):
        _dev_config(
            "https://user:pass@example.com/hook",
            allowed_hosts=frozenset({"example.com"}),
            allowed_cidrs=(ipaddress.ip_network("0.0.0.0/0"),),
        )
    with pytest.raises(ValueError, match="fragment"):
        _dev_config("http://127.0.0.1/hook#frag")
    with pytest.raises(ValueError, match="host|allowlist"):
        _dev_config(
            "http://evil.example/hook",
            allowed_hosts=frozenset({"127.0.0.1"}),
        )


def test_mixed_dns_results_fail_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    def _mixed(
        host: str,
        port: int,
        *args: Any,
        **kwargs: Any,
    ) -> list[tuple[Any, ...]]:
        return [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", port)),
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.8.8", port)),
        ]

    monkeypatch.setattr(socket, "getaddrinfo", _mixed)
    cfg = _dev_config(
        "http://mixed.example/hook",
        allowed_hosts=frozenset({"mixed.example"}),
        allowed_cidrs=(ipaddress.ip_network("127.0.0.0/8"),),
    )
    with pytest.raises(ValueError, match="DNS|allowlist|resolved|mixed"):
        HttpDeliveryTransport(cfg)


def test_redirects_are_permanent_not_followed(http_sink: Any) -> None:
    host, port = http_sink.server_address[:2]
    http_sink.status = 302
    http_sink.response_headers = {"Location": "http://evil.example/steal"}
    transport = HttpDeliveryTransport(_dev_config(f"http://{host}:{port}/start"))
    result = asyncio.run(transport.publish(_claimed()))
    assert result.disposition is DeliveryDisposition.PERMANENT
    assert result.failure_code is not None
    assert "redirect" in result.failure_code or "3xx" in result.failure_code
    assert len(http_sink.recorded) == 1


@pytest.mark.parametrize(
    ("status", "disposition"),
    [
        (200, DeliveryDisposition.ACKNOWLEDGED),
        (204, DeliveryDisposition.ACKNOWLEDGED),
        (408, DeliveryDisposition.RETRYABLE),
        (425, DeliveryDisposition.RETRYABLE),
        (429, DeliveryDisposition.RETRYABLE),
        (500, DeliveryDisposition.RETRYABLE),
        (503, DeliveryDisposition.RETRYABLE),
        (400, DeliveryDisposition.PERMANENT),
        (401, DeliveryDisposition.PERMANENT),
        (404, DeliveryDisposition.PERMANENT),
        (301, DeliveryDisposition.PERMANENT),
    ],
)
def test_status_classification(
    http_sink: Any, status: int, disposition: DeliveryDisposition
) -> None:
    host, port = http_sink.server_address[:2]
    http_sink.status = status
    transport = HttpDeliveryTransport(_dev_config(f"http://{host}:{port}/x"))
    result = asyncio.run(transport.publish(_claimed()))
    assert result.disposition is disposition


def test_timeout_is_retryable(http_sink: Any) -> None:
    host, port = http_sink.server_address[:2]
    http_sink.sleep_seconds = 2.0
    transport = HttpDeliveryTransport(
        _dev_config(
            f"http://{host}:{port}/slow",
            connect_timeout_seconds=0.2,
            read_timeout_seconds=0.2,
            total_timeout_seconds=0.3,
        )
    )
    result = asyncio.run(transport.publish(_claimed()))
    assert result.disposition is DeliveryDisposition.RETRYABLE
    assert result.failure_code in {
        "publish.uncertain",
        "http.timeout",
        "http.network",
    }


def test_retry_after_bounded_and_invalid_discarded(http_sink: Any) -> None:
    host, port = http_sink.server_address[:2]
    url = f"http://{host}:{port}/ra"
    transport = HttpDeliveryTransport(
        _dev_config(
            url,
            retry_after_cap_seconds=10.0,
            circuit_failure_threshold=100,
        )
    )

    http_sink.status = 429
    http_sink.response_headers = {"Retry-After": "5"}
    ok = asyncio.run(transport.publish(_claimed()))
    assert ok.disposition is DeliveryDisposition.RETRYABLE
    assert ok.retry_after_seconds == 5.0

    http_sink.response_headers = {"Retry-After": "999"}
    capped = asyncio.run(transport.publish(_claimed()))
    assert capped.retry_after_seconds == 10.0

    http_sink.response_headers = {"Retry-After": "-1"}
    neg = asyncio.run(transport.publish(_claimed()))
    assert neg.retry_after_seconds is None

    http_sink.response_headers = {"Retry-After": "not-a-date"}
    bad = asyncio.run(transport.publish(_claimed()))
    assert bad.retry_after_seconds is None

    future = format_datetime(datetime.now(UTC) + timedelta(seconds=3), usegmt=True)
    http_sink.response_headers = {"Retry-After": future}
    dated = asyncio.run(transport.publish(_claimed()))
    assert dated.retry_after_seconds is not None
    assert 0.0 <= dated.retry_after_seconds <= 10.0


def test_circuit_open_blocks_readiness_half_open_probe_bound(http_sink: Any) -> None:
    host, port = http_sink.server_address[:2]
    transport = HttpDeliveryTransport(
        _dev_config(
            f"http://{host}:{port}/cb",
            circuit_failure_threshold=2,
            circuit_open_seconds=0.3,
            half_open_max_probes=1,
            circuit_success_threshold=1,
        )
    )
    http_sink.status = 503
    assert asyncio.run(transport.readiness()).accepting is True
    asyncio.run(transport.publish(_claimed()))
    asyncio.run(transport.publish(_claimed()))
    ready = asyncio.run(transport.readiness())
    assert ready.accepting is False
    assert ready.reason_code == "circuit_open"
    assert ready.retry_after_seconds is not None
    assert ready.retry_after_seconds <= 0.3 + 0.05

    time.sleep(0.35)
    half = asyncio.run(transport.readiness())
    assert half.accepting is True

    http_sink.sleep_seconds = 0.4
    http_sink.status = 200

    async def _exercise() -> None:
        task = asyncio.create_task(transport.publish(_claimed()))
        await asyncio.sleep(0.05)
        blocked = await transport.readiness()
        assert blocked.accepting is False
        assert blocked.reason_code in {
            "circuit_half_open_busy",
            "circuit_open",
        }
        await task
        closed = await transport.readiness()
        assert closed.accepting is True

    asyncio.run(_exercise())


def test_response_body_bounded_and_secrets_redacted(
    http_sink: Any, caplog: pytest.LogCaptureFixture
) -> None:
    host, port = http_sink.server_address[:2]
    http_sink.status = 500
    http_sink.response_body = b"x" * 20_000 + b"super-secret-token"
    transport = HttpDeliveryTransport(
        _dev_config(
            f"http://{host}:{port}/big?token=query-secret",
            max_response_bytes=64,
            bearer_token=Secret("super-secret-token"),
        )
    )
    with caplog.at_level(logging.DEBUG):
        result = asyncio.run(transport.publish(_claimed()))
    assert result.disposition is DeliveryDisposition.RETRYABLE
    blob = "\n".join(r.getMessage() for r in caplog.records) + repr(result)
    assert "super-secret-token" not in blob
    assert "query-secret" not in blob
    assert "SHOULD_NOT_LEAK" not in blob
    assert "x" * 100 not in blob


def test_config_repr_redacts_secrets() -> None:
    cfg = _dev_config("http://127.0.0.1:9/hook")
    text = repr(cfg) + str(cfg)
    assert "super-secret-token" not in text
