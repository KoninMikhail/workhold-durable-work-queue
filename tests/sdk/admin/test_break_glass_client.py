"""BreakGlassClient recording-server tests (SDK-07 / SDK-08 / REC-03)."""

from __future__ import annotations

import inspect
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any
from urllib.parse import unquote, urlparse

import pytest

from _queue_service_client_core.errors import MalformedResponseError, ProtocolError
from _queue_service_client_core.transport import HttpJsonTransport
import queue_service_admin as admin_pkg
from queue_service_admin import AdminClient, BreakGlassClient, ObserverClient


BREAK_GLASS_METHODS = (
    "force_lease_expiry",
    "force_delivery_reclaim",
    "force_delivery_dead_letter",
    "reconcile_counters",
    "raise_replay_limit",
    "drop_expired_partition",
    "repair_registry_entry",
)

_ACK = {
    "reason": "stuck lease blocking drain",
    "incident_reference": "INC-4242",
    "risk_acknowledged": True,
}


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
                "headers": headers,
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

    def do_POST(self) -> None:  # noqa: N802
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


def _client(base_url: str, *, token: str = "break-glass-jit-token") -> BreakGlassClient:
    transport = HttpJsonTransport(base_url, timeout_s=2.0)
    return BreakGlassClient(transport, bearer_token=token)


def _mutation_body(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "operation": "forceLeaseExpiry",
        "queue": "orders",
        "target_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "outcome": "applied",
        "generation": 3,
    }
    body.update(overrides)
    return body


def test_package_exports_break_glass_surface() -> None:
    assert "BreakGlassClient" in admin_pkg.__all__
    assert admin_pkg.BreakGlassClient is BreakGlassClient
    for name in BREAK_GLASS_METHODS:
        assert hasattr(BreakGlassClient, name)
        assert callable(getattr(BreakGlassClient, name))


def test_break_glass_client_cannot_be_subclassed() -> None:
    with pytest.raises(TypeError, match="cannot be subclassed"):

        class _EvilBreakGlass(BreakGlassClient):  # type: ignore[misc, valid-type]
            pass


def test_admin_and_observer_lack_break_glass_methods() -> None:
    admin_methods = {
        name
        for name, member in inspect.getmembers(AdminClient, predicate=inspect.isfunction)
        if not name.startswith("_")
    }
    observer_methods = {
        name
        for name, member in inspect.getmembers(ObserverClient, predicate=inspect.isfunction)
        if not name.startswith("_")
    }
    for name in BREAK_GLASS_METHODS:
        assert name not in admin_methods
        assert name not in observer_methods
        assert not hasattr(AdminClient, name)
        assert not hasattr(ObserverClient, name)


def test_force_lease_expiry_wire_contract(recording_server: Any) -> None:
    server, base_url = recording_server
    task_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    path = f"/admin/v1/queues/orders/tasks/{task_id}:force-lease-expiry"
    server.routes[("POST", path)] = (
        lambda body, headers: (200, _mutation_body(operation="forceLeaseExpiry"))
    )
    result = _client(base_url).force_lease_expiry(
        "orders",
        task_id,
        **_ACK,
    )
    assert result.operation == "forceLeaseExpiry"
    assert result.generation == 3
    rec = server.recorded[0]
    assert rec["path"] == path
    payload = json.loads(rec["body"].decode("utf-8"))
    assert payload["task_id"] == task_id
    assert payload["risk_acknowledged"] is True


def test_force_delivery_reclaim_wire_contract(recording_server: Any) -> None:
    server, base_url = recording_server
    event_id = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
    path = f"/admin/v1/queues/orders/delivery-events/{event_id}:force-reclaim"
    server.routes[("POST", path)] = (
        lambda body, headers: (
            200,
            _mutation_body(
                operation="forceDeliveryReclaim",
                target_id=event_id,
                generation=2,
            ),
        )
    )
    result = _client(base_url).force_delivery_reclaim("orders", event_id, **_ACK)
    assert result.operation == "forceDeliveryReclaim"
    assert result.generation == 2
    payload = json.loads(server.recorded[0]["body"].decode("utf-8"))
    assert "failure_code" not in payload


def test_force_delivery_dead_letter_default_failure_code(recording_server: Any) -> None:
    server, base_url = recording_server
    event_id = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
    path = f"/admin/v1/queues/orders/delivery-events/{event_id}:force-dead-letter"
    server.routes[("POST", path)] = (
        lambda body, headers: (
            200,
            _mutation_body(operation="forceDeliveryDeadLetter", target_id=event_id),
        )
    )
    _client(base_url).force_delivery_dead_letter("orders", event_id, **_ACK)
    payload = json.loads(server.recorded[0]["body"].decode("utf-8"))
    assert payload["failure_code"] == "break_glass_force_dead_letter"


def test_reconcile_counters_wire_contract(recording_server: Any) -> None:
    server, base_url = recording_server
    path = "/admin/v1/queues/orders:reconcile-counters"
    server.routes[("POST", path)] = (
        lambda body, headers: (
            200,
            {
                "operation": "reconcileCounters",
                "queue": "orders",
                "target_id": "orders",
                "outcome": "applied",
                "delayed_count": 1,
                "ready_count": 2,
                "leased_count": 0,
            },
        )
    )
    result = _client(base_url).reconcile_counters("orders", **_ACK)
    assert result.ready_count == 2
    assert result.leased_count == 0


def test_raise_replay_limit_bounds_on_wire(recording_server: Any) -> None:
    server, base_url = recording_server
    path = "/admin/v1/queues/orders:raise-replay-limit"
    server.routes[("POST", path)] = (
        lambda body, headers: (
            200,
            {
                "operation": "raiseReplayLimit",
                "queue": "orders",
                "target_id": "orders",
                "outcome": "applied",
                "effective_rps": 20.0,
            },
        )
    )
    result = _client(base_url).raise_replay_limit(
        "orders",
        factor=5.0,
        ttl_seconds=120,
        **_ACK,
    )
    assert result.effective_rps == 20.0
    payload = json.loads(server.recorded[0]["body"].decode("utf-8"))
    assert payload["factor"] == 5.0
    assert payload["ttl_seconds"] == 120


def test_drop_expired_partition_wire_contract(recording_server: Any) -> None:
    server, base_url = recording_server
    partition = "task_attempt_20260919"
    path = f"/admin/v1/partitions/{partition}:force-drop"
    server.routes[("POST", path)] = (
        lambda body, headers: (
            200,
            _mutation_body(
                operation="dropExpiredPartition",
                queue=None,
                target_id=partition,
                generation=None,
            ),
        )
    )
    result = _client(base_url).drop_expired_partition(partition, **_ACK)
    assert result.target_id == partition
    assert server.recorded[0]["path"] == path


def test_repair_registry_entry_wire_contract(recording_server: Any) -> None:
    server, base_url = recording_server
    path = "/admin/v1/queues/orders/registry:repair"
    server.routes[("POST", path)] = (
        lambda body, headers: (
            200,
            _mutation_body(operation="repairRegistryEntry", target_id="42"),
        )
    )
    _client(base_url).repair_registry_entry(
        "orders",
        entry_id=42,
        acknowledge_duplicate_window=True,
        extend_seconds=3600,
        **_ACK,
    )
    payload = json.loads(server.recorded[0]["body"].decode("utf-8"))
    assert payload["entry_id"] == 42
    assert payload["registry"] == "enqueue_dedup"
    assert payload["acknowledge_duplicate_window"] is True
    assert payload["extend_seconds"] == 3600


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("reason", ""),
        ("incident_reference", ""),
        ("risk_acknowledged", False),
    ],
)
def test_ack_triad_rejected_before_transport(
    recording_server: Any,
    field: str,
    value: object,
) -> None:
    server, base_url = recording_server
    ack = dict(_ACK)
    ack[field] = value
    client = _client(base_url)
    with pytest.raises(ValueError):
        client.reconcile_counters("orders", **ack)  # type: ignore[arg-type]
    assert server.recorded == []


@pytest.mark.parametrize("factor", [0.5, 10.1, True])
def test_replay_factor_rejected_before_transport(
    recording_server: Any,
    factor: object,
) -> None:
    server, base_url = recording_server
    with pytest.raises(ValueError):
        _client(base_url).raise_replay_limit("orders", factor=factor, **_ACK)  # type: ignore[arg-type]
    assert server.recorded == []


@pytest.mark.parametrize("ttl_seconds", [0, 3601, True])
def test_replay_ttl_rejected_before_transport(
    recording_server: Any,
    ttl_seconds: object,
) -> None:
    server, base_url = recording_server
    with pytest.raises(ValueError):
        _client(base_url).raise_replay_limit(
            "orders",
            ttl_seconds=ttl_seconds,  # type: ignore[arg-type]
            **_ACK,
        )
    assert server.recorded == []


def test_partition_name_pattern_rejected_before_transport(recording_server: Any) -> None:
    server, base_url = recording_server
    with pytest.raises(ValueError, match="pattern"):
        _client(base_url).drop_expired_partition("BadPartition", **_ACK)
    assert server.recorded == []


def test_repair_registry_requires_duplicate_window_ack(recording_server: Any) -> None:
    server, base_url = recording_server
    with pytest.raises(ValueError, match="acknowledge_duplicate_window"):
        _client(base_url).repair_registry_entry(
            "orders",
            entry_id=1,
            acknowledge_duplicate_window=False,
            **_ACK,
        )
    assert server.recorded == []


@pytest.mark.parametrize(
    "secret_key",
    ["claimToken", "worker_claim_token", "lease_token"],
)
def test_response_rejects_claim_token_aliases(
    recording_server: Any, secret_key: str
) -> None:
    server, base_url = recording_server
    path = "/admin/v1/queues/orders:reconcile-counters"
    server.routes[("POST", path)] = (
        lambda body, headers, key=secret_key: (
            200,
            {
                "operation": "reconcileCounters",
                "queue": "orders",
                "target_id": "orders",
                "outcome": "applied",
                "delayed_count": 0,
                "ready_count": 0,
                "leased_count": 0,
                key: "must-not-surface",
            },
        )
    )
    with pytest.raises(MalformedResponseError, match=secret_key):
        _client(base_url).reconcile_counters("orders", **_ACK)


def test_response_rejects_claim_token(recording_server: Any) -> None:
    server, base_url = recording_server
    path = "/admin/v1/queues/orders:reconcile-counters"
    server.routes[("POST", path)] = (
        lambda body, headers: (
            200,
            {
                "operation": "reconcileCounters",
                "queue": "orders",
                "target_id": "orders",
                "outcome": "applied",
                "delayed_count": 0,
                "ready_count": 0,
                "leased_count": 0,
                "claim_token": "must-not-surface",
            },
        )
    )
    with pytest.raises(MalformedResponseError, match="claim_token"):
        _client(base_url).reconcile_counters("orders", **_ACK)


def test_error_and_repr_never_leak_secrets(recording_server: Any) -> None:
    server, base_url = recording_server
    secret = "super-secret-break-glass-token"
    reason = "operator reason must not leak in diagnostics"
    incident = "INC-SECRET-999"
    path = "/admin/v1/queues/orders:reconcile-counters"
    server.routes[("POST", path)] = (
        lambda body, headers: (
            403,
            {
                "code": "permission_denied",
                "message": "denied",
                "retryable": False,
                "request_id": "22222222-2222-4222-8222-222222222222",
                "details": {},
            },
        )
    )
    client = _client(base_url, token=secret)
    with pytest.raises(ProtocolError) as exc_info:
        client.reconcile_counters(
            "orders",
            reason=reason,
            incident_reference=incident,
            risk_acknowledged=True,
        )
    blob = f"{exc_info.value!s}{exc_info.value!r}{client!r}{client!s}"
    assert secret not in blob
    assert reason not in blob
    assert incident not in blob
    assert "redacted" in repr(client)


@pytest.mark.parametrize("token", [None, "", "   ", "\t", "\n"])
def test_constructor_requires_token(token: str | None) -> None:
    transport = HttpJsonTransport("http://127.0.0.1:9", timeout_s=0.1)
    with pytest.raises(ValueError, match="bearer_token"):
        BreakGlassClient(transport, bearer_token=token)  # type: ignore[arg-type]


def test_bearer_token_preserved_exactly() -> None:
    transport = HttpJsonTransport("http://127.0.0.1:9", timeout_s=0.1)
    exact = " tok en "
    client = BreakGlassClient(transport, bearer_token=exact)
    assert client._auth_headers()["Authorization"] == f"Bearer {exact}"
